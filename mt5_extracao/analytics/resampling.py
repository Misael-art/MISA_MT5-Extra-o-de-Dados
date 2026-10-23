"""Agregação M1 em barras derivadas com calendário observado e revisões incrementais."""
import hashlib
import json
import logging
import re
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Optional

import pandas as pd
from sqlalchemy import inspect, text

from mt5_extracao.analytics import calendar
from mt5_extracao import timeframes
from mt5_extracao.timeframes import Timeframe

log = logging.getLogger(__name__)

QUALITY_STATUSES = frozenset({"COMPLETE", "PARTIAL", "NO_TRADES", "MISSING", "CORRECTED"})
_CREATE_BARS = """
CREATE TABLE IF NOT EXISTS _bars_derived (
    symbol TEXT NOT NULL,
    tf TEXT NOT NULL,
    bar_start_utc TEXT NOT NULL,
    bar_end_utc TEXT NOT NULL,
    bar_start_broker TEXT NOT NULL,
    bar_end_broker TEXT NOT NULL,
    session_id TEXT NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    tick_volume BIGINT,
    real_volume BIGINT,
    real_volume_observed BIGINT,
    real_volume_known INTEGER NOT NULL,
    real_volume_known_count INTEGER NOT NULL,
    spread_min REAL,
    bar_count INTEGER NOT NULL,
    coverage_ratio REAL NOT NULL,
    is_partial INTEGER NOT NULL DEFAULT 0,
    quality_status TEXT NOT NULL,
    revision INTEGER NOT NULL,
    source_timeframe TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    computed_at_utc TEXT NOT NULL,
    source_seq BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (symbol, tf, session_id, bar_start_utc, revision)
)
"""
_CREATE_BARS_INDEX = "CREATE INDEX IF NOT EXISTS ix_bars_derived_lookup " \
                     "ON _bars_derived (symbol, tf, bar_start_utc)"
_CREATE_SOURCE_INDEX = "CREATE INDEX IF NOT EXISTS ix_bars_derived_source_sequence " \
                       "ON _bars_derived (symbol, tf, source_seq, bar_start_utc)"
_CREATE_SOURCE_SEQUENCES = """
CREATE TABLE IF NOT EXISTS _analytics_source_sequences (
    symbol TEXT NOT NULL,
    tf TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    PRIMARY KEY (symbol, tf)
)
"""


@dataclass(frozen=True)
class TargetTimeframe:
    label: str
    minutes: Optional[int]
    mode: str  # "intraday", "session" or "fixed_24h"


def parse_target(value):
    """Converte M5, H1, 1D, 1440min e múltiplos inteiros arbitrários em alvo canônico."""
    if isinstance(value, TargetTimeframe):
        return value
    if isinstance(value, Timeframe):
        raw = value.name
    else:
        raw = str(value).strip()
    normalized = raw.upper().replace(" ", "")
    if normalized in {"1D", "D1", "1DIA", "DIA", "DIARIO"}:
        return TargetTimeframe("1D", None, "session")
    if normalized in {"1440MIN", "M1440", "24H", "H24"}:
        return TargetTimeframe("1440min", 1440, "fixed_24h")

    minute_match = re.fullmatch(r"(?:M)?(\d+)(?:MIN)?", normalized)
    hour_match = re.fullmatch(r"H(\d+)", normalized)
    if hour_match:
        minutes = int(hour_match.group(1)) * 60
    elif minute_match:
        minutes = int(minute_match.group(1))
    else:
        parsed = timeframes.parse(raw)
        if parsed is Timeframe.D1:
            return TargetTimeframe("1D", None, "session")
        if parsed is None or parsed in {Timeframe.W1, Timeframe.MN1}:
            raise ValueError(f"Timeframe analítico inválido: {value!r}. Use M<n>, H<n>, 1D ou 1440min.")
        minutes = int(parsed.minutes)

    if minutes <= 1:
        raise ValueError("O timeframe derivado precisa ser maior que M1.")
    if minutes == 1440:
        return TargetTimeframe("1440min", minutes, "fixed_24h")
    known = next((tf for tf in Timeframe if tf.minutes == minutes and tf not in
                  {Timeframe.D1, Timeframe.W1, Timeframe.MN1}), None)
    label = known.name if known else f"{minutes}min"
    return TargetTimeframe(label, minutes, "intraday")


def ensure_schema(db):
    """Cria a tabela interna e acrescenta colunas novas sem reescrever dados antigos."""
    calendar.ensure_schema(db)
    with db.engine.begin() as conn:
        conn.execute(text(_CREATE_BARS))
        existing = {column["name"] for column in inspect(conn).get_columns("_bars_derived")}
        if "is_partial" not in existing:
            conn.execute(text("ALTER TABLE _bars_derived ADD COLUMN is_partial INTEGER NOT NULL DEFAULT 0"))
        if "source_seq" not in existing:
            conn.execute(text("ALTER TABLE _bars_derived ADD COLUMN source_seq BIGINT NOT NULL DEFAULT 0"))
        conn.execute(text(_CREATE_BARS_INDEX))
        conn.execute(text(_CREATE_SOURCE_INDEX))
        conn.execute(text(_CREATE_SOURCE_SEQUENCES))


def _number(value):
    if value is None or pd.isna(value):
        return None
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, float) and not pd.notna(value):
        return None
    if isinstance(value, (int, float)):
        return value
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _hash_value(value):
    if value is None or pd.isna(value):
        return None
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, (int, float, str, bool)):
        if isinstance(value, float) and not pd.notna(value):
            return None
        return value
    return str(value)


def _input_hash(group, target, start_broker, end_broker, coverage, quality, expected, failed_missing, is_partial):
    columns = [name for name in ("time", "open", "high", "low", "close", "tick_volume", "spread", "real_volume")
               if name in group.columns]
    records = [[_hash_value(value) for value in row] for row in group[columns].itertuples(index=False, name=None)]
    payload = {"target": target.label, "start": pd.Timestamp(start_broker).isoformat(),
               "end": pd.Timestamp(end_broker).isoformat(), "rows": records,
               "coverage_ratio": round(float(coverage), 10), "quality_status": quality,
               "expected_minutes": round(float(expected), 10), "failed_extraction": bool(failed_missing),
               "partial_window": bool(is_partial)}
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _overlap_minutes(start, end, intervals):
    total = 0.0
    for interval_start, interval_end in intervals:
        overlap_start = max(start, interval_start)
        overlap_end = min(end, interval_end)
        if overlap_end > overlap_start:
            total += (overlap_end - overlap_start).total_seconds() / 60.0
    return total


def _session_intervals(sessions):
    result = []
    for session in sessions:
        for segment in session["segments"]:
            result.append((pd.Timestamp(segment["open_broker"]), pd.Timestamp(segment["close_broker"])))
    return result


def _failed_extra_minutes(start, end, group, failed_ranges):
    """Minutos sem M1 dentro de blocos falhos e a parte ainda não contada na cobertura."""
    missing = 0.0
    observed = [pd.Timestamp(value) for value in group["time"]]
    for failed_start, failed_end in failed_ranges:
        overlap_start, overlap_end = max(start, failed_start), min(end, failed_end)
        if overlap_end <= overlap_start:
            continue
        failed_minutes = max(0.0, (overlap_end - overlap_start).total_seconds() / 60.0)
        observed_in_range = sum(overlap_start <= value < overlap_end for value in observed)
        missing += max(0.0, failed_minutes - observed_in_range)
    span_minutes = (max(observed) + pd.Timedelta(minutes=1) - min(observed)).total_seconds() / 60.0
    already_counted = max(0.0, span_minutes - len(group))
    return missing, max(0.0, missing - already_counted)


def _utc_text(broker_time, offset_hours):
    return (pd.Timestamp(broker_time) - pd.Timedelta(hours=float(offset_hours))).strftime(
        "%Y-%m-%dT%H:%M:%S.%f+00:00")


def _broker_text(value):
    return pd.Timestamp(value).strftime("%Y-%m-%d %H:%M:%S.%f")


def _make_bar(symbol, target, session_id, group, start_broker, end_broker, expected, failed_extra,
              failed_missing, offset_hours, is_partial):
    group = group.sort_values("time")
    first, last = group.iloc[0], group.iloc[-1]
    tick_values = [_number(value) for value in group["tick_volume"]] if "tick_volume" in group else []
    tick_volume = int(sum(value for value in tick_values if value is not None)) if tick_values else None
    real_values = [_number(value) for value in group["real_volume"]] if "real_volume" in group else [None] * len(group)
    known_values = [value for value in real_values if value is not None]
    real_known = bool(real_values) and len(known_values) == len(real_values)
    real_observed = int(sum(known_values)) if known_values else None
    real_volume = real_observed if real_known else None
    spread_values = [_number(value) for value in group["spread"]] if "spread" in group else []
    spread_values = [value for value in spread_values if value is not None]
    spread_min = min(spread_values) if spread_values else None
    missing_expected = max(0.0, float(expected) + float(failed_extra) - len(group))
    total_expected = max(float(expected) + float(failed_extra), float(len(group)))
    coverage = min(1.0, len(group) / total_expected) if total_expected > 0 else 1.0

    if failed_missing and missing_expected > 0:
        quality = "MISSING"
    elif tick_values and all(value is not None and value <= 0 for value in tick_values):
        quality = "NO_TRADES"
    elif coverage < 1.0 or is_partial:
        quality = "PARTIAL"
    else:
        quality = "COMPLETE"

    return {
        "symbol": str(symbol), "tf": target.label,
        "bar_start_utc": _utc_text(start_broker, offset_hours),
        "bar_end_utc": _utc_text(end_broker, offset_hours),
        "bar_start_broker": _broker_text(start_broker), "bar_end_broker": _broker_text(end_broker),
        "session_id": session_id,
        "open": _number(first["open"]), "high": max(_number(value) for value in group["high"]),
        "low": min(_number(value) for value in group["low"]), "close": _number(last["close"]),
        "tick_volume": tick_volume, "real_volume": real_volume, "real_volume_observed": real_observed,
        "real_volume_known": int(real_known), "real_volume_known_count": len(known_values),
        "spread_min": spread_min, "bar_count": int(len(group)), "coverage_ratio": float(coverage),
        "is_partial": int(bool(is_partial)),
        "quality_status": quality, "source_timeframe": "M1",
        "input_hash": _input_hash(group, target, start_broker, end_broker, coverage, quality,
                                   total_expected, failed_missing, is_partial),
    }


def build_bars(data, sessions, target, broker_utc_offset=-3.0, partial_last_bar="include", failed_ranges=None,
               start=None, end=None, symbol=""):
    """Monta barras de ``data`` sem criar linhas para janelas sem nenhuma barra M1."""
    if partial_last_bar not in {"include", "drop"}:
        raise ValueError("partial_last_bar deve ser 'include' ou 'drop'.")
    target = parse_target(target)
    data = calendar.prepare_m1(data)
    if data.empty:
        return []
    if "session_id" not in data.columns:
        raise ValueError("A série precisa ter session_id; use calendar.sessionize_m1 primeiro.")
    offset = float(broker_utc_offset)
    failed_ranges = list(failed_ranges or [])
    session_by_id = {session["session_id"]: session for session in sessions}
    intervals = _session_intervals(sessions)
    start_bound = pd.Timestamp(start) if start is not None else None
    end_bound = pd.Timestamp(end) if end is not None else None
    work = data.copy()
    built = []

    if target.mode == "session":
        groups = [(session["session_id"], session["daily_start_broker_time"], session["daily_end_broker_time"],
                   data[data["session_id"] == session["session_id"]]) for session in sessions]
    elif target.mode == "fixed_24h":
        utc_times = work["time"] - pd.to_timedelta(offset, unit="h")
        work["_utc_day"] = utc_times.dt.floor("D")
        work["_session_id"] = work["_utc_day"].dt.strftime("utc-day:%Y-%m-%d")
        groups = []
        for (session_id, utc_day), group in work.groupby(["_session_id", "_utc_day"], sort=True):
            utc_start = pd.Timestamp(utc_day)
            utc_end = utc_start + pd.Timedelta(days=1)
            broker_start = utc_start + pd.Timedelta(hours=offset)
            broker_end = utc_end + pd.Timedelta(hours=offset)
            groups.append((session_id, broker_start, broker_end, group))
    else:
        minutes = target.minutes
        work["_grid_start"] = work["time"].dt.floor(f"{minutes}min")
        groups = []
        for (session_id, grid_start), group in work.groupby(["session_id", "_grid_start"], sort=True):
            session = session_by_id[session_id]
            bucket_start = pd.Timestamp(grid_start)
            bucket_end = min(bucket_start + pd.Timedelta(minutes=minutes), session["close_broker_time"])
            groups.append((session_id, bucket_start, bucket_end, group))

    for session_id, bucket_start, bucket_end, group in groups:
        if group.empty or bucket_end <= bucket_start:
            continue
        if target.mode == "intraday":
            grid_start = pd.Timestamp(group["time"].iloc[0]).floor(f"{target.minutes}min")
            session = session_by_id[session_id]
            grid_end = grid_start + pd.Timedelta(minutes=target.minutes)
            is_partial = (grid_start < session["open_broker_time"] or
                          grid_end > session["close_broker_time"])
            if grid_end > session["close_broker_time"] and partial_last_bar == "drop":
                continue
        else:
            is_partial = False

        expected_active = _overlap_minutes(bucket_start, bucket_end, intervals)
        span_minutes = ((pd.Timestamp(group["time"].iloc[-1]) + pd.Timedelta(minutes=1)
                         - pd.Timestamp(group["time"].iloc[0])).total_seconds() / 60.0)
        expected = max(expected_active, span_minutes)
        failed_missing, failed_extra = _failed_extra_minutes(bucket_start, bucket_end, group, failed_ranges)
        if start_bound is not None and bucket_end <= start_bound:
            continue
        if end_bound is not None and bucket_start > end_bound:
            continue
        built.append(_make_bar(symbol=symbol, target=target, session_id=session_id, group=group,
                               start_broker=bucket_start, end_broker=bucket_end, expected=expected,
                               failed_extra=failed_extra, failed_missing=failed_missing > 0,
                               offset_hours=offset, is_partial=is_partial))
    return built


def _save_bars(db, bars):
    ensure_schema(db)
    computed = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")
    counts = {"inserted": 0, "corrected": 0, "unchanged": 0}
    with db.engine.begin() as conn:
        changed = []
        for bar in bars:
            latest = conn.execute(text(
                "SELECT revision, input_hash FROM _bars_derived WHERE symbol = :symbol AND tf = :tf "
                "AND session_id = :session_id AND bar_start_utc = :bar_start_utc ORDER BY revision DESC LIMIT 1"),
                {key: bar[key] for key in ("symbol", "tf", "session_id", "bar_start_utc")}).mappings().fetchone()
            if latest and latest["input_hash"] == bar["input_hash"]:
                counts["unchanged"] += 1
                continue
            revision = int(latest["revision"] + 1) if latest else 1
            if latest:
                counts["corrected"] += 1
                bar["quality_status"] = "CORRECTED"
            else:
                counts["inserted"] += 1
            bar["revision"] = revision
            bar["computed_at_utc"] = computed
            changed.append(bar)
        by_source = {}
        for bar in changed:
            by_source.setdefault((bar["symbol"], bar["tf"]), []).append(bar)
        for (symbol, timeframe), source_bars in by_source.items():
            conn.execute(text(
                "INSERT INTO _analytics_source_sequences (symbol, tf, sequence) VALUES (:symbol, :tf, 1) "
                "ON CONFLICT (symbol, tf) DO UPDATE SET sequence = _analytics_source_sequences.sequence + 1"),
                {"symbol": symbol, "tf": timeframe})
            sequence = int(conn.execute(text(
                "SELECT sequence FROM _analytics_source_sequences WHERE symbol = :symbol AND tf = :tf"),
                {"symbol": symbol, "tf": timeframe}).scalar_one())
            for bar in source_bars:
                bar["source_seq"] = sequence
                columns = tuple(bar)
                names = ", ".join(columns)
                params = ", ".join(f":{name}" for name in columns)
                conn.execute(text(f"INSERT INTO _bars_derived ({names}) VALUES ({params})"), bar)
    return counts


def latest_source_sequence(db, symbol, timeframe, *, connection=None):
    """Geração monotônica das últimas revisões derivadas, sem ler o histórico de barras."""
    if db is None or getattr(db, "engine", None) is None:
        return None
    target = parse_target(timeframe)
    with (nullcontext(connection) if connection is not None else db.engine.connect()) as conn:
        # A guarda pode já deter lock de escrita: inclusive a inspeção precisa
        # usar sua conexão, sem abrir outra conexão que bloqueie no próprio writer.
        if not inspect(conn).has_table("_analytics_source_sequences"):
            return None
        value = conn.execute(text(
            "SELECT sequence FROM _analytics_source_sequences WHERE symbol = :symbol AND tf = :tf"),
            {"symbol": str(symbol), "tf": target.label}).scalar()
        if value is None and inspect(conn).has_table("_bars_derived"):
            value = conn.execute(text(
                "SELECT MAX(source_seq) FROM _bars_derived WHERE symbol = :symbol AND tf = :tf"),
                {"symbol": str(symbol), "tf": target.label}).scalar()
    return int(value) if value is not None else 0


def has_closed_bars_after(db, symbol, timeframe, bar_start_utc, *, require_complete_session=True):
    """Checa, sem carregar o histórico, se uma barra nova ficou elegível pelo relógio."""
    if db is None or getattr(db, "engine", None) is None or not inspect(db.engine).has_table("_bars_derived"):
        return False
    target = parse_target(timeframe)
    columns = {column["name"] for column in inspect(db.engine).get_columns("_bars_derived")}
    partial_clause = "COALESCE(candidate.is_partial, 0) = 0" if "is_partial" in columns else (
        "NOT EXISTS (SELECT 1 FROM _bars_derived was_partial WHERE was_partial.symbol = candidate.symbol "
        "AND was_partial.tf = candidate.tf AND was_partial.session_id = candidate.session_id "
        "AND was_partial.bar_start_utc = candidate.bar_start_utc AND was_partial.revision < candidate.revision "
        "AND was_partial.quality_status = 'PARTIAL' AND was_partial.coverage_ratio >= 0.999999999)")
    eligibility = ("candidate.quality_status IN ('COMPLETE', 'NO_TRADES', 'CORRECTED') "
                   "AND candidate.coverage_ratio >= 0.999999999 AND candidate.bar_end_utc <= :as_of "
                   f"AND {partial_clause}")
    if target.minutes is not None:
        if db.engine.dialect.name == "sqlite":
            eligibility += " AND (julianday(candidate.bar_end_utc) - julianday(candidate.bar_start_utc)) " \
                           "* 1440.0 >= :duration_minutes - 0.000001"
        else:
            eligibility += " AND EXTRACT(EPOCH FROM (CAST(candidate.bar_end_utc AS TIMESTAMPTZ) - " \
                           "CAST(candidate.bar_start_utc AS TIMESTAMPTZ))) >= :duration_seconds - 0.000001"
    cursor_clause = "" if bar_start_utc is None else "AND candidate.bar_start_utc > :bar_start_after"
    latest_candidate = "NOT EXISTS (SELECT 1 FROM _bars_derived newer WHERE newer.symbol = candidate.symbol " \
                       "AND newer.tf = candidate.tf AND newer.session_id = candidate.session_id " \
                       "AND newer.bar_start_utc = candidate.bar_start_utc AND newer.revision > candidate.revision)"
    latest_incomplete = "NOT EXISTS (SELECT 1 FROM _bars_derived newer WHERE newer.symbol = incomplete.symbol " \
                        "AND newer.tf = incomplete.tf AND newer.session_id = incomplete.session_id " \
                        "AND newer.bar_start_utc = incomplete.bar_start_utc AND newer.revision > incomplete.revision)"
    session_clause = f"""AND NOT EXISTS (
              SELECT 1 FROM _bars_derived incomplete
              WHERE incomplete.symbol = candidate.symbol AND incomplete.tf = candidate.tf
                AND incomplete.session_id = candidate.session_id AND {latest_incomplete}
                AND NOT ({eligibility.replace('candidate.', 'incomplete.')})
          )""" if require_complete_session else ""
    sql = f"""
        SELECT 1 FROM _bars_derived candidate
        WHERE candidate.symbol = :symbol AND candidate.tf = :tf
          AND {latest_candidate} AND {eligibility} {cursor_clause}
          {session_clause}
        LIMIT 1
    """
    params = {"symbol": str(symbol), "tf": target.label, "as_of": _utc_now().strftime(
        "%Y-%m-%dT%H:%M:%S.%f+00:00")}
    if bar_start_utc is not None:
        stamp = pd.Timestamp(bar_start_utc)
        if stamp.tzinfo is None:
            raise ValueError("bar_start_utc precisa incluir fuso horário.")
        params["bar_start_after"] = stamp.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")
    if target.minutes is not None:
        params["duration_minutes"] = float(target.minutes)
        params["duration_seconds"] = float(target.minutes) * 60.0
    with db.engine.connect() as conn:
        return conn.execute(text(sql), params).first() is not None


def resample_symbol(db, symbol, targets, symbol_info=None, session_gap_minutes=calendar.DEFAULT_SESSION_GAP_MINUTES,
                    session_anchor="00:00", partial_last_bar="include", start=None, end=None):
    """Infere calendário M1, registra ``symbol_info`` e grava revisões das barras derivadas."""
    targets = [parse_target(value) for value in targets]
    if not targets:
        raise ValueError("Informe ao menos um timeframe para derivar.")
    if partial_last_bar not in {"include", "drop"}:
        raise ValueError("partial_last_bar deve ser 'include' ou 'drop'.")
    source = db.load_ohlcv(symbol, Timeframe.M1.label)
    if source.empty:
        raise ValueError(f"Não há barras M1 de {symbol} na base. Extraia antes: "
                         f"mt5x extract --symbols {symbol} --tf M1 --from AAAA-MM-DD")
    offset = float(getattr(db, "broker_utc_offset", -3.0))
    data, sessions = calendar.sessionize_m1(
        source, symbol, broker_utc_offset=offset, session_gap_minutes=session_gap_minutes,
        symbol_info=symbol_info, time_basis=getattr(db, "time_basis", "broker"),
        session_anchor=session_anchor)
    calendar_changes = calendar.save_sessions(db, sessions)
    failed_ranges = calendar.failed_extraction_ranges(db, symbol)
    summary = []
    for target in targets:
        bars = build_bars(data, sessions, target, broker_utc_offset=offset, partial_last_bar=partial_last_bar,
                          failed_ranges=failed_ranges, start=start, end=end, symbol=symbol)
        counts = _save_bars(db, bars)
        summary.append({"symbol": str(symbol), "timeframe": target.label, "bars": len(bars),
                        "source_bars": int(len(data)), "calendar_revisions": calendar_changes, **counts})
        log.info("%s %s: %s barras derivadas de M1 (%s novas, %s corrigidas, %s inalteradas)",
                 symbol, target.label, len(bars), counts["inserted"], counts["corrected"], counts["unchanged"])
    return summary


def latest_bars(db, symbol, timeframe, *, closed_only=True, source_seq_after=None, bar_start_after=None,
                connection=None):
    """Última revisão de cada barra derivada, em ordem cronológica.

    As colunas de horário UTC voltam como ``Timestamp`` com fuso. ``computed_at_utc``
    diz quando aquela revisão foi gravada: numa correção (``revision > 1``) é o
    instante a partir do qual o valor corrigido existia, e os produtores de
    características usam isso para não antecipar a correção. Por padrão, barras
    incompletas, com cobertura faltante, parciais de sessão ou ainda abertas no
    relógio local são excluídas. ``closed_only=False`` serve à inspeção de qualidade.
    """
    target = parse_target(timeframe)
    if db is None or getattr(db, "engine", None) is None or not inspect(db.engine).has_table("_bars_derived"):
        return pd.DataFrame()
    columns = {column["name"] for column in inspect(db.engine).get_columns("_bars_derived")}
    if source_seq_after is not None and "source_seq" not in columns:
        return pd.DataFrame()
    current_partial = "COALESCE(latest.is_partial, 0)" if "is_partial" in columns else "0"
    source_filters = []
    if source_seq_after is not None and "source_seq" in columns:
        source_filters.append("source_seq > :source_seq_after")
    elif source_seq_after is not None and bar_start_after is None:
        return pd.DataFrame()
    if bar_start_after is not None:
        source_filters.append("bar_start_utc > :bar_start_after")
    source_filter = " AND (" + " OR ".join(source_filters) + ")" if source_filters else ""
    sql = f"""
        WITH ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol, tf, session_id, bar_start_utc
                                         ORDER BY revision DESC) AS latest_rank
            FROM _bars_derived WHERE symbol = :symbol AND tf = :tf{source_filter}
        ), latest AS (
            SELECT * FROM ranked WHERE latest_rank = 1
        )
        SELECT latest.*,
               CASE WHEN {current_partial} = 1 OR EXISTS (
                   SELECT 1 FROM _bars_derived previous
                   WHERE previous.symbol = latest.symbol AND previous.tf = latest.tf
                     AND previous.session_id = latest.session_id
                     AND previous.bar_start_utc = latest.bar_start_utc
                     AND previous.revision < latest.revision
                     AND previous.quality_status = 'PARTIAL'
                     AND previous.coverage_ratio >= 0.999999999
               ) THEN 1 ELSE 0 END AS _partial_from_history
        FROM latest ORDER BY latest.bar_start_utc
    """
    params = {"symbol": str(symbol), "tf": target.label}
    if source_seq_after is not None and "source_seq" in columns:
        params["source_seq_after"] = int(source_seq_after)
    if bar_start_after is not None:
        stamp = pd.Timestamp(bar_start_after)
        if stamp.tzinfo is None:
            raise ValueError("bar_start_after precisa incluir fuso horário.")
        params["bar_start_after"] = stamp.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")
    with (nullcontext(connection) if connection is not None else db.engine.connect()) as conn:
        frame = pd.read_sql(text(sql), conn, params=params)
    if frame.empty:
        return frame
    frame = frame.drop(columns="latest_rank")
    for name in ("bar_start_utc", "bar_end_utc", "computed_at_utc"):
        frame[name] = pd.to_datetime(frame[name], utc=True, format="ISO8601")
    historical_partial = pd.to_numeric(frame.pop("_partial_from_history"), errors="coerce").fillna(1).eq(1)
    if "is_partial" not in frame:
        frame["is_partial"] = historical_partial.astype(int)
    else:
        frame["is_partial"] = (pd.to_numeric(frame["is_partial"], errors="coerce").fillna(1).eq(1) |
                               historical_partial).astype(int)
    if closed_only:
        acceptable = frame["quality_status"].isin(("COMPLETE", "NO_TRADES", "CORRECTED"))
        acceptable &= pd.to_numeric(frame["coverage_ratio"], errors="coerce").ge(1.0 - 1e-9)
        acceptable &= pd.to_numeric(frame["is_partial"], errors="coerce").fillna(1).eq(0)
        if target.mode == "intraday":
            duration = (frame["bar_end_utc"] - frame["bar_start_utc"]).dt.total_seconds() / 60.0
            acceptable &= duration.ge(float(target.minutes) - 1e-9)
        elif target.mode == "fixed_24h":
            duration = (frame["bar_end_utc"] - frame["bar_start_utc"]).dt.total_seconds() / 60.0
            acceptable &= duration.ge(1440.0 - 1e-9)
        acceptable &= frame["bar_end_utc"] <= _utc_now()
        incomplete_sessions = set(frame.loc[~acceptable, "session_id"])
        frame = frame.loc[acceptable].copy()
        frame["session_complete"] = ~frame["session_id"].isin(incomplete_sessions)
    return frame.reset_index(drop=True)


def _utc_now():
    """Relógio UTC isolado para a fronteira de disponibilidade de barras fechadas."""
    return pd.Timestamp.now(tz="UTC")
