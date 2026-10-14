"""Calendário observado a partir das barras M1, sem inferir sessões futuras.

O Python oficial do MT5 não fornece a grade semanal de sessões. Este módulo registra
somente os intervalos observados na base M1 e conserva a proveniência para que fontes
de calendário futuras possam ser adicionadas sem substituir este histórico.
"""
import hashlib
import json

import pandas as pd
from sqlalchemy import inspect, text

DEFAULT_SESSION_GAP_MINUTES = 60
_TIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
_SYMBOL_INFO_FIELDS = (
    "name", "description", "path", "visible", "digits", "point", "trade_mode", "trade_calc_mode",
    "trade_tick_size", "trade_tick_value", "trade_contract_size", "currency_base", "currency_profit",
    "currency_margin", "volume_min", "volume_step", "volume_max",
)

_CREATE_CALENDAR = """
CREATE TABLE IF NOT EXISTS _market_calendar (
    symbol TEXT NOT NULL,
    session_id TEXT NOT NULL,
    open_utc TEXT NOT NULL,
    close_utc TEXT NOT NULL,
    open_broker TEXT NOT NULL,
    close_broker TEXT NOT NULL,
    segments_json TEXT NOT NULL,
    source TEXT NOT NULL,
    source_metadata_json TEXT NOT NULL,
    timezone_offset_hours REAL NOT NULL,
    observed_at_utc TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    revision INTEGER NOT NULL,
    PRIMARY KEY (symbol, session_id, revision)
)
"""
_CREATE_CALENDAR_INDEX = "CREATE INDEX IF NOT EXISTS ix_market_calendar_symbol_open " \
                         "ON _market_calendar (symbol, open_utc)"


def _json_value(value):
    """Converte valores do MT5/numpy para JSON sem incluir objetos mutáveis ou cotações."""
    if value is None:
        return None
    if hasattr(value, "item"):
        try:
            value = value.item()
        except (ValueError, AttributeError):
            pass
    if isinstance(value, (str, int, float, bool)):
        if isinstance(value, float) and not pd.notna(value):
            return None
        return value
    return str(value)


def symbol_info_snapshot(info):
    """Campos descritivos estáveis de ``symbol_info``; bid/ask/last e spread atual são omitidos."""
    if info is None:
        return {}
    if isinstance(info, dict):
        get_value = info.get
    else:
        get_value = lambda key, default=None: getattr(info, key, default)
    return {field: _json_value(get_value(field)) for field in _SYMBOL_INFO_FIELDS
            if get_value(field) is not None}


def prepare_m1(bars):
    """Ordena e deduplica M1; mantém os horários sem fuso no horário do servidor."""
    columns = ["time", "open", "high", "low", "close", "tick_volume", "spread", "real_volume"]
    if bars is None:
        return pd.DataFrame(columns=columns)
    data = bars.copy()
    if data.empty:
        return data
    if "time" not in data.columns:
        raise ValueError("A série M1 precisa ter a coluna 'time'.")
    data["time"] = pd.to_datetime(data["time"], errors="coerce")
    data = data.dropna(subset=["time"])
    if data.empty:
        return data
    if getattr(data["time"].dt, "tz", None) is not None:
        raise ValueError("As barras M1 precisam estar no horário da corretora, sem timezone embutido.")
    data = data.sort_values("time").drop_duplicates("time", keep="last").reset_index(drop=True)
    required = [name for name in ("open", "high", "low", "close") if name not in data.columns]
    if required:
        raise ValueError("Faltam colunas M1 para montar OHLC: " + ", ".join(required))
    for name in ("open", "high", "low", "close"):
        data[name] = pd.to_numeric(data[name], errors="coerce")
        if data[name].isna().any():
            raise ValueError(f"A coluna M1 '{name}' contém valor vazio ou não numérico.")
    aligned = data["time"].dt.second.eq(0) & data["time"].dt.microsecond.eq(0)
    if not aligned.all():
        raise ValueError("Os horários M1 precisam estar alinhados ao início do minuto.")
    return data


def _utc_timestamp(broker_time, offset_hours):
    return pd.Timestamp(broker_time) - pd.Timedelta(hours=float(offset_hours))


def _utc_text(broker_time, offset_hours):
    return _utc_timestamp(broker_time, offset_hours).strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")


def _broker_text(value):
    return pd.Timestamp(value).strftime(_TIME_FORMAT)


def _canonical_hash(value):
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _parse_session_anchor(value):
    try:
        hour_text, minute_text = str(value).strip().split(":", 1)
        hour, minute = int(hour_text), int(minute_text)
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError("session_anchor precisa usar HH:MM no horário da corretora.") from exc
    if not (0 <= hour < 24 and 0 <= minute < 60):
        raise ValueError("session_anchor precisa usar HH:MM no horário da corretora.")
    return hour * 60 + minute, f"{hour:02d}:{minute:02d}"


def sessionize_m1(bars, symbol, broker_utc_offset=-3.0, session_gap_minutes=DEFAULT_SESSION_GAP_MINUTES,
                  symbol_info=None, time_basis="broker", session_anchor="00:00"):
    """Devolve ``(M1 com session_id, sessões)`` usando dia âncora e gaps observados.

    O dia da corretora, deslocado por ``session_anchor``, é a fronteira diária padrão. Uma diferença
    entre barras maior que ``session_gap_minutes`` também começa outra sessão inferida. Lacunas
    menores permanecem na mesma sessão e são gravadas como segmentos distintos.
    """
    try:
        gap_minutes = int(session_gap_minutes)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("session_gap_minutes precisa ser um inteiro positivo.") from exc
    if isinstance(session_gap_minutes, bool) or gap_minutes < 1:
        raise ValueError("session_gap_minutes precisa ser um inteiro positivo.")
    if gap_minutes != session_gap_minutes:
        raise ValueError("session_gap_minutes precisa ser um inteiro positivo.")
    anchor_minutes, anchor_text = _parse_session_anchor(session_anchor)
    offset = float(broker_utc_offset)
    if not pd.notna(offset):
        raise ValueError("O offset UTC da corretora precisa ser numérico.")
    data = prepare_m1(bars)
    if data.empty:
        data["session_id"] = pd.Series(dtype="object")
        return data, []

    gap = pd.Timedelta(minutes=gap_minutes)
    trading_day = (data["time"] - pd.Timedelta(minutes=anchor_minutes)).dt.strftime("%Y-%m-%d")
    data["_trading_day"] = trading_day
    day_boundary = trading_day.ne(trading_day.shift())
    gap_boundary = data["time"].diff().gt(gap)
    data["_session_group"] = (day_boundary | gap_boundary).cumsum()
    metadata = {
        "symbol_info": symbol_info_snapshot(symbol_info),
        "calendar_inference": {
            "source": "M1_observed",
            "source_timeframe": "M1",
            "session_gap_minutes": gap_minutes,
            "session_anchor_broker_time": anchor_text,
            "time_basis": str(time_basis),
            "broker_utc_offset_hours": offset,
            "timezone_rule": "fixed_offset_from_database_config",
        },
    }
    metadata_json = json.dumps(metadata, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    sessions = []
    session_ids = {}
    group_keys = data.drop_duplicates("_session_group")["_session_group"].to_frame()
    group_keys["_trading_day"] = data.drop_duplicates("_session_group")["_trading_day"].to_numpy()
    group_keys["_session_ordinal"] = group_keys.groupby("_trading_day", sort=False).cumcount()
    ordinals = dict(zip(group_keys["_session_group"], group_keys["_session_ordinal"]))
    for group_id, group in data.groupby("_session_group", sort=True):
        group = group.copy().reset_index(drop=True)
        open_broker = pd.Timestamp(group["time"].iloc[0])
        close_broker = pd.Timestamp(group["time"].iloc[-1]) + pd.Timedelta(minutes=1)
        open_utc = _utc_timestamp(open_broker, offset)
        trading_day_value = str(group["_trading_day"].iloc[0])
        session_ordinal = int(ordinals[group_id])
        daily_start = pd.Timestamp(trading_day_value) + pd.Timedelta(minutes=anchor_minutes)
        daily_end = daily_start + pd.Timedelta(days=1)
        session_key = f"{symbol}|{trading_day_value}|{session_ordinal}|{anchor_text}"
        session_id = hashlib.sha256(session_key.encode("utf-8")).hexdigest()[:24]

        breaks = group["time"].diff().gt(pd.Timedelta(minutes=1)).cumsum()
        segments = []
        for _, segment in group.groupby(breaks, sort=True):
            segment_open = pd.Timestamp(segment["time"].iloc[0])
            segment_close = pd.Timestamp(segment["time"].iloc[-1]) + pd.Timedelta(minutes=1)
            segments.append({
                "open_broker": _broker_text(segment_open),
                "close_broker": _broker_text(segment_close),
                "open_utc": _utc_text(segment_open, offset),
                "close_utc": _utc_text(segment_close, offset),
                "observed_bars": int(len(segment)),
            })

        fingerprint = _canonical_hash({"trading_day": trading_day_value, "session_ordinal": session_ordinal,
                                       "session_anchor": anchor_text, "open_utc": _utc_text(open_broker, offset),
                                       "close_utc": _utc_text(close_broker, offset),
                                       "segments": segments, "metadata": metadata})
        session = {
            "symbol": str(symbol),
            "session_id": session_id,
            "trading_day": trading_day_value,
            "session_ordinal": session_ordinal,
            "session_anchor_broker_time": anchor_text,
            "daily_start_broker_time": daily_start,
            "daily_end_broker_time": daily_end,
            "open_broker_time": open_broker,
            "close_broker_time": close_broker,
            "open_broker": _broker_text(open_broker),
            "close_broker": _broker_text(close_broker),
            "open_utc": _utc_text(open_broker, offset),
            "close_utc": _utc_text(close_broker, offset),
            "segments": segments,
            "segments_json": json.dumps(segments, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
            "source": "m1_observed",
            "source_metadata_json": metadata_json,
            "timezone_offset_hours": offset,
            "input_hash": fingerprint,
            "_group_id": group_id,
        }
        sessions.append(session)
        session_ids[group_id] = session_id

    data["session_id"] = data["_session_group"].map(session_ids)
    return data.drop(columns=["_session_group", "_trading_day"]), sessions


def ensure_schema(db):
    """Cria apenas as tabelas analíticas novas; não altera tabelas de barras existentes."""
    if db is None or getattr(db, "engine", None) is None:
        raise RuntimeError("Banco analítico não conectado.")
    with db.engine.begin() as conn:
        conn.execute(text(_CREATE_CALENDAR))
        conn.execute(text(_CREATE_CALENDAR_INDEX))


def save_sessions(db, sessions):
    """Grava revisões somente quando calendário ou proveniência observada mudam."""
    ensure_schema(db)
    now = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")
    inserted = 0
    with db.engine.begin() as conn:
        for session in sessions:
            latest = conn.execute(text(
                "SELECT revision, input_hash FROM _market_calendar WHERE symbol = :symbol AND session_id = :sid "
                "ORDER BY revision DESC LIMIT 1"),
                {"symbol": session["symbol"], "sid": session["session_id"]}).mappings().fetchone()
            if latest and latest["input_hash"] == session["input_hash"]:
                continue
            revision = int(latest["revision"] + 1) if latest else 1
            conn.execute(text(
                "INSERT INTO _market_calendar (symbol, session_id, open_utc, close_utc, open_broker, close_broker, "
                "segments_json, source, source_metadata_json, timezone_offset_hours, observed_at_utc, input_hash, "
                "revision) VALUES (:symbol, :session_id, :open_utc, :close_utc, :open_broker, :close_broker, "
                ":segments_json, :source, :source_metadata_json, :timezone_offset_hours, :observed_at_utc, "
                ":input_hash, :revision)"),
                {**session, "observed_at_utc": now, "revision": revision})
            inserted += 1
    return inserted


def failed_extraction_ranges(db, symbol):
    """Lê blocos de extração M1 com falha; resultado vazio para bases antigas sem log."""
    if db is None or getattr(db, "engine", None) is None or not inspect(db.engine).has_table("_extraction_log"):
        return []
    table = db.find_table_for_symbol(symbol, "1 minuto")
    if not table:
        return []
    with db.engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT block_start, block_end FROM _extraction_log WHERE table_name = :table AND status = 'failed' "
            "ORDER BY block_start"), {"table": table}).fetchall()
    ranges = []
    for start, end in rows:
        start, end = pd.Timestamp(start), pd.Timestamp(end)
        if start.tzinfo is not None:
            start = start.tz_localize(None)
        if end.tzinfo is not None:
            end = end.tz_localize(None)
        ranges.append((start, end))
    return ranges
