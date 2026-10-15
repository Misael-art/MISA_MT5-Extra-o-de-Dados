"""Produtores das características da T13.3: calculam a partir de ``_bars_derived`` e gravam na feature store.

    summary = producers.compute(db, "EURUSD", "M5")             # todas as famílias
    summary = producers.compute(db, "EURUSD", "M5", families=["regime"])

Disponibilidade (regra *point-in-time*):

- valor de barra: ``available_at`` = fim da barra; valor de sessão: fim da sessão (ou o instante
  declarado pela família, ver ``session_structure``);
- o histórico é **reconstruído** agora a partir das barras gravadas. ``observed_at`` = ``available_at``
  e ``metadata.reconstructed = true`` registram isso: o valor é o que se saberia naquele instante
  *se* as barras de hoje já existissem então;
- correção tardia de barra (``revision > 1`` em ``_bars_derived``) não pode ser antecipada: o valor
  que depende de uma barra corrigida dentro da sua janela de cálculo só fica disponível no
  ``computed_at_utc`` da correção. Novos candles continuam o estado exato de Wilder guardado no
  checkpoint; uma correção anterior à posição do checkpoint reconstrói a família desde o histórico.
  Fingerprints recursivos cobrem o prefixo inteiro e percentis/janelas finitas mantêm sua janela causal;
- a última sessão da base é tratada como possivelmente aberta: valores de sessão dela não são gravados.

A escrita respeita o ``write_guard`` da Fase 15 quando ele é passado (limite de disco/base).
"""
import hashlib
import json
import logging
import re
import time
from collections import deque

import numpy as np
import pandas as pd

from mt5_extracao.analytics import (feature_store, features, market_regimes, resampling, session_structure,
                                    producer_jobs, volume_profiles)

log = logging.getLogger(__name__)

ALGORITHM_VERSION = "1.2"
FAMILIES = ("volume", "rvol", "volatility", "regime", "structure", "calendar")
WILDER_MEMORY = 5
MAX_BATCH_SIZE = 50_000
MAX_JOB_TIMEOUT_SECONDS = 3_600
MAX_JOB_MEMORY_MB = 8_192
MAX_BATCH_RETRIES = 5
DEFAULTS = {
    "volume": {"windows": [5, 10, 20, 60], "slope_window": 15, "percentile_window": 60, "min_sessions": 5},
    "rvol": {"lookback": 20, "min_sessions": 5, "same_weekday": False},
    "volatility": {"atr_n": 14, "rv_n": 20, "bb_n": 20, "percentile_window": 100, "compression_pct": 20.0,
                   "expansion_pct": 80.0},
    "regime": {"adx_n": 14, "er_n": 20, "r2_n": 50, "trend_adx": 25.0, "range_adx": 20.0, "er_cut": 0.3,
               "r2_cut": 0.5, "min_duration": 3},
    "structure": {"opening_minutes": 30, "atr_n": 14},
    "calendar": {},
}
CONFIDENCE = {
    "volume": "sem confiança própria; a amostra é o número de sessões da janela (mínimo min_sessions)",
    "rvol": "rvol.reference_sessions = sessões usadas na mediana; não é probabilidade",
    "volatility": "sem confiança própria; percentis exigem a janela anterior completa",
    "regime": "regime.confidence = fração (1/3, 2/3, 1) de ADX, eficiência e R² que concordam; não é probabilidade",
    "structure": "sem confiança própria; rompimento ambíguo (as duas pontas na mesma barra) = 0",
    "calendar": "fato do calendário observado; reconstrução histórica com offset fixo",
}


class WriteBlocked(RuntimeError):
    pass


def _window(family, params):
    """Barras de entrada que influenciam um valor (para a regra das correções tardias)."""
    if family == "volatility":
        return max(WILDER_MEMORY * params["atr_n"], params["rv_n"], params["bb_n"]) + params["percentile_window"]
    if family == "regime":
        return max(WILDER_MEMORY * 2 * params["adx_n"], params["er_n"], params["r2_n"])
    return 1


def _correction_times(bars, window=None, *, cumulative=False):
    """Último ``computed_at`` de correção na janela móvel ou em todo o prefixo causal."""
    corrected = bars["computed_at_utc"].where(bars["revision"].astype(int) > 1)
    stamps = corrected.tolist()
    result = []
    if cumulative:
        latest = pd.NaT
        for stamp in stamps:
            if pd.notna(stamp) and (pd.isna(latest) or stamp > latest):
                latest = stamp
            result.append(latest)
    else:
        if window is None or int(window) < 1:
            raise ValueError("window precisa ser positivo quando cumulative=False.")
        window = int(window)
        queue = deque()
        for index, stamp in enumerate(stamps):
            while queue and queue[0][0] <= index - window:
                queue.popleft()
            if pd.notna(stamp):
                while queue and queue[-1][1] <= stamp:
                    queue.pop()
                queue.append((index, stamp))
            result.append(queue[0][1] if queue else pd.NaT)
    return pd.Series(pd.to_datetime(result, utc=True), index=bars.index)


def _json_safe(value):
    if value is None:
        return None
    if hasattr(value, "item"):
        return _json_safe(value.item())
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, float) and not np.isfinite(value):
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if pd.isna(value):
        return None
    return str(value)


def _source_tokens(bars):
    """Pré-calcula um token compacto para cada barra antes dos fingerprints por janela."""
    positions = {name: i for i, name in enumerate(bars.columns)}
    fallback = [name for name in ("open", "high", "low", "close", "tick_volume", "real_volume",
                                  "real_volume_known") if name in positions]
    tokens = []
    for idx, values in enumerate(bars.itertuples(index=False, name=None)):
        input_hash = values[positions["input_hash"]] if "input_hash" in positions else None
        if input_hash is None or (not isinstance(input_hash, (list, dict)) and pd.isna(input_hash)):
            input_hash = [values[positions[name]] for name in fallback]
        token = [str(values[positions["bar_start_utc"]]) if "bar_start_utc" in positions else str(idx),
                 _json_safe(values[positions["session_id"]]) if "session_id" in positions else None,
                 int(values[positions["revision"]]) if "revision" in positions else 1, _json_safe(input_hash)]
        tokens.append(hashlib.sha256(json.dumps(token, ensure_ascii=False, separators=(",", ":"),
                                              allow_nan=False).encode("utf-8")).digest())
    return tokens


def _source_revision(tokens, indices):
    digest = hashlib.sha256(b"".join(tokens[int(idx)] for idx in indices)).hexdigest()
    return f"bars:{digest[:24]}"


def _extend_source_chain(chain, tokens):
    digest = bytes(32) if chain is None else bytes.fromhex(chain) if isinstance(chain, str) else chain
    revisions = []
    for token in tokens:
        digest = hashlib.sha256(digest + token).digest()
        revisions.append(f"bars:{digest.hex()[:24]}")
    return digest, revisions


def _prefix_source_revisions(tokens):
    return _extend_source_chain(None, tokens)[1]


def _source_chain_state(bars):
    digest, _ = _extend_source_chain(None, _source_tokens(bars))
    corrected = bars["computed_at_utc"].where(bars["revision"].astype(int) > 1)
    latest_correction = corrected.dropna().max() if corrected.notna().any() else pd.NaT
    return digest.hex(), latest_correction


def _bar_rows(bars, frame, columns, corrections, window, cumulative_columns=()):
    rows = []
    tokens = _source_tokens(bars)
    revision_cache = {}
    cumulative_columns = set(cumulative_columns)
    prefix_revisions = _prefix_source_revisions(tokens) if cumulative_columns else None
    for column, feature_id in columns.items():
        values = frame[column]
        for k, value in values.items():
            if value is None or (isinstance(value, float) and not np.isfinite(value)):
                continue
            start = max(0, int(k) - window + 1)
            indices = range(start, int(k) + 1)
            source_revision = revision_cache.get(int(k))
            if source_revision is None and column not in cumulative_columns:
                source_revision = _source_revision(tokens, indices)
                revision_cache[int(k)] = source_revision
            if column in cumulative_columns:
                source_revision = prefix_revisions[int(k)]
            available = bars["bar_end_utc"].iloc[k]
            correction_series = corrections.get(column) if isinstance(corrections, dict) else corrections
            fix = correction_series.iloc[k]
            if pd.notna(fix) and fix > available:
                available = fix
            rows.append({"observation_time": bars["bar_start_utc"].iloc[k], "available_at": available,
                         "feature_id": feature_id, "value": value,
                         "source_revision": source_revision,
                         "metadata": {"source_window_bars": int(k) + 1 if column in cumulative_columns else
                                      len(indices)}})
    return rows


def _session_dependencies(bars, family, feature_id, session_id, row, params, session_indices, session_order,
                          session_positions, bar_by_start):
    """Índices de barras consultadas pelo valor; inclui uma margem conservadora por família."""
    position = session_positions[session_id]
    current = session_indices[session_id]
    if family == "volume":
        if "session_per_minute" in feature_id:
            deps = [session_id]
        else:
            match = re.search(r"_(\d+)(?:\.|$)", feature_id)
            lookback = int(match.group(1)) if match else int(params.get("slope_window", 1))
            if "session_slope_" in feature_id:
                start = max(0, position - lookback + 1)
                deps = session_order[start:position + 1]
            elif "session_ratio_median_" in feature_id:
                start = max(0, position - lookback)
                deps = session_order[start:position + 1]
            elif "session_percentile_" in feature_id:
                start = max(0, position - lookback)
                deps = session_order[start:position + 1]
            else:  # médias e medianas usam apenas sessões anteriores
                start = max(0, position - lookback)
                deps = session_order[start:position]
    elif family == "rvol":
        lookback = int(params.get("lookback", 20))
        prior = session_order[:position]
        if params.get("same_weekday"):
            weekday = bars.loc[current[0], "bar_start_utc"].weekday()
            prior = [sid for sid in prior if bars.loc[session_indices[sid][0], "bar_start_utc"].weekday() == weekday]
        minute = int(row.metadata.get("minute_of_session", 0))
        deps = []
        for sid in prior[-lookback:]:
            cutoff = bars["bar_start_utc"].iloc[session_indices[sid][0]] + pd.Timedelta(minutes=minute)
            deps.extend(i for i in session_indices[sid] if bars["bar_end_utc"].iloc[i] <= cutoff)
        current_idx = bar_by_start.get(pd.Timestamp(row.observation_time))
        if current_idx is not None:
            deps.extend(i for i in current if i <= current_idx)
        return deps
    elif family == "structure":
        if feature_id.startswith("level.dist_prev_"):
            stamp = pd.Timestamp(row.observation_time)
            bar_idx = bar_by_start.get(stamp)
            if bar_idx is None:
                deps = session_order[max(0, position - 1):position + 1]
                return [i for sid in deps for i in session_indices[sid]]
            else:
                atr_window = WILDER_MEMORY * int(params.get("atr_n", 14))
                rolling = list(range(max(0, bar_idx - atr_window), bar_idx + 1))
                previous = session_order[position - 1] if position else None
                deps = [previous] if previous else []
                return [i for sid in deps for i in session_indices[sid]] + rolling
        elif feature_id in {"session.return", "session.gap"}:
            previous = session_order[position - 1] if position else None
            previous_close = session_indices[previous][-1] if previous else None
            if feature_id == "session.gap":
                deps = [current[0]] + ([previous_close] if previous_close is not None else [])
            else:
                reached = [i for i in current if bars["bar_end_utc"].iloc[i] <= row.available_at]
                deps = reached + ([previous_close] if previous_close is not None else [])
        else:
            deps = [i for i in current if bars["bar_end_utc"].iloc[i] <= row.available_at]
        return deps
    else:
        deps = [session_id]
    return [i for sid in deps for i in session_indices[sid]]


def _long_rows(frame, bars, family, params):
    """Linhas das famílias que já devolvem quadro longo (volume, rvol, estrutura)."""
    rows = []
    if frame.empty:
        return rows
    session_indices = {sid: list(indexes) for sid, indexes in bars.groupby("session_id", sort=False).groups.items()}
    session_order = sorted(session_indices, key=lambda sid: bars["bar_start_utc"].iloc[session_indices[sid][0]])
    session_positions = {sid: position for position, sid in enumerate(session_order)}
    bar_by_start = {pd.Timestamp(start): i for i, start in enumerate(bars["bar_start_utc"])}
    corrected = bars["computed_at_utc"].where(bars["revision"].astype(int) > 1)
    tokens = _source_tokens(bars)
    revision_cache = {}
    for row in frame.itertuples(index=False):
        available = row.available_at
        dependencies = _session_dependencies(bars, family, row.feature_id, row.session_id, row, params,
                                             session_indices, session_order, session_positions, bar_by_start)
        if dependencies:
            fixes = corrected.iloc[dependencies].dropna()
            if not fixes.empty and fixes.max() > available:
                available = fixes.max()
        source_key = tuple(dependencies or session_indices[row.session_id])
        if source_key not in revision_cache:
            revision_cache[source_key] = _source_revision(tokens, source_key)
        source_revision = revision_cache[source_key]
        rows.append({"observation_time": row.observation_time, "available_at": available,
                     "feature_id": row.feature_id, "value": row.value,
                     "source_revision": source_revision,
                     "metadata": dict(row.metadata, source_bars=len(dependencies)), "session_id": row.session_id})
    return rows


def calendar_rows(bars):
    """Fatos de calendário conhecidos no início da barra (dia, semana do mês, minuto da sessão)."""
    from mt5_extracao.trade_analytics.enrich import calendar_columns

    bars = bars.sort_values("bar_start_utc").reset_index(drop=True)
    first = bars.groupby("session_id", sort=False).first()
    time_features = calendar_columns(bars["bar_start_utc"])
    rows = []
    for k in range(len(bars)):
        start = bars["bar_start_utc"].iloc[k]
        base = {"observation_time": start, "available_at": start, "source_revision": "clock:utc-v1",
                "metadata": {"clock": "UTC"}}
        rows.append(dict(base, feature_id="calendar.weekday_utc", value=float(time_features["day_of_week"].iloc[k])))
        rows.append(dict(base, feature_id="calendar.week_of_month_utc",
                         value=float(time_features["week_of_month"].iloc[k])))
        rows.append(dict(base, feature_id="calendar.hour_utc", value=float(time_features["hour"].iloc[k])))
        sid = bars["session_id"].iloc[k]
        open_start = first.loc[sid, "bar_start_utc"]
        open_end = first.loc[sid, "bar_end_utc"]
        available = max(start, open_end)
        fingerprint = _calendar_session_revision(sid, open_start, open_end)
        rows.append({"observation_time": start, "available_at": available,
                     "feature_id": "calendar.minute_of_session",
                     "value": float((start - open_start).total_seconds() // 60),
                     "source_revision": fingerprint,
                     "metadata": {"clock": "UTC", "source_bars": 1}})
    return rows


def _calendar_session_revision(session_id, open_start, open_end):
    payload = f"{session_id}|{pd.Timestamp(open_start).isoformat()}|{pd.Timestamp(open_end).isoformat()}|calendar-v2"
    return "calendar:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def _calendar_open_state(bars):
    if bars.empty:
        return {}
    ordered = bars.sort_values("bar_start_utc")
    current = ordered.iloc[-1]
    session = ordered[ordered["session_id"] == current["session_id"]]
    first = session.iloc[0]
    return {"session_id": str(current["session_id"]), "session_open_utc": pd.Timestamp(first["bar_start_utc"]).isoformat(),
            "session_open_end_utc": pd.Timestamp(first["bar_end_utc"]).isoformat()}


def calendar_append_rows(bars, prior_state):
    """Produz fatos de relógio só para barras anexadas, com o estado da abertura da sessão atual."""
    from mt5_extracao.trade_analytics.enrich import calendar_columns

    if bars.empty:
        return [], dict(prior_state or {})
    bars = bars.sort_values("bar_start_utc").reset_index(drop=True)
    clocks = calendar_columns(bars["bar_start_utc"])
    state = dict(prior_state or {})
    rows = []
    for session_id, group in bars.groupby("session_id", sort=False):
        group = group.sort_values("bar_start_utc")
        if state.get("session_id") == str(session_id):
            open_start = pd.Timestamp(state["session_open_utc"])
            open_end = pd.Timestamp(state["session_open_end_utc"])
        else:
            open_start = pd.Timestamp(group["bar_start_utc"].iloc[0])
            open_end = pd.Timestamp(group["bar_end_utc"].iloc[0])
        fingerprint = _calendar_session_revision(session_id, open_start, open_end)
        for index in group.index:
            start = pd.Timestamp(group.loc[index, "bar_start_utc"])
            base = {"observation_time": start, "available_at": start, "source_revision": "clock:utc-v1",
                    "metadata": {"clock": "UTC"}}
            rows.extend((dict(base, feature_id="calendar.weekday_utc", value=float(clocks["day_of_week"].iloc[index])),
                         dict(base, feature_id="calendar.week_of_month_utc",
                              value=float(clocks["week_of_month"].iloc[index])),
                         dict(base, feature_id="calendar.hour_utc", value=float(clocks["hour"].iloc[index])),
                         {"observation_time": start, "available_at": max(start, open_end),
                          "feature_id": "calendar.minute_of_session",
                          "value": float((start - open_start).total_seconds() // 60),
                          "source_revision": fingerprint, "metadata": {"clock": "UTC", "source_bars": 1}}))
        last = group.iloc[-1]
        state = {"session_id": str(session_id), "session_open_utc": pd.Timestamp(open_start).isoformat(),
                 "session_open_end_utc": pd.Timestamp(open_end).isoformat()}
    return rows, state


def _family_rows(family, bars, params, volume_source):
    if family == "volatility":
        frame = market_regimes.volatility_frame(bars, **params)
        columns = {name: f"volatility.{name}" for name in
                   ("atr", "atr_pct", "realized_vol", "range_pct", "bb_width", "atr_pct_percentile",
                    "bb_width_percentile", "range_percentile", "volatility_state")}
        window = _window(family, params)
        recent = _correction_times(bars, window)
        cumulative = _correction_times(bars, cumulative=True)
        corrections = {column: recent for column in columns}
        recursive = ("atr", "atr_pct", "atr_pct_percentile")
        for column in recursive:
            corrections[column] = cumulative
        return _bar_rows(bars, frame, columns, corrections, window, cumulative_columns=recursive), None
    if family == "regime":
        frame = market_regimes.regime_frame(bars, **params)
        columns = {"regime": "regime.state", "regime_raw": "regime.raw", "regime_duration": "regime.duration",
                   "regime_confidence": "regime.confidence", "adx": "regime.adx", "efficiency": "regime.efficiency",
                   "r2": "regime.r2"}
        window = _window(family, params)
        recent_corrections = _correction_times(bars, window)
        corrections = {column: recent_corrections for column in columns}
        recursive = ("regime", "regime_raw", "regime_duration", "regime_confidence", "adx")
        cumulative = _correction_times(bars, cumulative=True)
        for column in recursive:
            corrections[column] = cumulative
        return _bar_rows(bars, frame, columns, corrections, window,
                         cumulative_columns=recursive), None
    if family == "volume":
        frame, source = volume_profiles.session_volume_features(bars, volume_source=volume_source, **params)
        return _long_rows(frame, bars, family, params), source
    if family == "rvol":
        frame, source = volume_profiles.intraday_rvol(bars, volume_source=volume_source, **params)
        return _long_rows(frame, bars, family, params), source
    if family == "structure":
        frame = session_structure.structure_features(bars, **params)
        rows = _long_rows(frame, bars, family, params)
        tokens = _source_tokens(bars)
        prefix = _prefix_source_revisions(tokens)
        cumulative = _correction_times(bars, cumulative=True)
        positions = {pd.Timestamp(value): index for index, value in enumerate(bars["bar_start_utc"])}
        for row in rows:
            if not row["feature_id"].startswith("level.dist_prev_"):
                continue
            index = positions[pd.Timestamp(row["observation_time"])]
            row["source_revision"] = prefix[index]
            row["metadata"]["source_window_bars"] = index + 1
            fix = cumulative.iloc[index]
            if pd.notna(fix) and fix > row["available_at"]:
                row["available_at"] = fix
        return rows, None
    if family == "calendar":
        return calendar_rows(bars), None
    raise ValueError(f"Família desconhecida: {family}. Use: {', '.join(FAMILIES)}.")


def register_definitions(db, family, feature_ids, params, timeframe, volume_source=None, algorithm_version=None):
    timeframe = resampling.parse_target(timeframe).label
    dependencies = [f"bars_derived:{timeframe}", "market_calendar"]
    algorithm_version = algorithm_version or _feature_algorithm_version(family, params, volume_source, timeframe)
    for feature_id in sorted(feature_ids):
        parameters = _definition_parameters(family, params, volume_source)
        features.register(db, feature_id, feature_id.replace(".", " "), algorithm_version, parameters=parameters,
                          dependencies=dependencies)


def _config_hash(family, params, volume_source, timeframe):
    # Mesmo mecanismo de identidade por configuração da origem canônica T13.3b
    # (03570b0), incluindo o timeframe declarado nas dependências imutáveis.
    value = json.dumps({"family": family, "parameters": params, "volume_source": volume_source,
                        "timeframe": resampling.parse_target(timeframe).label,
                        "algorithm_version": ALGORITHM_VERSION}, ensure_ascii=False, sort_keys=True,
                       separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _feature_algorithm_version(family, params, volume_source, timeframe):
    return f"{ALGORITHM_VERSION}+{_config_hash(family, params, volume_source, timeframe)[:12]}"


def _definition_parameters(family, params, volume_source):
    parameters = dict(params, family=family, confidence=CONFIDENCE[family])
    if volume_source:
        parameters["volume_source"] = volume_source
    return json.loads(json.dumps(parameters, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                                 allow_nan=False))


def _invalidated_records(db, symbol, timeframe, family, params, volume_source, algorithm_version, rows):
    """Gera tombstones para valores que uma reconstrução completa deixou de produzir."""
    expected_parameters = _definition_parameters(family, params, volume_source)
    feature_ids = {
        item["feature_id"] for item in feature_store.list_definitions(db)
        if item["algorithm_version"] == algorithm_version and item["parameters"] == expected_parameters
    }
    if not feature_ids:
        return []
    previous = feature_store.latest_values(db, symbol=symbol, timeframe=timeframe, feature_ids=feature_ids,
                                           algorithm_version=algorithm_version)
    if previous.empty:
        return []
    current_keys = pd.MultiIndex.from_tuples(
        [(row["feature_id"], pd.Timestamp(row["observation_time"]).tz_convert("UTC")) for row in rows],
        names=["feature_id", "observation_time"],
    )
    previous_keys = pd.MultiIndex.from_frame(previous[["feature_id", "observation_time"]])
    missing = previous.loc[~previous_keys.isin(current_keys) & previous["quality"].ne("INVALIDATED")]
    if missing.empty:
        return []

    source_bars = resampling.latest_bars(db, symbol, timeframe, closed_only=False)
    if source_bars.empty:
        return []
    fingerprint = _source_revision(_source_tokens(source_bars), range(len(source_bars)))
    source_available = source_bars["computed_at_utc"].max()
    invalidations = []
    for old in missing.itertuples(index=False):
        available = max(source_available, old.available_at_utc)
        invalidations.append({
            "symbol": symbol,
            "timeframe": timeframe,
            "feature_id": old.feature_id,
            "algorithm_version": old.algorithm_version,
            "observation_time": old.observation_time,
            "value": None,
            "available_at": available,
            "observed_at": available,
            "quality": "INVALIDATED",
            "source_revision": f"{fingerprint}:invalidated",
            "metadata": {"reason": "indisponível após recálculo das barras", "family": family,
                         "source_scope": "reconstrução completa da família", "reconstructed": True,
                         "previous_revision": int(old.revision)},
        })
    return invalidations


def _record_batches(records, batch_size):
    """Divide o lote em prefixos temporais sem separar características do mesmo instante."""
    ordered = sorted(records, key=lambda item: (pd.Timestamp(item["observation_time"]), item["feature_id"]))
    batch, batch_time = [], None
    for record in ordered:
        stamp = pd.Timestamp(record["observation_time"])
        if batch and stamp != batch_time and len(batch) >= batch_size:
            yield batch
            batch = []
        batch.append(record)
        batch_time = stamp
    if batch:
        yield batch


def _check_limits(run, timeout_seconds, max_memory_mb):
    if timeout_seconds is not None and time.monotonic() - run["started_monotonic"] > timeout_seconds:
        raise TimeoutError(f"O job ultrapassou o limite de {timeout_seconds} segundos.")
    if max_memory_mb is not None and producer_jobs.current_rss_bytes() > max_memory_mb * 1024 * 1024:
        raise MemoryError(f"O job ultrapassou o limite de memória de {max_memory_mb} MiB.")


def _checkpoint_hook(**checkpoint):
    return lambda conn, _counts: producer_jobs.advance_checkpoint(conn, **checkpoint)


_TAIL_DATETIME_COLUMNS = ("bar_start_utc", "bar_end_utc", "computed_at_utc")


def _tail_session_limit(family, params):
    if family == "volume":
        return max([int(params["slope_window"]), int(params["percentile_window"])] +
                   [int(value) for value in params["windows"]]) + 2
    if family == "rvol":
        factor = 7 if params.get("same_weekday") else 1
        return factor * int(params["lookback"]) + 2
    return 0


def _encode_tail(bars, session_limit):
    if bars.empty or session_limit < 1:
        return []
    ordered = bars.sort_values("bar_start_utc").reset_index(drop=True)
    sessions = ordered.groupby("session_id", sort=False)["bar_start_utc"].min().sort_values().index.tolist()
    keep = set(sessions[-int(session_limit):])
    return [{key: _json_safe(value) for key, value in row.items()}
            for row in ordered[ordered["session_id"].isin(keep)].to_dict("records")]


def _encode_last_bars(bars, count):
    ordered = bars.sort_values("bar_start_utc").tail(int(count))
    return [{key: _json_safe(value) for key, value in row.items()} for row in ordered.to_dict("records")]


def _encode_structure_bars(bars, window):
    if bars.empty:
        return []
    ordered = bars.sort_values("bar_start_utc").reset_index(drop=True)
    sessions = ordered.groupby("session_id", sort=False)["bar_start_utc"].min().sort_values().index.tolist()
    session_rows = ordered[ordered["session_id"].isin(set(sessions[-3:]))]
    recent_rows = ordered.tail(int(window))
    context = pd.concat([session_rows, recent_rows], ignore_index=True)
    context = context.drop_duplicates(subset=["session_id", "bar_start_utc"], keep="last")
    context = context.sort_values("bar_start_utc")
    return [{key: _json_safe(value) for key, value in row.items()} for row in context.to_dict("records")]


def _decode_tail(state):
    records = (state or {}).get("tail_bars") or []
    frame = pd.DataFrame(records)
    for name in _TAIL_DATETIME_COLUMNS:
        if name in frame:
            frame[name] = pd.to_datetime(frame[name], utc=True, errors="coerce")
    for name in ("revision", "real_volume_known", "real_volume_known_count", "is_partial", "bar_count"):
        if name in frame:
            frame[name] = pd.to_numeric(frame[name], errors="coerce")
    return frame.sort_values("bar_start_utc").reset_index(drop=True) if not frame.empty else frame


def _family_checkpoint_state(family, bars, params, volume_source=None):
    if family == "calendar":
        return _calendar_open_state(bars)
    if family == "volatility":
        frame = market_regimes.volatility_frame(bars, **params)
        true_range = market_regimes.ind.true_range(bars)
        atr_state = true_range.ewm(alpha=1.0 / int(params["atr_n"]), adjust=False).mean().iloc[-1]
        returns = np.log(bars["close"].astype(float)).diff()
        width = _window(family, params)
        chain, last_correction = _source_chain_state(bars)
        return {"tail_bars": _encode_last_bars(bars, width), "window": width,
                "bars_seen": int(len(bars)), "source_chain": chain,
                "cumulative_correction": None if pd.isna(last_correction) else pd.Timestamp(last_correction).isoformat(),
                "atr_state": _json_safe(atr_state), "atr_count": int(len(bars)),
                "previous_close": _json_safe(bars["close"].iloc[-1]),
                "return_tail": _json_safe(returns.tail(int(params["rv_n"])).tolist()),
                "close_tail": _json_safe(bars["close"].tail(int(params["bb_n"])).tolist()),
                "atr_pct_tail": _json_safe(frame["atr_pct"].tail(int(params["percentile_window"])).tolist()),
                "bb_width_tail": _json_safe(frame["bb_width"].tail(int(params["percentile_window"])).tolist()),
                "range_pct_tail": _json_safe(frame["range_pct"].tail(int(params["percentile_window"])).tolist())}
    if family == "regime":
        frame = market_regimes.regime_frame(bars, **params)
        period = int(params["adx_n"])
        high, low = bars["high"].astype(float), bars["low"].astype(float)
        up, down = high.diff(), -low.diff()
        plus_dm = pd.Series(np.where((up > down) & (up > 0), up, 0.0), index=bars.index)
        minus_dm = pd.Series(np.where((down > up) & (down > 0), down, 0.0), index=bars.index)
        tr = market_regimes.ind.true_range(bars)
        alpha = 1.0 / period
        tr_state = tr.ewm(alpha=alpha, adjust=False).mean().iloc[-1]
        plus_state = plus_dm.ewm(alpha=alpha, adjust=False).mean().iloc[-1]
        minus_state = minus_dm.ewm(alpha=alpha, adjust=False).mean().iloc[-1]
        tr_visible = tr.ewm(alpha=alpha, adjust=False, min_periods=period).mean()
        plus_visible = plus_dm.ewm(alpha=alpha, adjust=False, min_periods=period).mean()
        minus_visible = minus_dm.ewm(alpha=alpha, adjust=False, min_periods=period).mean()
        with np.errstate(divide="ignore", invalid="ignore"):
            plus_di = 100 * plus_visible / tr_visible
            minus_di = 100 * minus_visible / tr_visible
            dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di)
        dx = dx.replace([np.inf, -np.inf], np.nan).fillna(0.0).where(tr_visible.notna())
        adx_state = dx.ewm(alpha=alpha, adjust=False).mean().iloc[-1]
        last_raw = frame["regime_raw"].iloc[-1]
        run_length = 0
        for label in reversed(frame["regime_raw"].tolist()):
            if label != last_raw:
                break
            run_length += 1
        chain, last_correction = _source_chain_state(bars)
        window = _window(family, params)
        tail_size = max(window, int(params["er_n"]) + 1, int(params["r2_n"]))
        return {"tail_bars": _encode_last_bars(bars, tail_size), "window": window,
                "bars_seen": int(len(bars)), "source_chain": chain,
                "tr_state": _json_safe(tr_state), "plus_state": _json_safe(plus_state),
                "minus_state": _json_safe(minus_state), "adx_state": _json_safe(adx_state),
                "tr_count": int(len(bars)), "dx_count": int(dx.notna().sum()),
                "previous_high": _json_safe(high.iloc[-1]), "previous_low": _json_safe(low.iloc[-1]),
                "previous_close": _json_safe(bars["close"].iloc[-1]), "regime_state": frame["regime"].iloc[-1],
                "regime_duration": int(frame["regime_duration"].iloc[-1]), "last_raw": last_raw,
                "raw_run_length": int(run_length),
                "cumulative_correction": None if pd.isna(last_correction) else pd.Timestamp(last_correction).isoformat()}
    if family == "structure":
        true_range = market_regimes.ind.true_range(bars)
        period = int(params["atr_n"])
        atr_state = true_range.ewm(alpha=1.0 / period, adjust=False).mean().iloc[-1]
        window = WILDER_MEMORY * period
        chain, last_correction = _source_chain_state(bars)
        return {"tail_bars": _encode_structure_bars(bars, window), "window": window,
                "atr_state": _json_safe(atr_state), "atr_count": int(len(bars)),
                "previous_close": _json_safe(bars["close"].iloc[-1]),
                "bars_seen": int(len(bars)), "source_chain": chain,
                "cumulative_correction": None if pd.isna(last_correction) else pd.Timestamp(last_correction).isoformat(),
                "last_session_id": str(bars.sort_values("bar_start_utc").iloc[-1]["session_id"]),
                "last_session_complete": bool(bars.sort_values("bar_start_utc").iloc[-1]["session_complete"])
                if "session_complete" in bars else True}
    session_limit = _tail_session_limit(family, params)
    if session_limit:
        state = {"tail_bars": _encode_tail(bars, session_limit),
                 "session_limit": session_limit,
                 "last_session_complete": bool(bars.sort_values("bar_start_utc").iloc[-1]["session_complete"])
                 if "session_complete" in bars and len(bars) else True}
        if volume_source in {"real", "tick"}:
            state["resolved_volume_source"] = volume_source
        return state
    return {}


def _finite_number(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return np.nan
    return number if np.isfinite(number) else np.nan


def _prior_percentile_value(history, current, window):
    values = list(history)
    if len(values) < window or not np.isfinite(current):
        return np.nan
    prior = np.asarray(values[-window:], dtype=float)
    if not np.isfinite(prior).all():
        return np.nan
    return float(100.0 * (prior < current).mean())


def _volatility_append_rows(prior, delta, params, cursor):
    state = dict(prior["state"] or {})
    context = _decode_tail(state)
    if context.empty:
        return None
    if delta.empty:
        return [], state
    if cursor is not None and delta["bar_start_utc"].min() <= cursor:
        return None
    delta = delta.sort_values("bar_start_utc").reset_index(drop=True)
    combined = pd.concat([context, delta], ignore_index=True).sort_values("bar_start_utc").reset_index(drop=True)
    prior_close = _finite_number(state.get("previous_close"))
    atr_state = _finite_number(state.get("atr_state"))
    atr_count = int(state.get("atr_count", len(context)))
    alpha = 1.0 / int(params["atr_n"])
    closes_for_bb = list(state.get("close_tail") or [])
    returns = [_finite_number(value) for value in (state.get("return_tail") or [])]
    percentile_window = int(params["percentile_window"])
    atr_history = list(state.get("atr_pct_tail") or [])
    bb_history = list(state.get("bb_width_tail") or [])
    range_history = list(state.get("range_pct_tail") or [])
    bars_seen = int(state.get("bars_seen", len(context)))
    try:
        chain = bytes.fromhex(state["source_chain"])
    except (KeyError, ValueError):
        return None
    cumulative_fix = pd.to_datetime(state.get("cumulative_correction"), utc=True, errors="coerce")
    row_chains, cumulative_fixes = [], []
    tokens = _source_tokens(delta)
    output = {name: [] for name in ("atr", "atr_pct", "realized_vol", "range_pct", "bb_width",
                                    "atr_pct_percentile", "bb_width_percentile", "range_percentile",
                                    "volatility_state")}
    start = len(context)
    for offset, row in enumerate(delta.itertuples(index=False)):
        high, low, close = float(row.high), float(row.low), float(row.close)
        tr_values = [high - low]
        if np.isfinite(prior_close):
            tr_values.extend((abs(high - prior_close), abs(low - prior_close)))
        true_range = max(tr_values)
        atr_state = true_range if not np.isfinite(atr_state) else (1.0 - alpha) * atr_state + alpha * true_range
        atr_count += 1
        atr_value = atr_state if atr_count >= int(params["atr_n"]) else np.nan
        with np.errstate(divide="ignore", invalid="ignore"):
            atr_pct = float(np.divide(atr_value, close))
            range_pct = float(np.divide(high - low, close))
        closes_for_bb.append(close)
        closes_for_bb = closes_for_bb[-int(params["bb_n"]):]
        if len(closes_for_bb) >= int(params["bb_n"]):
            close_window = pd.Series(closes_for_bb, dtype=float)
            bb_mean = float(close_window.rolling(int(params["bb_n"]), min_periods=int(params["bb_n"])).mean().iloc[-1])
            bb_std = float(close_window.rolling(int(params["bb_n"]), min_periods=int(params["bb_n"])).std(
                ddof=0).iloc[-1])
            with np.errstate(divide="ignore", invalid="ignore"):
                bb_width = float(np.divide(4.0 * bb_std, bb_mean))
        else:
            bb_width = np.nan
        log_close = float(np.log(close)) if close > 0 else np.nan
        previous_log = float(np.log(prior_close)) if np.isfinite(prior_close) and prior_close > 0 else np.nan
        returns.append(log_close - previous_log if np.isfinite(log_close) and np.isfinite(previous_log) else np.nan)
        returns = returns[-int(params["rv_n"]):]
        realized_series = pd.Series(returns, dtype=float).rolling(int(params["rv_n"]),
                                                                  min_periods=int(params["rv_n"])).std()
        realized = float(realized_series.iloc[-1]) if len(returns) >= int(params["rv_n"]) else np.nan
        atr_pct_pct = _prior_percentile_value(atr_history, atr_pct, percentile_window)
        bb_width_pct = _prior_percentile_value(bb_history, bb_width, percentile_window)
        range_pct_pct = _prior_percentile_value(range_history, range_pct, percentile_window)
        state_label = None
        if np.isfinite(bb_width_pct) and np.isfinite(range_pct_pct):
            state_label = "NORMAL"
            if bb_width_pct <= float(params["compression_pct"]):
                state_label = "COMPRESSION"
            if range_pct_pct >= float(params["expansion_pct"]):
                state_label = "EXPANSION"
        for name, value in (("atr", atr_value), ("atr_pct", atr_pct), ("realized_vol", realized),
                            ("range_pct", range_pct), ("bb_width", bb_width),
                            ("atr_pct_percentile", atr_pct_pct), ("bb_width_percentile", bb_width_pct),
                            ("range_percentile", range_pct_pct), ("volatility_state", state_label)):
            output[name].append(value)
        atr_history.append(atr_pct)
        bb_history.append(bb_width)
        range_history.append(range_pct)
        atr_history = atr_history[-percentile_window:]
        bb_history = bb_history[-percentile_window:]
        range_history = range_history[-percentile_window:]
        chain = hashlib.sha256(chain + tokens[offset]).digest()
        row_chains.append(f"bars:{chain.hex()[:24]}")
        if int(row.revision) > 1:
            fix = pd.Timestamp(row.computed_at_utc)
            if pd.isna(cumulative_fix) or fix > cumulative_fix:
                cumulative_fix = fix
        cumulative_fixes.append(cumulative_fix)
        bars_seen += 1
        prior_close = close

    context_length = len(context)
    frame = pd.DataFrame(output, index=range(context_length, context_length + len(delta)))
    columns = {name: f"volatility.{name}" for name in output}
    window = _window("volatility", params)
    recent = _correction_times(combined, window)
    corrections = {name: recent for name in output}
    recursive = ("atr", "atr_pct", "atr_pct_percentile")
    cumulative = pd.Series(pd.NaT, index=combined.index, dtype="datetime64[ns, UTC]")
    cumulative.iloc[start:] = pd.to_datetime(cumulative_fixes, utc=True)
    for name in recursive:
        corrections[name] = cumulative
    rows = _bar_rows(combined, frame, columns, corrections, window, cumulative_columns=recursive)
    chains_by_time = {pd.Timestamp(row.bar_start_utc): (row_chains[offset], bars_seen - len(delta) + offset + 1)
                      for offset, row in enumerate(delta.itertuples(index=False))}
    recursive_ids = {columns[name] for name in recursive}
    for item in rows:
        if item["feature_id"] in recursive_ids:
            fingerprint, source_count = chains_by_time[pd.Timestamp(item["observation_time"])]
            item["source_revision"] = fingerprint
            item["metadata"]["source_window_bars"] = source_count
    new_state = {"tail_bars": _encode_last_bars(combined, window), "window": window,
                 "bars_seen": int(bars_seen), "source_chain": chain.hex(),
                 "cumulative_correction": None if pd.isna(cumulative_fix) else cumulative_fix.isoformat(),
                 "atr_state": _json_safe(atr_state), "atr_count": int(atr_count),
                 "previous_close": _json_safe(prior_close), "return_tail": _json_safe(returns),
                 "close_tail": _json_safe(closes_for_bb), "atr_pct_tail": _json_safe(atr_history),
                 "bb_width_tail": _json_safe(bb_history), "range_pct_tail": _json_safe(range_history)}
    return rows, new_state


def _regime_append_rows(prior, delta, params, cursor):
    state = dict(prior["state"] or {})
    context = _decode_tail(state)
    if context.empty:
        return None
    if delta.empty:
        return [], state
    if cursor is not None and delta["bar_start_utc"].min() <= cursor:
        return None
    delta = delta.sort_values("bar_start_utc").reset_index(drop=True)
    combined = pd.concat([context, delta], ignore_index=True).sort_values("bar_start_utc").reset_index(drop=True)
    period = int(params["adx_n"])
    alpha = 1.0 / period
    tr_state = _finite_number(state.get("tr_state"))
    plus_state = _finite_number(state.get("plus_state"))
    minus_state = _finite_number(state.get("minus_state"))
    adx_state = _finite_number(state.get("adx_state"))
    tr_count = int(state.get("tr_count", len(context)))
    dx_count = int(state.get("dx_count", 0))
    previous_high = _finite_number(state.get("previous_high"))
    previous_low = _finite_number(state.get("previous_low"))
    previous_close = _finite_number(state.get("previous_close"))
    close_tail = list(context["close"].tail(max(int(params["er_n"]) + 1, int(params["r2_n"]))).astype(float))
    current_state = state.get("regime_state", market_regimes.UNCERTAIN)
    run_label = state.get("last_raw")
    run_length = int(state.get("raw_run_length", 0))
    state_length = int(state.get("regime_duration", 0))
    bars_seen = int(state.get("bars_seen", len(context)))
    try:
        chain = bytes.fromhex(state["source_chain"])
    except (KeyError, ValueError):
        return None
    cumulative_fix = pd.to_datetime(state.get("cumulative_correction"), utc=True, errors="coerce")
    recent_window = _window("regime", params)
    output = {name: [] for name in ("regime", "regime_raw", "regime_duration", "regime_confidence", "adx",
                                    "efficiency", "r2")}
    row_chains = []
    cumulative_fixes = []
    for row in delta.itertuples(index=False):
        high, low, close = float(row.high), float(row.low), float(row.close)
        tr_values = [high - low]
        if np.isfinite(previous_close):
            tr_values.extend((abs(high - previous_close), abs(low - previous_close)))
        tr = max(tr_values)
        up = high - previous_high if np.isfinite(previous_high) else np.nan
        down = previous_low - low if np.isfinite(previous_low) else np.nan
        plus_dm = up if np.isfinite(up) and up > down and up > 0 else 0.0
        minus_dm = down if np.isfinite(down) and down > up and down > 0 else 0.0
        tr_state = tr if not np.isfinite(tr_state) else (1.0 - alpha) * tr_state + alpha * tr
        plus_state = plus_dm if not np.isfinite(plus_state) else (1.0 - alpha) * plus_state + alpha * plus_dm
        minus_state = minus_dm if not np.isfinite(minus_state) else (1.0 - alpha) * minus_state + alpha * minus_dm
        tr_count += 1
        if tr_count >= period:
            with np.errstate(divide="ignore", invalid="ignore"):
                plus_di = 100.0 * plus_state / tr_state
                minus_di = 100.0 * minus_state / tr_state
                dx = 100.0 * abs(plus_di - minus_di) / (plus_di + minus_di)
            if not np.isfinite(dx):
                dx = 0.0
            adx_state = dx if not np.isfinite(adx_state) else (1.0 - alpha) * adx_state + alpha * dx
            dx_count += 1
            adx_value = adx_state if dx_count >= period else np.nan
        else:
            adx_value = np.nan

        close_tail.append(close)
        close_tail = close_tail[-max(int(params["er_n"]) + 1, int(params["r2_n"])):]
        er_value = np.nan
        if len(close_tail) >= int(params["er_n"]) + 1:
            er_closes = np.asarray(close_tail[-int(params["er_n"]) - 1:], dtype=float)
            path = float(np.abs(np.diff(er_closes)).sum())
            change = abs(float(er_closes[-1] - er_closes[0]))
            er_value = change / path if path else 0.0
        r2_value = np.nan
        if len(close_tail) >= int(params["r2_n"]):
            r2_value = float(market_regimes.ind.linreg_r2(
                pd.Series(close_tail[-int(params["r2_n"]):], dtype=float), int(params["r2_n"])).iloc[-1])
        raw = market_regimes.raw_regime(adx_value, params["trend_adx"], params["range_adx"])
        confidence = np.nan
        if raw in (market_regimes.TREND, market_regimes.RANGE) and np.isfinite(er_value) and np.isfinite(r2_value):
            trendy = (er_value >= float(params["er_cut"]), r2_value >= float(params["r2_cut"]))
            agree = sum(trendy) if raw == market_regimes.TREND else sum(not flag for flag in trendy)
            confidence = (1 + agree) / 3.0
        run_length = run_length + 1 if raw == run_label else 1
        run_label = raw
        if raw != current_state and (run_length >= int(params["min_duration"]) or raw == market_regimes.UNCERTAIN):
            current_state, state_length = raw, run_length
        else:
            state_length += 1
        values = (current_state, raw, state_length, confidence, adx_value, er_value, r2_value)
        for name, value in zip(output, values):
            output[name].append(value)

        bar_token = _source_tokens(pd.DataFrame([row._asdict()]))[0]
        chain = hashlib.sha256(chain + bar_token).digest()
        row_chains.append(f"bars:{chain.hex()[:24]}")
        if int(row.revision) > 1:
            fix = pd.Timestamp(row.computed_at_utc)
            if pd.isna(cumulative_fix) or fix > cumulative_fix:
                cumulative_fix = fix
        cumulative_fixes.append(cumulative_fix)
        bars_seen += 1
        previous_high, previous_low, previous_close = high, low, close

    start = len(context)
    frame = pd.DataFrame(output, index=range(start, start + len(delta)))
    columns = {"regime": "regime.state", "regime_raw": "regime.raw", "regime_duration": "regime.duration",
               "regime_confidence": "regime.confidence", "adx": "regime.adx",
               "efficiency": "regime.efficiency", "r2": "regime.r2"}
    recent = _correction_times(combined, recent_window)
    corrections = {name: recent for name in columns}
    cumulative = pd.Series(pd.NaT, index=combined.index, dtype="datetime64[ns, UTC]")
    cumulative.iloc[start:] = pd.to_datetime(cumulative_fixes, utc=True)
    recursive = ("regime", "regime_raw", "regime_duration", "regime_confidence", "adx")
    for name in recursive:
        corrections[name] = cumulative
    rows = _bar_rows(combined, frame, columns, corrections, recent_window, cumulative_columns=recursive)
    chain_by_time = {pd.Timestamp(row.bar_start_utc): (row_chains[index], bars_seen - len(delta) + index + 1)
                     for index, row in enumerate(delta.itertuples(index=False))}
    for item in rows:
        if item["feature_id"] in {columns[name] for name in recursive}:
            fingerprint, source_count = chain_by_time[pd.Timestamp(item["observation_time"])]
            item["source_revision"] = fingerprint
            item["metadata"]["source_window_bars"] = source_count
    tail_size = max(recent_window, int(params["er_n"]) + 1, int(params["r2_n"]))
    new_state = {"tail_bars": _encode_last_bars(combined, tail_size), "window": recent_window,
                 "bars_seen": int(bars_seen), "source_chain": chain.hex(), "tr_state": _json_safe(tr_state),
                 "plus_state": _json_safe(plus_state), "minus_state": _json_safe(minus_state),
                 "adx_state": _json_safe(adx_state), "tr_count": int(tr_count), "dx_count": int(dx_count),
                 "previous_high": _json_safe(previous_high), "previous_low": _json_safe(previous_low),
                 "previous_close": _json_safe(previous_close), "regime_state": current_state,
                 "regime_duration": int(state_length), "last_raw": run_label, "raw_run_length": int(run_length),
                 "cumulative_correction": None if pd.isna(cumulative_fix) else cumulative_fix.isoformat()}
    return rows, new_state


def _structure_append_rows(prior, delta, params, cursor):
    state = dict(prior["state"] or {})
    context = _decode_tail(state)
    if context.empty:
        return None
    if delta.empty:
        return [], state
    if cursor is not None and delta["bar_start_utc"].min() <= cursor:
        return None
    old_session = str(state.get("last_session_id", context["session_id"].iloc[-1]))
    delta = delta.sort_values("bar_start_utc").reset_index(drop=True)
    new_sessions = set(delta["session_id"].astype(str))
    session_changed = any(session != old_session for session in new_sessions)
    if session_changed and not state.get("last_session_complete", True):
        return None
    combined = pd.concat([context, delta], ignore_index=True).sort_values("bar_start_utc").reset_index(drop=True)
    if not session_changed and "session_complete" in combined:
        combined.loc[combined["session_id"].astype(str) == old_session, "session_complete"] = \
            bool(state.get("last_session_complete", True))
    frame = session_structure.structure_features(combined, **params)
    atr_state = _finite_number(state.get("atr_state"))
    atr_count = int(state.get("atr_count", len(context)))
    previous_close = _finite_number(state.get("previous_close"))
    alpha = 1.0 / int(params["atr_n"])
    atr_before = {}
    bars_seen = int(state.get("bars_seen", len(context)))
    try:
        chain = bytes.fromhex(state["source_chain"])
    except (KeyError, ValueError):
        return None
    cumulative_fix = pd.to_datetime(state.get("cumulative_correction"), utc=True, errors="coerce")
    chain_by_time = {}
    tokens = _source_tokens(delta)
    for offset, row in enumerate(delta.itertuples(index=False)):
        stamp = pd.Timestamp(row.bar_start_utc)
        atr_before[stamp] = atr_state if atr_count >= int(params["atr_n"]) else np.nan
        chain = hashlib.sha256(chain + tokens[offset]).digest()
        if int(row.revision) > 1:
            fix = pd.Timestamp(row.computed_at_utc)
            if pd.isna(cumulative_fix) or fix > cumulative_fix:
                cumulative_fix = fix
        bars_seen += 1
        chain_by_time[stamp] = (f"bars:{chain.hex()[:24]}", bars_seen, cumulative_fix)
        high, low, close = float(row.high), float(row.low), float(row.close)
        tr_values = [high - low]
        if np.isfinite(previous_close):
            tr_values.extend((abs(high - previous_close), abs(low - previous_close)))
        tr = max(tr_values)
        atr_state = tr if not np.isfinite(atr_state) else (1.0 - alpha) * atr_state + alpha * tr
        atr_count += 1
        previous_close = close
    if not frame.empty:
        summaries = session_structure.session_summary(combined)
        positions = {sid: index for index, sid in enumerate(summaries["session_id"])}
        new_stamps = {pd.Timestamp(row.bar_start_utc) for row in delta.itertuples(index=False)}
        for index, item in frame.iterrows():
            stamp = pd.Timestamp(item["observation_time"])
            if not item["feature_id"].startswith("level.dist_prev_") or stamp not in new_stamps:
                continue
            session_pos = positions[item["session_id"]]
            if session_pos < 1 or not bool(summaries.iloc[session_pos - 1]["complete"]):
                continue
            atr = atr_before.get(stamp)
            if not np.isfinite(atr) or atr <= 0:
                continue
            previous = summaries.iloc[session_pos - 1]
            bar = combined[combined["bar_start_utc"] == stamp].iloc[0]
            level = "high" if item["feature_id"] == "level.dist_prev_high_atr" else "low"
            frame.at[index, "value"] = float((bar["close"] - previous[level]) / atr)
    rows = _long_rows(frame, combined, "structure", params)
    cursor = pd.Timestamp(cursor) if cursor is not None else pd.Timestamp.min.tz_localize("UTC")
    newly_closed = old_session if session_changed and state.get("last_session_complete", True) else None
    filtered = []
    for row in rows:
        stamp = pd.Timestamp(row["observation_time"])
        available = pd.Timestamp(row["available_at"])
        if row["feature_id"].startswith("level.dist_prev_") and stamp in chain_by_time:
            fingerprint, source_count, fix = chain_by_time[stamp]
            row["source_revision"] = fingerprint
            row["metadata"]["source_window_bars"] = source_count
            if pd.notna(fix) and fix > available:
                row["available_at"] = fix
                available = fix
        if row["feature_id"] == "session.return" and row["session_id"] == newly_closed:
            filtered.append(row)
        elif row["feature_id"].startswith("opening_range.") and available > cursor:
            filtered.append(row)
        elif stamp > cursor:
            filtered.append(row)
    window = WILDER_MEMORY * int(params["atr_n"])
    new_state = {"tail_bars": _encode_structure_bars(combined, window), "window": window,
                 "atr_state": _json_safe(atr_state), "atr_count": int(atr_count),
                 "previous_close": _json_safe(previous_close), "bars_seen": int(bars_seen),
                 "source_chain": chain.hex(),
                 "cumulative_correction": None if pd.isna(cumulative_fix) else cumulative_fix.isoformat(),
                 "last_session_id": str(combined["session_id"].iloc[-1]),
                 "last_session_complete": bool(combined["session_complete"].iloc[-1])
                 if "session_complete" in combined else True}
    return filtered, new_state


def _append_family_rows(family, prior, delta, params, requested_volume_source, cursor):
    prior_state = dict(prior["state"] or {})
    tail = _decode_tail(prior_state)
    if tail.empty:
        return None
    if delta.empty:
        return [], dict(prior_state), prior_state.get("resolved_volume_source")
    if cursor is not None and delta["bar_start_utc"].min() <= cursor:
        return None
    if family in {"volume", "rvol"} and requested_volume_source == "auto" and \
            prior_state.get("resolved_volume_source") == "real" and \
            ("real_volume_known" not in delta or not delta["real_volume_known"].fillna(0).astype(int).eq(1).all()):
        return None
    old_session = str(tail["session_id"].iloc[-1])
    new_sessions = set(delta["session_id"].astype(str))
    session_changed = any(item != old_session for item in new_sessions)
    if session_changed and not prior_state.get("last_session_complete", True):
        # A sessão ainda incompleta pode ter sido encerrada ou conter lacunas; confirme com a fonte completa.
        return None
    combined = pd.concat([tail, delta], ignore_index=True).sort_values("bar_start_utc").reset_index(drop=True)
    if "session_complete" in combined:
        if not session_changed:
            combined.loc[combined["session_id"].astype(str) == old_session, "session_complete"] = \
                bool(prior_state.get("last_session_complete", True))
        if session_changed:
            # A última sessão do lote incremental pode ainda estar aberta; mantenha a marca do próprio lote.
            pass
    source = prior_state.get("resolved_volume_source", requested_volume_source)
    rows, resolved_source = _family_rows(family, combined, params, source)
    if family == "rvol":
        rows = [row for row in rows if pd.Timestamp(row["observation_time"]) > cursor]
    state = _family_checkpoint_state(family, combined, params, resolved_source)
    return rows, state, resolved_source


def compute(db, symbol, timeframe, *, families=None, volume_source="auto", overrides=None, write_guard=None,
            batch_size=5000, timeout_seconds=600, max_memory_mb=2048, max_retries=2):
    """Calcula as famílias pedidas para ``symbol``/``timeframe`` e grava; devolve um resumo por família."""
    families = list(families or FAMILIES)
    unknown = [name for name in families if name not in FAMILIES]
    if unknown:
        raise ValueError(f"Família desconhecida: {', '.join(unknown)}. Use: {', '.join(FAMILIES)}.")
    if not (1 <= int(batch_size) <= MAX_BATCH_SIZE) or not (0 <= int(max_retries) <= MAX_BATCH_RETRIES) or \
            not (0 < float(timeout_seconds) <= MAX_JOB_TIMEOUT_SECONDS) or \
            not (0 < float(max_memory_mb) <= MAX_JOB_MEMORY_MB):
        raise ValueError("batch_size, timeout e memória precisam estar dentro dos limites do job; "
                         f"max_retries deve estar entre 0 e {MAX_BATCH_RETRIES}.")
    label = resampling.parse_target(timeframe).label
    source_sequence = resampling.latest_source_sequence(db, symbol, label)
    source_sequence = 0 if source_sequence is None else int(source_sequence)
    closed_after_cache = {}

    def closed_bars_after(cursor):
        key = str(cursor) if cursor is not None else None
        if key not in closed_after_cache:
            closed_after_cache[key] = resampling.has_closed_bars_after(db, symbol, label, cursor)
        return closed_after_cache[key]

    configurations = {}
    checkpoints = {}
    skipped = {}
    for family in families:
        params = dict(DEFAULTS[family], **(overrides or {}).get(family, {}))
        config_hash = _config_hash(family, params, volume_source, label)
        configurations[family] = (params, config_hash)
        checkpoint = producer_jobs.get_checkpoint(db, symbol, label, family)
        checkpoints[family] = checkpoint
        if checkpoint and checkpoint["algorithm_version"] == ALGORITHM_VERSION and \
                checkpoint["config_hash"] == config_hash and checkpoint["source_sequence"] == source_sequence and \
                checkpoint["status"] == "COMPLETE" and not closed_bars_after(checkpoint["last_bar_start_utc"]):
            run = producer_jobs.start_run(db, symbol, label, family, ALGORITHM_VERSION, config_hash, source_sequence)
            measurement = producer_jobs.finish_run(db, run, source_sequence=source_sequence, status="SKIPPED")
            resolved_source = checkpoint["state"].get("resolved_volume_source") if family in {"volume", "rvol"} else None
            feature_version = _feature_algorithm_version(family, params, resolved_source, label)
            skipped[family] = {"family": family, "symbol": symbol, "timeframe": label, "features": 0,
                               "algorithm_version": feature_version, "volume_source": resolved_source,
                               "invalidations": 0,
                               "values": 0, "inserted": 0, "revised": 0, "unchanged": 0, "skipped": True,
                               "job": measurement}
    active_families = [family for family in families if family not in skipped]
    if not active_families:
        return [skipped[family] for family in families]

    append_inputs = {}
    delta_cache = {}
    for family in active_families:
        prior = checkpoints[family]
        params, config_hash = configurations[family]
        if family not in {"calendar", "volume", "rvol", "volatility", "regime", "structure"} or not (
                prior and prior["algorithm_version"] == ALGORITHM_VERSION and prior["config_hash"] == config_hash and
                prior["status"] == "COMPLETE" and
                (prior["source_sequence"] < source_sequence or closed_bars_after(prior["last_bar_start_utc"]))):
            continue
        after_sequence = prior["source_sequence"]
        if after_sequence not in delta_cache:
            delta_cache[after_sequence] = resampling.latest_bars(
                db, symbol, label, source_seq_after=after_sequence,
                bar_start_after=prior["last_bar_start_utc"])
        delta = delta_cache[after_sequence]
        cursor = pd.Timestamp(prior["last_bar_start_utc"]) if prior["last_bar_start_utc"] else None
        if family == "calendar":
            if delta.empty or (cursor is not None and delta["bar_start_utc"].min() > cursor):
                state = (calendar_append_rows(delta, prior["state"])[1] if not delta.empty else prior["state"])
                append_inputs[family] = (delta, state, None,
                                         delta["bar_start_utc"].max().strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")
                                         if not delta.empty else prior["last_bar_start_utc"])
        elif family == "volatility":
            result = _volatility_append_rows(prior, delta, params, cursor)
            if result is not None:
                rows, state = result
                append_inputs[family] = (delta, state, (rows, None),
                                         delta["bar_start_utc"].max().strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")
                                         if not delta.empty else prior["last_bar_start_utc"])
        elif family == "regime":
            result = _regime_append_rows(prior, delta, params, cursor)
            if result is not None:
                rows, state = result
                append_inputs[family] = (delta, state, (rows, None),
                                         delta["bar_start_utc"].max().strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")
                                         if not delta.empty else prior["last_bar_start_utc"])
        elif family == "structure":
            result = _structure_append_rows(prior, delta, params, cursor)
            if result is not None:
                rows, state = result
                append_inputs[family] = (delta, state, (rows, None),
                                         delta["bar_start_utc"].max().strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")
                                         if not delta.empty else prior["last_bar_start_utc"])
        else:
            result = _append_family_rows(family, prior, delta, params, volume_source, cursor)
            if result is not None:
                rows, state, source = result
                final_bar = (delta["bar_start_utc"].max().strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")
                             if not delta.empty else prior["last_bar_start_utc"])
                append_inputs[family] = (delta, state, (rows, source), final_bar)

    summary = []
    full_bars = None
    for family in active_families:
        params, config_hash = configurations[family]
        run = producer_jobs.start_run(db, symbol, label, family, ALGORITHM_VERSION, config_hash, source_sequence)
        total = {"inserted": 0, "revised": 0, "unchanged": 0}
        try:
            _check_limits(run, timeout_seconds, max_memory_mb)
            prior = checkpoints[family]
            state = {}
            full_recompute = family not in append_inputs
            if family in append_inputs:
                input_bars, state, calculated, final_bar = append_inputs[family]
                if family == "calendar":
                    rows, state = calendar_append_rows(input_bars, prior["state"])
                    source = None
                else:
                    rows, source = calculated
                    if family == "volume" and not input_bars.empty:
                        cursor = pd.Timestamp(prior["last_bar_start_utc"])
                        old_session = str(_decode_tail(prior["state"])["session_id"].iloc[-1])
                        latest_session = str(input_bars["session_id"].iloc[-1])
                        delta_sessions = set(input_bars["session_id"].astype(str))
                        newly_closed = delta_sessions - {latest_session}
                        if latest_session != old_session and prior["state"].get("last_session_complete", True):
                            newly_closed.add(old_session)
                        rows = [row for row in rows if str(row.get("session_id")) in newly_closed and
                                (pd.Timestamp(row["observation_time"]) > cursor or
                                 str(row.get("session_id")) == old_session)]
            else:
                if full_bars is None:
                    full_bars = resampling.latest_bars(db, symbol, label)
                    if full_bars.empty:
                        raise ValueError(f"Não há barras derivadas de {symbol} {label}. Gere antes: "
                                         f"mt5x resample --symbols {symbol} --base M1 --tf {label} "
                                         "--session-aware")
                input_bars = full_bars
                rows, source = _family_rows(family, input_bars, params, volume_source)
                final_bar = input_bars["bar_start_utc"].max().strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")
                state = _family_checkpoint_state(family, input_bars, params, source)
            feature_ids = {row["feature_id"] for row in rows}
            feature_version = _feature_algorithm_version(family, params, source, label)
            register_definitions(db, family, feature_ids, params, label, source, feature_version)
            records = [{"symbol": symbol, "timeframe": label, "feature_id": row["feature_id"],
                        "algorithm_version": feature_version, "observation_time": row["observation_time"],
                        "value": row["value"], "available_at": row["available_at"],
                        "observed_at": row["available_at"], "source_revision": row["source_revision"],
                        "metadata": dict(row["metadata"], reconstructed=True)} for row in rows]
            invalidations = (_invalidated_records(db, symbol, label, family, params, source, feature_version, rows)
                            if full_recompute else [])
            records.extend(invalidations)
            resumable = bool(prior and prior["algorithm_version"] == ALGORITHM_VERSION and
                             prior["config_hash"] == config_hash and prior["source_sequence"] == source_sequence and
                             prior["status"] in {"RUNNING", "FAILED"} and prior["last_bar_start_utc"])
            if resumable:
                cursor = pd.Timestamp(prior["last_bar_start_utc"])
                records = [item for item in records if pd.Timestamp(item["observation_time"]) > cursor]
            pending_invalidations = sum(item.get("quality") == "INVALIDATED" for item in records)
            batches = list(_record_batches(records, int(batch_size)))
            for batch in batches:
                _check_limits(run, timeout_seconds, max_memory_mb)
                if write_guard is not None:
                    blocked = write_guard()
                    if blocked:
                        raise WriteBlocked(f"Gravação bloqueada pelo limite de recursos: {blocked}")
                cursor = pd.Timestamp(batch[-1]["observation_time"]).strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")
                hook = _checkpoint_hook(symbol=symbol, timeframe=label, family=family,
                                        algorithm_version=ALGORITHM_VERSION, config_hash=config_hash,
                                        source_sequence=source_sequence, last_bar_start=cursor, state=state,
                                        status="RUNNING")
                attempt = 0
                while True:
                    try:
                        counts = feature_store.save_values(db, batch, transaction_hook=hook)
                        break
                    except Exception:
                        if attempt >= max_retries:
                            raise
                        attempt += 1
                for key in total:
                    total[key] += counts[key]
            _check_limits(run, timeout_seconds, max_memory_mb)
            latest_source = resampling.latest_source_sequence(db, symbol, label)
            if latest_source is not None and int(latest_source) != source_sequence:
                raise RuntimeError("A fonte mudou durante o cálculo; execute novamente para incluir a nova revisão.")
            hook = _checkpoint_hook(symbol=symbol, timeframe=label, family=family,
                                    algorithm_version=ALGORITHM_VERSION, config_hash=config_hash,
                                    source_sequence=source_sequence, last_bar_start=final_bar, state=state,
                                    status="COMPLETE")
            if write_guard is not None:
                blocked = write_guard()
                if blocked:
                    raise WriteBlocked(f"Gravação bloqueada pelo limite de recursos: {blocked}")
            # O checkpoint final e o sucesso do lote vazio são confirmados na mesma transação.
            feature_store.save_values(db, [], transaction_hook=hook)
            measurement = producer_jobs.finish_run(db, run, source_sequence=source_sequence, status="COMPLETE",
                                                   counts=total)
            item = {"family": family, "symbol": symbol, "timeframe": label, "features": len(feature_ids),
                    "algorithm_version": feature_version, "values": len(records),
                    "invalidations": pending_invalidations, "volume_source": source,
                    **total, "job": measurement}
            summary.append(item)
            log.info("%s %s %s: %s valores (%s novos, %s revisados, %s inalterados)", symbol, label, family,
                     len(records), total["inserted"], total["revised"], total["unchanged"])
        except Exception as exc:
            producer_jobs.finish_run(db, run, source_sequence=source_sequence, status="FAILED", counts=total,
                                     error=exc)
            raise
    by_family = {item["family"]: item for item in summary}
    by_family.update(skipped)
    return [by_family[family] for family in families]


def recalculators(db, symbol, timeframe, **kwargs):
    """Handlers para ``features.recalculate_affected``; cada família é recalculada uma vez por chamada."""
    done = {}

    def handler(family):
        def run(db_, definition):
            if family not in done:
                done[family] = compute(db_, symbol, timeframe, families=[family], **kwargs)
            return done[family]
        return run

    return {item["feature_id"]: handler(item["parameters"]["family"]) for item in features.list_features(db)
            if item["parameters"].get("family") in FAMILIES}


def recalculate_affected(db, symbol, timeframe, changed_dependencies, **kwargs):
    """Recalcula o contexto solicitado no registro canônico, com mapa novo por execução.

    Versões de outro timeframe/configuração permanecem consultáveis e não são
    selecionadas. O registro sem filtro mantém a seleção global atual por ID.
    """
    label = resampling.parse_target(timeframe).label
    bars = resampling.latest_bars(db, symbol, label)
    if bars.empty:
        raise ValueError(f"Não há barras derivadas de {symbol} {label}. Gere antes com mt5x resample.")
    _, volume_source = volume_profiles.resolve_source(bars, kwargs.get("volume_source", "auto"))
    versions = {}
    for family in FAMILIES:
        params = dict(DEFAULTS[family], **(kwargs.get("overrides") or {}).get(family, {}))
        source = volume_source if family in {"volume", "rvol"} else None
        versions[family] = _feature_algorithm_version(family, params, source, label)

    def in_context(definition):
        family = definition["parameters"].get("family")
        return (f"bars_derived:{label}" in definition["dependencies"]
                and definition["algorithm_version"] == versions.get(family))

    return features.recalculate_affected(db, changed_dependencies,
                                        recalculators(db, symbol, label, **kwargs),
                                        definition_filter=in_context)
