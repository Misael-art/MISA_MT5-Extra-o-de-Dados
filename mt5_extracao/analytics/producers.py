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
  ``computed_at_utc`` da correção. Indicadores de Wilder usam uma janela truncada: após 5 períodos,
  ATR retém no máximo 0,7% do efeito de uma perturbação inicial; ADX usa duas suavizações e após
  10 períodos retém menos de 0,1%. O fingerprint registra as revisões de todas as barras usadas;
- a última sessão da base é tratada como possivelmente aberta: valores de sessão dela não são gravados.

A escrita respeita o ``write_guard`` da Fase 15 quando ele é passado (limite de disco/base).
"""
import hashlib
import json
import logging
import re
from collections import deque

import numpy as np
import pandas as pd

from mt5_extracao.analytics import (feature_store, features, market_regimes, resampling, session_structure,
                                    volume_profiles)

log = logging.getLogger(__name__)

ALGORITHM_VERSION = "1.1"
FAMILIES = ("volume", "rvol", "volatility", "regime", "structure", "calendar")
WILDER_MEMORY = 5
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


def _wilder_residual_fraction(period, memory_periods, layers=1):
    """Limite do efeito relativo a uma perturbação inicial após a memória truncada."""
    period = int(period)
    steps = int(np.ceil(period * memory_periods))
    q = (period - 1.0) / period
    if q == 0:
        return 0.0
    if layers == 1:
        return q ** steps
    # Limite para duas suavizações de Wilder em cascata, com ambos os estados
    # inicialmente perturbados pelo mesmo máximo.
    return q ** steps * (1.0 + steps / (period - 1.0))


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


def _prefix_source_revisions(tokens):
    state = hashlib.sha256()
    revisions = []
    for token in tokens:
        state.update(token)
        revisions.append(f"bars:{state.copy().hexdigest()[:24]}")
    return revisions


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

    opens = bars.groupby("session_id")["bar_start_utc"].transform("min")
    time_features = calendar_columns(bars["bar_start_utc"])
    session_indices = {sid: list(indexes) for sid, indexes in bars.groupby("session_id", sort=False).groups.items()}
    tokens = _source_tokens(bars)
    corrected = bars["computed_at_utc"].where(bars["revision"].astype(int) > 1)
    session_fingerprints = {sid: _source_revision(tokens, indexes) for sid, indexes in session_indices.items()}
    session_corrections = {sid: corrected.iloc[indexes].dropna().max() for sid, indexes in session_indices.items()}
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
        session_fix = session_corrections[sid]
        available = max(start, session_fix) if pd.notna(session_fix) else start
        rows.append({"observation_time": start, "available_at": available,
                     "feature_id": "calendar.minute_of_session",
                     "value": float((start - opens.iloc[k]).total_seconds() // 60),
                     "source_revision": session_fingerprints[sid],
                     "metadata": {"clock": "UTC", "source_bars": len(session_indices[sid])}})
    return rows


def _family_rows(family, bars, params, volume_source):
    if family == "volatility":
        frame = market_regimes.volatility_frame(bars, **params)
        columns = {name: f"volatility.{name}" for name in
                   ("atr", "atr_pct", "realized_vol", "range_pct", "bb_width", "atr_pct_percentile",
                    "bb_width_percentile", "range_percentile", "volatility_state")}
        window = _window(family, params)
        return _bar_rows(bars, frame, columns, _correction_times(bars, window), window), None
    if family == "regime":
        frame = market_regimes.regime_frame(bars, **params)
        columns = {"regime": "regime.state", "regime_raw": "regime.raw", "regime_duration": "regime.duration",
                   "regime_confidence": "regime.confidence", "adx": "regime.adx", "efficiency": "regime.efficiency",
                   "r2": "regime.r2"}
        window = _window(family, params)
        recent_corrections = _correction_times(bars, window)
        corrections = {column: recent_corrections for column in columns}
        corrections["regime"] = _correction_times(bars, cumulative=True)
        corrections["regime_duration"] = _correction_times(bars, cumulative=True)
        return _bar_rows(bars, frame, columns, corrections, window,
                         cumulative_columns=("regime", "regime_duration")), None
    if family == "volume":
        frame, source = volume_profiles.session_volume_features(bars, volume_source=volume_source, **params)
        return _long_rows(frame, bars, family, params), source
    if family == "rvol":
        frame, source = volume_profiles.intraday_rvol(bars, volume_source=volume_source, **params)
        return _long_rows(frame, bars, family, params), source
    if family == "structure":
        frame = session_structure.structure_features(bars, **params)
        return _long_rows(frame, bars, family, params), None
    if family == "calendar":
        return calendar_rows(bars), None
    raise ValueError(f"Família desconhecida: {family}. Use: {', '.join(FAMILIES)}.")


def register_definitions(db, family, feature_ids, params, timeframe, volume_source=None):
    dependencies = [f"bars_derived:{timeframe}", "market_calendar"]
    for feature_id in sorted(feature_ids):
        parameters = dict(params, family=family, confidence=CONFIDENCE[family])
        if volume_source:
            parameters["volume_source"] = volume_source
        features.register(db, feature_id, feature_id.replace(".", " "), ALGORITHM_VERSION, parameters=parameters,
                          dependencies=dependencies)


def compute(db, symbol, timeframe, *, families=None, volume_source="auto", overrides=None, write_guard=None):
    """Calcula as famílias pedidas para ``symbol``/``timeframe`` e grava; devolve um resumo por família."""
    families = list(families or FAMILIES)
    unknown = [name for name in families if name not in FAMILIES]
    if unknown:
        raise ValueError(f"Família desconhecida: {', '.join(unknown)}. Use: {', '.join(FAMILIES)}.")
    bars = resampling.latest_bars(db, symbol, timeframe)
    if bars.empty:
        raise ValueError(f"Não há barras derivadas de {symbol} {timeframe}. Gere antes: "
                         f"mt5x resample --symbols {symbol} --base M1 --tf {timeframe} --session-aware")
    label = resampling.parse_target(timeframe).label
    summary = []
    for family in families:
        params = dict(DEFAULTS[family], **(overrides or {}).get(family, {}))
        rows, source = _family_rows(family, bars, params, volume_source)
        feature_ids = {row["feature_id"] for row in rows}
        if write_guard is not None:
            blocked = write_guard()
            if blocked:
                raise WriteBlocked(f"Gravação bloqueada pelo limite de recursos: {blocked}")
        register_definitions(db, family, feature_ids, params, label, source)
        records = [{"symbol": symbol, "timeframe": label, "feature_id": row["feature_id"],
                    "algorithm_version": ALGORITHM_VERSION, "observation_time": row["observation_time"],
                    "value": row["value"], "available_at": row["available_at"], "observed_at": row["available_at"],
                    "source_revision": row["source_revision"],
                    "metadata": dict(row["metadata"], reconstructed=True)}
                   for row in rows]
        counts = feature_store.save_values(db, records)
        summary.append({"family": family, "symbol": symbol, "timeframe": label, "features": len(feature_ids),
                        "values": len(records), "volume_source": source, **counts})
        log.info("%s %s %s: %s valores (%s novos, %s revisados, %s inalterados)", symbol, label, family,
                 len(records), counts["inserted"], counts["revised"], counts["unchanged"])
    return summary


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
