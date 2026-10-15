"""Contratos versionados e fórmulas puras dos indicadores usados em relações.

As fórmulas reutilizam ``strategies.indicators``. MACD é definido aqui com EMA
pura para que a disponibilidade de ``pandas-ta`` não altere a pesquisa. Valores
ausentes são devolvidos como ``None`` com estado e motivo; volume real e ticks
permanecem séries distintas.
"""
from copy import deepcopy

import numpy as np
import pandas as pd

from mt5_extracao.strategies import indicators as technical


ALGORITHM_VERSION = "1.0"
_BAR_AVAILABLE = "closed_bar_received; late revisions become available when received"


def _definition(feature_id, name, family, formula, parameters, unit, warmup_count, source_id, required_columns,
                *, denominator=None):
    return {
        "feature_id": feature_id,
        "name": name,
        "family": family,
        "algorithm_version": ALGORITHM_VERSION,
        "formula": formula,
        "parameters": parameters,
        "unit": unit,
        "warmup_count": warmup_count,
        "warmup_unit": "bars",
        "source_id": source_id,
        "timeframe_policy": "same_as_source",
        "availability_rule": _BAR_AVAILABLE,
        "value_type": "NUMBER",
        "required_columns": required_columns,
        "denominator": denominator,
    }


_DEFINITIONS = (
    _definition("momentum.rsi_14", "RSI (14)", "momentum", "Wilder RSI(14) applied to close",
                {"period": 14, "method": "Wilder", "source": "close"}, "points_0_100", 15, "bars.close",
                ("close",)),
    _definition("momentum.rsi_14_slope_3", "RSI slope (3)", "momentum", "RSI(14)[t] - RSI(14)[t-3]",
                {"period": 14, "slope_bars": 3}, "points_per_3_bars", 18, "bars.close", ("close",)),
    _definition("volatility.atr_14", "ATR (14)", "volatility", "Wilder RMA(14) of true range",
                {"period": 14, "method": "Wilder"}, "price_units", 14, "bars.high_low_close",
                ("high", "low", "close")),
    _definition("volatility.atr_pct_14", "ATR / close (14)", "volatility", "ATR(14) / close",
                {"period": 14}, "fraction", 14, "bars.high_low_close", ("high", "low", "close"),
                denominator="close"),
    _definition("volatility.bb_20_width", "Bollinger width (20, 2)", "volatility",
                "(upper - lower) / mid; population standard deviation",
                {"period": 20, "deviations": 2.0, "std_ddof": 0}, "fraction", 20, "bars.close", ("close",),
                denominator="mid"),
    _definition("volatility.bb_20_position", "Bollinger position (20, 2)", "volatility",
                "(close - lower) / (upper - lower)", {"period": 20, "deviations": 2.0}, "channel_fraction", 20,
                "bars.close", ("close",), denominator="band_width"),
    _definition("volatility.kc_20_width", "Keltner width (20, 1.5)", "volatility",
                "(upper - lower) / mid; EMA(20) center and 1.5 x ATR(20)",
                {"period": 20, "atr_period": 20, "multiplier": 1.5}, "fraction", 20,
                "bars.high_low_close", ("high", "low", "close"), denominator="mid"),
    _definition("volatility.kc_20_position", "Keltner position (20, 1.5)", "volatility",
                "(close - lower) / (upper - lower); EMA(20) center and 1.5 x ATR(20)",
                {"period": 20, "atr_period": 20, "multiplier": 1.5}, "channel_fraction", 20,
                "bars.high_low_close", ("high", "low", "close"), denominator="channel_width"),
    _definition("momentum.macd_line_12_26_9", "MACD line (12, 26, 9)", "momentum",
                "EMA(close, 12) - EMA(close, 26); adjust=false; min_periods=period",
                {"fast": 12, "slow": 26, "signal": 9}, "price_units", 26, "bars.close", ("close",)),
    _definition("momentum.macd_signal_12_26_9", "MACD signal (12, 26, 9)", "momentum",
                "EMA(MACD line, 9); adjust=false; min_periods=9",
                {"fast": 12, "slow": 26, "signal": 9}, "price_units", 34, "bars.close", ("close",)),
    _definition("momentum.macd_histogram_12_26_9", "MACD histogram (12, 26, 9)", "momentum",
                "MACD line - MACD signal", {"fast": 12, "slow": 26, "signal": 9}, "price_units", 34,
                "bars.close", ("close",)),
    _definition("momentum.macd_histogram_atr_14", "MACD histogram / ATR (14)", "momentum",
                "MACD histogram(12, 26, 9) / ATR(14)",
                {"fast": 12, "slow": 26, "signal": 9, "atr_period": 14}, "atr_units", 34,
                "bars.high_low_close", ("high", "low", "close"), denominator="atr"),
    _definition("price.return_1", "Return (1 bar)", "price", "close[t] / close[t-1] - 1",
                {"horizon_bars": 1, "price_basis": "close"}, "fraction", 2, "bars.close", ("close",),
                denominator="previous_close"),
    _definition("price.return_5", "Return (5 bars)", "price", "close[t] / close[t-5] - 1",
                {"horizon_bars": 5, "price_basis": "close"}, "fraction", 6, "bars.close", ("close",),
                denominator="previous_close"),
    _definition("price.return_20", "Return (20 bars)", "price", "close[t] / close[t-20] - 1",
                {"horizon_bars": 20, "price_basis": "close"}, "fraction", 21, "bars.close", ("close",),
                denominator="previous_close"),
    _definition("trend.ema20_distance_atr14", "EMA (20) distance / ATR (14)", "trend",
                "(close - EMA(close, 20)) / ATR(14)", {"ema_period": 20, "atr_period": 14}, "atr_units", 20,
                "bars.high_low_close", ("high", "low", "close"), denominator="atr"),
    _definition("volume.real", "Real volume", "volume", "real_volume from source bar; never replaced by ticks",
                {"column": "real_volume", "source_kind": "real"}, "volume_units:instrument", 0,
                "bars.real_volume", ("real_volume",)),
    _definition("volume.ticks", "Tick volume", "volume", "tick_volume from source bar; never mixed with real volume",
                {"column": "tick_volume", "source_kind": "ticks"}, "ticks", 0,
                "bars.tick_volume", ("tick_volume",)),
)
_BY_ID = {item["feature_id"]: item for item in _DEFINITIONS}


def definitions():
    """Retorna cópias das definições nativas; alterações do chamador não mudam o registry."""
    return [deepcopy(item) for item in _DEFINITIONS]


def get_definition(feature_id):
    try:
        return deepcopy(_BY_ID[feature_id])
    except KeyError as exc:
        raise ValueError(f"Indicador nativo desconhecido: {feature_id}.") from exc


def macd(close, fast=12, slow=26, signal=9):
    """MACD determinístico em pandas/numpy; não consulta nem depende de pandas-ta."""
    if any(isinstance(value, bool) or not isinstance(value, int) or value < 1 for value in (fast, slow, signal)):
        raise ValueError("fast, slow e signal precisam ser inteiros positivos.")
    if fast >= slow:
        raise ValueError("fast precisa ser menor que slow.")
    fast_line = technical.ema(pd.to_numeric(close, errors="coerce"), fast)
    slow_line = technical.ema(pd.to_numeric(close, errors="coerce"), slow)
    line = fast_line - slow_line
    signal_line = technical.ema(line, signal)
    histogram = line - signal_line
    return pd.DataFrame({"line": line, "signal": signal_line, "histogram": histogram}, index=close.index)


def _column(bars, name):
    if name not in bars.columns:
        return None
    return pd.to_numeric(bars[name], errors="coerce").astype(float)


def _calculated_series(bars):
    close = _column(bars, "close")
    high, low = _column(bars, "high"), _column(bars, "low")
    if close is None:
        close = pd.Series(np.nan, index=bars.index, dtype=float)
    if high is None:
        high = pd.Series(np.nan, index=bars.index, dtype=float)
    if low is None:
        low = pd.Series(np.nan, index=bars.index, dtype=float)
    ohlc = pd.DataFrame({"high": high, "low": low, "close": close}, index=bars.index)
    atr14 = technical.atr(ohlc, 14)
    atr20 = technical.atr(ohlc, 20)
    rsi14 = technical.rsi(close, 14)
    bb = technical.bollinger(close, 20, 2.0)
    kc = technical.keltner(ohlc, 20, 1.5)
    macd_frame = macd(close)
    macd_hist = macd_frame["histogram"]
    bb_width = bb["upper"] - bb["lower"]
    kc_width = kc["upper"] - kc["lower"]
    return {
        "momentum.rsi_14": rsi14,
        "momentum.rsi_14_slope_3": rsi14 - rsi14.shift(3),
        "volatility.atr_14": atr14,
        "volatility.atr_pct_14": atr14 / close.where(close > 0),
        "volatility.bb_20_width": bb_width / bb["mid"].where(bb["mid"] > 0),
        "volatility.bb_20_position": (close - bb["lower"]) / bb_width.where(bb_width > 0),
        "volatility.kc_20_width": kc_width / kc["mid"].where(kc["mid"] > 0),
        "volatility.kc_20_position": (close - kc["lower"]) / kc_width.where(kc_width > 0),
        "momentum.macd_line_12_26_9": macd_frame["line"],
        "momentum.macd_signal_12_26_9": macd_frame["signal"],
        "momentum.macd_histogram_12_26_9": macd_hist,
        "momentum.macd_histogram_atr_14": macd_hist / atr14.where(atr14 > 0),
        "price.return_1": close / close.shift(1).where(close.shift(1) > 0) - 1.0,
        "price.return_5": close / close.shift(5).where(close.shift(5) > 0) - 1.0,
        "price.return_20": close / close.shift(20).where(close.shift(20) > 0) - 1.0,
        "trend.ema20_distance_atr14": (close - technical.ema(close, 20)) / atr14.where(atr14 > 0),
        "volume.real": _column(bars, "real_volume"),
        "volume.ticks": _column(bars, "tick_volume"),
    }


def _bad_denominator(feature_id, position, bars, close, atr14, bb, kc):
    if feature_id in {"volatility.atr_pct_14", "price.return_1", "price.return_5", "price.return_20"}:
        denominator = close.iloc[position] if feature_id == "volatility.atr_pct_14" else None
        if feature_id.startswith("price.return_"):
            horizon = int(feature_id.rsplit("_", 1)[1])
            denominator = close.iloc[position - horizon] if position >= horizon else None
        return denominator is not None and (not np.isfinite(denominator) or denominator <= 0)
    if feature_id in {"momentum.macd_histogram_atr_14", "trend.ema20_distance_atr14"}:
        denominator = atr14.iloc[position]
        return np.isfinite(denominator) and denominator <= 0
    if feature_id == "volatility.bb_20_width":
        denominator = bb["mid"].iloc[position]
        return np.isfinite(denominator) and denominator <= 0
    if feature_id == "volatility.bb_20_position":
        denominator = (bb["upper"] - bb["lower"]).iloc[position]
        return np.isfinite(denominator) and denominator <= 0
    if feature_id == "volatility.kc_20_width":
        denominator = kc["mid"].iloc[position]
        return np.isfinite(denominator) and denominator <= 0
    if feature_id == "volatility.kc_20_position":
        denominator = (kc["upper"] - kc["lower"]).iloc[position]
        return np.isfinite(denominator) and denominator <= 0
    return False


def compute_core(bars, *, feature_ids=None):
    """Calcula indicadores nativos e devolve linhas tipadas com ausência explícita.

    ``bar_index`` preserva o índice da série. A função não infere timestamp de
    disponibilidade: a persistência deve receber ``available_at`` do produtor,
    conforme o contrato point-in-time do feature store.
    """
    if not isinstance(bars, pd.DataFrame):
        raise ValueError("bars precisa ser um DataFrame.")
    selected = [item["feature_id"] for item in _DEFINITIONS] if feature_ids is None else list(feature_ids)
    unknown = sorted(set(selected) - set(_BY_ID))
    if unknown:
        raise ValueError("Indicadores nativos desconhecidos: " + ", ".join(unknown))
    series_by_id = _calculated_series(bars)
    close = _column(bars, "close")
    if close is None:
        close = pd.Series(np.nan, index=bars.index, dtype=float)
    if all(column in bars.columns for column in ("high", "low", "close")):
        ohlc = pd.DataFrame({name: _column(bars, name) for name in ("high", "low", "close")}, index=bars.index)
        atr14 = technical.atr(ohlc, 14)
        bb = technical.bollinger(close, 20, 2.0)
        kc = technical.keltner(ohlc, 20, 1.5)
    else:
        atr14 = pd.Series(np.nan, index=bars.index)
        bb = pd.DataFrame({"mid": np.nan, "upper": np.nan, "lower": np.nan}, index=bars.index)
        kc = pd.DataFrame({"mid": np.nan, "upper": np.nan, "lower": np.nan}, index=bars.index)

    rows = []
    for feature_id in selected:
        spec = _BY_ID[feature_id]
        missing_columns = [column for column in spec["required_columns"] if column not in bars.columns]
        values = series_by_id[feature_id]
        for position, bar_index in enumerate(bars.index):
            raw = values.iloc[position] if values is not None else np.nan
            reason = None
            state = "AVAILABLE"
            value = None
            if missing_columns:
                state, reason = "SOURCE_UNAVAILABLE", "MISSING_COLUMNS:" + ",".join(missing_columns)
            elif position + 1 < spec["warmup_count"]:
                state, reason = "WARMUP", f"REQUIRES_{spec['warmup_count']}_{spec['warmup_unit'].upper()}"
            elif any(pd.isna(bars[column].iloc[position]) for column in spec["required_columns"]):
                state, reason = "SOURCE_VALUE_MISSING", "REQUIRED_SOURCE_VALUE_IS_NULL"
            elif _bad_denominator(feature_id, position, bars, close, atr14, bb, kc):
                state, reason = "INVALID_DENOMINATOR", "DENOMINATOR_MUST_BE_POSITIVE"
            elif raw is None or pd.isna(raw):
                state, reason = "DEPENDENCY_UNAVAILABLE", "INPUT_WINDOW_OR_DEPENDENCY_IS_INCOMPLETE"
            elif not np.isfinite(float(raw)):
                state, reason = "INVALID_RESULT", "FORMULA_RETURNED_NON_FINITE_VALUE"
            else:
                value = float(raw)
            rows.append({"bar_index": bar_index, "feature_id": feature_id,
                         "algorithm_version": spec["algorithm_version"], "value": value, "state": state,
                         "reason": reason})
    return rows


def register_core(db, timeframe, *, write_guard=None):
    """Registra as definições nativas para um timeframe no feature store existente."""
    timeframe = str(timeframe).strip()
    if not timeframe:
        raise ValueError("timeframe precisa ser informado.")
    from mt5_extracao.analytics import features

    registered = 0
    for spec in _DEFINITIONS:
        if write_guard is not None:
            blocked = write_guard()
            if blocked:
                raise RuntimeError(f"Registro bloqueado pelo limite de recursos: {blocked}")
        registered += bool(features.register(
            db, spec["feature_id"], spec["name"], spec["algorithm_version"],
            parameters=deepcopy(spec["parameters"]), dependencies=["bars_derived:{timeframe}"],
            family=spec["family"], formula=spec["formula"], unit=spec["unit"],
            warmup_count=spec["warmup_count"], warmup_unit=spec["warmup_unit"],
            source_id=f"bars_derived:{{timeframe}}:{spec['source_id'].split('.', 1)[1]}",
            timeframe="same_as_source",
            availability_rule=spec["availability_rule"], value_type=spec["value_type"],
        ))
    return {"registered": registered, "total": len(_DEFINITIONS), "timeframe": timeframe}


def producer_contract(feature_id, family, parameters, timeframe, volume_source=None):
    """Descreve os valores T13.3 sem recalcular nem trocar sua fonte/versionamento."""
    timeframe = str(timeframe).strip()
    if not timeframe:
        raise ValueError("timeframe precisa ser informado.")
    params = dict(parameters or {})
    prefix = str(feature_id)
    value_type, unit, warmup_count, warmup_unit = "NUMBER", "source_units", 0, "bars"
    source_id, availability_rule = "bars_derived:{timeframe}", _BAR_AVAILABLE
    formula = "Feature calculada pelo produtor T13.3; parâmetros e versão registrados em parameters."
    if family == "volatility":
        count = {"volatility.atr": params.get("atr_n", 14),
                 "volatility.atr_pct": params.get("atr_n", 14),
                 "volatility.realized_vol": params.get("rv_n", 20) + 1,
                 "volatility.range_pct": 1,
                 "volatility.bb_width": params.get("bb_n", 20),
                 "volatility.atr_pct_percentile": params.get("atr_n", 14) + params.get("percentile_window", 100),
                 "volatility.bb_width_percentile": params.get("bb_n", 20) + params.get("percentile_window", 100),
                 "volatility.range_percentile": params.get("percentile_window", 100) + 1}
        formula = {
            "volatility.atr": f"Wilder RMA({params.get('atr_n', 14)}) of true range",
            "volatility.atr_pct": f"ATR({params.get('atr_n', 14)}) / close",
            "volatility.realized_vol": f"rolling std of log returns, window={params.get('rv_n', 20)}",
            "volatility.range_pct": "(high - low) / close",
            "volatility.bb_width": f"Bollinger width / mid; period={params.get('bb_n', 20)}, k=2",
            "volatility.atr_pct_percentile": "causal percentile versus prior ATR% observations",
            "volatility.bb_width_percentile": "causal percentile versus prior Bollinger widths",
            "volatility.range_percentile": "causal percentile versus prior range percentages",
            "volatility.volatility_state": "COMPRESSION/NORMAL/EXPANSION from prior-window percentiles",
        }.get(prefix, formula)
        unit = ("category" if prefix.endswith("_state") else
                "percentile_0_100" if "percentile" in prefix else
                "fraction" if prefix.endswith(("_pct", "_width", "_vol")) else "price_units")
        if prefix.endswith("_state"):
            value_type = "CATEGORY"
        warmup_count = count.get(prefix, max(params.get("percentile_window", 100), params.get("bb_n", 20)))
    elif family == "regime":
        value_type = "CATEGORY" if prefix in {"regime.state", "regime.raw"} else "NUMBER"
        unit = {
            "regime.state": "category", "regime.raw": "category", "regime.confidence": "fraction",
            "regime.adx": "points_0_100", "regime.efficiency": "fraction", "regime.r2": "fraction",
            "regime.duration": "bars",
        }.get(prefix, "source_units")
        warmup_count = max(params.get("adx_n", 14) * 2, params.get("er_n", 20), params.get("r2_n", 50))
        if prefix in {"regime.state", "regime.duration"}:
            warmup_count += max(0, params.get("min_duration", 3) - 1)
        formula = {
            "regime.state": "confirmed ADX/efficiency/R2 regime with minimum duration; warmup=UNCERTAIN",
            "regime.raw": "ADX thresholds: TREND/RANGE/TRANSITION/UNCERTAIN",
            "regime.duration": "duration of the confirmed current regime state",
            "regime.confidence": "fraction of ADX, efficiency and R2 votes in agreement; not probability",
            "regime.adx": f"ADX({params.get('adx_n', 14)}), two Wilder smoothings",
            "regime.efficiency": f"Kaufman efficiency ratio({params.get('er_n', 20)})",
            "regime.r2": f"rolling linear-regression R2({params.get('r2_n', 50)})",
        }.get(prefix, formula)
    elif family in {"volume", "rvol"}:
        source = next((item for item in ("real", "tick", "ticks") if prefix.endswith("." + item)), volume_source)
        if source not in {"real", "tick", "ticks"}:
            raise ValueError(f"Fonte de volume explícita ausente para {feature_id}.")
        source_id = f"bars_derived:{{timeframe}}:{'real_volume' if source == 'real' else 'tick_volume'}"
        volume_unit = "volume_units:instrument/minute" if source == "real" else "ticks/minute"
        if family == "rvol":
            unit = "count_sessions" if prefix.endswith("reference_sessions") else "ratio"
            value_type = "NUMBER"
            warmup_count, warmup_unit = params.get("min_sessions", 5), "sessions"
            availability_rule = "closed_bar_received_after_prior_session_reference_is_mature"
            formula = "same-minute-of-session cumulative volume / prior-session median; source is fixed"
        else:
            unit = volume_unit
            warmup_count = params.get("min_sessions", 5)
            warmup_unit = "sessions"
            availability_rule = "completed_session_received_after_source_revisions"
            if "percentile" in prefix:
                unit = "percentile_0_100"
                formula = "volume-per-minute percentile versus prior complete sessions only"
            elif "ratio_median" in prefix:
                unit = "ratio"
                formula = "session volume-per-minute / prior complete-session median"
            elif "slope" in prefix:
                unit = "log_change_per_session"
                formula = "Theil-Sen slope of log volume-per-minute over prior sessions"
            elif "median" in prefix:
                formula = "median volume-per-minute over prior complete sessions only"
            else:
                formula = "session volume divided by observed session minutes"
    elif family == "calendar":
        source_id = "market_calendar:observed_utc"
        unit = {"calendar.weekday_utc": "weekday_0_6", "calendar.week_of_month_utc": "week_1_5",
                "calendar.hour_utc": "hour_0_23", "calendar.minute_of_session": "minutes"}.get(prefix, "category")
        warmup_count = 0
        availability_rule = "UTC_timestamp_available_at_bar_open; session_offset_requires_observed_open"
        formula = "UTC calendar fields from observed source timestamps; no future outcomes"
    elif family == "structure":
        value_type = "NUMBER"
        unit = ("atr_units" if "_atr" in prefix else
                "direction_code_-1_0_1" if prefix.startswith("opening_range.breakout_") else
                "price_units" if prefix.startswith("opening_range") else "fraction")
        if prefix.startswith("opening_range"):
            warmup_count, warmup_unit = params.get("opening_minutes", 30), "minutes"
        elif prefix.startswith("level.dist_prev_"):
            warmup_count, warmup_unit = params.get("atr_n", 14) + 1, "bars"
        else:
            warmup_count, warmup_unit = 2, "sessions"
        availability_rule = "event_or_level_available_at_declared_source_bar_close"
        formula = {
            "session.return": "complete current-session close / complete prior-session close - 1",
            "session.gap": "current-session open / complete prior-session close - 1",
            "level.dist_prev_high_atr": "(current close - prior-session high) / prior-bar ATR",
            "level.dist_prev_low_atr": "(current close - prior-session low) / prior-bar ATR",
        }.get(prefix, "opening range or first post-range break; 0 marks an ambiguous same-bar break")
    else:
        raise ValueError(f"Família sem contrato T18.1: {family}.")
    return {"family": family, "formula": formula, "unit": unit, "warmup_count": int(warmup_count),
            "warmup_unit": warmup_unit, "source_id": source_id, "timeframe": "same_as_source",
            "availability_rule": availability_rule, "value_type": value_type}
