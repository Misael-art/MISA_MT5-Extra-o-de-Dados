"""Construtores adicionais da T13.4, com entradas e horizontes causalmente definidos."""
import numpy as np
import pandas as pd

from mt5_extracao.analytics import calendar, market_regimes
from mt5_extracao.analytics.pattern_engine import _session_context, _trade_return


def _sessions(bars, symbol, definition, offset, *, regime=False):
    _data, sessions, source = _session_context(
        bars, symbol, definition, offset, include_regime=regime)
    # A última janela observada pode estar aberta e não fornece resultado.
    return sessions[:-1], source


def _direction(group, index, lookback=5):
    lookback = int(lookback)
    if index < lookback or lookback < 1:
        return 0
    start = index - lookback
    window = group.iloc[start:index + 1]
    if not _continuous_path(window, start=window["time"].iloc[0], end=window["_bar_end"].iloc[-1]):
        return 0
    first = float(group["close"].iloc[start])
    last = float(group["close"].iloc[index])
    return 1 if last > first else -1 if last < first else 0


def _atr(group, period=14):
    high, low, close = (pd.to_numeric(group[name], errors="coerce") for name in ("high", "low", "close"))
    prior = close.shift(1)
    true_range = pd.concat([high - low, (high - prior).abs(), (low - prior).abs()], axis=1).max(axis=1)
    return true_range.rolling(int(period), min_periods=int(period)).mean()


def _continuous_path(path, *, start=None, end=None):
    """Exige barras M1 completas, sem lacunas, entre os limites declarados."""
    if path.empty:
        return False
    times = pd.to_datetime(path["time"], errors="coerce")
    ends = pd.to_datetime(path["_bar_end"], errors="coerce")
    if times.isna().any() or ends.isna().any() or times.diff().dropna().ne(pd.Timedelta(minutes=1)).any():
        return False
    return (start is None or pd.Timestamp(times.iloc[0]) == pd.Timestamp(start)) and \
        (end is None or pd.Timestamp(ends.iloc[-1]) == pd.Timestamp(end))


def _event_timestamp(value, field):
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        raise ValueError(f"signal/trade event.{field} precisa conter horário válido.")
    if timestamp.tzinfo is not None:
        raise ValueError(f"signal/trade event.{field} precisa estar no horário da corretora, sem timezone.")
    return pd.Timestamp(timestamp)


def _base_row(group, signal_index, direction, definition, point, horizon_minutes, *, entry_index=None,
              observation_time=None):
    entry_index = signal_index + 1 if entry_index is None else int(entry_index)
    if entry_index < 0 or entry_index >= len(group):
        return None
    entry_bar = group.iloc[entry_index]
    if entry_index == signal_index + 1 and \
            pd.Timestamp(entry_bar["time"]) != pd.Timestamp(group["_bar_end"].iloc[signal_index]):
        return None
    target_end = pd.Timestamp(entry_bar["time"]) + pd.Timedelta(minutes=int(horizon_minutes))
    exits = group[(group["_bar_end"] <= target_end) & (group["time"] >= entry_bar["time"])]
    if len(exits) != int(horizon_minutes) or not _continuous_path(
            exits, start=entry_bar["time"], end=target_end):
        return None
    exit_bar = exits.iloc[-1]
    trade = _trade_return(entry_bar, exit_bar, direction, definition, point)
    return {"observation_time": pd.Timestamp(observation_time) if observation_time is not None
            else pd.Timestamp(group["_bar_end"].iloc[signal_index]),
            "entry_time": pd.Timestamp(entry_bar["time"]), "entry_price": float(entry_bar["open"]),
            "horizon_end": target_end,
            "gross_return": trade["gross_return"], "outcome_return": trade["outcome_return"],
            "cost_bps": trade["cost_bps"], "cost_observed": trade["cost_observed"],
            "target_success": bool(trade["outcome_return"] > 0), "trade_executed": True,
            "signal_direction": int(direction), "exit_price": float(exit_bar["close"]),
            "outcome_field": "fixed_horizon_net_return"}


def _session_close_row(group, signal_index, direction, definition, point, *, observation_time=None):
    entry_index = signal_index + 1
    if entry_index >= len(group):
        return None
    entry_bar, exit_bar = group.iloc[entry_index], group.iloc[-1]
    if pd.Timestamp(entry_bar["time"]) != pd.Timestamp(group["_bar_end"].iloc[signal_index]):
        return None
    if pd.Timestamp(entry_bar["time"]) >= pd.Timestamp(exit_bar["_bar_end"]):
        return None
    path = group.iloc[entry_index:]
    if not _continuous_path(path, start=entry_bar["time"], end=exit_bar["_bar_end"]):
        return None
    trade = _trade_return(entry_bar, exit_bar, direction, definition, point)
    return {"observation_time": pd.Timestamp(observation_time) if observation_time is not None
            else pd.Timestamp(group["_bar_end"].iloc[signal_index]),
            "entry_time": pd.Timestamp(entry_bar["time"]), "entry_price": float(entry_bar["open"]),
            "horizon_end": pd.Timestamp(exit_bar["_bar_end"]),
            "gross_return": trade["gross_return"], "outcome_return": trade["outcome_return"],
            "cost_bps": trade["cost_bps"], "cost_observed": trade["cost_observed"],
            "target_success": bool(trade["outcome_return"] > 0), "trade_executed": True,
            "signal_direction": int(direction), "exit_price": float(exit_bar["close"]),
            "outcome_field": "observed_session_close"}


def _minute_from_open(group, index):
    return int((pd.Timestamp(group["time"].iloc[index]) - pd.Timestamp(group["time"].iloc[0])).total_seconds() // 60)


def _build_h06(sessions, definition, point):
    minutes = int(definition["parameters"].get("higher_timeframe_minutes", 15))
    lookback = int(definition["parameters"].get("direction_lookback_bars", 3))
    higher_rows = []
    for session in sessions:
        group = session["bars"]
        if group.empty:
            continue
        bucket = group["time"].dt.floor(f"{minutes}min")
        for start, part in group.groupby(bucket, sort=True):
            start = pd.Timestamp(start)
            expected_times = pd.date_range(start, periods=minutes, freq="min")
            if len(part) != minutes or not part["time"].reset_index(drop=True).equals(pd.Series(expected_times)):
                continue
            higher_rows.append({"session_id": session["id"], "time": start, "_bar_end": start +
                                pd.Timedelta(minutes=minutes), "open": float(part["open"].iloc[0]),
                                "high": float(part["high"].max()), "low": float(part["low"].min()),
                                "close": float(part["close"].iloc[-1])})
    if not higher_rows:
        return []
    higher = pd.DataFrame(higher_rows).sort_values("time").reset_index(drop=True)
    regime_frames = []
    for _session_id, session_bars in higher.groupby("session_id", sort=False):
        session_bars = session_bars.sort_values("time").copy()
        segment = session_bars["time"].diff().ne(pd.Timedelta(minutes=minutes)).cumsum()
        for _segment_id, part in session_bars.groupby(segment, sort=True):
            part = part.copy().reset_index(drop=True)
            part["segment_id"] = int(_segment_id)
            regime = market_regimes.regime_frame(
                part, min_duration=int(definition["parameters"].get("minimum_regime_duration", 3)))
            regime_frames.append(part.join(regime))
    higher = pd.concat(regime_frames, ignore_index=True).sort_values("time").reset_index(drop=True)
    rows = []
    for session in sessions:
        group = session["bars"]
        candidates = higher[higher["session_id"] == session["id"]]
        if candidates.empty:
            continue
        candidates = candidates.sort_values("time").reset_index(drop=True)
        higher_ends = candidates["_bar_end"].to_numpy(dtype="datetime64[ns]")
        lower_ends = group["_bar_end"].to_numpy(dtype="datetime64[ns]")
        closed_positions = np.searchsorted(higher_ends, lower_ends, side="right") - 1
        higher_direction = np.zeros(len(candidates), dtype=np.int8)
        higher_closes = candidates["close"].to_numpy(dtype=float)
        higher_segments = candidates["segment_id"].to_numpy()
        higher_times = candidates["time"].to_numpy(dtype="datetime64[ns]")
        for position in range(max(lookback, 0), len(candidates)):
            older_position = position - lookback
            if older_position < 0 or higher_segments[older_position] != higher_segments[position]:
                continue
            if higher_times[position] - higher_times[older_position] != np.timedelta64(minutes * lookback, "m"):
                continue
            higher_direction[position] = int(np.sign(higher_closes[position] - higher_closes[older_position]))

        valid_position = closed_positions >= 0
        safe_positions = np.maximum(closed_positions, 0)
        ages = lower_ends - higher_ends[safe_positions]
        fresh = ages <= np.timedelta64(minutes, "m")
        lower_direction = np.sign(group["close"].to_numpy(dtype=float) - group["open"].to_numpy(dtype=float))
        higher_regimes = candidates["regime"].to_numpy()
        selected_directions = higher_direction[safe_positions]
        eligible = valid_position & fresh & (higher_regimes[safe_positions] == "TREND") & \
            (selected_directions != 0) & (lower_direction == selected_directions)
        for index in np.flatnonzero(eligible[:max(0, len(group) - 1)]):
            higher_position = int(closed_positions[index])
            higher_row = candidates.iloc[higher_position]
            high_direction = int(selected_directions[index])
            low_direction = int(lower_direction[index])
            asof = pd.Timestamp(group["_bar_end"].iloc[index])
            row = _base_row(group, index, low_direction, definition, point,
                            int(definition["exit"].get("minutes", 30)))
            if row:
                row.update({"higher_tf_regime": higher_row["regime"],
                            "higher_tf_direction": high_direction, "lower_tf_direction": low_direction,
                            "lower_tf_direction_confirmed": bool(low_direction == high_direction),
                            "higher_tf_bar_end": pd.Timestamp(higher_row["_bar_end"]),
                            "feature_available_at": asof})
                rows.append(row)
                break
    return rows


def _build_h07(sessions, definition, point):
    rows = []
    required_minute = int(definition["parameters"].get("session_minute", 60))
    for index in range(1, len(sessions)):
        current, prior = sessions[index], sessions[index - 1]
        group = current["bars"]
        previous_group = prior["bars"]
        if not _continuous_path(previous_group, start=previous_group["time"].iloc[0],
                                end=previous_group["_bar_end"].iloc[-1]):
            continue
        if current["weekday"] != int(definition["parameters"].get("weekday", 0)):
            continue
        prior_regime = prior["bars"].get("regime", pd.Series(dtype=object)).iloc[-1] \
            if len(prior["bars"]) and "regime" in prior["bars"] else None
        if prior_regime != "RANGE":
            continue
        for signal_index in range(5, len(group) - 1):
            if _minute_from_open(group, signal_index) < required_minute:
                continue
            direction = _direction(group, signal_index, int(definition["parameters"].get("direction_lookback", 5)))
            if not direction:
                continue
            row = _base_row(group, signal_index, direction, definition, point,
                            int(definition["exit"].get("minutes", 30)))
            if row:
                row.update({"weekday": current["weekday"], "session_minute": _minute_from_open(group, signal_index),
                            "prior_regime": prior_regime, "direction_source": "preceding_bar_drift"})
                rows.append(row)
                break
    return rows


def _build_h08(sessions, definition, point):
    rows, history = [], {}
    minimum = int(definition["parameters"].get("minimum_baseline_sessions", 5))
    recent = int(definition["parameters"].get("baseline_sessions", 20))
    rvol_threshold = float(definition["parameters"].get("rvol_threshold", 1.5))
    for session in sessions:
        group = session["bars"]
        session_contiguous = _continuous_path(group, start=group["time"].iloc[0], end=group["_bar_end"].iloc[-1])
        volatility_lookback = int(definition["parameters"].get("volatility_lookback", 30))
        volatility = market_regimes.volatility_frame(
            group, percentile_window=volatility_lookback,
            compression_pct=float(definition["parameters"].get("compression_percentile", 20)))
        volume_values = pd.to_numeric(group["_volume"], errors="coerce").to_numpy(dtype=float)
        minute_keys = ((group["time"] - group["time"].iloc[0]).dt.total_seconds() // 60).astype(int).to_numpy()
        volatility_states = volatility["volatility_state"].to_numpy()
        for minute in range(len(group)):
            minute_key = int(minute_keys[minute])
            prior = history.get(minute_key, [])[-recent:]
            baseline = float(np.median(prior)) if len(prior) >= minimum else np.nan
            volume = volume_values[minute]
            rvol = float(volume / baseline) if baseline > 0 and np.isfinite(volume) else np.nan
            state = volatility_states[minute]
            if minute < 5 or minute >= len(group) - 1 or not np.isfinite(rvol) or rvol < rvol_threshold or \
                    state not in {"COMPRESSION", "EXPANSION"}:
                continue
            feature_bars = volatility_lookback + 20
            feature_start = minute - feature_bars + 1
            if feature_start < 0 or not _continuous_path(
                    group.iloc[feature_start:minute + 1], start=group["time"].iloc[feature_start],
                    end=group["_bar_end"].iloc[minute]):
                continue
            direction = _direction(group, minute, int(definition["parameters"].get("direction_lookback", 5)))
            row = _base_row(group, minute, direction, definition, point,
                            int(definition["exit"].get("minutes", 30))) if direction else None
            if row:
                row.update({"rvol_same_minute": rvol, "volatility_state": state,
                            "volume_source": session["volume_source"], "direction_source": "preceding_bar_drift"})
                rows.append(row)
                break
        if session_contiguous:
            for minute_key, volume in zip(minute_keys, volume_values):
                if np.isfinite(volume):
                    history.setdefault(int(minute_key), []).append(float(volume))
    return rows


def _build_h09(sessions, definition, point):
    rows = []
    start = int(definition["parameters"].get("window_start_minute", 120))
    end = int(definition["parameters"].get("window_end_minute", 180))
    if end <= start:
        raise ValueError("H09 exige window_end_minute maior que window_start_minute.")
    for session in sessions:
        group = session["bars"]
        signal_index = next((i for i in range(5, len(group) - 1) if _minute_from_open(group, i) == start), None)
        if signal_index is None or _minute_from_open(group, signal_index) >= end:
            continue
        direction = _direction(group, signal_index, int(definition["parameters"].get("direction_lookback", 5)))
        row = _base_row(group, signal_index, direction, definition, point,
                        int(definition["exit"].get("minutes", 15))) if direction else None
        if row:
            row.update({"session_minute": start, "window_start_minute": start, "window_end_minute": end,
                        "direction_source": "preceding_bar_drift"})
            rows.append(row)
    return rows


def _build_h10(sessions, definition, point):
    rows = []
    lookback = int(definition["parameters"].get("prior_peak_lookback_bars", 60))
    pct_limit = float(definition["parameters"].get("drawdown_pct", -0.01))
    atr_limit = float(definition["parameters"].get("drawdown_atr", -1.5))
    horizon = int(definition["validation"]["horizon_minutes"])
    recovery_bars = max(1, min(int(definition["parameters"].get("recovery_bars", 20)), horizon))
    for session in sessions:
        group = session["bars"]
        atr = _atr(group, int(definition["parameters"].get("atr_period", 14)))
        atr_period = int(definition["parameters"].get("atr_period", 14))
        prior_peak = group["high"].shift(1).rolling(lookback, min_periods=lookback).max()
        drawdown_pct = group["close"] / prior_peak - 1.0
        drawdown_atr = (group["close"] - prior_peak) / atr
        for index in range(max(lookback, atr_period - 1, 1), len(group) - 1):
            if not (drawdown_pct.iloc[index] <= pct_limit and drawdown_atr.iloc[index] <= atr_limit):
                continue
            if index and drawdown_pct.iloc[index - 1] <= pct_limit and drawdown_atr.iloc[index - 1] <= atr_limit:
                continue
            feature_start = index - max(lookback, atr_period - 1)
            if not _continuous_path(group.iloc[feature_start:index + 1],
                                    start=group["time"].iloc[feature_start], end=group["_bar_end"].iloc[index]):
                continue
            direction = 1
            entry_index = index + 1
            entry_bar = group.iloc[entry_index]
            if pd.Timestamp(entry_bar["time"]) != pd.Timestamp(group["_bar_end"].iloc[index]):
                continue
            horizon_end = pd.Timestamp(entry_bar["time"]) + pd.Timedelta(minutes=horizon)
            future = group[(group["time"] >= entry_bar["time"]) & (group["_bar_end"] <= horizon_end)]
            if future.empty or pd.Timestamp(future["time"].iloc[0]) != pd.Timestamp(entry_bar["time"]):
                continue
            recovery_window = future.iloc[:recovery_bars]
            recovered = recovery_window[recovery_window["close"] >= float(prior_peak.iloc[index])]
            if not recovered.empty:
                exit_bar = recovered.iloc[0]
                success = True
                path_to_exit = future.loc[:recovered.index[0]]
                if not _continuous_path(path_to_exit, start=entry_bar["time"], end=exit_bar["_bar_end"]):
                    continue
                recovery_minutes = int((pd.Timestamp(exit_bar["_bar_end"]) -
                                        pd.Timestamp(group["_bar_end"].iloc[index])).total_seconds() // 60)
            else:
                if not _continuous_path(future, start=entry_bar["time"], end=horizon_end):
                    continue
                exit_bar, success, recovery_minutes = future.iloc[-1], False, None
            trade = _trade_return(entry_bar, exit_bar, direction, definition, point)
            rows.append({"observation_time": pd.Timestamp(group["_bar_end"].iloc[index]),
                         "entry_time": pd.Timestamp(entry_bar["time"]), "horizon_end": pd.Timestamp(exit_bar["_bar_end"]),
                         "drawdown_pct_from_prior_peak": float(drawdown_pct.iloc[index]),
                         "drawdown_atr_from_prior_peak": float(drawdown_atr.iloc[index]),
                         "prior_peak": float(prior_peak.iloc[index]), "atr_at_signal": float(atr.iloc[index]),
                         "recovered_within_recovery_window": success, "recovery_minutes": recovery_minutes,
                         "target_success": success, "gross_return": trade["gross_return"],
                         "outcome_return": trade["outcome_return"], "cost_bps": trade["cost_bps"],
                         "cost_observed": trade["cost_observed"], "trade_executed": True,
                         "signal_direction": direction, "outcome_field": "recovery_or_fixed_horizon"})
            break
    return rows


def _build_h11(sessions, definition, point):
    rows = []
    threshold = float(definition["parameters"].get("gap_threshold", -0.005))
    horizon = int(definition["parameters"].get("horizon_minutes", 5))
    for index in range(1, len(sessions)):
        current, previous = sessions[index], sessions[index - 1]
        group = current["bars"]
        previous_group = previous["bars"]
        if not _continuous_path(previous_group, start=previous_group["time"].iloc[0],
                                end=previous_group["_bar_end"].iloc[-1]):
            continue
        gap = current["open_price"] / previous["close_price"] - 1.0 if previous["close_price"] else np.nan
        if not np.isfinite(gap) or gap > threshold or len(group) <= 1:
            continue
        row = _base_row(group, 0, -1, definition, point, horizon, entry_index=0,
                        observation_time=current["open"])
        if row:
            row.update({"opening_gap_return": float(gap), "prior_close": float(previous["close_price"]),
                        "entry_delay_bars": 0, "gap_known_at": current["open"]})
            rows.append(row)
    return rows


def _build_h12(sessions, definition, point):
    rows = []
    opening_minutes = int(definition["parameters"].get("opening_range_minutes", 30))
    tolerance = float(definition["parameters"].get("retest_tolerance_atr", 0.2))
    for session in sessions:
        group = session["bars"]
        opening = group[group["time"] < pd.Timestamp(session["open"]) + pd.Timedelta(minutes=opening_minutes)]
        if not _continuous_path(group, start=group["time"].iloc[0], end=group["_bar_end"].iloc[-1]):
            rows.append({"observation_time": pd.Timestamp(group["_bar_end"].iloc[-1]),
                         "horizon_end": pd.Timestamp(group["_bar_end"].iloc[-1]),
                         "opening_range_available": False, "breakout_available": False,
                         "breakout_seen": pd.NA, "breakout_ambiguous": False,
                         "retest_available": False, "opening_range_retest_confirmed": False,
                         "trade_executed": False, "target_success": pd.NA, "outcome_return": np.nan})
            continue
        if len(opening) != opening_minutes or not opening["time"].diff().dropna().eq(pd.Timedelta(minutes=1)).all():
            rows.append({"observation_time": pd.Timestamp(group["_bar_end"].iloc[-1]),
                         "horizon_end": pd.Timestamp(group["_bar_end"].iloc[-1]),
                         "opening_range_available": False, "breakout_available": False,
                         "breakout_seen": pd.NA, "breakout_ambiguous": False,
                         "retest_available": False, "opening_range_retest_confirmed": False,
                         "trade_executed": False, "target_success": pd.NA, "outcome_return": np.nan})
            continue
        high, low = float(opening["high"].max()), float(opening["low"].min())
        breakout_index, direction = None, 0
        ambiguous_index = None
        for i in range(opening_minutes, len(group)):
            bar = group.iloc[i]
            cross_high, cross_low = float(bar["high"]) > high, float(bar["low"]) < low
            if cross_high and cross_low:
                ambiguous_index = i
                break
            if float(bar["close"]) > high:
                breakout_index, direction = i, 1
                break
            if float(bar["close"]) < low:
                breakout_index, direction = i, -1
                break
        if ambiguous_index is not None:
            bar = group.iloc[ambiguous_index]
            rows.append({"observation_time": pd.Timestamp(bar["_bar_end"]),
                         "horizon_end": pd.Timestamp(bar["_bar_end"]),
                         "opening_range_available": True, "breakout_available": False,
                         "breakout_seen": pd.NA, "breakout_ambiguous": True,
                         "retest_available": False, "opening_range_retest_confirmed": False,
                         "trade_executed": False, "target_success": pd.NA, "outcome_return": np.nan})
            continue
        if breakout_index is None:
            rows.append({"observation_time": pd.Timestamp(group["_bar_end"].iloc[-1]),
                         "horizon_end": pd.Timestamp(group["_bar_end"].iloc[-1]),
                         "opening_range_available": True, "breakout_available": True,
                         "breakout_seen": False, "breakout_ambiguous": False,
                         "retest_available": True, "opening_range_retest_confirmed": False,
                         "trade_executed": False, "target_success": pd.NA, "outcome_return": np.nan})
            continue
        atr = _atr(group, int(definition["parameters"].get("atr_period", 14)))
        resolved = False
        for i in range(breakout_index + 1, len(group) - 1):
            bar = group.iloc[i]
            cross_high, cross_low = float(bar["high"]) > high, float(bar["low"]) < low
            if cross_high and cross_low or (direction == 1 and float(bar["close"]) < low) or \
                    (direction == -1 and float(bar["close"]) > high):
                rows.append({"observation_time": pd.Timestamp(bar["_bar_end"]),
                             "horizon_end": pd.Timestamp(bar["_bar_end"]),
                             "opening_range_available": True, "breakout_available": True,
                             "breakout_seen": True, "breakout_ambiguous": bool(cross_high and cross_low),
                             "retest_available": False, "opening_range_retest_confirmed": False,
                             "breakout_direction": direction, "opening_range_high": high,
                             "opening_range_low": low, "trade_executed": False,
                             "target_success": pd.NA, "outcome_return": np.nan})
                resolved = True
                break
            atr_value = float(atr.iloc[i]) if pd.notna(atr.iloc[i]) else np.nan
            if not np.isfinite(atr_value) or atr_value <= 0:
                continue
            level = high if direction == 1 else low
            touched = float(bar["low"]) <= level + tolerance * atr_value if direction == 1 \
                else float(bar["high"]) >= level - tolerance * atr_value
            confirmed = float(bar["close"]) > high if direction == 1 else float(bar["close"]) < low
            if touched and confirmed:
                row = _session_close_row(group, i, direction, definition, point)
                if row:
                    row.update({"opening_range_available": True, "breakout_available": True,
                                "breakout_seen": True, "breakout_ambiguous": False,
                                "retest_available": True, "opening_range_retest_confirmed": True,
                                "breakout_direction": direction,
                                "opening_range_high": high, "opening_range_low": low,
                                "retest_tolerance_atr": tolerance,
                                "retest_resolution": "M1 closed bar; dual-side bars excluded"})
                    rows.append(row)
                else:
                    rows.append({"observation_time": pd.Timestamp(bar["_bar_end"]),
                                 "horizon_end": pd.Timestamp(group["_bar_end"].iloc[-1]),
                                 "opening_range_available": True, "breakout_available": True,
                                 "breakout_seen": True, "breakout_ambiguous": False,
                                 "retest_available": True, "opening_range_retest_confirmed": True,
                                 "breakout_direction": direction, "opening_range_high": high,
                                 "opening_range_low": low, "trade_executed": False,
                                 "target_success": pd.NA, "outcome_return": np.nan})
                resolved = True
                break
        if not resolved:
            rows.append({"observation_time": pd.Timestamp(group["_bar_end"].iloc[-1]),
                         "horizon_end": pd.Timestamp(group["_bar_end"].iloc[-1]),
                         "opening_range_available": True, "breakout_available": True,
                         "breakout_seen": True, "breakout_ambiguous": False,
                         "retest_available": True, "opening_range_retest_confirmed": False,
                         "breakout_direction": direction, "opening_range_high": high,
                         "opening_range_low": low, "trade_executed": False,
                         "target_success": pd.NA, "outcome_return": np.nan})
    return rows


def _build_h13(sessions, definition, point):
    rows = []
    distance_limit = float(definition["parameters"].get("distance_atr", 0.25))
    horizon = int(definition["validation"]["horizon_minutes"])
    for index in range(1, len(sessions)):
        current, previous = sessions[index], sessions[index - 1]
        group = current["bars"]
        previous_group = previous["bars"]
        if not _continuous_path(previous_group, start=previous_group["time"].iloc[0],
                                end=previous_group["_bar_end"].iloc[-1]):
            continue
        prior_high = float(previous["bars"]["high"].max())
        atr = _atr(group, int(definition["parameters"].get("atr_period", 14)))
        for i in range(14, len(group) - 1):
            atr_value = float(atr.iloc[i]) if pd.notna(atr.iloc[i]) else np.nan
            close = float(group["close"].iloc[i])
            if not np.isfinite(atr_value) or atr_value <= 0 or close > prior_high:
                continue
            distance = abs(prior_high - close) / atr_value
            previous_distance = abs(prior_high - float(group["close"].iloc[i - 1])) / atr_value
            if distance <= distance_limit and previous_distance > distance_limit:
                atr_period = int(definition["parameters"].get("atr_period", 14))
                atr_start = i - atr_period + 1
                if atr_start < 0 or not _continuous_path(
                        group.iloc[atr_start:i + 1], start=group["time"].iloc[atr_start],
                        end=group["_bar_end"].iloc[i]):
                    continue
                row = _base_row(group, i, -1, definition, point, horizon)
                if row:
                    row.update({"distance_previous_high_atr": float(distance),
                                "previous_session_high": prior_high,
                                "direction_source": "short_near_prior_high"})
                    rows.append(row)
                break
    return rows


def _event_frame(events, required, name):
    if events is None:
        raise ValueError(f"A hipótese exige {name} com as colunas: {', '.join(required)}.")
    frame = pd.DataFrame(events).copy()
    missing = [column for column in required if column not in frame]
    if missing:
        raise ValueError(f"{name} sem colunas obrigatórias: {', '.join(missing)}.")
    return frame


def _direction_value(value):
    if isinstance(value, str):
        value = {"buy": 1, "long": 1, "sell": -1, "short": -1}.get(value.strip().lower(), 0)
    if isinstance(value, (bool, np.bool_)):
        return 0
    try:
        numeric = float(value)
    except (TypeError, ValueError, OverflowError):
        return 0
    return int(numeric) if np.isfinite(numeric) and numeric in (-1.0, 1.0) else 0


def _event_direction(value, field):
    direction = _direction_value(value)
    if not direction:
        raise ValueError(f"signal/trade event.{field} precisa ser buy/long/1 ou sell/short/-1.")
    return direction


def _event_price(value):
    try:
        price = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("trade_events.entry_price precisa ser numérico e positivo.") from exc
    if not np.isfinite(price) or price <= 0:
        raise ValueError("trade_events.entry_price precisa ser numérico e positivo.")
    return price


def _ledger_rows(sessions, symbol, definition, point, trades, builder):
    events = _event_frame(trades, ("entry_time", "entry_price", "direction"), "trade_events")
    if "symbol" in events:
        events = events[events["symbol"].astype(str) == str(symbol)]
    data = pd.concat([session["bars"] for session in sessions], ignore_index=True) if sessions else pd.DataFrame()
    if data.empty:
        return []
    rows = []
    horizon = int(definition["validation"]["horizon_minutes"])
    for trade in events.to_dict("records"):
        entry_time = _event_timestamp(trade.get("entry_time"), "entry_time")
        entry_price = _event_price(trade.get("entry_price"))
        direction = _event_direction(trade.get("direction"), "direction")
        session_id = data.loc[(data["time"] <= entry_time) & (data["_bar_end"] > entry_time),
                              "session_id"].iloc[0] if ((data["time"] <= entry_time) &
                                                         (data["_bar_end"] > entry_time)).any() else None
        if session_id is None:
            session_id = data.loc[(data["time"] <= entry_time) & (data["_bar_end"] <= entry_time),
                                  "session_id"].iloc[-1] if ((data["time"] <= entry_time) &
                                                             (data["_bar_end"] <= entry_time)).any() else None
        if session_id is None:
            continue
        group = data[data["session_id"] == session_id].sort_values("time").reset_index(drop=True)
        prior = group[group["_bar_end"] <= entry_time]
        if prior.empty:
            continue
        prior_bar = prior.iloc[-1]
        regime = prior_bar.get("regime")
        atr_period = int(definition["parameters"].get("atr_period", 14))
        atr_value = float(_atr(group, atr_period).iloc[prior.index[-1]]) if prior.index[-1] < len(group) else np.nan
        atr_start = max(0, int(prior.index[-1]) - atr_period + 1)
        if not _continuous_path(group.iloc[atr_start:int(prior.index[-1]) + 1],
                                start=group["time"].iloc[atr_start], end=prior_bar["_bar_end"]):
            atr_value = np.nan
        horizon_end = entry_time + pd.Timedelta(minutes=horizon)
        path = group[(group["time"] >= entry_time) & (group["_bar_end"] <= horizon_end)]
        if path.empty:
            continue
        first_expected = entry_time.ceil("min")
        last_expected_end = horizon_end.floor("min")
        if not _continuous_path(path, start=first_expected, end=last_expected_end):
            continue
        high, low = float(path["high"].max()), float(path["low"].min())
        favorable = max(0.0, high - entry_price) if direction > 0 else max(0.0, entry_price - low)
        adverse = max(0.0, entry_price - low) if direction > 0 else max(0.0, high - entry_price)
        mae_atr = adverse / atr_value if np.isfinite(atr_value) and atr_value > 0 else np.nan
        mfe_atr = favorable / atr_value if np.isfinite(atr_value) and atr_value > 0 else np.nan
        first_bar = path.iloc[0]
        exit_bar = path.iloc[-1]
        spread = float(first_bar["spread"]) if pd.notna(first_bar.get("spread")) else 0.0
        observed = point is not None and float(point) > 0 and pd.notna(first_bar.get("spread"))
        cost = spread * float(point) / entry_price * 10000.0 if observed else 0.0
        gross = direction * (float(exit_bar["close"]) / entry_price - 1.0)
        net = gross - cost / 10000.0 - float(definition["costs"].get("commission_bps", 0.0)) / 10000.0 \
            - float(definition["costs"].get("slippage_bps", 0.0)) / 10000.0
        row = {"observation_time": entry_time, "entry_time": entry_time,
               "horizon_end": pd.Timestamp(exit_bar["_bar_end"]), "requested_horizon_end": horizon_end,
               "prior_regime": regime,
               "mae": float(adverse), "mfe": float(favorable), "mae_atr": float(mae_atr),
               "mfe_atr": float(mfe_atr), "gross_return": float(gross), "outcome_return": float(net),
               "cost_bps": float(cost), "cost_observed": bool(observed), "trade_executed": True,
               "target_success": bool(net > 0), "signal_direction": direction,
               "excursion_resolution": "M1 closed bars; entry bar included only when entry is at its open",
               "outcome_field": "fixed_horizon_trade_return", "trade_id": trade.get("trade_id"),
               "strategy_id": trade.get("strategy_id")}
        if builder == "H15":
            threshold = float(definition["parameters"].get("mae_atr", 1.0))
            adverse_mask = ((path["low"] <= entry_price - threshold * atr_value) if direction > 0
                            else (path["high"] >= entry_price + threshold * atr_value)) \
                if np.isfinite(atr_value) and atr_value > 0 else pd.Series(False, index=path.index)
            adverse_rows = path.loc[adverse_mask]
            if adverse_rows.empty:
                continue
            first_adverse_end = pd.Timestamp(adverse_rows["_bar_end"].iloc[0])
            after_adverse = path[path["_bar_end"] > first_adverse_end]
            recovered = after_adverse[after_adverse["close"] >= entry_price] if direction > 0 \
                else after_adverse[after_adverse["close"] <= entry_price]
            row["recovered_within_horizon"] = bool(not recovered.empty)
            row["recovery_minutes"] = int((pd.Timestamp(recovered["_bar_end"].iloc[0]) -
                                           first_adverse_end).total_seconds() // 60) if not recovered.empty else None
            row["target_success"] = row["recovered_within_horizon"]
            row["outcome_field"] = "recovery_minutes_after_adverse_excursion"
        if pd.notna(regime):
            rows.append(row)
    return rows


def _build_h14(sessions, symbol, definition, point, trade_events):
    return _ledger_rows(sessions, symbol, definition, point, trade_events, "H14")


def _build_h15(sessions, symbol, definition, point, trade_events):
    return _ledger_rows(sessions, symbol, definition, point, trade_events, "H15")


def _build_h16(sessions, symbol, definition, point, signal_events):
    events = _event_frame(signal_events, ("signal_time", "direction", "exit_time"), "signal_events")
    if "symbol" in events:
        events = events[events["symbol"].astype(str) == str(symbol)]
    data = pd.concat([session["bars"] for session in sessions], ignore_index=True) if sessions else pd.DataFrame()
    if data.empty:
        return []
    rows = []
    delays = definition["parameters"].get("delay_bars", [0, 1, 3])
    if not isinstance(delays, list) or not delays or any(
            isinstance(delay, bool) or not isinstance(delay, int) or delay < 0 for delay in delays) or \
            len(set(delays)) != len(delays):
        raise ValueError("H16 delay_bars precisa listar inteiros não negativos sem repetição.")
    for event in events.to_dict("records"):
        signal_time = _event_timestamp(event.get("signal_time"), "signal_time")
        exit_time = _event_timestamp(event.get("exit_time"), "exit_time")
        direction = _event_direction(event.get("direction"), "direction")
        if exit_time <= signal_time:
            raise ValueError("signal_events.exit_time precisa ser posterior a signal_time.")
        if exit_time != exit_time.floor("min"):
            raise ValueError("signal_events.exit_time precisa coincidir com o fechamento de uma barra M1.")
        group = data[(data["time"] <= exit_time) & (data["time"] >= signal_time)].copy()
        if group.empty:
            continue
        session_id = group["session_id"].iloc[0]
        group = group[group["session_id"] == session_id].sort_values("time").reset_index(drop=True)
        base_index = next((i for i, time in enumerate(group["time"]) if pd.Timestamp(time) >= signal_time), None)
        if base_index is None:
            continue
        exit_rows = group[group["_bar_end"] <= exit_time]
        if exit_rows.empty or pd.Timestamp(exit_rows["_bar_end"].iloc[-1]) != exit_time:
            continue
        exit_index = int(exit_rows.index[-1])
        if not _continuous_path(group.iloc[base_index:exit_index + 1],
                                start=group["time"].iloc[base_index], end=exit_time):
            continue
        exit_bar = exit_rows.iloc[-1]
        outcomes = {}
        for delay in delays:
            entry_index = base_index + int(delay)
            if entry_index >= len(group) or pd.Timestamp(group["time"].iloc[entry_index]) >= exit_time:
                continue
            trade = _trade_return(group.iloc[entry_index], exit_bar, direction, definition, point)
            outcomes[int(delay)] = trade
        if 0 not in outcomes:
            continue
        base = outcomes[0]
        row = {"observation_time": signal_time, "entry_time": pd.Timestamp(group["time"].iloc[base_index]),
               "entry_price": float(group["open"].iloc[base_index]),
               "horizon_end": pd.Timestamp(exit_bar["_bar_end"]), "gross_return": base["gross_return"],
               "exit_price": float(exit_bar["close"]),
               "outcome_return": base["outcome_return"], "cost_bps": base["cost_bps"],
               "cost_observed": base["cost_observed"], "target_success": bool(base["outcome_return"] > 0),
               "trade_executed": True, "strategy_signal": True, "signal_direction": direction,
               "signal_id": event.get("signal_id"),
               "strategy_id": event.get("strategy_id"), "outcome_field": "declared_signal_exit"}
        for delay, trade in outcomes.items():
            delayed_index = base_index + delay
            row[f"entry_time_delay_{delay}"] = pd.Timestamp(group["time"].iloc[delayed_index])
            row[f"entry_price_delay_{delay}"] = float(group["open"].iloc[delayed_index])
            row[f"gross_return_delay_{delay}"] = trade["gross_return"]
            row[f"outcome_return_delay_{delay}"] = trade["outcome_return"]
            row[f"cost_bps_delay_{delay}"] = trade["cost_bps"]
        rows.append(row)
    return rows


def build_extended_observations(bars, symbol, definition, *, point=None, broker_utc_offset=-3.0,
                                signal_events=None, trade_events=None):
    """Constrói H06–H16; estratégia e ledger entram por quadros explícitos, nunca são inventados."""
    builder = definition["sample_builder"]
    include_regime = builder in {"H06", "H07", "H14", "H15"}
    sessions, _source = _sessions(bars, str(symbol), definition, float(broker_utc_offset), regime=include_regime)
    if builder == "H06":
        rows = _build_h06(sessions, definition, point)
    elif builder == "H07":
        rows = _build_h07(sessions, definition, point)
    elif builder == "H08":
        rows = _build_h08(sessions, definition, point)
    elif builder == "H09":
        rows = _build_h09(sessions, definition, point)
    elif builder == "H10":
        rows = _build_h10(sessions, definition, point)
    elif builder == "H11":
        rows = _build_h11(sessions, definition, point)
    elif builder == "H12":
        rows = _build_h12(sessions, definition, point)
    elif builder == "H13":
        rows = _build_h13(sessions, definition, point)
    elif builder == "H14":
        rows = _build_h14(sessions, str(symbol), definition, point, trade_events)
    elif builder == "H15":
        rows = _build_h15(sessions, str(symbol), definition, point, trade_events)
    elif builder == "H16":
        rows = _build_h16(sessions, str(symbol), definition, point, signal_events)
    else:
        raise ValueError(f"Construtor de amostras indisponível: {builder}.")
    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame["target_success"] = pd.to_numeric(frame["target_success"], errors="coerce").astype("boolean")
        frame = frame.sort_values("observation_time", kind="stable").reset_index(drop=True)
    return frame
