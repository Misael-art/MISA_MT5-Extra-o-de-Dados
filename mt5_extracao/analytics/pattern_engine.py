"""Construção causal de observações e execução de condições declaradas (T13.4)."""
import numpy as np
import pandas as pd

from mt5_extracao.analytics import calendar, market_regimes
from mt5_extracao.analytics.hypotheses import validate_definition


_OPERATORS = {
    "eq": lambda series, value: series.eq(value),
    "ne": lambda series, value: series.ne(value),
    "gt": lambda series, value: series.gt(value),
    "gte": lambda series, value: series.ge(value),
    "lt": lambda series, value: series.lt(value),
    "lte": lambda series, value: series.le(value),
    "in": lambda series, value: series.isin(value),
    "not_in": lambda series, value: ~series.isin(value),
}


def condition_mask(frame, condition):
    """Avalia uma árvore JSON de comparações; o arquivo nunca contém código executável."""
    if "field" in condition:
        field = condition["field"]
        if field not in frame:
            return pd.Series(False, index=frame.index)
        mask = _OPERATORS[condition["operator"]](frame[field], condition["value"])
        return mask.fillna(False).astype(bool)
    operator, children = condition["operator"], condition["conditions"]
    masks = [condition_mask(frame, child) for child in children]
    if operator == "and":
        return pd.concat(masks, axis=1).all(axis=1)
    if operator == "or":
        return pd.concat(masks, axis=1).any(axis=1)
    return ~masks[0]


def _volume(bars):
    real = pd.to_numeric(bars["real_volume"], errors="coerce") if "real_volume" in bars else None
    ticks = pd.to_numeric(bars["tick_volume"], errors="coerce") if "tick_volume" in bars else None
    if real is not None and len(real) and real.notna().all():
        return real, "real_volume"
    if ticks is not None:
        return ticks, "tick_volume"
    return pd.Series(np.nan, index=bars.index), None


def _target_value(definition):
    target = definition["target"]
    if "parameter" in target:
        return float(definition["parameters"][target["parameter"]])
    return float(target["value"])


def _spread_cost_bps(entry_bar, exit_bar, price, point, definition):
    costs = definition["costs"]
    commission = float(costs.get("commission_bps", 0.0))
    slippage = float(costs.get("slippage_bps", 0.0))
    if point is None or pd.isna(point) or float(point) <= 0 or "spread" not in entry_bar:
        return commission + slippage, False
    spread_points = pd.to_numeric(pd.Series([entry_bar.get("spread")]), errors="coerce").iloc[0]
    if pd.isna(spread_points) or pd.isna(price) or float(price) <= 0:
        return commission + slippage, False
    return float(spread_points) * float(point) / float(price) * 10000.0 + commission + slippage, True


def _net_return(entry_bar, exit_bar, direction, definition, point):
    entry, exit_ = float(entry_bar["open"]), float(exit_bar["close"])
    gross = float(direction) * (exit_ / entry - 1.0) if entry else np.nan
    cost_bps, cost_observed = _spread_cost_bps(entry_bar, exit_bar, entry, point, definition)
    return gross, gross - cost_bps / 10000.0, cost_bps, cost_observed


def _trade_return(entry_bar, exit_bar, direction, definition, point):
    """Spread is measured once at entry; extra slippage/commission are explicit hypothesis inputs."""
    entry = float(entry_bar["open"])
    exit_ = float(exit_bar["close"])
    gross = float(direction) * (exit_ / entry - 1.0) if entry else np.nan
    cost, observed = _spread_cost_bps(entry_bar, exit_bar, entry, point, definition)
    return {"gross_return": gross, "outcome_return": gross - cost / 10000.0,
            "cost_bps": cost, "cost_observed": observed}


def _session_context(bars, symbol, definition, broker_utc_offset, include_regime=False):
    bars = calendar.prepare_m1(bars)
    if bars.empty:
        return bars, [], None
    volume, source = _volume(bars)
    bars = bars.assign(_volume=volume, _bar_end=bars["time"] + pd.Timedelta(minutes=1))
    sessions_bars, sessions = calendar.sessionize_m1(
        bars.drop(columns=["_volume", "_bar_end"]), symbol,
        broker_utc_offset=broker_utc_offset,
        session_gap_minutes=int(definition["session"].get("session_gap_minutes", 60)),
        session_anchor=definition["session"].get("session_anchor", "00:00"))
    sessions_bars["_volume"] = volume.to_numpy()
    sessions_bars["_bar_end"] = sessions_bars["time"] + pd.Timedelta(minutes=1)
    if include_regime:
        regimes = market_regimes.regime_frame(
            sessions_bars, min_duration=int(definition["parameters"].get("minimum_regime_duration", 3)))
        sessions_bars = sessions_bars.join(regimes[["regime", "adx", "efficiency", "r2"]])
    grouped = []
    for session in sessions:
        group = sessions_bars[sessions_bars["session_id"] == session["session_id"]].copy()
        if group.empty:
            continue
        group = group.sort_values("time").reset_index(drop=True)
        session_open = group["time"].iloc[0]
        close_time = group["_bar_end"].iloc[-1]
        observed_minutes = int(group["_volume"].notna().sum())
        volume_total = float(group["_volume"].sum()) if group["_volume"].notna().all() else np.nan
        grouped.append({"id": session["session_id"], "open": session_open, "close_time": close_time,
                        "open_price": float(group["open"].iloc[0]), "close_price": float(group["close"].iloc[-1]),
                        "weekday": int(session_open.weekday()), "observed_minutes": observed_minutes,
                        "volume_per_minute": volume_total / observed_minutes if observed_minutes else np.nan,
                        "volume_source": source, "bars": group})
    return sessions_bars, grouped, source


def _weekday_volume_ratios(sessions, occurrences, volume_key="volume_per_minute"):
    ratios = []
    for index, current in enumerate(sessions):
        prior = [row[volume_key] for row in sessions[:index]
                 if row["weekday"] == current["weekday"] and np.isfinite(row[volume_key])]
        prior = prior[-max(occurrences * 4, occurrences):]
        baseline = float(np.median(prior[-occurrences:])) if len(prior) >= occurrences else np.nan
        current_value = current[volume_key]
        ratios.append(current_value / baseline if baseline > 0 and np.isfinite(current_value) else np.nan)
    return ratios


def _breakout_outcome(group, definition, point, minutes=30):
    opening = group[group["time"] < group["time"].iloc[0] + pd.Timedelta(minutes=minutes)]
    if len(opening) < minutes or not opening["time"].diff().dropna().eq(pd.Timedelta(minutes=1)).all():
        return {"breakout_success": np.nan, "breakout_return": np.nan, "breakout_direction": None,
                "breakout_ambiguous": False, "breakout_available": False,
                "breakout_reason": "opening_range_incomplete", "trade_executed": False, "entry_time": pd.NaT}
    high, low = float(opening["high"].max()), float(opening["low"].min())
    after = group[group["time"] >= opening["time"].iloc[-1] + pd.Timedelta(minutes=1)]
    if after.empty:
        return {"breakout_success": np.nan, "breakout_return": np.nan, "breakout_direction": None,
                "breakout_ambiguous": False, "breakout_available": False,
                "breakout_reason": "no_bars_after_opening_range", "trade_executed": False, "entry_time": pd.NaT}
    direction, trigger_index = None, None
    for row_index, bar in after.iterrows():
        # A bar-close trigger is observable; a high/low touch alone has unknown intrabar order.
        high_cross = float(bar["high"]) > high
        low_cross = float(bar["low"]) < low
        if high_cross and low_cross:
            return {"breakout_success": np.nan, "breakout_return": np.nan, "breakout_gross_return": np.nan,
                    "breakout_cost_bps": np.nan, "breakout_cost_observed": False, "breakout_direction": None,
                    "breakout_ambiguous": True, "breakout_available": False,
                    "breakout_reason": "both_range_sides_crossed_in_one_bar",
                    "trade_executed": False, "entry_time": pd.NaT}
        close = float(bar["close"])
        up, down = close > high, close < low
        if up or down:
            direction, trigger_index = (1 if up else -1), int(row_index)
            break
    if direction is None:
        return {"breakout_success": False, "breakout_return": 0.0, "breakout_gross_return": 0.0,
                "breakout_cost_bps": 0.0, "breakout_cost_observed": False, "breakout_direction": None,
                "breakout_ambiguous": False, "breakout_available": True,
                "breakout_reason": "no_confirmed_close_outside_range", "trade_executed": False,
                "entry_time": pd.NaT}
    # The trigger is known at bar close; entry uses the next observed bar's open.
    entry_index = trigger_index + 1
    if entry_index >= len(group):
        return {"breakout_success": np.nan, "breakout_return": np.nan, "breakout_gross_return": np.nan,
                "breakout_cost_bps": np.nan, "breakout_cost_observed": False, "breakout_direction": direction,
                "breakout_ambiguous": False, "breakout_available": False,
                "breakout_reason": "no_next_bar_for_execution", "trade_executed": False, "entry_time": pd.NaT}
    entry_bar = group.iloc[entry_index]
    exit_bar = group.iloc[-1]
    if entry_bar["time"] >= exit_bar["_bar_end"]:
        return {"breakout_success": np.nan, "breakout_return": np.nan, "breakout_gross_return": np.nan,
                "breakout_cost_bps": np.nan, "breakout_cost_observed": False, "breakout_direction": direction,
                "breakout_ambiguous": False, "breakout_available": False,
                "breakout_reason": "empty_execution_horizon", "trade_executed": False, "entry_time": pd.NaT}
    gross, net, cost, observed = _net_return(entry_bar, exit_bar, direction, definition, point)
    delays = _entry_delay_returns(group, entry_index, exit_bar, direction, definition, point)
    return {"breakout_success": bool(net > 0), "breakout_return": net,
            "breakout_gross_return": gross, "breakout_cost_bps": cost,
            "breakout_cost_observed": observed, "breakout_direction": direction,
            "breakout_ambiguous": False, "breakout_available": True, "breakout_reason": None,
            "trade_executed": True,
            "entry_time": entry_bar["time"], **delays}


def _entry_delay_returns(group, entry_index, exit_bar, direction, definition, point, offsets=(0, 1, 3)):
    result = {}
    for delay in offsets:
        index = entry_index + delay
        if index < 0 or index >= len(group) or group["time"].iloc[index] >= exit_bar["_bar_end"]:
            continue
        row = _trade_return(group.iloc[index], exit_bar, direction, definition, point)
        result[f"gross_return_delay_{delay}"] = row["gross_return"]
        result[f"outcome_return_delay_{delay}"] = row["outcome_return"]
        result[f"cost_bps_delay_{delay}"] = row["cost_bps"]
    return result


def _session_records(bars, symbol, definition, point, broker_utc_offset):
    builder = definition["sample_builder"]
    include_breakout = builder in {"H03", "H04", "H05"}
    data, sessions, source = _session_context(
        bars, symbol, definition, broker_utc_offset, include_regime=builder == "H05")
    if len(sessions) < 2:
        return pd.DataFrame(), source, {}
    # The newest observed session may still be open; it never supplies an outcome.
    sessions = sessions[:-1]
    records = []
    weekday_ratios = _weekday_volume_ratios(sessions, int(definition["parameters"].get(
        "weekday_baseline_occurrences", 5)))
    # Each value is current volume divided by a same-weekday baseline built only from earlier sessions.
    normalized_volume = weekday_ratios
    opening_range_minutes = int(definition["parameters"].get("opening_range_minutes", 30))
    for row in sessions:
        opening_range = row["bars"][row["bars"]["time"] < row["open"] + pd.Timedelta(minutes=opening_range_minutes)]
        complete = len(opening_range) == opening_range_minutes and \
            opening_range["time"].diff().dropna().eq(pd.Timedelta(minutes=1)).all()
        opening_volume = opening_range["_volume"] if complete else pd.Series(dtype=float)
        row["opening_range_volume_per_minute"] = (
            float(opening_volume.mean()) if len(opening_volume) and opening_volume.notna().all() else np.nan)
    opening_range_ratios = _weekday_volume_ratios(
        sessions, int(definition["parameters"].get("weekday_baseline_occurrences", 5)),
        volume_key="opening_range_volume_per_minute")
    recent_count = int(definition["parameters"].get("recent_sessions", 15))
    baseline_count = int(definition["parameters"].get("baseline_sessions", 20))
    maximum_baseline = int(definition["parameters"].get("maximum_baseline_sessions", baseline_count))
    for index, current in enumerate(sessions):
        group = current["bars"]
        previous = sessions[index - 1] if index else None
        before_previous = sessions[index - 2] if index > 1 else None
        record = {"session_id": current["id"], "observation_time": current["open"], "entry_time": current["open"],
                  "horizon_end": current["close_time"], "weekday": current["weekday"],
                  "volume_source": current["volume_source"], "volume_per_minute": current["volume_per_minute"],
                  "weekday_volume_ratio": weekday_ratios[index], "weekday_normalized_volume": normalized_volume[index],
                  "opening_range_volume_per_minute": current["opening_range_volume_per_minute"],
                  "opening_range_weekday_volume_ratio": opening_range_ratios[index],
                  "open_price": current["open_price"],
                  "close_price": current["close_price"]}
        if include_breakout:
            record.update(_breakout_outcome(group, definition, point, opening_range_minutes))
            if builder == "H04":
                record["observation_time"] = current["open"] + pd.Timedelta(minutes=opening_range_minutes)
        if previous is not None:
            record["gap_return"] = current["open_price"] / previous["close_price"] - 1.0
            if builder == "H05":
                record["prior_regime"] = previous["bars"]["regime"].iloc[-1]
            if before_previous is not None and before_previous["close_price"]:
                record["previous_session_return"] = previous["close_price"] / before_previous["close_price"] - 1.0
        if index >= recent_count + baseline_count:
            recent = [value for value in normalized_volume[index - recent_count:index] if np.isfinite(value)]
            baseline_start = max(0, index - recent_count - maximum_baseline)
            baseline = [value for value in normalized_volume[baseline_start:index - recent_count]
                        if np.isfinite(value)]
            if len(recent) == recent_count and len(baseline) >= baseline_count and np.median(baseline) > 0:
                record["volume_decline_ratio"] = float(np.median(recent) / np.median(baseline))
        records.append(record)
    return pd.DataFrame(records), source, {row["id"]: row["bars"] for row in sessions}


def _build_h01(bars, symbol, definition, point, offset):
    records, source, by_id = _session_records(bars, symbol, definition, point, offset)
    if records.empty:
        return records, source
    output = []
    for row in records.to_dict("records"):
        if "previous_session_return" not in row or "gap_return" not in row:
            continue
        group = by_id[row["session_id"]]
        open_time = row["observation_time"]
        exit_time = open_time + pd.Timedelta(minutes=5)
        exit_rows = group[group["_bar_end"] <= exit_time]
        if len(group) < 5 or exit_rows.empty:
            continue
        entry_index = 1
        entry_bar, exit_bar = group.iloc[entry_index], exit_rows.iloc[-1]
        row["entry_time"] = entry_bar["time"]
        trade = _trade_return(entry_bar, exit_bar, -1, definition, point)
        row.update({"target_success": trade["outcome_return"] > _target_value(definition),
                    "gross_return": trade["gross_return"], "outcome_return": trade["outcome_return"],
                    "cost_bps": trade["cost_bps"], "cost_observed": trade["cost_observed"],
                    "outcome_field": "short_return_5m"})
        row["horizon_end"] = exit_bar["_bar_end"]
        row.update(_entry_delay_returns(group, entry_index, exit_bar, -1, definition, point))
        output.append(row)
    return pd.DataFrame(output), source


def _build_h02(bars, symbol, definition, point, offset):
    records, source, by_id = _session_records(bars, symbol, definition, point, offset)
    if records.empty:
        return records, source
    output = []
    for row in records.to_dict("records"):
        group = by_id[row["session_id"]]
        trigger = row["observation_time"].normalize() + pd.Timedelta(
            hours=int(definition["trigger"]["time"][:2]), minutes=int(definition["trigger"]["time"][3:]))
        closed = group[group["_bar_end"] <= trigger]
        entries = group[group["time"] >= trigger]
        if closed.empty or closed["_bar_end"].iloc[-1] != trigger or entries.empty:
            continue
        trigger_close = float(closed["close"].iloc[-1])
        return_to_trigger = trigger_close / row["open_price"] - 1.0 if row["open_price"] else np.nan
        entry_index = int(entries.index[0])
        # The session frame has a fresh RangeIndex; the first bar at/after the trigger is the executable entry.
        entry_index = int(group.index.get_indexer_for([entries.index[0]])[0])
        entry_bar, exit_bar = group.iloc[entry_index], group.iloc[-1]
        if entry_bar["time"] >= exit_bar["_bar_end"]:
            continue
        trade = _trade_return(entry_bar, exit_bar, 1, definition, point)
        row.update({"return_to_trigger": return_to_trigger,
                    "return_after_trigger": float(exit_bar["close"]) / float(entry_bar["open"]) - 1.0,
                    "target_success": float(exit_bar["close"]) / float(entry_bar["open"]) - 1.0 >=
                                     _target_value(definition),
                    "gross_return": trade["gross_return"], "outcome_return": trade["outcome_return"],
                    "cost_bps": trade["cost_bps"], "cost_observed": trade["cost_observed"],
                    "outcome_field": "long_return_to_close"})
        row["entry_time"] = entry_bar["time"]
        row["observation_time"] = trigger
        row["horizon_end"] = exit_bar["_bar_end"]
        row.update(_entry_delay_returns(group, entry_index, exit_bar, 1, definition, point))
        output.append(row)
    return pd.DataFrame(output), source


def _build_session_hypothesis(bars, symbol, definition, point, offset):
    records, source, _by_id = _session_records(bars, symbol, definition, point, offset)
    if records.empty:
        return records, source
    builder = definition["sample_builder"]
    if builder == "H03":
        if "volume_decline_ratio" not in records:
            records["volume_decline_ratio"] = np.nan
        records = records.dropna(subset=["volume_decline_ratio"])
    elif builder == "H04":
        records = records.dropna(subset=["opening_range_weekday_volume_ratio"])
    elif builder == "H05":
        records = records.dropna(subset=["prior_regime"])
    records["target_success"] = records["breakout_success"]
    records["gross_return"] = records["breakout_gross_return"]
    records["outcome_return"] = records["breakout_return"]
    records["cost_bps"] = records["breakout_cost_bps"]
    records["cost_observed"] = records["breakout_cost_observed"]
    records["outcome_field"] = "breakout_return"
    records["trade_executed"] = records["trade_executed"].fillna(False).astype(bool)
    records["entry_time"] = pd.to_datetime(records["entry_time"], errors="coerce").fillna(
        pd.to_datetime(records["observation_time"]))
    return records.reset_index(drop=True), source


_BUILDERS = {"H01": _build_h01, "H02": _build_h02, "H03": _build_session_hypothesis,
             "H04": _build_session_hypothesis, "H05": _build_session_hypothesis}


def build_observations(bars, symbol, definition, *, point=None, broker_utc_offset=-3.0,
                       signal_events=None, trade_events=None):
    """Calcula amostras em memória sem escrita; sinais e operações vêm por contratos explícitos."""
    validate_definition(definition)
    builder = definition["sample_builder"]
    if builder in _BUILDERS:
        frame = _BUILDERS[builder](bars, str(symbol), definition, point, float(broker_utc_offset))[0]
    else:
        from mt5_extracao.analytics.pattern_extensions import build_extended_observations
        frame = build_extended_observations(
            bars, str(symbol), definition, point=point, broker_utc_offset=float(broker_utc_offset),
            signal_events=signal_events, trade_events=trade_events)
    if frame.empty:
        return frame
    frame["target_success"] = pd.to_numeric(frame["target_success"], errors="coerce").astype("boolean")
    return frame.reset_index(drop=True)
