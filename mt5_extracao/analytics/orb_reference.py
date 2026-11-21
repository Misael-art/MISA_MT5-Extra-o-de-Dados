"""Comparação do ORB legado com o recorte de pregão qualificado por F19-PESQ-01."""
from collections import defaultdict
from math import isfinite

import pandas as pd

from mt5_extracao.analytics import opening_coverage
from mt5_extracao.strategies.orb import OpeningRangeBreakout


def _events(data, signals, time_utc, dates):
    events = defaultdict(list)
    for index in signals.index[signals.entry_long | signals.entry_short]:
        side = "long" if signals.at[index, "entry_long"] else "short"
        start = time_utc.loc[index]
        events[dates.loc[index]].append({"bar_open_utc": start.isoformat(),
                       "available_at_utc": (start + opening_coverage.MINUTE).isoformat(),
                       "side": side, "close": float(data.at[index, "close"]),
                       "stop": float(signals.at[index, "stop_" + side]),
                       "target": float(signals.at[index, "target_" + side])})
    return events


def compare(snapshot, calendar, *, as_of, range_minutes=30, target_mult=1.5, min_range_atr=0.0,
            width_limits=None, distance_limits=None, direction="both"):
    """Compara sinais Python pontuais. SL, TP e decisão saem do mesmo ORB."""
    if (type(target_mult) not in (int, float) or not isfinite(target_mult) or target_mult <= 0 or
            type(min_range_atr) not in (int, float) or not isfinite(min_range_atr) or min_range_atr < 0):
        raise ValueError("O alvo precisa de múltiplo positivo e o filtro ATR de múltiplo não negativo.")
    if direction not in ("both", "long", "short"):
        raise ValueError("Escolha direção both, long ou short.")
    limits = {name: _validate_limits(value) for name, value in (("width", width_limits), ("distance", distance_limits))}
    coverage = opening_coverage.report([snapshot], calendar, as_of=as_of, opening_minutes=range_minutes)
    params = {"range_minutes": range_minutes, "target_mult": target_mult, "min_range_atr": min_range_atr}
    strategy = OpeningRangeBreakout(**params)
    data = snapshot.bars
    clock = snapshot.manifest["time"]["broker_utc_offset_hours"]
    time_utc = pd.to_datetime(data.time, format="ISO8601").dt.tz_localize("UTC") - pd.Timedelta(hours=clock)
    visible = time_utc + opening_coverage.MINUTE <= pd.Timestamp(coverage["as_of"])
    data, time_utc = data.loc[visible], time_utc.loc[visible]
    legacy = strategy.gerar_sinais(data)
    day = time_utc.dt.tz_convert(coverage["timezone"]).dt.strftime("%Y-%m-%d")
    qualified_sessions = {row["date"]: row for row in coverage["sessions"] if row["status"] == "eligible"}
    high = day.map({key: row["range"]["high"] for key, row in qualified_sessions.items()})
    low = day.map({key: row["range"]["low"] for key, row in qualified_sessions.items()})
    ready_at = pd.to_datetime(day.map({key: row["range"]["available_at_utc"]
                                       for key, row in qualified_sessions.items()}), utc=True)
    close = pd.to_datetime(day.map({key: row["expected_close_utc"] for key, row in qualified_sessions.items()}), utc=True)
    ready = (time_utc >= ready_at) & (time_utc < close)
    qualified = legacy.copy()
    atr = qualified.atr if min_range_atr else qualified.atr.fillna(0)
    strategy._breakouts(data, qualified, day, ready, high, low, atr)
    legacy_events = _events(data, legacy, time_utc, day)
    nominal_events = _events(data, qualified, time_utc, day)
    sessions = [{**row, "legacy_signals": legacy_events[row["date"]],
                 "qualified_signals": nominal_events[row["date"]]} for row in coverage["sessions"]]
    first = next((row for row in sessions if row["legacy_signals"] != row["qualified_signals"]), None)
    result = {"version": 1, "contract": snapshot.manifest["symbol"], "params": params,
              "coverage_manifest_id": coverage["manifest_id"], "snapshot_id": snapshot.manifest_id,
              "first_divergent_session": first["date"] if first else None,
              "execution_evidence": "signals_python_only", "sessions": sessions}
    if width_limits is not None or distance_limits is not None or direction != "both":
        result["filters"] = {"unit": "index_points", "width_limits": limits["width"], "distance_limits": limits["distance"],
                             "direction": direction, "first_breakout_consumes_session": True}
        for row in sessions:
            row["filter_decision"] = _filter_decision(row, limits, direction)
    return result


def _validate_limits(bounds):
    if bounds is None:
        return None
    if (not isinstance(bounds, (tuple, list)) or len(bounds) != 2 or
            any(type(value) not in (int, float) or not isfinite(value) for value in bounds) or
            not 0 <= bounds[0] <= bounds[1]):
        raise ValueError("Limites devem ser dois valores finitos ordenados e não negativos em pontos do índice.")
    return list(bounds)


def _filter_decision(session, limits, direction):
    if session["status"] != "eligible":
        return {"status": "excluded", "reasons": session["reasons"], "event": None}
    if not session["qualified_signals"]:
        return {"status": "no_breakout", "reasons": [], "event": None}
    event = session["qualified_signals"][0]
    band = session["range"]
    width = band["width_points"]
    distance = event["close"] - band["high"] if event["side"] == "long" else band["low"] - event["close"]
    reasons = []
    for name, measured in (("width", width), ("distance", distance)):
        bounds = limits[name]
        if bounds and not bounds[0] <= measured <= bounds[1]:
            reasons.append(f"{name}_outside_limits")
    if direction != "both" and direction != event["side"]:
        reasons.append("direction_filtered")
    return {"status": "blocked" if reasons else "accepted", "reasons": reasons, "event": event,
            "width_points": width, "distance_points": distance}
