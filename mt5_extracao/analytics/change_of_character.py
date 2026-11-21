"""F19-PESQ-09: virada do fechamento e quebra de estrutura confirmada em H4."""
from collections import deque
from math import ceil, floor, isfinite

import pandas as pd

from mt5_extracao.analytics import instruments
from mt5_extracao.analytics.retest_reference import _price_prefix
from mt5_extracao.strategies.indicators import causal_fractals


def _entry_reference(frame, i, trend, anchor, history):
    bar = frame.iloc[i]
    available = pd.Timestamp(bar.available_at_utc)
    spec = instruments.specification(history, available) if history is not None else None
    tick = spec["payload"].get("tick_size") if spec else None
    if tick is None or not isfinite(tick) or tick <= 0:
        return {"status": "tick_size_unavailable"}
    if i + 1 == len(frame):
        return {"status": "waiting_next_h4"}
    following = frame.iloc[i + 1]
    start = pd.Timestamp(following.bar_start_utc)
    if following["_pivot_reset_before"] or not isfinite(following.open) or start < available:
        return {"status": "next_open_unavailable"}
    stop = (floor(anchor / tick + 1e-9) + 1 if trend > 0 else ceil(anchor / tick - 1e-9) - 1) * tick
    entry = float(following.open)
    risk = (stop - entry) * trend
    if risk <= 0 or stop <= 0:
        return {"status": "gap_through_stop", "reference_price": entry, "stop": stop}
    target = entry - trend * risk
    if not isfinite(target) or target <= 0:
        return {"status": "target_invalid", "reference_price": entry, "stop": stop}
    return {"status": "modeled", "reference_price": entry, "open_utc": start.isoformat(),
            "observed_at_utc": following.available_at_utc, "stop": stop, "target_1r": target}


def report(snapshot, *, as_of, session_mode="observed"):
    if snapshot.manifest["timeframe"] != "H4":
        raise ValueError("A referência CHoCH exige barras H4 no relógio declarado do snapshot.")
    prepared, frame, cutoff = _price_prefix(snapshot, as_of, session_mode, reset_by_session=False)
    confirmed = causal_fractals(frame)
    levels = {"top": deque(maxlen=2), "bottom": deque(maxlen=2)}
    previous, slope, trend = None, 0, 0
    used = set()
    turns, breaks = [], []
    history = snapshot.instrument_history
    for i, bar in enumerate(frame.itertuples(index=False)):
        pivots = confirmed[i]
        if frame["_pivot_reset_before"].iat[i] or not pivots["valid"]:
            previous, slope, trend = None, 0, 0
            used.clear()
            for values in levels.values():
                values.clear()
        if not pivots["valid"]:
            continue
        if previous is not None:
            change = int(bar.close > previous) - int(bar.close < previous)
            event = {"bar_start_utc": bar.bar_start_utc, "available_at_utc": bar.available_at_utc,
                     "close": float(bar.close), "session_id": bar.session_id}
            if change:
                if slope and slope != change:
                    turns.append({**event, "side": "long" if change > 0 else "short"})
                slope = change

            if trend:
                broken, anchor = (levels[key][-1] for key in (("bottom", "top") if trend > 0 else ("top", "bottom")))
                price = broken["price"]
                crossed = (previous - price) * trend >= 0 and (bar.close - price) * trend < 0
                identity = (broken["kind"], broken["origin"])
                if crossed and identity not in used:
                    used.add(identity)
                    breaks.append({**event, "side": "short" if trend > 0 else "long", "broken_swing_price": price,
                                   "swing_origin_utc": frame.bar_start_utc.iat[broken["origin"]],
                                   "swing_available_at_utc": frame.available_at_utc.iat[broken["confirmation"]],
                                   "stop_reference_price": anchor["price"],
                                   "entry": _entry_reference(frame, i, trend, anchor["price"], history)})

        for key, values in levels.items():
            pivot = pivots[key]
            if pivot is not None and pivot["confirmation"] == i:
                values.append(pivot)
        if all(len(values) == 2 for values in levels.values()):
            moves = [values[1]["price"] - values[0]["price"] for values in levels.values()]
            if all(delta > 0 for delta in moves):
                trend = 1
            elif all(delta < 0 for delta in moves):
                trend = -1
        previous = bar.close

    return {"version": 1, "snapshot_id": prepared["manifest_id"], "symbol": prepared["symbol"], "timeframe": "H4",
            "as_of": cutoff.isoformat(), "session_basis": session_mode, "clock_basis": prepared["clock_basis"],
            "pivot": "strict_fractal_5", "continuity": "contiguous_h4_only", "close_turns": turns, "swing_breaks": breaks}
