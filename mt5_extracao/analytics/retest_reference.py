"""F19-PESQ-08: quebras e revisitas de fractais conhecidos no fechamento."""
from math import isfinite

import pandas as pd

from mt5_extracao.analytics.pattern_flow import snapshot_observations
from mt5_extracao.strategies.indicators import causal_fractals


def _events(frame, tolerance, horizon):
    levels = {"top": None, "bottom": None}
    events = []
    pivots = causal_fractals(frame)
    for pos, bar in enumerate(frame.itertuples(index=False)):
        if frame["_pivot_reset_before"].iat[pos] or not pivots[pos]["valid"]:
            levels = {"top": None, "bottom": None}
        if not pivots[pos]["valid"]:
            continue
        for kind, direction in (("top", 1), ("bottom", -1)):
            level = levels[kind]
            if level is not None:
                price = level["price"]
                touched = bar.low <= price + tolerance and bar.high >= price - tolerance
                revisited = touched and not level["touching"]
                signed_close = direction * (bar.close - price)
                event = {"side": "long" if direction == 1 else "short", "level": price,
                         "pivot_origin_utc": level["origin"], "pivot_available_at_utc": level["confirmed"],
                         "bar_start_utc": bar.bar_start_utc, "available_at_utc": bar.available_at_utc,
                         "session_id": bar.session_id, "prior_touches": level["prior_touches"]}
                if level["broken"] is None:
                    if signed_close > tolerance:
                        level.update(broken=pos, break_available=bar.available_at_utc, touching=False)
                        events.append({**event, "kind": "break"})
                    else:
                        level["prior_touches"] += int(revisited)
                        level["touching"] = touched
                elif pos - level["broken"] > horizon:
                    levels[kind] = None
                else:
                    if revisited:
                        level["retests"] += 1
                        events.append({**event, "kind": "retest", "retest_number": level["retests"],
                                       "break_available_at_utc": level["break_available"],
                                       "held_at_close": signed_close > tolerance})
                    level["touching"] = touched
                    if signed_close < -tolerance:
                        levels[kind] = None

            pivot = pivots[pos][kind]
            active = levels[kind]
            if pivot is not None and pivot["confirmation"] == pos and (active is None or active["broken"] is None):
                levels[kind] = {"price": pivot["price"], "origin": frame.bar_start_utc.iat[pivot["origin"]],
                                "confirmed": bar.available_at_utc, "broken": None,
                                "prior_touches": 0, "retests": 0, "touching": False}
    return events


def _price_prefix(snapshot, as_of, session_mode, *, reset_by_session=True):
    cutoff = pd.Timestamp(as_of)
    if pd.isna(cutoff) or cutoff.tzinfo is None:
        raise ValueError("O corte da referência precisa de fuso explícito.")
    cutoff = cutoff.tz_convert("UTC")
    prepared = snapshot_observations(snapshot, session_mode=session_mode)
    frame = prepared["backtest_bars"].assign(session_id=[item["session_id"] for item in prepared["observations"]])
    frame = frame.loc[pd.to_datetime(frame.available_at_utc, utc=True, format="ISO8601").le(cutoff)].reset_index(drop=True)
    starts = pd.to_datetime(frame.bar_start_utc, utc=True, format="ISO8601")
    ends = pd.to_datetime(frame.bar_end_utc, utc=True, format="ISO8601")
    frame["_pivot_reset_before"] = starts.ne(ends.shift())
    if reset_by_session:
        frame["_pivot_reset_before"] |= frame.session_id.ne(frame.session_id.shift())
    prices = frame[["open", "high", "low", "close"]].apply(pd.to_numeric, errors="coerce")
    valid = (prices.apply(lambda col: col.map(isfinite)).all(axis=1) & prices.low.gt(0) &
             prices.open.between(prices.low, prices.high) & prices.close.between(prices.low, prices.high))
    frame[["open", "high", "low", "close"]] = prices.where(valid)
    return prepared, frame, cutoff


def report(snapshot, *, as_of, tolerance_price, horizon_bars, session_mode="observed"):
    if (type(tolerance_price) not in (int, float) or not isfinite(tolerance_price) or tolerance_price < 0 or
            type(horizon_bars) is not int or horizon_bars < 1):
        raise ValueError("Informe tolerância finita em preço e horizonte de ao menos uma barra.")
    prepared, frame, cutoff = _price_prefix(snapshot, as_of, session_mode)
    return {"version": 1, "snapshot_id": prepared["manifest_id"], "symbol": prepared["symbol"],
            "timeframe": prepared["timeframe"], "as_of": cutoff.isoformat(), "tolerance_price": tolerance_price,
            "horizon_bars": horizon_bars, "pivot": "strict_fractal_5", "session_basis": session_mode,
            "clock_basis": prepared["clock_basis"], "events": _events(frame, tolerance_price, horizon_bars)}
