"""F19-PESQ-07: referência causal do primeiro M15 e gatilho modelado em barras M1."""
from math import ceil, floor, isfinite

import pandas as pd

from mt5_extracao.analytics import instruments, opening_coverage
from mt5_extracao.resample import resample_bars
from mt5_extracao.strategies import indicators
from mt5_extracao.timeframes import Timeframe


WINDOW = pd.Timedelta(minutes=15)
BAND_PERIOD = 10


def _closed_m15(minutes, sessions, cutoff):
    """Grade por pregão em UTC. O histórico reinicia depois de lacuna, inclusive pregão ausente."""
    closes = []
    for session in sessions:
        if session["session_kind"] == "closed":
            continue
        opened = session["expected_open_utc"]
        if session["status"] != "eligible" or not opened or pd.Timestamp(opened).minute % 15:
            closes.append(pd.Series([float("nan")]))
            continue
        start, end = (pd.Timestamp(session[key]) for key in ("expected_open_utc", "expected_close_utc"))
        starts = pd.date_range(start, min(end, cutoff) - WINDOW, freq="15min")
        observed = minutes.loc[(minutes.index >= start) & (minutes.index + opening_coverage.MINUTE <= min(end, cutoff))]
        prices = resample_bars(observed, Timeframe.M1, Timeframe.M15).set_index("time")
        counts = observed.groupby(observed.index.floor("15min"))["valid"].agg(["size", "sum"])
        complete = counts["size"].eq(15) & counts["sum"].eq(15)
        closes.append(prices["close"].where(complete).reindex(starts))
    if not closes:
        return pd.DataFrame(columns=["ema", "sma", "deviation"])
    series = pd.concat(closes, ignore_index=False).astype(float)
    group = series.isna().cumsum()
    bands = pd.DataFrame({"ema": series.groupby(group).transform(lambda values: indicators.ema(values, BAND_PERIOD)),
                          "sma": series.groupby(group).transform(lambda values: indicators.sma(values, BAND_PERIOD)),
                          "deviation": series.groupby(group).transform(
                              lambda values: indicators.rolling_std(values, BAND_PERIOD, ddof=0, stable_window=True))})
    bands = bands.loc[series.notna()]
    return bands


def _side(opening, close, center, deviation):
    if close > opening and close > center + deviation:
        return "long"
    if close < opening and close < center - deviation:
        return "short"
    return None


def _entry_models(minutes, start, end, cutoff, signal, tick):
    if start >= end:
        return {"market": {"status": "session_closed"}, "pending": {"status": "session_closed"}}
    market = {"status": "awaiting_bar"}
    pending = {"status": "tick_size_unavailable"}
    level = None
    if tick is not None and isfinite(tick) and tick > 0:
        level = ((floor(signal["high"] / tick + 1e-9) + 1) if signal["side"] == "long"
                 else ceil(signal["low"] / tick - 1e-9) - 1) * tick
        pending = {"status": "pending", "trigger": level, "expires_at_utc": end.isoformat()}
    long = signal["side"] == "long"
    for stamp in pd.date_range(start, end, freq="min", inclusive="left"):
        observed_at = stamp + opening_coverage.MINUTE
        if observed_at > cutoff:
            break
        if stamp not in minutes.index or not minutes.at[stamp, "valid"]:
            if stamp == start:
                market = {"status": "unavailable_m1"}
            if level is not None:
                pending.update(status="unavailable_m1", missing_bar_utc=stamp.isoformat())
            break
        bar = minutes.loc[stamp]
        if stamp == start:
            market = {"status": "modeled", "reference_price": float(bar.open), "open_utc": stamp.isoformat(),
                      "observed_at_utc": observed_at.isoformat()}
        if level is not None:
            jumped = bar.open >= level if long else bar.open <= level
            touched = bar.high >= level if long else bar.low <= level
            if touched:
                pending.update(status="modeled", reference_price=float(bar.open) if jumped else level,
                               event="at_open" if jumped else "within_bar", bar_start_utc=stamp.isoformat(),
                               observed_at_utc=observed_at.isoformat())
                break
    if pending["status"] == "pending" and cutoff >= end:
        pending["status"] = "expired"
    return {"market": market, "pending": pending}


def report(snapshot, calendar, *, as_of):
    coverage = opening_coverage.report([snapshot], calendar, as_of=as_of)
    manifest = snapshot.manifest
    cutoff = pd.Timestamp(coverage["as_of"])
    minutes = snapshot.bars
    minutes["time"] = pd.to_datetime(minutes.time, format="ISO8601").dt.tz_localize("UTC") - pd.Timedelta(
        hours=manifest["time"]["broker_utc_offset_hours"])
    minutes = minutes.set_index("time", drop=False).rename_axis(None).sort_index()
    if not minutes.index.is_unique:
        raise ValueError("A referência Open Strike exige horários M1 únicos no snapshot.")
    price_columns = ["open", "high", "low", "close"]
    prices = minutes[price_columns].apply(pd.to_numeric, errors="coerce")
    minutes[price_columns] = prices
    minutes["valid"] = (prices.apply(lambda col: col.map(isfinite)).all(axis=1) &
                        prices.low.gt(0) & prices.open.between(prices.low, prices.high) &
                        prices.close.between(prices.low, prices.high) & (minutes.index.floor("min") == minutes.index))
    bands = _closed_m15(minutes, coverage["sessions"], cutoff)
    history = snapshot.instrument_history
    rows = []
    for session in coverage["sessions"]:
        row = {"date": session["date"], "status": session["status"], "reasons": list(session["reasons"]),
               "available_at_utc": None, "bands": None, "signal": None, "entries": None}
        rows.append(row)
        if session["status"] != "eligible":
            continue
        opening = pd.Timestamp(session["expected_open_utc"])
        available = opening + WINDOW
        row["available_at_utc"] = available.isoformat()
        if opening not in bands.index or bands.loc[opening].isna().any():
            row.update(status="unavailable_bands", reasons=["insufficient_complete_m15"])
            continue
        band = bands.loc[opening]
        first = minutes.loc[(minutes.index >= opening) & (minutes.index < available)]
        op, close = float(first.open.iloc[0]), float(first.close.iloc[-1])
        deviation = float(band.deviation)
        row["bands"] = {name: {"mid": float(band[name]), "upper": float(band[name]) + deviation,
                               "lower": float(band[name]) - deviation,
                               "side": _side(op, close, band[name], deviation)} for name in ("ema", "sma")}
        side = row["bands"]["ema"]["side"]
        row["status"] = "signal" if side else "no_signal"
        if not side:
            continue
        signal = {"side": side, "open": op, "close": close, "high": float(first.high.max()), "low": float(first.low.min())}
        row["signal"] = signal
        spec = instruments.specification(history, available) if history is not None else None
        tick = spec["payload"].get("tick_size") if spec else None
        limit = pd.Timestamp(row["date"] + " 17:00", tz=opening_coverage.VENUE_TZ).tz_convert("UTC")
        end = min(available + WINDOW, pd.Timestamp(session["expected_close_utc"]), limit,
                  pd.Timestamp(session["expiration"]["at_utc"]))
        row["entries"] = _entry_models(minutes, available, end, cutoff, signal, tick)
    return {"version": 1, "snapshot_id": snapshot.manifest_id, "coverage_manifest_id": coverage["manifest_id"],
            "contract": manifest["symbol"], "as_of": coverage["as_of"], "band_period": BAND_PERIOD,
            "band_deviation": "population_rolling_close", "execution_evidence": "modeled_m1_price_path_only",
            "sessions": rows}
