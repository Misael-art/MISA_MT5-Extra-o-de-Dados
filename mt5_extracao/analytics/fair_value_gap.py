"""F19-PESQ-10: formação, visitas e validade causal de zonas em três barras."""
from math import isfinite

from mt5_extracao.analytics.retest_reference import _price_prefix


VALID_BARS = 5


def _zone(a, b, c, position):
    if c.low > a.high:
        side, lower, upper = "long", float(a.high), float(c.low)
    elif c.high < a.low:
        side, lower, upper = "short", float(c.high), float(a.low)
    else:
        return None
    amplitude = float(b.high - b.low)
    midpoint = float((b.high + b.low) / 2)
    directional = b.close > b.open if side == "long" else b.close < b.open
    body_ratio = abs(float(b.close - b.open)) / amplitude if amplitude > 0 else None
    inside = lower <= midpoint <= upper
    return {"side": side, "lower": lower, "upper": upper, "zone_midpoint": (lower + upper) / 2,
            "impulse_midpoint": midpoint, "impulse_body_ratio": body_ratio,
            "impulse_variant_eligible": bool(inside and directional and body_ratio is not None and body_ratio >= .5),
            "source_bars_utc": [a.bar_start_utc, b.bar_start_utc, c.bar_start_utc],
            "available_at_utc": c.available_at_utc, "formed_at_position": position,
            "first_touches": {}, "status": "open", "ended_at_utc": None}


def _observe(zone, bar, age):
    lower, upper = zone["lower"], zone["upper"]
    long = zone["side"] == "long"
    violated = bar.low < lower if long else bar.high > upper
    overlap = bar.low <= upper and bar.high >= lower
    if overlap:
        levels = {"edge": upper if long else lower, "zone_midpoint": zone["zone_midpoint"]}
        if zone["impulse_variant_eligible"]:
            levels["impulse_midpoint"] = zone["impulse_midpoint"]
        depth = (upper - max(lower, bar.low)) if long else (min(upper, bar.high) - lower)
        for name, price in levels.items():
            if bar.low <= price <= bar.high:
                zone["first_touches"].setdefault(name, {"price": price, "bars_after_formation": age,
                                                         "bar_start_utc": bar.bar_start_utc,
                                                         "available_at_utc": bar.available_at_utc,
                                                         "depth_fraction": float(depth / (upper - lower)),
                                                         "violation_same_bar": bool(violated),
                                                         "fill_status": "unverified_ohlc"})
    if violated or age == VALID_BARS:
        zone.update(status="violated" if violated else "expired", ended_at_utc=bar.available_at_utc)


def report(snapshot, *, as_of, session_mode="observed"):
    prepared, frame, cutoff = _price_prefix(snapshot, as_of, session_mode)
    open_zones, zones, recent = [], [], []
    for i, bar in enumerate(frame.itertuples(index=False)):
        valid = isfinite(bar.close)
        if not valid or frame["_pivot_reset_before"].iat[i]:
            for zone in open_zones:
                zone.update(status="interrupted", ended_at_utc=bar.available_at_utc)
            open_zones, recent = [], []
        if not valid:
            continue
        for zone in open_zones:
            _observe(zone, bar, i - zone["formed_at_position"])
        open_zones = [zone for zone in open_zones if zone["status"] == "open"]
        if len(recent) == 2:
            zone = _zone(*recent, bar, i)
            if zone is not None:
                zones.append(zone)
                open_zones.append(zone)
        recent = [*recent[-1:], bar]

    return {"version": 1, "snapshot_id": prepared["manifest_id"], "symbol": prepared["symbol"],
            "timeframe": prepared["timeframe"], "as_of": cutoff.isoformat(), "session_basis": session_mode,
            "clock_basis": prepared["clock_basis"], "valid_bars": VALID_BARS, "overlap_policy": "independent_zones",
            "zones": [{key: value for key, value in zone.items() if key != "formed_at_position"} for zone in zones]}
