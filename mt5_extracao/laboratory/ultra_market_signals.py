"""Paridade de decisões de barra no Tester usando o prefixo registrado pelo Expert."""
from collections import defaultdict
from hashlib import file_digest
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

from ..robots.mt5_tools import read_log_text
from ..strategies import get_strategy, indicators
from .ultra_market_portfolio_audit import _records


_BITS = ("entry_long", "entry_short", "exit_long", "exit_short")
_LEVELS = ("stop_long", "stop_short", "target_long", "target_short")
_FIELDS = (*_BITS, *_LEVELS)
_OPTIONS = {
    "orb": {"range_minutes": "InpRangeMinutes", "target_mult": "InpTargetMult", "min_range_atr": "InpMinRangeAtr"},
    "s09_reference": {"fast": "InpFastPeriod", "slow": "InpSlowPeriod", "warmup_bars": "InpWarmupBars",
                      "stop_points": "InpStopPoints", "target_points": "InpTargetPoints"},
    "vwap_bands": {"entry_band_pct": "InpEntryBandPct", "target_band_pct": "InpTargetBandPct",
                   "stop_offset_points": "InpStopPoints", "volume_source": "InpVolumeSource"},
}


def _reference(slot):
    inputs = slot["inputs"]
    return get_strategy(slot["key"], **{name: json.loads(inputs[param]) for name, param in
                                       _OPTIONS.get(slot["key"], {}).items()})


def _raw(model, rates):
    expected = pd.DataFrame({field: False for field in _BITS} | {field: np.nan for field in _LEVELS},
                            index=rates.index)
    expected["atr"] = indicators.atr(rates, 14)
    model._signals(rates, expected)
    return expected


def _decisions(slot, records, rates, indices, point):
    model = _reference(slot)
    offset = model.exit_rules().signal_stop_offset_points * point
    key = slot["key"]
    # A janela de 1500 barras contém todo o dia M1, usado por ORB e VWAP.
    indexed = _raw(model, rates) if key in ("orb", "vwap_bands") else None
    if key == "s09_reference" and records:
        closed = int(records[0][0][4])
        anchor = indices[closed] + 1 - model.params["warmup_bars"]
        indexed = _raw(model, rates.iloc[anchor:])
    counts = dict(observations=0, raw_matches=0, operable_matches=0, raw_level_checks=0, raw_level_matches=0,
                  positive_signal_bars=0, invalid_stop=0, missing_prefix=0)
    first = {}
    for raw, bar in records:
        _, msc, _, start, closed, n, *observed = raw
        start, closed, n = int(start), int(closed), int(n)
        if start not in indices or closed not in indices or indices[closed] - indices[start] + 1 != n:
            counts["missing_prefix"] += 1
            continue
        if indexed is None:
            expected = _raw(model, rates.iloc[indices[start]:indices[closed] + 1]).iloc[-1]
        else:
            expected = indexed.loc[closed]
        counts["observations"] += 1
        when = dict(tick_msc=int(msc), bar_at=closed)
        raw_bits = [bool(expected[name]) for name in _BITS]
        raw_values = dict(zip(_FIELDS, observed, strict=True))
        operable = raw_bits.copy()
        close = rates.at[closed, "close"]
        levels = {name: float(expected[name]) for name in _LEVELS}
        if key == "s09_reference":
            levels["stop_long"] = close - point * model.params["stop_points"]
            levels["stop_short"] = close + point * model.params["stop_points"]
        else:
            levels["stop_long"] -= offset
            levels["stop_short"] += offset
            if points := model.exit_rules().signal_target_points:
                levels["target_long"], levels["target_short"] = close + point * points, close - point * points
        for i, side in enumerate(("long", "short")):
            stop = levels["stop_" + side]
            operable[i] = raw_bits[i] and stop > 0 and (stop < close if i == 0 else stop > close)
        counts["positive_signal_bars"] += any(raw_bits[:2])
        counts["invalid_stop"] += any(raw_bits[i] and not operable[i] for i in (0, 1))

        def compare(stage, fields, want, got, matches):
            difference = next((dict(field=name, expected=left, observed=right)
                               for name, left, right in zip(fields, want, got, strict=True)
                               if (abs(left - right) > point / 2 + 1e-12 if stage == "levels" else left != right)), None)
            if difference is None:
                counts[matches] += 1
            else:
                first.setdefault(stage, dict(when, **difference))

        compare("raw", _BITS, raw_bits, [raw_values[name] == "1" for name in _BITS], "raw_matches")
        compare("operable", _BITS, operable, [value == "1" for value in bar[4:8]], "operable_matches")
        relevant = [name for i, side in enumerate(("long", "short")) if raw_bits[i]
                    for name in ("stop_" + side, "target_" + side) if math.isfinite(levels[name])]
        if relevant:
            counts["raw_level_checks"] += 1
            compare("levels", relevant, [levels[name] for name in relevant],
                    [float(raw_values[name]) for name in relevant], "raw_level_matches")
    return dict(key=key, **counts, first_divergence=first)


def compare(source, *, strategies=None):
    receipt_path = Path(source)
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    journal = receipt_path.parent / receipt["journal"]
    with journal.open("rb") as stream:
        if file_digest(stream, "sha256").hexdigest() != receipt["journal_sha256"]:
            raise ValueError("journal_sha256: journal diverge do recibo original.")
    records = _records(read_log_text(journal))
    rates = {int(at): dict(open=float(op), high=float(high), low=float(low), close=float(close),
                           tick_volume=int(ticks), real_volume=int(real), time=pd.Timestamp(int(at), unit="s"))
             for at, op, high, low, close, ticks, real in records["RATE"]}
    if not rates or len(rates) != len(records["RATE"]):
        raise ValueError("RATE: prefixo ausente ou duplicado no journal.")
    frame = pd.DataFrame.from_dict(rates, orient="index").sort_index()
    indices = {at: i for i, at in enumerate(frame.index)}
    pairs = defaultdict(list)
    for raw, bar in zip(records["RAW"], records["BAR"], strict=True):
        if raw[:3] + raw[4:5] != bar[:4]:
            raise ValueError("RAW/BAR: as decisões brutas e operáveis não correspondem.")
        pairs[int(raw[0])].append((raw, bar))
    point = float(receipt["comparison"]["units"]["tick_size"])
    supported = {slot["key"] for slot in receipt["slots"]} - {"pattern_definition", "super_atr_triple"}
    selected = supported if strategies is None else set(strategies)
    if not selected or selected - supported:
        raise ValueError("strategy: escolha estratégias Python presentes neste recibo.")
    return dict(attempt=receipt["attempt"], preset_sha256=receipt["preset_sha256"], journal_sha256=receipt["journal_sha256"],
                results=[_decisions(slot, pairs[slot["magic"]], frame, indices, point)
                         for slot in receipt["slots"] if slot["key"] in selected])
