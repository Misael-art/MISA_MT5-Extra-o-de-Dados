"""HA/HiLo: propostas por janela observada, sem prova de fechamento/reversão."""
import hashlib
import math

import pandas as pd

from ..composition.schema import canonical_json
from ..strategies import get_strategy
from .strategy_context import read_journal, windows

_EMPTY = float.fromhex("0x1.fffffffffffffp+1023")


def _fail():
    raise ValueError("HA/HiLo: contexto/janela/decisão divergente; preserve o diário e confira a primeira diferença.")


def validate(directory, params, signals):
    strategy = get_strategy("ha_hilo", **params)
    digest = hashlib.sha256()
    by_event, decisions = {}, []
    for row, window in windows(read_journal(directory), _fail):
        frame = pd.DataFrame(window, columns=["at", "open", "high", "low", "close"])
        # O callback espera a coluna do Strategy; ATR não participa desta referência.
        out = pd.DataFrame({"atr": float("nan")}, index=frame.index)
        strategy._signals(frame, out)
        expected, close = out.iloc[-1], window[-1][-1]
        levels = dict.fromkeys(("stop_long", "stop_short", "target_long", "target_short", "guard_long", "guard_short"), 0.0)
        valid = lambda value: math.isfinite(value) and value != _EMPTY
        raw_long, raw_short = bool(expected.entry_long), bool(expected.entry_short)
        long = short = emitted = False
        buffer = params["stop_buffer_points"] * row["point"]
        if raw_long or raw_short:
            stop_long, stop_short = float(expected.stop_long)-buffer, float(expected.stop_short)+buffer
            if valid(buffer) and buffer >= 0 and valid(stop_long) and valid(stop_short):
                emitted = True
                levels.update(stop_long=stop_long, stop_short=stop_short)
                long = raw_long and 0 < stop_long < close
                short = raw_short and stop_short > close
        tolerance = max(1e-10, abs(close)*1e-12)
        if (any(not valid(value) or abs(row[key]-value) > max(tolerance, 4*math.ulp(value)) for key, value in levels.items())
                or row["long"] != long or row["short"] != short):
            _fail()
        by_event[row["event_at"]] = row
        digest.update(canonical_json({"context": row, "window": window}).encode("utf-8"))
        if emitted:
            decisions.append({"event_at": row["event_at"], "direction": 1 if raw_long else -1,
                              "entry_allowed_by_reference": bool(long or short),
                              "exit_intent_from_reference": "short" if raw_long else "long", **levels})
    for signal in signals:
        row = by_event.get(signal["event_at"])
        if (row is None or not row["long" if signal["direction"] == 1 else "short"] or
                signal["confirmed_at"] != row["confirmed_at"] or signal["available_at"] < row["available_at"]):
            _fail()
    return {"strategy": "ha_hilo", "reference": "Python variant on each observed native closed window",
            "contexts": len(by_event), "trace_sha256": digest.hexdigest(), "decisions": decisions,
            "native_window_bars": 1500, "external_parity": False, "source_qualified": False,
            "reversal_execution_qualified": False, "execution_levels_qualified": False}
