"""Madrid: confere propostas por janela observada, sem certificar ModifySL ou fonte."""
import hashlib
import math

import pandas as pd

from ..composition.schema import canonical_json
from ..strategies import get_strategy
from .strategy_context import read_journal, windows

_EMPTY = float.fromhex("0x1.fffffffffffffp+1023")


def _fail():
    raise ValueError("Madrid: contexto/janela/decisão divergente; preserve o diário e confira a primeira diferença.")


def validate(directory, params, signals):
    strategy = get_strategy("madrid", **params)
    digest = hashlib.sha256()
    by_event, decisions = {}, []
    for row, window in windows(read_journal(directory), _fail):
        frame = pd.DataFrame(window, columns=["at", "open", "high", "low", "close"])
        # O callback bruto antecede o filtro de lado do SL no motor nativo.
        out = pd.DataFrame(index=frame.index)
        strategy._signals(frame, out)
        expected = out.iloc[-1]
        close = window[-1][-1]
        distance = (params["distance_points"] * row["point"] if params["distance_points"] else
                    params["distance_atr"] * float(expected.atr))
        valid = lambda value: math.isfinite(value) and value != _EMPTY
        ready = (valid(distance) and distance > 0 and
                 valid(float(expected.stop_long)) and valid(float(expected.stop_short)))
        levels = dict.fromkeys(("stop_long", "stop_short", "target_long", "target_short", "guard_long", "guard_short"), 0.0)
        long = short = False
        if ready:
            levels.update(guard_long=float(expected.stop_long), guard_short=float(expected.stop_short))
            target_long, target_short = float(expected.target_long), float(expected.target_short)
            raw_long, raw_short = bool(expected.entry_long), bool(expected.entry_short)
            if params["distance_points"]:
                reference = target_long  # As duas colunas contêm M1, não TP.
                raw_long &= valid(reference) and abs(close-reference) <= distance
                raw_short &= valid(reference) and abs(close-reference) <= distance
                target_long, target_short = reference+distance, reference-distance
            selected = target_long if raw_long else target_short
            if (raw_long or raw_short) and valid(selected) and selected > 0:
                levels.update(stop_long=levels["guard_long"], stop_short=levels["guard_short"],
                              target_long=target_long, target_short=target_short)
                long = raw_long and 0 < levels["stop_long"] < close
                short = raw_short and levels["stop_short"] > close
        tolerance = max(1e-10, abs(close)*1e-12)
        if (any(not valid(value) or abs(row[key]-value) > tolerance for key, value in levels.items())
                or row["long"] != long or row["short"] != short):
            _fail()
        by_event[row["event_at"]] = row
        digest.update(canonical_json({"context": row, "window": window}).encode("utf-8"))
        if long or short:
            decisions.append({"event_at": row["event_at"], "direction": 1 if long else -1, **levels})
    for signal in signals:
        row = by_event.get(signal["event_at"])
        if (row is None or not row["long" if signal["direction"] == 1 else "short"] or
                signal["confirmed_at"] != row["confirmed_at"] or signal["available_at"] < row["available_at"]):
            _fail()
    return {"strategy": "madrid", "reference": "Python variant on each observed native closed window",
            "contexts": len(by_event), "trace_sha256": digest.hexdigest(), "decisions": decisions,
            "native_window_bars": 1500, "external_parity": False,
            "move_guard_execution_qualified": False, "source_qualified": False}
