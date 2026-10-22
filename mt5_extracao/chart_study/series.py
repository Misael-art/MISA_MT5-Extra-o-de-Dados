"""Séries para estudo sobre OHLC congelado, sem simular posições ou enviar ordens."""
import hashlib
import math

import pandas as pd

from mt5_extracao.analytics import indicator_registry
from mt5_extracao.strategies import get_strategy
from . import catalog, snapshot


def _sign(value):
    return 1 if value > 0 else -1 if value < 0 else 0


def build(capture, ident):
    snapshot.validate_capture(capture)
    spec = catalog.get(ident)
    frame = pd.DataFrame(capture.get("rates", []))
    rows = []
    if not frame.empty:
        # Preserva o relógio nativo do candle; não converte fuso da corretora nem
        # afirma que o buffer observado já existia naquele instante histórico.
        frame["time"] = pd.to_datetime(frame["time"], unit="s")
        if ident.startswith("strategy:"):
            strategy = get_strategy(ident.removeprefix("strategy:"))
            signals = strategy.gerar_sinais(frame)
            for position, (at, signal) in enumerate(signals.iterrows()):
                available = position + 1 >= strategy.min_bars
                long, short = bool(signal["entry_long"]), bool(signal["entry_short"])
                value = (1 if long else -1 if short else 0) if available else None
                reason = "LONG_PRIORITY" if long and short else "ENTRY_LONG" if long else "ENTRY_SHORT" if short else "NO_ENTRY"
                rows.append(dict(time=capture["rates"][position]["time"], value=value, direction=value,
                                 state="AVAILABLE" if available else "WARMUP", reason=reason if available else "MIN_BARS"))
        else:
            feature = ident.removeprefix("feature:")
            values = list(indicator_registry.iter_core(frame, feature_ids=[feature]))
            previous = None
            for row in values:
                index = frame.index.get_loc(row["bar_index"])
                value, state = row["value"], row["state"]
                if value is not None and (not math.isfinite(value) or state != "AVAILABLE"):
                    value = None
                direction = None
                if value is not None:
                    rule = spec["rule"]
                    if rule == "price":
                        direction = _sign(frame.iloc[index]["close"] - value)
                    elif rule == "midpoint_50":
                        direction = _sign(value - 50)
                    elif rule == "midpoint_half":
                        direction = _sign(value - .5)
                    elif rule == "zero":
                        direction = _sign(value)
                    elif previous is not None:
                        direction = _sign(value - previous)
                rows.append(dict(time=capture["rates"][index]["time"], value=value, direction=direction,
                                 state=state, reason=row.get("reason", "")))
                previous = value
    return dict(contract=1, feature=spec, symbol=capture["symbol"], timeframe=capture["timeframe"],
                source_sha256=hashlib.sha256(snapshot.json_text(capture).encode()).hexdigest(), rows=rows,
                basis="Canonical extractor formulas/signals over frozen MT5 bars; no position simulation or management acceptance")
