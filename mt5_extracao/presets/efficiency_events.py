"""Janela ER nativa conferida por Decimal e pelo indicador canônico existente."""
from decimal import Decimal
import math
import re
from ..strategies.temporal_er import ER_FEATURE, ER_EQUALITY_BAND

CONTEXT = re.compile(r" er_value=(\S+) er_observed_msc=(\d+) er_source_count=(\d+) er_bar_open_at=(\d+) er_times=(\S+) er_closes=(\S+)")


def read_context(suffix, decision_msc):
    match = CONTEXT.fullmatch(suffix.rstrip())
    if match is None:
        raise ValueError("Contexto ER ausente/desconhecido; preservar diário e conferir versão do perfil.")
    value, observed, count, bar_open, times, closes = match.groups()
    observed, count, bar_open = map(int, (observed, count, bar_open))
    if value == "unavailable":
        if count or observed or bar_open or times != "none" or closes != "none":
            raise ValueError("Ausência ER contém observação presumida; não preencher como zero.")
        return {"er_feature": ER_FEATURE, "er_value": None, "er_observed_msc": None}, None
    raw_times = [int(v) for v in times.split(",")]
    raw_closes = [Decimal(v) for v in closes.split(",")]
    actual = float(value)
    if (count != 21 or len(raw_times) != 21 or len(raw_closes) != 21 or observed != decision_msc
            or bar_open * 1000 > observed or any(t <= 0 or t >= bar_open for t in raw_times)
            or any(a >= b for a, b in zip(raw_times, raw_times[1:]))
            or not all(c.is_finite() and c > 0 for c in raw_closes)
            or not math.isfinite(actual) or not 0 <= actual <= 1):
        raise ValueError("Janela ER incompleta, inválida ou futura; confirmar somente21 closes fechados recebidos.")
    path = sum((abs(b-a) for a, b in zip(raw_closes, raw_closes[1:])), Decimal(0))
    reference = float(abs(raw_closes[-1]-raw_closes[0])/path) if path else 0.
    from ..strategies.indicators import efficiency_ratio
    import pandas as pd
    canonical = float(efficiency_ratio(pd.Series([float(v) for v in raw_closes]), 20).iloc[-1])
    if not math.isfinite(canonical) or abs(reference-actual) > ER_EQUALITY_BAND or abs(reference-canonical) > ER_EQUALITY_BAND:
        raise ValueError("ER do diário diverge da referência independente/canônica; não ampliar tolerância.")
    return dict(er_feature=ER_FEATURE, er_value=actual, er_reference=reference,
                er_observed_msc=observed, er_bar_open_at=bar_open,
                er_source_times=raw_times, er_source_closes=closes.split(",")), reference
