"""Quatro famílias iniciais T13.4-P06 traduzidas para a gramática P02."""

from decimal import Decimal, InvalidOperation
import hashlib
from pathlib import Path

import pandas as pd

from .hypotheses import compile_definition


FAMILIES = ("reversal", "continuation_time", "gap", "breakout_failure")
PARAMETERS = {
    "reversal": {"drop_fraction"},
    "continuation_time": {"rise_fraction", "session_start_minute", "session_end_minute"},
    "gap": {"gap_fraction"},
    "breakout_failure": {"breakout_fraction", "lookback_bars"},
}


def _fraction(value, label):
    if not isinstance(value, str):
        raise ValueError(f"{label} exige fração decimal textual explícita.")
    try:
        number = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"{label} não é decimal.") from exc
    if not number.is_finite() or not 0 < number < 1:
        raise ValueError(f"{label} precisa estar entre zero e um.")
    return str(number)


def _field(name):
    return {"op": "field", "name": name}


def _literal(value, unit):
    return {"op": "literal", "value": str(value), "unit": unit}


def _compare(left, op, right):
    return {"op": "compare", "comparison": op, "left": left, "right": right}


def _change(value, reference):
    return {"op": "percent_change", "value": _field(value), "reference": _field(reference)}


def compile_family(family, parameters, *, symbol, timeframe, clock_version,
                   calendar_version, cost_model_id, horizon_bars, origin="exploratory"):
    """Requer parâmetros explícitos; referência de rompimento é calculada no passado."""
    if family not in FAMILIES or not isinstance(parameters, dict) or set(parameters) != PARAMETERS.get(family):
        raise ValueError("Família ou parâmetros explícitos incompatíveis.")
    if not all(isinstance(value, str) and value for value in
               (symbol, timeframe, clock_version, calendar_version, cost_model_id)):
        raise ValueError("Fonte temporal, ativo, timeframe e custo precisam de versões explícitas.")
    side, context, reference = "long", None, "prior_close"
    if family == "reversal":
        fraction = _fraction(parameters["drop_fraction"], "Queda")
        condition = _compare(_change("close", "prior_close"), "lte", _literal("-" + fraction, "fraction"))
    elif family == "continuation_time":
        fraction = _fraction(parameters["rise_fraction"], "Alta")
        start, end = parameters["session_start_minute"], parameters["session_end_minute"]
        if type(start) is not int or type(end) is not int or not 0 <= start < end <= 1440:
            raise ValueError("Faixa de minutos da sessão inválida.")
        context = {"op": "and", "terms": [
            _compare(_field("session_minute"), "gte", _literal(start, "minutes")),
            _compare(_field("session_minute"), "lt", _literal(end, "minutes"))]}
        condition = _compare(_change("close", "prior_close"), "gte", _literal(fraction, "fraction"))
    elif family == "gap":
        fraction = _fraction(parameters["gap_fraction"], "Gap")
        context = _compare(_field("session_minute"), "lt", _literal(1, "minutes"))
        condition = _compare(_change("open", "prior_close"), "gte", _literal(fraction, "fraction"))
    else:
        fraction = _fraction(parameters["breakout_fraction"], "Rompimento")
        lookback = parameters["lookback_bars"]
        if type(lookback) is not int or not 2 <= lookback <= 500:
            raise ValueError("Referência de rompimento exige 2 a 500 barras passadas.")
        side, reference = "short", "reference_price"
        condition = {"op": "and", "terms": [
            _compare(_change("high", "reference_price"), "gte", _literal(fraction, "fraction")),
            _compare(_field("close"), "lt", _field("reference_price"))]}
    signature = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    return compile_definition({
        "schema_version": 1, "pattern_id": family, "strategy_version": "initial_v1",
        "original_hypothesis": "Família inicial " + family, "origin": origin,
        "parent_attempt_ids": [], "universe_definition": {"mode": "explicit", "symbols": [symbol]},
        "data_requirements": ["ohlcv", *(["observed_session_bars"] if family in ("continuation_time", "gap") else [])],
        "timeframe": timeframe, "timezone_calendar_versions": {"clock": clock_version,
                                                                  "calendar": calendar_version},
        "features_and_versions": [], "context_expression": context,
        "condition_expression": condition, "decision_clock": "closed_bar_available",
        "price_reference": reference, "side": side,
        "simulated_entry": "next_executable_open", "outcome_definition": {"horizon_bars": horizon_bars},
        "repetition_policy": "all", "invalidation_rules": [], "execution_policy": "research_only",
        "cost_model_id": cost_model_id, "complexity": {"family": family, "parameters": parameters},
        "implementation_signature": "sha256:" + signature,
    })


def prior_breakout_references(observations, lookback_bars):
    """Anexa máxima anterior sem usar high/close da barra corrente ou futura."""
    if type(lookback_bars) is not int or not 2 <= lookback_bars <= 500:
        raise ValueError("Lookback fora do contrato da família.")
    def utc(value):
        stamp = pd.Timestamp(value)
        if pd.isna(stamp) or stamp.tzinfo is None:
            raise ValueError("Referência exige relógio UTC explícito.")
        return stamp.tz_convert("UTC")

    result, previous = [], []
    identity, last_start = None, None
    for raw in observations:
        row = dict(raw)
        current_identity = (row["source_namespace"], row["symbol"], row["timeframe"])
        start, available, end = (utc(row[key]) for key in
                                 ("bar_start_utc", "available_at_utc", "bar_end_utc"))
        if (identity is not None and current_identity != identity) or (last_start is not None and start <= last_start):
            raise ValueError("Referência exige uma série única e barras em ordem causal.")
        if previous and previous[-1][2] > start:
            raise ValueError("Referência anterior ainda não havia fechado.")
        if previous and previous[-1][1] > available:
            raise ValueError("Referência anterior ainda não estava disponível.")
        identity, last_start = current_identity, start
        if len(previous) >= lookback_bars:
            row["reference_price"] = str(max(value for value, _, _ in previous[-lookback_bars:]))
            row["reference_available_at_utc"] = previous[-1][1].isoformat()
        else:
            row["reference_price"] = None
        if row["high"] is not None:
            try:
                high = Decimal(str(row["high"]))
            except InvalidOperation as exc:
                raise ValueError("Máxima anterior inválida.") from exc
            if not high.is_finite():
                raise ValueError("Máxima anterior inválida.")
            previous.append((high, available, end))
        result.append(row)
    return result
