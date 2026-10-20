"""Tipos e unidades da composição; não é outro registry de indicadores.

Suporte é decidido pelo adaptador instalado, nunca por uma declaração importada.
"""
from __future__ import annotations

from dataclasses import dataclass
import math


@dataclass(frozen=True)
class Parameter:
    kind: str
    unit: str
    minimum: float = 0
    maximum: float | None = None
    exclusive_minimum: bool = False

    def accepts(self, value):
        if self.kind == "bool":
            return type(value) is bool
        if type(value) not in (int, float):
            return False
        try:
            finite = math.isfinite(value)
        except OverflowError:
            finite = False
        if not finite:
            return False
        if self.kind == "int" and type(value) is not int:
            return False
        return (value > self.minimum if self.exclusive_minimum else value >= self.minimum) and (
            self.maximum is None or value <= self.maximum)


PERIOD = Parameter("int", "bar", 1)
POSITIVE = Parameter("number", "multiplier", exclusive_minimum=True)
ATR = Parameter("number", "ATR", exclusive_minimum=True)
OSCILLATOR = Parameter("number", "index_0_100", 0, 100)
# Nomes explícitos: não converter nome Python em nome de input MQL5.
STRATEGY_PARAMETERS = {
    "s09_reference": {"fast": PERIOD, "slow": PERIOD, "warmup_bars": PERIOD,
                      "stop_points": Parameter("int", "point", 1),
                      "target_points": Parameter("int", "point", 1)},
    "ema_atr": {"fast": PERIOD, "slow": PERIOD, "trend": PERIOD, "stop_atr": ATR, "trail_atr": ATR},
    "donchian": {"entry_period": PERIOD, "exit_period": PERIOD, "adx_min": OSCILLATOR,
                 "atr_avg": PERIOD, "stop_atr": ATR},
    "bb_rsi": {"bb_period": PERIOD, "bb_k": POSITIVE, "rsi_period": PERIOD,
               "rsi_low": Parameter("number", "index_0_100", 0, 50), "adx_max": OSCILLATOR,
               "stop_atr": ATR, "max_bars": PERIOD},
    "orb": {"range_minutes": Parameter("number", "minute", exclusive_minimum=True),
            "target_mult": POSITIVE, "min_range_atr": Parameter("number", "ATR")},
    "pullback": {"trend": PERIOD, "adx_min": OSCILLATOR, "rsi_period": PERIOD,
                 "rsi_pullback": Parameter("number", "index_0_100", 0, 50), "swing_bars": PERIOD,
                 "target_r": Parameter("number", "R", exclusive_minimum=True)},
    "squeeze": {"period": PERIOD, "bb_k": POSITIVE, "kc_mult": POSITIVE, "lookback": PERIOD,
                "stop_atr": ATR, "trail_atr": ATR},
}

# O schema pode representar rascunho indisponível. Só o modo base tem adaptador.
MODULE_PARAMETERS = {
    "orientation": {"normal": {}, "inverted": {}},
    "entry": {"market_next_open": {}, "limit": {"distance_points": Parameter("number", "point", exclusive_minimum=True)},
              "stop": {"distance_points": Parameter("number", "point", exclusive_minimum=True)}},
    "profit_exit": {"strategy_default": {}, "fixed_r": {"target_r": Parameter("number", "R", exclusive_minimum=True)},
                    "fixed_points": {"target_points": Parameter("number", "point", exclusive_minimum=True)}},
    "protection": {"strategy_default": {}, "fixed_points": {"stop_points": Parameter("number", "point", exclusive_minimum=True)}},
    "sizing": {"risk_fraction": {"risk_fraction": Parameter("number", "fraction", 0, 1, True),
                                "compound": Parameter("bool", "boolean")},
               "fixed_volume": {"volume": Parameter("number", "lot", exclusive_minimum=True),
                                "risk_fraction": Parameter("number", "fraction", 0, 1, True),
                                "compound": Parameter("bool", "boolean")}},
    "partial_exit_plan": {"strategy_default": {}, "fixed_r": {
        "trigger_r": Parameter("number", "R", exclusive_minimum=True),
        "fraction": Parameter("number", "fraction", 0, 1, True),
        "breakeven_after_confirmation": Parameter("bool", "boolean")}},
    "conditional_policy": {"disabled": {}},
}
# T19.57 inicial: até duas etapas, mesma unidade na agenda; base por etapa.
for _mode,_unit in (("staged_lots","lot"),("staged_fraction","fraction")):
    MODULE_PARAMETERS["partial_exit_plan"][_mode]={
        "count":Parameter("int","stage",1,2),
        "trigger1_r":Parameter("number","R",exclusive_minimum=True),
        "quantity1":Parameter("number",_unit,exclusive_minimum=True),"base1":Parameter("int","volume_base",0,1),
        "trigger2_r":Parameter("number","R",exclusive_minimum=True),
        "quantity2":Parameter("number",_unit,exclusive_minimum=True),"base2":Parameter("int","volume_base",0,1),
        "breakeven_after_confirmation":Parameter("bool","boolean")}

FILTER_PARAMETERS = {"adx_min": {"period": PERIOD, "threshold": OSCILLATOR}}
BASE_MODES = {slot: next(iter(modes)) for slot, modes in MODULE_PARAMETERS.items()}
TIMEFRAMES = frozenset(("M1", "M2", "M3", "M4", "M5", "M6", "M10", "M12", "M15", "M20", "M30",
                        "H1", "H2", "H3", "H4", "H6", "H8", "H12", "D1", "W1", "MN1"))
EXECUTION_ORDER = ("signal_close", "filters", "sizing", "next_open")


def parameter_paths(composition):
    """Tipos/unidades dos caminhos realmente presentes para bindings explícitos."""
    result = {f"strategy.params.{key}": spec for key, spec in
              STRATEGY_PARAMETERS[composition["strategy"]["key"]].items()}
    for slot, modes in MODULE_PARAMETERS.items():
        for key, spec in modes[composition[slot]["mode"]].items():
            result[f"{slot}.params.{key}"] = spec
    for index, module in enumerate(composition["filters"]):
        for key, spec in FILTER_PARAMETERS[module["mode"]].items():
            result[f"filters.{index}.params.{key}"] = spec
    return result
