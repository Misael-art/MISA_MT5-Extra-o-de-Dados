"""
Estratégia declarada: a regra de entrada escrita como dados, para rodar igual em Python e em MQL5.

    spec = SignalSpec.from_dict({
        "key": "cruzamento", "name": "Cruzamento de médias",
        "indicators": {"rapida": {"kind": "sma", "period": 10}, "lenta": {"kind": "sma", "period": 40}},
        "long": [["cross_above", "rapida", "lenta"]], "short": [["cross_below", "rapida", "lenta"]],
        "atr_period": 14, "stop_atr": 1.5, "target_r": 2.0})
    strategy = DeclaredStrategy(spec)          # mesma interface gerar_sinais das outras estratégias

`mt5_extracao/robots/generate.py` transforma a mesma declaração no sinal MQL5 de um robô. Só entra aqui o que os
dois lados calculam do mesmo jeito: médias simples e exponenciais e RSI sobre o fechamento; condições de
cruzamento e de comparação; stop em múltiplos do ATR e alvo em múltiplos do risco. Não é um tradutor de Python.

O ATR do stop é a média **simples** do true range, como o `iATR` do MetaTrader 5 (as outras estratégias do projeto
usam a média de Wilder).
"""
import re
from dataclasses import dataclass, field
from typing import Dict, List, Tuple

from . import indicators as ind
from .base import ExitRules, Strategy, StrategyInfo, as_int

KINDS = ("sma", "ema", "rsi")
OPERATORS = ("cross_above", "cross_below", "above", "below")
PRICE = "close"
_NAME = re.compile(r"^[a-z][a-z0-9_]{0,23}$")


@dataclass(frozen=True)
class Indicator:
    kind: str
    period: int


@dataclass
class SignalSpec:
    key: str
    name: str
    indicators: Dict[str, Indicator]
    long: List[Tuple] = field(default_factory=list)       # condições que precisam ser TODAS verdadeiras para comprar
    short: List[Tuple] = field(default_factory=list)
    atr_period: int = 14
    stop_atr: float = 1.5
    target_r: float = 2.0

    def __post_init__(self):
        self.validate()

    @classmethod
    def from_dict(cls, data: dict) -> "SignalSpec":
        if not isinstance(data, dict):
            raise ValueError("A declaração deve ser um objeto com key, name, indicators, long e short.")
        unknown = set(data) - {"key", "name", "indicators", "long", "short", "atr_period", "stop_atr", "target_r"}
        if unknown:
            raise ValueError("Campo(s) desconhecido(s) na declaração: " + ", ".join(sorted(unknown)))
        raw = data.get("indicators")
        if not isinstance(raw, dict) or not raw:
            raise ValueError("A declaração precisa de pelo menos um indicador em \"indicators\".")
        indicators = {}
        for name, item in raw.items():
            if not isinstance(item, dict) or set(item) != {"kind", "period"}:
                raise ValueError(f"Indicador '{name}': informe \"kind\" e \"period\".")
            indicators[name] = Indicator(str(item["kind"]), item["period"])
        return cls(key=str(data.get("key", "")), name=str(data.get("name", "")), indicators=indicators,
                   long=[tuple(c) for c in data.get("long") or []], short=[tuple(c) for c in data.get("short") or []],
                   atr_period=data.get("atr_period", 14), stop_atr=data.get("stop_atr", 1.5),
                   target_r=data.get("target_r", 2.0))

    def to_dict(self) -> dict:
        return {"key": self.key, "name": self.name,
                "indicators": {n: {"kind": i.kind, "period": i.period} for n, i in self.indicators.items()},
                "long": [list(c) for c in self.long], "short": [list(c) for c in self.short],
                "atr_period": self.atr_period, "stop_atr": self.stop_atr, "target_r": self.target_r}

    def validate(self) -> None:
        if not _NAME.match(self.key or ""):
            raise ValueError("\"key\" inválida: use letras minúsculas, números e '_', começando por letra.")
        if not self.name:
            raise ValueError("Informe o \"name\" da estratégia.")
        for name, item in self.indicators.items():
            if not _NAME.match(name) or name == PRICE:
                raise ValueError(f"Nome de indicador inválido: '{name}' (minúsculas, números e '_'; 'close' é reservado).")
            if item.kind not in KINDS:
                raise ValueError(f"Indicador '{name}': tipo '{item.kind}' não suportado. Suportados: " + ", ".join(KINDS))
            if isinstance(item.period, bool) or not isinstance(item.period, int) or item.period < 2:
                raise ValueError(f"Indicador '{name}': \"period\" deve ser um inteiro a partir de 2.")
        if not self.long and not self.short:
            raise ValueError("Declare ao menos uma condição em \"long\" ou em \"short\".")
        for side, conditions in (("long", self.long), ("short", self.short)):
            for condition in conditions:
                if len(condition) != 3 or condition[0] not in OPERATORS:
                    raise ValueError(f"Condição inválida em \"{side}\": {list(condition)}. Formato: [operador, a, b], "
                                     "com operador em " + ", ".join(OPERATORS))
                for operand in condition[1:]:
                    is_number = isinstance(operand, (int, float)) and not isinstance(operand, bool)
                    if not is_number and operand != PRICE and operand not in self.indicators:
                        raise ValueError(f"Condição em \"{side}\" usa '{operand}', que não é um indicador declarado, "
                                         "'close' nem um número.")
                if all(isinstance(o, (int, float)) for o in condition[1:]):
                    raise ValueError(f"Condição em \"{side}\" compara dois números: {list(condition)}.")
        if isinstance(self.atr_period, bool) or not isinstance(self.atr_period, int) or self.atr_period < 2:
            raise ValueError("\"atr_period\" deve ser um inteiro a partir de 2.")
        if not isinstance(self.stop_atr, (int, float)) or self.stop_atr <= 0:
            raise ValueError("\"stop_atr\" deve ser maior que zero: toda entrada precisa de stop.")
        if not isinstance(self.target_r, (int, float)) or self.target_r < 0:
            raise ValueError("\"target_r\" deve ser zero (sem alvo) ou positivo.")


class DeclaredStrategy(Strategy):
    """Executa uma SignalSpec no backtester do projeto. Parâmetros: <indicador>_period, atr_period, stop_atr, target_r."""

    def __init__(self, spec: SignalSpec, **params):
        self.spec = spec
        self.key = spec.key
        self.default_params = {**{f"{name}_period": item.period for name, item in spec.indicators.items()},
                               "atr_period": spec.atr_period, "stop_atr": spec.stop_atr, "target_r": spec.target_r}
        self.param_grid = {}
        self.info = StrategyInfo(name=spec.name, regime="", win_rate="", reward_risk="", complexity="baixa",
                                 timeframes=(), markets="", summary="Estratégia declarada (gera também o robô MQL5).")
        self.min_bars = 3 * max([item.period for item in spec.indicators.values()] + [spec.atr_period])
        super().__init__(**params)

    def _series(self, df) -> dict:
        close = df["close"]
        series = {PRICE: close}
        for name, item in self.spec.indicators.items():
            period = as_int(self.params[f"{name}_period"])
            series[name] = {"sma": ind.sma, "ema": ind.ema, "rsi": ind.rsi}[item.kind](close, period)
        return series

    def _condition(self, condition, series, index):
        operator, a, b = condition
        left = series[a] if isinstance(a, str) else float(a)
        right = series[b] if isinstance(b, str) else float(b)
        if operator in ("above", "below"):
            return (left > right) if operator == "above" else (left < right)
        import pandas as pd
        left = left if hasattr(left, "shift") else pd.Series(left, index=index)
        right = right if hasattr(right, "shift") else pd.Series(right, index=index)
        return self.crossed_above(left, right) if operator == "cross_above" else self.crossed_below(left, right)

    def _signals(self, df, out):
        import pandas as pd
        series = self._series(df)
        for column, conditions in (("entry_long", self.spec.long), ("entry_short", self.spec.short)):
            result = pd.Series(bool(conditions), index=df.index)
            for condition in conditions:
                result &= self._condition(condition, series, df.index)
            out[column] = result
        both = out["entry_long"] & out["entry_short"]          # sinal dos dois lados na mesma barra: não opera
        out["entry_long"] &= ~both
        out["entry_short"] &= ~both
        distance = float(self.params["stop_atr"]) * ind.sma(ind.true_range(df), as_int(self.params["atr_period"]))
        out["stop_long"] = df["close"] - distance
        out["stop_short"] = df["close"] + distance

    def exit_rules(self):
        target = float(self.params["target_r"])
        return ExitRules(target_r=target if target > 0 else None)
