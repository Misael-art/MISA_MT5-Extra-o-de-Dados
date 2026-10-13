"""
Interface comum das estratégias.

    estrategia = get_strategy("donchian", entry_period=55)
    sinais = estrategia.gerar_sinais(df)   # df: time, open, high, low, close[, spread, tick_volume]

Sinais são gerados no FECHAMENTO da barra; o backtester executa na abertura da
próxima. Nenhuma coluna pode usar dados de barras futuras.
"""
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from . import indicators as ind

SIGNAL_COLUMNS = ("entry_long", "entry_short", "exit_long", "exit_short",
                  "stop_long", "stop_short", "target_long", "target_short", "atr")
REQUIRED_COLUMNS = ("time", "open", "high", "low", "close")


@dataclass(frozen=True)
class StrategyInfo:
    name: str
    regime: str               # tendência | lateral | rompimento | compressão
    win_rate: str             # taxa de acerto típica (referência de literatura, não promessa)
    reward_risk: str          # relação risco:retorno típica
    complexity: str           # baixa | média | alta
    timeframes: Tuple[str, ...]
    markets: str
    summary: str


@dataclass(frozen=True)
class ExitRules:
    target_r: Optional[float] = None      # alvo em múltiplos do risco inicial (R)
    partial_r: Optional[float] = None     # realiza parte da posição em partial_r × R
    partial_fraction: float = 0.5
    breakeven_after_partial: bool = True  # após a parcial, stop no preço de entrada
    trailing_atr: Optional[float] = None  # stop móvel a N × ATR do fechamento
    max_bars: Optional[int] = None        # saída por tempo
    eod_exit: bool = False                # zera no fim do dia (intradiário)


class Strategy:
    key: str = ""
    info: StrategyInfo
    default_params: Dict[str, float] = {}
    # Grade pequena usada pela otimização do walk-forward (mantenha ≤ 12 combinações)
    param_grid: Dict[str, List[float]] = {}
    min_bars: int = 250

    def __init__(self, **params):
        unknown = set(params) - set(self.default_params)
        if unknown:
            raise ValueError(f"Parâmetro(s) desconhecido(s) para '{self.key}': {', '.join(sorted(unknown))}. "
                             f"Aceitos: {', '.join(sorted(self.default_params))}")
        self.params = {**self.default_params, **params}

    # --- a ser implementado por cada estratégia ---
    def _signals(self, df: pd.DataFrame, out: pd.DataFrame) -> None:
        raise NotImplementedError

    def exit_rules(self) -> ExitRules:
        return ExitRules()

    # --- interface pública ---
    def gerar_sinais(self, df: pd.DataFrame) -> pd.DataFrame:
        missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
        if missing:
            raise ValueError(f"Faltam colunas nos dados: {', '.join(missing)}")
        out = pd.DataFrame(index=df.index)
        for col in ("entry_long", "entry_short", "exit_long", "exit_short"):
            out[col] = False
        for col in ("stop_long", "stop_short", "target_long", "target_short"):
            out[col] = np.nan
        out["atr"] = ind.atr(df, 14)
        self._signals(df, out)
        for col in ("entry_long", "entry_short", "exit_long", "exit_short"):
            out[col] = out[col].fillna(False).astype(bool)
        # Entrada sem stop válido não é operável
        out["entry_long"] &= out["stop_long"].notna() & (out["stop_long"] < df["close"])
        out["entry_short"] &= out["stop_short"].notna() & (out["stop_short"] > df["close"])
        return out[list(SIGNAL_COLUMNS)]

    generate_signals = gerar_sinais

    def describe(self) -> str:
        params = ", ".join(f"{k}={v:g}" if isinstance(v, (int, float)) else f"{k}={v}"
                           for k, v in self.params.items())
        return f"{self.info.name} ({params})"

    @staticmethod
    def crossed_above(a: pd.Series, b: pd.Series) -> pd.Series:
        return (a > b) & (a.shift(1) <= b.shift(1))

    @staticmethod
    def crossed_below(a: pd.Series, b: pd.Series) -> pd.Series:
        return (a < b) & (a.shift(1) >= b.shift(1))


def as_int(value) -> int:
    return max(1, int(round(float(value))))

