"""S09: variante mínima declarada EMA fechada + SL/TP fixos, sem filtro EMA200."""
import numpy as np

from . import indicators as ind
from .base import ExitRules, Strategy, StrategyInfo


class S09Reference(Strategy):
    key = "s09_reference"
    info = StrategyInfo(
        name="S09 · Referência EMA com SL/TP fixos", regime="tendência", win_rate="não qualificada",
        reward_risk="TP/SL configurável, antes dos custos", complexity="baixa",
        timeframes=("M1", "M5", "M15", "H1"), markets="símbolos com especificação disponível",
        summary="Variante mínima declarada: cruzamento EMA curta/longa em barras fechadas; "
                "entrada seguinte e SL/TP fixos no fill. Sem parcial, BE ou trailing.")
    default_params = {"fast": 20, "slow": 50, "warmup_bars": 250, "stop_points": 100, "target_points": 200}
    param_grid = {}

    def __init__(self, **params):
        super().__init__(**params)
        p = self.params
        if any(type(value) is not int or value <= 0 for value in p.values()):
            raise ValueError("S09 requer períodos, aquecimento e pontos inteiros positivos; confira o perfil.")
        if not (p["fast"] < p["slow"] < p["warmup_bars"] <= 100000):
            raise ValueError("S09 requer EMA curta < longa < aquecimento, até 100000 barras.")
        self.min_bars = p["warmup_bars"]

    def _signals(self, df, out):
        p = self.params
        close = df["close"]
        fast, slow = ind.ema(close, p["fast"]), ind.ema(close, p["slow"])
        # EMA recursiva parte da primeira barra do snapshot de aquecimento, sem janela móvel.
        valid = close.notna() & close.gt(0) & close.abs().lt(float("inf"))
        # Uma lacuna invalida o prefixo; não esconder ausência com o preenchimento do pandas.
        ready = valid.cummin() & (np.arange(len(df)) >= p["warmup_bars"]-1)
        out["entry_long"] = ready & self.crossed_above(fast, slow)
        out["entry_short"] = ready & self.crossed_below(fast, slow)

    def exit_rules(self):
        return ExitRules(stop_points=self.params["stop_points"],target_points=self.params["target_points"],
                         breakeven_after_partial=False)
