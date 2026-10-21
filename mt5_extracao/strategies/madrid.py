"""T19.39: Madrid fechado; EMAs e ATR com semente no início do prefixo fixo.

Modo pontos publica candidatos e M1: só o motor com SymbolSpec valida a distância.
Não implementa FC/FV persistente, TP móvel ou paridade da janela nativa1500.
"""
import math

import numpy as np
import pandas as pd

from .base import ExitRules, Strategy, StrategyInfo

_EMPTY = np.finfo(float).max


def _number(value):
    return math.isfinite(value) and value != _EMPTY


def _periods(first, step):
    if type(first) is not int or type(step) is not int or not 1 <= first <= 1000 or not 1 <= step <= (1000-first)//9:
        raise ValueError("Madrid requer first_period/period_step inteiros positivos com M10 <= 1000.")
    return tuple(first+j*step for j in range(10))


def madrid_averages(df, first_period=5, period_step=5):
    """EMA0=C0; ATR0=H0−L0, depois RMA1/14. HLC inválido bloqueia o prefixo restante."""
    periods = _periods(first_period, period_step)
    values = df[["high", "low", "close"]].to_numpy(dtype=float)
    out = np.full((len(df), 11), np.nan)
    alpha = [2.0/(p+1) for p in periods]
    previous, atr, previous_close = None, None, None
    for i, row in enumerate(values):
        high, low, close = map(float, row)
        if any(not _number(v) or v <= 0 for v in (high, low, close)):
            break
        tr = high-low if previous_close is None else max(high-low, abs(high-previous_close), abs(low-previous_close))
        current = [close]*10 if previous is None else [a*close+(1-a)*old for a, old in zip(alpha, previous)]
        atr = tr if atr is None else (1.0/14)*tr+(1-1.0/14)*atr
        if any(not _number(v) for v in (*current, tr, atr)):
            break
        out[i] = [*current, atr]
        previous, previous_close = current, close
    return pd.DataFrame(out, index=df.index, columns=[*(f"m{i}" for i in range(1, 11)), "atr"])


class Madrid(Strategy):
    key = "madrid"
    info = StrategyInfo(
        name="S05 · Madrid Trend Scalper", regime="tendência", win_rate="não qualificada",
        reward_risk="média/offset e afastamento configuráveis", complexity="média",
        timeframes=("M5", "M15", "H1"), markets="símbolos com especificação disponível",
        summary="Dez EMAs alinhadas, cruzamento fechado de M1 e afastamento limitado; mercado seguinte, "
                "guard compartilhado e TP fixado no sinal. Reset implícito por cruzamento, sem FC/FV persistente.")
    default_params = dict(first_period=5, period_step=5, distance_atr=1.0, distance_points=0,
                          trail_line=10, offset_atr=.25)
    min_bars = 300
    param_grid = {}

    def __init__(self, **params):
        super().__init__(**params)
        p = self.params
        _periods(p["first_period"], p["period_step"])
        if type(p["trail_line"]) is not int or not 1 <= p["trail_line"] <= 10:
            raise ValueError("Madrid requer trail_line inteiro entre 1 e 10.")
        if type(p["distance_points"]) is not int or not 0 <= p["distance_points"] <= 2147483647:
            raise ValueError("Madrid requer distance_points inteiro entre 0 e 2147483647.")
        for key, positive in (("distance_atr", True), ("offset_atr", False)):
            value = p[key]
            try:
                valid = type(value) in (int, float) and _number(value) and (value > 0 if positive else value >= 0)
            except OverflowError:
                valid = False
            if not valid:
                raise ValueError("Madrid requer distance_atr positivo e offset_atr não negativo, finitos.")

    def _signals(self, df, out):
        p = self.params
        context = madrid_averages(df, p["first_period"], p["period_step"])
        out["atr"] = context.atr
        m1, atr = context.m1, context.atr
        with np.errstate(over="ignore", invalid="ignore"):
            offset = atr*float(p["offset_atr"])
            long_stop, short_stop = context[f"m{p['trail_line']}"]-offset, context[f"m{p['trail_line']}"]+offset
        ready = ((np.arange(len(df)) >= 299) & atr.gt(0) & np.isfinite(offset) & offset.ne(_EMPTY)
                 & np.isfinite(long_stop) & long_stop.ne(_EMPTY) & np.isfinite(short_stop) & short_stop.ne(_EMPTY))
        # Guard continua disponível sem novo sinal/alinhamento; o motor decide se aperta.
        out["stop_long"], out["stop_short"] = long_stop.where(ready), short_stop.where(ready)
        fan = context.iloc[:, :10].to_numpy()
        up, down = (fan[:, :-1] > fan[:, 1:]).all(axis=1), (fan[:, :-1] < fan[:, 1:]).all(axis=1)
        buy = ready & up & self.crossed_above(df.close, m1)
        sell = ready & down & self.crossed_below(df.close, m1)
        if p["distance_points"]:
            # Referência em preço; não é TP nem entrada operável antes do SymbolSpec.
            out["target_long"] = out["target_short"] = m1.where(ready)
        else:
            with np.errstate(over="ignore", invalid="ignore"):
                distance = atr*float(p["distance_atr"])
                long_target, short_target = m1+distance, m1-distance
            valid = np.isfinite(distance) & distance.gt(0) & distance.ne(_EMPTY)
            buy &= valid & df.close.sub(m1).le(distance)
            sell &= valid & m1.sub(df.close).le(distance)
            out["stop_long"] = out["stop_long"].where(valid)
            out["stop_short"] = out["stop_short"].where(valid)
            out["target_long"], out["target_short"] = long_target.where(ready & valid), short_target.where(ready & valid)
        out["entry_long"], out["entry_short"] = buy, sell

    def exit_rules(self):
        return ExitRules(move_guard_from_signal_stop=True, breakeven_after_partial=False,
                         absolute_signal_target=self.params["distance_points"] == 0,
                         signal_reference_distance_points=self.params["distance_points"] or None)
