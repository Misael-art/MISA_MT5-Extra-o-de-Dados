"""T19.38/S10: variante declarada estocástico lento por razão de somas + EMA.

EMA recursiva com âncora no primeiro fechamento do frame, sem janela deslizante.
Não implica paridade com Profit, iStochastic ou execução nativa (ver docs/sdk/stoch_ema9.md).
"""
import math

import numpy as np
import pandas as pd

from .base import ExitRules, Strategy, StrategyInfo

_EMPTY_VALUE = np.finfo(float).max  # sentinela MQL5: é finita, mas não é observação
_PERIOD_LIMITS = {"k_period": (2, 1000), "slowing": (1, 100),
                  "d_period": (1, 100), "ema_period": (1, 1000)}


def _number(value):
    return math.isfinite(value) and value != _EMPTY_VALUE


def slow_stochastic(df, k_period=14, slowing=3, d_period=3):
    """K=100·Σ(C−LL)/Σ(HH−LL); D=SMA(K). Ausência/amplitude zero → NaN.

    Cada LL/HH usa sua própria janela fechada de k_period barras. Não é a média
    dos K brutos. Uma janela inválida pode recuperar após a observação sair dela.
    """
    for name, value in (("k_period", k_period), ("slowing", slowing), ("d_period", d_period)):
        minimum, maximum = _PERIOD_LIMITS[name]
        if type(value) is not int or not minimum <= value <= maximum:
            raise ValueError(f"S10 requer {name} inteiro entre {minimum} e {maximum}.")
    values = df[["high", "low", "close"]].to_numpy(dtype=float)
    valid = np.isfinite(values).all(axis=1) & (values != _EMPTY_VALUE).all(axis=1)
    window_valid = pd.Series(valid).rolling(k_period).sum().eq(k_period).to_numpy()
    high, low, close = values.T
    hh = pd.Series(high).rolling(k_period).max().to_numpy()
    ll = pd.Series(low).rolling(k_period).min().to_numpy()
    with np.errstate(over="ignore", invalid="ignore"):
        numerators, denominators = close - ll, hh - ll
    k, d = np.full(len(df), np.nan), np.full(len(df), np.nan)
    for i in range(k_period + slowing - 2, len(df)):
        start = i - slowing + 1
        if not window_valid[start:i+1].all():
            continue
        # Ordem cronológica explícita, como a variante declarada no fonte H.
        numerator = sum(float(v) for v in numerators[start:i+1])
        denominator = sum(float(v) for v in denominators[start:i+1])
        if _number(numerator) and _number(denominator) and denominator > 0:
            value = 100.0 * numerator / denominator
            if _number(value):
                k[i] = value
    for i in range(d_period - 1, len(df)):
        window = k[i-d_period+1:i+1]
        if np.isfinite(window).all():
            total = sum(float(v) for v in window)
            if _number(total):
                d[i] = total / d_period
    return pd.Series(k, index=df.index, name="k"), pd.Series(d, index=df.index, name="d")


def _ema_closed_prefix(df, period):
    """Sem preenchimento nem reinício silencioso após OHLC inválido/overflow."""
    values = df[["open", "high", "low", "close"]].to_numpy(dtype=float)
    valid = np.isfinite(values).all(axis=1) & (values != _EMPTY_VALUE).all(axis=1)
    ema = np.full(len(df), np.nan)
    alpha = 2.0 / (period + 1)
    previous = None
    for i, close in enumerate(values[:, 3]):
        if not valid[i]:
            break
        value = float(close) if previous is None else alpha * float(close) + (1-alpha) * previous
        if not _number(value):
            break
        ema[i] = previous = value
    return pd.Series(ema, index=df.index)


class StochEma9(Strategy):
    key = "stoch_ema9"
    info = StrategyInfo(
        name="S10 · Estocástico lento + EMA9", regime="tendência", win_rate="não qualificada",
        reward_risk="SL na EMA com offset; alvo opcional", complexity="média",
        timeframes=("M5", "M15", "H1"), markets="símbolos com especificação disponível",
        summary="Variante declarada: K por razão de somas cruza D acima de 75/abaixo de 25, "
                "filtrado pela EMA. Sinal fechado, entrada seguinte e Move Guard compartilhado.")
    default_params = dict(k_period=14, slowing=3, d_period=3, ema_period=9,
                          upper_level=75.0, lower_level=25.0, stop_offset_points=50, target_points=0)
    param_grid = {}

    def __init__(self, **params):
        super().__init__(**params)
        p = self.params
        for name, (minimum, maximum) in _PERIOD_LIMITS.items():
            if type(p[name]) is not int or not minimum <= p[name] <= maximum:
                raise ValueError(f"S10 requer {name} inteiro entre {minimum} e {maximum}.")
        for name in ("stop_offset_points", "target_points"):
            if type(p[name]) is not int or not 0 <= p[name] <= 2147483647:
                raise ValueError(f"S10 requer {name} inteiro não negativo até 2147483647 pontos.")
        levels = (p["lower_level"], p["upper_level"])
        if (any(type(v) not in (int, float) for v in levels)
                or not 0 <= levels[0] < levels[1] <= 100
                or not all(math.isfinite(v) for v in levels)):
            raise ValueError("S10 requer 0 <= lower_level < upper_level <= 100, finitos.")
        self.min_bars = max(300, p["k_period"] + p["slowing"] + p["d_period"] - 1)

    def _signals(self, df, out):
        p = self.params
        k, d = slow_stochastic(df, p["k_period"], p["slowing"], p["d_period"])
        ema = _ema_closed_prefix(df, p["ema_period"])
        ready = (np.arange(len(df)) >= self.min_bars-1) & ema.notna() & ema.gt(0)
        out["stop_long"] = out["stop_short"] = ema.where(ready)
        out["entry_long"] = ready & self.crossed_above(k, d) & k.gt(p["upper_level"]) & df.close.gt(ema)
        out["entry_short"] = ready & self.crossed_below(k, d) & k.lt(p["lower_level"]) & df.close.lt(ema)

    def exit_rules(self):
        return ExitRules(signal_stop_offset_points=self.params["stop_offset_points"],
                         move_guard_from_signal_stop=True,
                         signal_target_points=self.params["target_points"] or None,
                         breakeven_after_partial=False)
