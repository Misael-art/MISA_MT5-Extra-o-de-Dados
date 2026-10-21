"""T19.40: HA/HiLo declarado, calculado causalmente no prefixo fixo recebido.

HiLo inicia indefinido e atualiza a SMA ativa mesmo na igualdade. Não é o
indicador gráfico HDK_HiloActivator nem uma reprodução qualificada do Profit.
"""
import math

import numpy as np
import pandas as pd

from .base import ExitRules, Strategy, StrategyInfo

_EMPTY = np.finfo(float).max


def _number(value):
    return math.isfinite(value) and value != _EMPTY


def _hilo_parameters(period, offset):
    if type(period) is not int or not 2 <= period <= 500:
        raise ValueError("HA/HiLo requer hilo_period inteiro entre 2 e 500.")
    if type(offset) is not int or offset != 0:
        raise ValueError("HA/HiLo requer hilo_offset inteiro igual a 0 nesta variante.")


def ha_hilo_context(df, hilo_period=10, hilo_offset=0):
    """HA seed (O0+C0)/2; HiLo usa SMAs anteriores para o rompimento.

    OHLC inválido ou agregação não representável torna indefinido todo o
    restante do contexto. Não recupera/resemeia depois da barra inválida.
    Geometria OHLC, ordem temporal e tamanho das colunas são precondições.
    """
    _hilo_parameters(hilo_period, hilo_offset)
    values = df[["open", "high", "low", "close"]].to_numpy(dtype=float)
    highs, lows = values[:, 1].tolist(), values[:, 2].tolist()
    result = np.full((len(df), 3), np.nan)
    states = np.zeros(len(df), dtype=int)
    state = 0
    for i, row in enumerate(values):
        op, high, low, close = map(float, row)
        if any(not _number(v) or v <= 0 for v in (op, high, low, close)):
            break
        ha_close = (op+high+low+close)/4.0
        ha_open = (op+close)/2.0 if i == 0 else (float(result[i-1, 0])+float(result[i-1, 1]))/2.0
        if any(not _number(v) or v <= 0 for v in (ha_open, ha_close)):
            break
        line = np.nan
        if i >= hilo_period:
            previous_high = previous_low = current_high = current_low = 0.0
            for j in range(i-hilo_period, i):
                previous_high += highs[j]
                previous_low += lows[j]
            for j in range(i-hilo_period+1, i+1):
                current_high += highs[j]
                current_low += lows[j]
            if any(not _number(v) for v in (previous_high, previous_low, current_high, current_low)):
                break
            if close > previous_high/hilo_period:
                state = 1
            elif close < previous_low/hilo_period:
                state = -1
            if state:
                line = (current_low if state > 0 else current_high)/hilo_period
        result[i] = [ha_open, ha_close, line]
        states[i] = state
    out = pd.DataFrame(result, index=df.index, columns=["ha_open", "ha_close", "line"])
    out["state"] = states
    return out


class HaHiLo(Strategy):
    key = "ha_hilo"
    info = StrategyInfo(
        name="S11 · Heiken-Ashi + HiLo", regime="tendência", win_rate="não qualificada",
        reward_risk="saída por sinal oposto ou SL fixo, sem TP padrão", complexity="média",
        timeframes=("M5", "M15", "H1"), markets="símbolos com especificação disponível",
        summary="Condição fechada de HA e HiLo declarado; saída e tentativa oposta na abertura seguinte, "
                "sujeitas às proteções. SL na linha do sinal com folga opcional, sem garantia de sempre posicionado.")
    default_params = dict(hilo_period=10, hilo_offset=0, stop_buffer_points=0)
    min_bars = 300
    param_grid = {}

    def __init__(self, **params):
        super().__init__(**params)
        p = self.params
        _hilo_parameters(p["hilo_period"], p["hilo_offset"])
        if type(p["stop_buffer_points"]) is not int or not 0 <= p["stop_buffer_points"] <= 2147483647:
            raise ValueError("HA/HiLo requer stop_buffer_points inteiro entre 0 e 2147483647.")

    def _signals(self, df, out):
        p = self.params
        context = ha_hilo_context(df, p["hilo_period"], p["hilo_offset"])
        ready = (np.arange(len(df)) >= 299) & context.line.gt(0)
        buy = ready & context.ha_close.gt(context.line) & context.ha_close.gt(context.ha_open)
        sell = ready & context.ha_close.lt(context.line) & context.ha_close.lt(context.ha_open)
        out["entry_long"], out["exit_short"] = buy, buy
        out["entry_short"], out["exit_long"] = sell, sell
        # Níveis brutos em preço: só o motor conhece point/tick e aplica a folga.
        out["stop_long"] = out["stop_short"] = context.line.where(buy | sell)
        out["atr"] = out["atr"].where(context.ha_open.notna())

    def exit_rules(self):
        return ExitRules(signal_stop_offset_points=self.params["stop_buffer_points"],
                         move_guard_from_signal_stop=False, breakeven_after_partial=False)
