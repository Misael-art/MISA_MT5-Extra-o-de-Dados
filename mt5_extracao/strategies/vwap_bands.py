"""T19.36/S01: HLC3 ponderado, bandas percentuais e volume escolhido causalmente.

Âncora aproximada pela primeira barra disponível da data do servidor. Não é VWAP
de negócios/leilão nem paridade da janela nativa1500 (docs/sdk/vwap_bands.md).
"""
import math

import numpy as np
import pandas as pd

from .base import ExitRules, REQUIRED_COLUMNS, Strategy, StrategyInfo

_EMPTY = np.finfo(float).max


def _volume_columns(source):
    if type(source) is not int or not 0 <= source <= 2:
        raise ValueError("VWAP requer volume_source inteiro: 0 Auto, 1 ticks ou 2 real.")
    return ("real_volume", "tick_volume") if source == 0 else (("tick_volume",) if source == 1 else ("real_volume",))


def _number(value):
    return math.isfinite(value) and value != _EMPTY


def _accumulate(state, high, low, close, volume):
    """Estado puramente matemático [ΣPV, ΣV, válido]; ausência envenena a data."""
    if not state[2]:
        return np.nan
    if (not _number(volume) or volume < 0
            or any(not _number(v) or v <= 0 for v in (high, low, close))):
        state[2] = False
        return np.nan
    if volume > 0:
        typical = (high + low + close) / 3.0
        weighted = typical * volume
        pv, total = state[0]+weighted, state[1]+volume
        if any(not _number(v) for v in (typical, weighted, pv, total)) or total <= 0:
            state[2] = False
            return np.nan
        state[0], state[1] = pv, total
    if state[1] <= 0:
        return np.nan
    value = state[0] / state[1]
    if not _number(value) or value <= 0:
        state[2] = False
        return np.nan
    return value


def daily_vwap(df, volume_source=0):
    """Retorna vwap e volume_source efetivo (1 ticks/2 real) por prefixo conhecido.

    Horários naive são o calendário do servidor fornecido pelo chamador. Não
    converte UTC/fuso, ordena, repara lacunas ou fabrica colunas de volume.
    Auto mantém dois acumulados diários: ao surgir real positivo conhecido,
    seleciona a série real para TODA a data conhecida, sem misturar ticks nem
    reescrever passado. A escolha persiste entre datas em todo o prefixo fixo.
    """
    columns = _volume_columns(volume_source)
    missing = [c for c in ("time", "high", "low", "close", *columns) if c not in df]
    if missing:
        raise ValueError("Faltam colunas para VWAP: " + ", ".join(missing))
    try:
        if len(df) and pd.api.types.is_numeric_dtype(df["time"]):
            raise ValueError
        times = pd.to_datetime(df["time"], errors="raise")
        if (times.dt.tz is not None or times.isna().any() or times.duplicated().any()
                or not times.is_monotonic_increasing):
            raise ValueError
        dates = times.dt.normalize()
    except (ValueError, TypeError, OverflowError, AttributeError):
        raise ValueError("VWAP requer horários naive válidos e crescentes no calendário do servidor; "
                         "converta epoch/fuso explicitamente antes de montar o frame.") from None
    values = df[["high", "low", "close", *columns]].to_numpy(dtype=float)
    out, sources = np.full(len(df), np.nan), np.empty(len(df), dtype=int)
    day, real_seen, states = None, False, {}
    for i, row in enumerate(values):
        current_day = dates.iloc[i]
        if current_day != day:
            anchored = i > 0 or times.iloc[i] == current_day
            states = {name: [0.0, 0.0, anchored] for name in columns}
            day = current_day
        high, low, close = (float(v) for v in row[:3])
        volumes = {name: float(row[3+j]) for j, name in enumerate(columns)}
        if volume_source == 0 and _number(volumes["real_volume"]) and volumes["real_volume"] > 0:
            real_seen = True
        effective = 2 if volume_source == 2 or (volume_source == 0 and real_seen) else 1
        selected = "real_volume" if effective == 2 else "tick_volume"
        for name in columns:
            value = _accumulate(states[name], high, low, close, volumes[name])
            if name == selected:
                out[i] = value
        sources[i] = effective
    return pd.DataFrame({"vwap": out, "volume_source": sources}, index=df.index)


class VwapBands(Strategy):
    key = "vwap_bands"
    info = StrategyInfo(
        name="S01 · Bandas percentuais da VWAP", regime="lateral", win_rate="não qualificada",
        reward_risk="bandas e offset configuráveis, antes dos custos", complexity="média",
        timeframes=("M1", "M5", "M15"), markets="símbolos com volumes e especificação disponíveis",
        summary="Variante HLC3 por data do servidor: toque na banda do fechamento, mercado seguinte, "
                "SL além da banda e TP fixado no sinal. Sem VWAP de negócios/leilão ou ordem limite.")
    default_params = dict(entry_band_pct=1.0, target_band_pct=.5, stop_offset_points=200, volume_source=0)
    param_grid = {}
    min_bars = 300

    def __init__(self, **params):
        super().__init__(**params)
        p = self.params
        if (any(type(p[k]) not in (int, float) for k in ("entry_band_pct", "target_band_pct"))
                or not 0 < p["entry_band_pct"] <= 10
                or not 0 <= p["target_band_pct"] < p["entry_band_pct"]):
            raise ValueError("VWAP requer 0 <= target_band_pct < entry_band_pct <= 10, finitos.")
        if type(p["stop_offset_points"]) is not int or not 1 <= p["stop_offset_points"] <= 2147483647:
            raise ValueError("VWAP requer stop_offset_points inteiro positivo até 2147483647 pontos.")
        self.required_columns = (*REQUIRED_COLUMNS, *_volume_columns(p["volume_source"]))

    def _signals(self, df, out):
        p = self.params
        vwap = daily_vwap(df, p["volume_source"])["vwap"]
        lower, upper = vwap*(1-p["entry_band_pct"]/100), vwap*(1+p["entry_band_pct"]/100)
        valid = np.isfinite(lower) & lower.gt(0) & lower.ne(_EMPTY) & np.isfinite(upper) & upper.gt(0) & upper.ne(_EMPTY)
        ready = (np.arange(len(df)) >= self.min_bars-1) & valid
        buy, sell = df.low.le(lower), df.high.ge(upper)
        active = ready & (buy != sell)
        out["entry_long"], out["entry_short"] = active & buy, active & sell
        out["stop_long"], out["stop_short"] = lower.where(active), upper.where(active)
        out["target_long"] = (vwap*(1-p["target_band_pct"]/100)).where(active)
        out["target_short"] = (vwap*(1+p["target_band_pct"]/100)).where(active)

    def exit_rules(self):
        return ExitRules(signal_stop_offset_points=self.params["stop_offset_points"],
                         absolute_signal_target=True, breakeven_after_partial=False)
