"""Volatilidade, compressão/expansão e regime de mercado (T13.3), por barra fechada.

Reaproveita ``strategies.indicators`` (ATR, ADX, Bollinger, eficiência, R²) e os limiares de
ADX da triagem (``strategies.asset_screener``: acima de 25 tendência, abaixo de 20 lateral).

Diferença em relação à triagem, documentada em T13.3_PERFIS_REGIMES.md:

- a triagem classifica só a última barra, sem confiança nem duração; aqui cada barra recebe o
  estado bruto (o mesmo cálculo da triagem) e o estado confirmado, que só muda depois de
  ``min_duration`` barras seguidas no novo estado bruto. Com ``min_duration=1`` os dois coincidem;
- antes de haver barras suficientes para o ADX o estado é ``UNCERTAIN`` (nunca um estado neutro).

**Confiança do regime** (proposta para a decisão em aberto 5): fração de três leituras que
concordam com o estado bruto — ADX (sempre concorda, por construção), eficiência do percurso e R²
da regressão — portanto 1/3, 2/3 ou 1. Não é probabilidade. ``TRANSITION`` e ``UNCERTAIN`` não têm
confiança (ausente, não zero).

Percentis comparam a barra com as ``percentile_window`` barras **anteriores**. Todo valor de barra
fica disponível no fim da barra (``bar_end_utc``).
"""
import numpy as np
import pandas as pd

from mt5_extracao.strategies import indicators as ind

TREND, RANGE, TRANSITION, UNCERTAIN = "TREND", "RANGE", "TRANSITION", "UNCERTAIN"
REGIMES = (TREND, RANGE, TRANSITION, UNCERTAIN)
SCREENER_LABELS = {"tendência": TREND, "lateral": RANGE, "transição": TRANSITION}


def prior_percentile(series, window, min_periods=None):
    """Percentil 0-100 do valor atual contra as ``window`` observações anteriores (a atual não entra)."""
    min_periods = window if min_periods is None else min_periods

    def pct(values):
        current, history = values[-1], values[:-1]
        history = history[~np.isnan(history)]
        if np.isnan(current) or len(history) < min_periods:
            return np.nan
        return 100.0 * (history < current).mean()

    return series.rolling(window + 1, min_periods=min_periods + 1).apply(pct, raw=True)


def volatility_frame(bars, *, atr_n=14, rv_n=20, bb_n=20, percentile_window=100, compression_pct=20.0,
                     expansion_pct=80.0):
    """ATR, ATR%, volatilidade realizada, amplitude, largura de Bollinger, percentis e estado."""
    data = bars.reset_index(drop=True)
    close = data["close"].astype(float)
    out = pd.DataFrame(index=data.index)
    out["atr"] = ind.atr(data, atr_n)
    out["atr_pct"] = out["atr"] / close
    out["realized_vol"] = np.log(close).diff().rolling(rv_n, min_periods=rv_n).std()
    out["range_pct"] = (data["high"] - data["low"]) / close
    bands = ind.bollinger(close, bb_n, 2.0)
    out["bb_width"] = (bands["upper"] - bands["lower"]) / bands["mid"]
    out["atr_pct_percentile"] = prior_percentile(out["atr_pct"], percentile_window)
    out["bb_width_percentile"] = prior_percentile(out["bb_width"], percentile_window)
    out["range_percentile"] = prior_percentile(out["range_pct"], percentile_window)
    state = pd.Series(None, index=data.index, dtype=object)
    known = out["bb_width_percentile"].notna() & out["range_percentile"].notna()
    state[known] = "NORMAL"
    state[known & (out["bb_width_percentile"] <= compression_pct)] = "COMPRESSION"
    state[known & (out["range_percentile"] >= expansion_pct)] = "EXPANSION"
    out["volatility_state"] = state
    return out


def raw_regime(adx_value, trend_adx=25.0, range_adx=20.0):
    """O mesmo corte da triagem; ADX ausente = ``UNCERTAIN``."""
    if adx_value is None or not np.isfinite(adx_value):
        return UNCERTAIN
    if adx_value > trend_adx:
        return TREND
    if adx_value < range_adx:
        return RANGE
    return TRANSITION


def regime_frame(bars, *, adx_n=14, er_n=20, r2_n=50, trend_adx=25.0, range_adx=20.0, er_cut=0.3, r2_cut=0.5,
                 min_duration=3):
    """Estado bruto, estado confirmado, duração em barras e confiança por barra."""
    if min_duration < 1:
        raise ValueError("min_duration precisa ser pelo menos 1.")
    data = bars.reset_index(drop=True)
    close = data["close"].astype(float)
    adx = ind.adx(data, adx_n)["adx"]
    er = ind.efficiency_ratio(close, er_n)
    r2 = ind.linreg_r2(close, r2_n)
    raw = [raw_regime(value, trend_adx, range_adx) for value in adx]
    confidence = []
    for label, er_value, r2_value in zip(raw, er, r2):
        if label not in (TREND, RANGE) or not np.isfinite(er_value) or not np.isfinite(r2_value):
            confidence.append(np.nan)
            continue
        trendy = (er_value >= er_cut, r2_value >= r2_cut)
        agree = sum(trendy) if label == TREND else sum(not flag for flag in trendy)
        confidence.append((1 + agree) / 3.0)
    confirmed, duration = [], []
    state, run_label, run_length, state_length = UNCERTAIN, None, 0, 0
    for label in raw:
        run_length = run_length + 1 if label == run_label else 1
        run_label = label
        if label != state and (run_length >= min_duration or label == UNCERTAIN):
            state, state_length = label, run_length
        else:
            state_length += 1
        confirmed.append(state)
        duration.append(state_length)
    return pd.DataFrame({"adx": adx, "efficiency": er, "r2": r2, "regime_raw": raw, "regime": confirmed,
                         "regime_duration": duration, "regime_confidence": confidence}, index=data.index)
