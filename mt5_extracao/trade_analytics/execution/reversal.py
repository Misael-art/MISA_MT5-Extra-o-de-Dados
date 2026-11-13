"""
Sinais causais de reversão e de continuação adversa (T16.15), para a recuperação condicional do Laboratório.

    signals = compute(bars, ("rsi", "bollinger"), min_signals=1)
    signals["reversal"][1][i]        # True: no FECHAMENTO da barra i há sinal de reversão a favor de uma compra
    signals["continuation"][-1][i]   # True: no fechamento da barra i o movimento contra uma venda continua

Cada valor em i usa só barras fechadas até i (nenhuma consulta ao futuro); quem consome decide no fechamento e executa
na abertura seguinte. Direção 1 = posição comprada sofrendo queda; -1 = vendida sofrendo alta. Nenhuma família é
presumida preditiva: o estudo pareado (`runner.recovery_study`) é que mede.
"""
from typing import Callable, Dict, Iterable, Tuple

import numpy as np
import pandas as pd

from mt5_extracao.strategies import indicators

LOOKBACK = 10          # barras da mínima/máxima de referência (falso rompimento e continuação)


def _shift(values: np.ndarray, n: int = 1) -> np.ndarray:
    out = np.full(len(values), np.nan)
    if n < len(values):
        out[n:] = values[:-n]
    return out


def _prior_extreme(values: pd.Series, n: int, kind: str) -> np.ndarray:
    """Mínima/máxima das n barras ANTERIORES à barra i (não inclui i)."""
    rolled = values.rolling(n, min_periods=n)
    return (rolled.min() if kind == "min" else rolled.max()).shift(1).to_numpy(dtype=float)


def _rsi(bars) -> Tuple[np.ndarray, np.ndarray]:
    value = indicators.rsi(bars["close"], 14).to_numpy(dtype=float)
    before = _shift(value)
    return (before < 30) & (value > before), (before > 70) & (value < before)


def _bollinger(bars) -> Tuple[np.ndarray, np.ndarray]:
    close = bars["close"].to_numpy(dtype=float)
    bands = indicators.bollinger(bars["close"], 20, 2.0)
    lower, upper = bands["lower"].to_numpy(dtype=float), bands["upper"].to_numpy(dtype=float)
    return ((_shift(close) < _shift(lower)) & (close >= lower),
            (_shift(close) > _shift(upper)) & (close <= upper))


def _structure(bars) -> Tuple[np.ndarray, np.ndarray]:
    close = bars["close"].to_numpy(dtype=float)
    high, low = bars["high"].to_numpy(dtype=float), bars["low"].to_numpy(dtype=float)
    c1, c2, c3 = _shift(close), _shift(close, 2), _shift(close, 3)
    return ((c1 < c2) & (c2 < c3) & (close > _shift(high)),
            (c1 > c2) & (c2 > c3) & (close < _shift(low)))


def _false_break(bars) -> Tuple[np.ndarray, np.ndarray]:
    close = bars["close"].to_numpy(dtype=float)
    floor = _prior_extreme(bars["low"], LOOKBACK, "min")
    ceiling = _prior_extreme(bars["high"], LOOKBACK, "max")
    return ((bars["low"].to_numpy(dtype=float) < floor) & (close > floor),
            (bars["high"].to_numpy(dtype=float) > ceiling) & (close < ceiling))


def _exhaustion(bars) -> Tuple[np.ndarray, np.ndarray]:
    o, h, l, c = (bars[k].to_numpy(dtype=float) for k in ("open", "high", "low", "close"))
    atr = indicators.atr(bars, 14).to_numpy(dtype=float)
    spread = h - l
    with np.errstate(divide="ignore", invalid="ignore"):
        lower_wick = (np.minimum(o, c) - l) / spread
        upper_wick = (h - np.maximum(o, c)) / spread
        position = (c - l) / spread               # 1 = fechou na máxima
    wide = (spread > 0) & (spread >= 0.8 * atr)
    return (wide & (lower_wick >= 0.6) & (position >= 2 / 3),
            wide & (upper_wick >= 0.6) & (position <= 1 / 3))


FAMILIES: Dict[str, Tuple[str, Callable]] = {
    "rsi": ("RSI 14 sai do extremo (30/70) virando a favor", _rsi),
    "bollinger": ("fecha de volta dentro da banda de Bollinger 20/2", _bollinger),
    "estrutura": ("fecha além do extremo da barra anterior depois de dois fechamentos contra", _structure),
    "falso_rompimento": (f"fura o extremo de {LOOKBACK} barras e fecha de volta", _false_break),
    "exaustao": ("pavio de rejeição ≥ 60% em barra de amplitude ≥ 0,8 ATR", _exhaustion),
}
DEFAULT = tuple(FAMILIES)


def parse(names) -> Tuple[str, ...]:
    """Lista validada de famílias ('' ou None = todas)."""
    if isinstance(names, str):
        names = [n.strip() for n in names.split(",") if n.strip()]
    chosen = tuple(dict.fromkeys(names or DEFAULT))
    unknown = [n for n in chosen if n not in FAMILIES]
    if unknown:
        raise ValueError(f"Sinal de reversão desconhecido: {', '.join(unknown)}. Disponíveis: {', '.join(FAMILIES)}.")
    return chosen


def compute(bars: pd.DataFrame, names: Iterable[str] = DEFAULT, min_signals: int = 1) -> dict:
    """
    {"reversal": {1: bool[], -1: bool[]}, "continuation": {1: ..., -1: ...}, "detail": {família: {1: ..., -1: ...}},
     "names": (...), "min_signals": k}, alinhado às barras (ordenadas por tempo).
    """
    chosen = parse(names)
    if isinstance(min_signals, bool) or int(min_signals) < 1 or int(min_signals) > len(chosen):
        raise ValueError(f"min_sinais deve ficar entre 1 e {len(chosen)} (famílias escolhidas).")
    data = bars.reset_index(drop=True)
    detail = {}
    count = {1: np.zeros(len(data), dtype=int), -1: np.zeros(len(data), dtype=int)}
    for name in chosen:
        long_, short = FAMILIES[name][1](data)
        detail[name] = {1: np.nan_to_num(long_, nan=0).astype(bool), -1: np.nan_to_num(short, nan=0).astype(bool)}
        for d in (1, -1):
            count[d] += detail[name][d]
    reversal = {d: count[d] >= int(min_signals) for d in (1, -1)}
    close = data["close"].to_numpy(dtype=float)
    floor = _prior_extreme(data["low"], LOOKBACK, "min")
    ceiling = _prior_extreme(data["high"], LOOKBACK, "max")
    any_family = {d: np.any([FAMILIES[n][1](data)[0 if d > 0 else 1] for n in FAMILIES], axis=0) for d in (1, -1)}
    continuation = {1: np.nan_to_num(close < floor, nan=0).astype(bool) & ~any_family[1],
                    -1: np.nan_to_num(close > ceiling, nan=0).astype(bool) & ~any_family[-1]}
    return {"reversal": reversal, "continuation": continuation, "detail": detail, "names": chosen,
            "min_signals": int(min_signals)}
