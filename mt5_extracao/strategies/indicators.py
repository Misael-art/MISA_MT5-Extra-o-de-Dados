"""
Indicadores técnicos puros (pandas/numpy), usados pelas estratégias, pelo backtester
e pela triagem de ativos. Sem MT5, sem banco, sem estado.

Convenções:
- Entradas: Series de preço ou DataFrame com colunas open/high/low/close.
- Médias de Wilder (ATR, RSI, ADX) usam ewm(alpha=1/n, adjust=False), como o MT5/TradingView.
- O valor na barra i usa somente dados até a barra i (sem olhar o futuro).
"""
import numpy as np
import pandas as pd


def sma(series: pd.Series, n: int) -> pd.Series:
    return series.rolling(n, min_periods=n).mean()


def ema(series: pd.Series, n: int) -> pd.Series:
    return series.ewm(span=n, adjust=False, min_periods=n).mean()


def rma(series: pd.Series, n: int) -> pd.Series:
    """Média móvel de Wilder."""
    return series.ewm(alpha=1.0 / n, adjust=False, min_periods=n).mean()


def true_range(df: pd.DataFrame) -> pd.Series:
    prev_close = df["close"].shift(1)
    ranges = pd.concat([df["high"] - df["low"],
                        (df["high"] - prev_close).abs(),
                        (df["low"] - prev_close).abs()], axis=1)
    return ranges.max(axis=1, skipna=True)


def atr(df: pd.DataFrame, n: int = 14) -> pd.Series:
    return rma(true_range(df), n)


def rsi(close: pd.Series, n: int = 14) -> pd.Series:
    delta = close.diff()
    gain = rma(delta.clip(lower=0), n)
    loss = rma((-delta).clip(lower=0), n)
    with np.errstate(divide="ignore", invalid="ignore"):
        rs = gain / loss
        out = 100 - 100 / (1 + rs)
    # Só altas: loss == 0 -> 100; sem movimento: 50
    out = out.where(loss != 0, 100.0)
    out = out.where(~((gain == 0) & (loss == 0)), 50.0)
    return out.where(gain.notna())


def adx(df: pd.DataFrame, n: int = 14) -> pd.DataFrame:
    """Retorna DataFrame com plus_di, minus_di e adx."""
    up = df["high"].diff()
    down = -df["low"].diff()
    plus_dm = pd.Series(np.where((up > down) & (up > 0), up, 0.0), index=df.index)
    minus_dm = pd.Series(np.where((down > up) & (down > 0), down, 0.0), index=df.index)
    tr = rma(true_range(df), n)
    with np.errstate(divide="ignore", invalid="ignore"):
        plus_di = 100 * rma(plus_dm, n) / tr
        minus_di = 100 * rma(minus_dm, n) / tr
        dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di)
    dx = dx.replace([np.inf, -np.inf], np.nan).fillna(0.0).where(tr.notna())
    return pd.DataFrame({"plus_di": plus_di, "minus_di": minus_di, "adx": rma(dx, n)})


def bollinger(close: pd.Series, n: int = 20, k: float = 2.0) -> pd.DataFrame:
    mid = sma(close, n)
    std = close.rolling(n, min_periods=n).std(ddof=0)
    return pd.DataFrame({"mid": mid, "upper": mid + k * std, "lower": mid - k * std})


def keltner(df: pd.DataFrame, n: int = 20, mult: float = 1.5) -> pd.DataFrame:
    mid = ema(df["close"], n)
    width = mult * atr(df, n)
    return pd.DataFrame({"mid": mid, "upper": mid + width, "lower": mid - width})


def donchian(df: pd.DataFrame, n: int) -> pd.DataFrame:
    """Máxima/mínima das últimas n barras, incluindo a atual (use .shift(1) para rompimento)."""
    return pd.DataFrame({"upper": df["high"].rolling(n, min_periods=n).max(),
                         "lower": df["low"].rolling(n, min_periods=n).min()})


def efficiency_ratio(close: pd.Series, n: int = 10) -> pd.Series:
    """Kaufman: |variação em n barras| / soma das variações absolutas. 1 = movimento limpo."""
    change = (close - close.shift(n)).abs()
    path = close.diff().abs().rolling(n, min_periods=n).sum()
    with np.errstate(divide="ignore", invalid="ignore"):
        er = change / path
    return er.where(path != 0, 0.0).where(change.notna())


def _linreg_windows(series: pd.Series, n: int):
    values = series.to_numpy(dtype=float)
    if len(values) < n:
        return None
    windows = np.lib.stride_tricks.sliding_window_view(values, n)
    x = np.arange(n, dtype=float)
    x_c = x - x.mean()
    y_c = windows - windows.mean(axis=1, keepdims=True)
    slope = (y_c @ x_c) / (x_c @ x_c)
    ss_tot = (y_c ** 2).sum(axis=1)
    with np.errstate(divide="ignore", invalid="ignore"):
        r2 = np.where(ss_tot > 0, (slope * (y_c @ x_c)) / ss_tot, 0.0)
    pad = np.full(n - 1, np.nan)
    return np.concatenate([pad, slope]), np.concatenate([pad, r2])


def linreg_slope(series: pd.Series, n: int = 50) -> pd.Series:
    res = _linreg_windows(series, n)
    return pd.Series(np.nan if res is None else res[0], index=series.index, dtype=float)


def linreg_r2(series: pd.Series, n: int = 50) -> pd.Series:
    res = _linreg_windows(series, n)
    return pd.Series(np.nan if res is None else res[1], index=series.index, dtype=float)


def rolling_percentile(series: pd.Series, n: int = 100) -> pd.Series:
    """Percentil (0–100) do valor atual dentro das últimas n barras."""
    return series.rolling(n, min_periods=n).apply(
        lambda w: 100.0 * (w[:-1] < w[-1]).sum() / (len(w) - 1) if len(w) > 1 else np.nan, raw=True)


def swing_low(df: pd.DataFrame, n: int = 10) -> pd.Series:
    """Menor mínima das últimas n barras (referência de stop para compras)."""
    return df["low"].rolling(n, min_periods=n).min()


def swing_high(df: pd.DataFrame, n: int = 10) -> pd.Series:
    return df["high"].rolling(n, min_periods=n).max()
