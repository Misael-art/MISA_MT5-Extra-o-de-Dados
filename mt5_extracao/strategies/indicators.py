"""
Indicadores técnicos puros (pandas/numpy), usados pelas estratégias, pelo backtester
e pela triagem de ativos. Sem MT5, sem banco, sem estado.

Convenções:
- Entradas: Series de preço ou DataFrame com colunas open/high/low/close.
- Médias de Wilder (ATR, RSI, ADX) usam ewm(alpha=1/n, adjust=False), como o MT5/TradingView.
- O valor na barra i usa somente dados até a barra i (sem olhar o futuro).
"""
import math

import numpy as np
import pandas as pd


def sma(series: pd.Series, n: int) -> pd.Series:
    return series.rolling(n, min_periods=n).mean()


def ema(series: pd.Series, n: int) -> pd.Series:
    return series.ewm(span=n, adjust=False, min_periods=n).mean()


def rma(series: pd.Series, n: int) -> pd.Series:
    """Média móvel de Wilder."""
    return series.ewm(alpha=1.0 / n, adjust=False, min_periods=n).mean()


ALLIGATOR_LINES = (("jaw", 13, 8), ("teeth", 8, 5), ("lips", 5, 3))


def smma(series: pd.Series, n: int) -> pd.Series:
    """SMMA com semente SMA de n valores consecutivos; lacuna reinicia a semente.

    Variante MetaQuotes: (anterior * (n - 1) + preço) / n. Não substitui o
    rma legado, cuja semente e tratamento de lacunas são diferentes.
    """
    if isinstance(n, bool) or not isinstance(n, (int, np.integer)) or n < 1:
        raise ValueError("O período da SMMA precisa ser um inteiro positivo.")
    values = pd.to_numeric(series, errors="coerce").to_numpy(dtype=float, na_value=np.nan)
    result = np.full(len(values), np.nan)
    seed, previous = [], None
    for position, value in enumerate(values):
        if not np.isfinite(value):
            seed, previous = [], None
        elif previous is None:
            seed.append(float(value))
            if len(seed) == n:
                previous = math.fsum(seed) / n
                result[position] = previous
                seed = []
        else:
            previous = (previous * (n - 1) + value) / n
            result[position] = previous
    return pd.Series(result, index=series.index, dtype=float)


def alligator(df: pd.DataFrame) -> pd.DataFrame:
    """Linhas NÃO deslocadas: só usam high/low da barra de cálculo e anteriores.

    ALLIGATOR_LINES declara o deslocamento exclusivamente visual. Alinhamento
    é +1 para lips>teeth>jaw, -1 para lips<teeth<jaw, 0 para mistura/empate;
    não é ordem nem diagnóstico estatístico de regime. Ausência nunca vira 0.
    """
    high = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    valid = np.isfinite(high) & np.isfinite(low) & high.ge(low)
    median = ((high + low) / 2.0).where(valid)
    lines = pd.DataFrame({name: smma(median, period) for name, period, _shift in ALLIGATOR_LINES}, index=df.index)
    available = lines.notna().all(axis=1)
    up = (lines.lips > lines.teeth) & (lines.teeth > lines.jaw)
    down = (lines.lips < lines.teeth) & (lines.teeth < lines.jaw)
    lines["alignment"] = (up.astype(float) - down.astype(float)).where(available)
    lines["separation"] = (lines[["jaw", "teeth", "lips"]].max(axis=1, skipna=False) -
                           lines[["jaw", "teeth", "lips"]].min(axis=1, skipna=False))
    return lines


def ao_ac(df: pd.DataFrame) -> pd.DataFrame:
    """AO 5/34 e AC com SMA5 do mesmo AO, sem deslocamento ou sinal de ordem.

    Janelas completas de medianos finitos; ausência nunca vira zero. Cor é uma
    transformação separada da variação: +1 subida/verde, -1 descida/vermelho,
    0 empate/neutro. Essa política de empate é explícita, não uma paleta nativa.
    """
    high = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    valid = np.isfinite(high) & np.isfinite(low) & high.ge(low)
    median = (high + low) / 2.0
    # Preserva o caminho normal/subnormal; só a soma excessiva usa metades.
    median = median.mask(valid & ~np.isfinite(median), high / 2.0 + low / 2.0).where(valid)
    median = median.where(np.isfinite(median))
    def finite_mean(values):
        try:
            return _finite_window_mean(values)
        except OverflowError:
            # A soma pode exceder float64 mesmo quando a média é finita.
            scale = max(abs(float(value)) for value in values)
            return math.fsum(float(value) / scale for value in values) / len(values) * scale
    def mean(series, period):
        return series.rolling(period, min_periods=period).apply(finite_mean, raw=True)
    ao = mean(median, 5) - mean(median, 34)
    ac = ao - mean(ao, 5)
    result = pd.DataFrame({"ao": ao, "ac": ac}, index=df.index)
    for name in ("ao", "ac"):
        slope = result[name].diff()
        result[name + "_slope_1"] = slope
        result[name + "_color"] = np.sign(slope)
    result["ao_acceleration_1"] = result["ao_slope_1"].diff()
    previous = ao.shift(1)
    result["ao_zero_cross"] = (((previous <= 0) & (ao > 0)).astype(float) -
                                ((previous >= 0) & (ao < 0)).astype(float)).where(ao.notna() & previous.notna())
    return result


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


def _finite_window_mean(values):
    return math.fsum(float(value) for value in values) / len(values) if len(values) else np.nan


def _finite_window_std(values, ddof=0):
    if len(values) <= ddof:
        return np.nan
    mean = _finite_window_mean(values)
    return math.sqrt(math.fsum((float(value) - mean) ** 2 for value in values) / (len(values) - ddof))


def rolling_std(series: pd.Series, n: int, *, ddof: int = 1, stable_window: bool = False) -> pd.Series:
    """Desvio por janela; o modo estável usa somente os valores da própria janela."""
    window = series.rolling(n, min_periods=n)
    if stable_window:
        return window.apply(lambda values: _finite_window_std(values, ddof), raw=True)
    return window.std(ddof=ddof)


def bollinger(close: pd.Series, n: int = 20, k: float = 2.0, *, stable_window: bool = False) -> pd.DataFrame:
    """Bandas populacionais; stable_window independe do prefixo anterior à janela.

    O modo padrão preserva o cálculo pandas das estratégias existentes. O modo
    finito usa soma compensada e variância centrada em duas passagens, para que
    reconstrução e retomada usem exatamente a mesma janela e ordem de operações.
    """
    if stable_window:
        window = close.rolling(n, min_periods=n)
        mid = window.apply(_finite_window_mean, raw=True)
        std = rolling_std(close, n, ddof=0, stable_window=True)
    else:
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



def _pivot_inputs(bars):
    """Valores por posição, sem preencher ausências nem inferir ordem intrabarra."""
    columns = [pd.to_numeric(bars.get(name, pd.Series(np.nan, index=bars.index)), errors="coerce")
               .to_numpy(dtype=float) for name in ("high", "low")]
    reset = bars.get("_pivot_reset_before", pd.Series(False, index=bars.index)).fillna(False).to_numpy(dtype=bool)
    return columns[0], columns[1], reset


def causal_fractals(bars, width=2):
    """Fractal estrito: origem central, publicação duas barras depois (width=2).

    Empates rejeitam o pivô; uma fonte inválida reinicia a janela e os níveis.
    Cada snapshot contém apenas o que era conhecido naquele prefixo. Candidatos
    são desenho provisório e nunca níveis confirmados para uma estratégia.
    """
    if isinstance(width, bool) or not isinstance(width, int) or width < 1:
        raise ValueError("A largura do fractal precisa ser um inteiro positivo.")
    high, low, reset = _pivot_inputs(bars)
    result, start, levels = [], 0, {"top": None, "bottom": None}
    for i, (h, l) in enumerate(zip(high, low)):
        if reset[i]:
            start, levels = i, {"top": None, "bottom": None}
        valid = np.isfinite(h) and np.isfinite(l) and h >= l
        if not valid:
            start, levels = i + 1, {"top": None, "bottom": None}
        count = i - start + 1
        if count >= 2 * width + 1:
            origin = i - width
            for kind, values, greater in (("top", high, True), ("bottom", low, False)):
                others = [values[j] for j in range(origin - width, i + 1) if j != origin]
                if all(values[origin] > x if greater else values[origin] < x for x in others):
                    levels[kind] = dict(kind=kind, price=float(values[origin]), origin=origin, confirmation=i)
        pending = []
        if valid:
            for origin in range(max(start + width, i - width + 1), i + 1):
                for kind, values, greater in (("top", high, True), ("bottom", low, False)):
                    others = [values[j] for j in range(origin - width, i + 1) if j != origin]
                    if all(values[origin] > x if greater else values[origin] < x for x in others):
                        pending.append(dict(kind=kind, price=float(values[origin]), origin=origin, confirmation=None))
        result.append(dict(valid=bool(valid), ready=count >= 2 * width + 1,
                           **levels, provisional=pending))
    return result


def causal_zigzag(bars, depth=12, deviation=0.01, backstep=3):
    """Variante causal de reversão percentual, distinta do ZigZag retrospectivo MT5.

    Depth exige uma janela válida e qualifica a reversão como extremo da janela.
    Backstep exige esse número de barras entre candidato e reversão. O candidato
    move-se só com extremo estritamente melhor; empate conserva a primeira origem.
    Uma barra que melhora o candidato não o confirma, mesmo que também reverta:
    OHLC não informa a ordem intrabarra. Confirmados nunca são apagados por append.
    """
    if (isinstance(depth, bool) or not isinstance(depth, int) or depth < 2 or
            isinstance(backstep, bool) or not isinstance(backstep, int) or not 1 <= backstep < depth or
            isinstance(deviation, bool) or not isinstance(deviation, (int, float, np.integer, np.floating)) or
            not np.isfinite(deviation) or not 0 < deviation < 1):
        raise ValueError("Confira depth>=2, 1<=backstep<depth e desvio percentual entre 0 e 1.")
    high, low, reset = _pivot_inputs(bars)
    result, start, candidate = [], 0, None
    levels = {"top": None, "bottom": None}
    for i, (h, l) in enumerate(zip(high, low)):
        if reset[i]:
            start, candidate, levels = i, None, {"top": None, "bottom": None}
        valid = np.isfinite(h) and np.isfinite(l) and h >= l > 0
        if not valid:
            start, candidate, levels = i + 1, None, {"top": None, "bottom": None}
        ready = i - start + 1 >= depth
        if valid and ready:
            left = i - depth + 1
            hi = max(range(left, i + 1), key=lambda j: high[j])
            lo = min(range(left, i + 1), key=lambda j: low[j])
            if candidate is None:
                # Empate de origens (inclui janela plana/outside) não define direção.
                if hi != lo:
                    kind, origin = ("top", hi) if hi < lo else ("bottom", lo)
                    candidate = dict(kind=kind, origin=origin,
                                     price=float(high[origin] if kind == "top" else low[origin]), confirmation=None)
            if candidate is not None:
                top = candidate["kind"] == "top"
                improved = h > candidate["price"] if top else l < candidate["price"]
                if improved:
                    candidate = dict(kind=candidate["kind"], origin=i, price=float(h if top else l), confirmation=None)
                else:
                    age = i - candidate["origin"]
                    reversal = (l <= low[lo] and l / candidate["price"] <= 1 - deviation if top else
                                h >= high[hi] and candidate["price"] / h <= 1 / (1 + deviation))
                    if age >= backstep and reversal:
                        levels[candidate["kind"]] = {**candidate, "confirmation": i}
                        candidate = dict(kind="bottom" if top else "top", origin=i,
                                         price=float(l if top else h), confirmation=None)
        result.append(dict(valid=bool(valid), ready=bool(ready), **levels,
                           provisional=[dict(candidate)] if candidate is not None else []))
    return result
