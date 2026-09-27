"""T6.1: indicadores puros."""
import numpy as np
import pandas as pd
import pytest

from mt5_extracao.strategies import indicators as ind
from tests.market_data import ohlc_from_close, random_walk, sine, trend


def test_ema_sma_constant_series():
    s = pd.Series([5.0] * 30)
    assert ind.sma(s, 10).iloc[-1] == pytest.approx(5.0)
    assert ind.ema(s, 10).iloc[-1] == pytest.approx(5.0)
    assert ind.sma(s, 10).iloc[:9].isna().all()


def test_atr_constant_range():
    df = ohlc_from_close([100.0] * 50, wick=1.0)  # high-low = 2 em todas as barras
    assert ind.atr(df, 14).iloc[-1] == pytest.approx(2.0)
    assert ind.atr(df, 14).iloc[:13].isna().all()


def test_true_range_uses_gap_from_previous_close():
    df = pd.DataFrame({"open": [10, 20], "high": [11, 21], "low": [9, 19], "close": [10, 20]}, dtype=float)
    assert ind.true_range(df).iloc[1] == pytest.approx(11.0)  # 21 - 10


def test_rsi_extremes_and_bounds():
    up = pd.Series(np.arange(1, 60, dtype=float))
    assert ind.rsi(up, 14).iloc[-1] == pytest.approx(100.0)
    down = pd.Series(np.arange(60, 1, -1, dtype=float))
    assert ind.rsi(down, 14).iloc[-1] == pytest.approx(0.0)
    r = ind.rsi(random_walk(500)["close"], 14).dropna()
    assert ((r >= 0) & (r <= 100)).all()


def test_rsi_matches_wilder_reference():
    close = random_walk(400, seed=7)["close"]
    delta = close.diff()
    g, l = delta.clip(lower=0), (-delta).clip(lower=0)
    ag, al = g.iloc[1:15].mean(), l.iloc[1:15].mean()  # Wilder clássico: semente com média simples
    for i in range(15, len(close)):
        ag = (ag * 13 + g.iloc[i]) / 14
        al = (al * 13 + l.iloc[i]) / 14
    ref = 100 - 100 / (1 + ag / al)
    # A semente difere só no início; após centenas de barras os valores coincidem
    assert ind.rsi(close, 14).iloc[-1] == pytest.approx(ref, abs=1e-6)


def test_adx_high_in_trend_low_in_range():
    assert ind.adx(trend(400), 14)["adx"].iloc[-1] > 40
    assert ind.adx(sine(400, amp=2, period=10), 14)["adx"].iloc[-1] < 25
    t = ind.adx(trend(400), 14)
    assert t["plus_di"].iloc[-1] > t["minus_di"].iloc[-1]


def test_bollinger_keltner_donchian():
    df = random_walk(300)
    bb = ind.bollinger(df["close"], 20, 2)
    valid = bb.dropna()
    assert (valid["upper"] >= valid["mid"]).all() and (valid["lower"] <= valid["mid"]).all()
    std = df["close"].iloc[-20:].std(ddof=0)
    assert bb["upper"].iloc[-1] - bb["mid"].iloc[-1] == pytest.approx(2 * std)
    kc = ind.keltner(df, 20, 1.5)
    assert (kc.dropna()["upper"] > kc.dropna()["lower"]).all()
    dc = ind.donchian(df, 20)
    assert dc["upper"].iloc[-1] == df["high"].iloc[-20:].max()
    assert dc["lower"].iloc[-1] == df["low"].iloc[-20:].min()


def test_efficiency_ratio():
    line = pd.Series(np.arange(50, dtype=float))
    assert ind.efficiency_ratio(line, 10).iloc[-1] == pytest.approx(1.0)
    zigzag = pd.Series([0.0, 1.0] * 25)
    assert ind.efficiency_ratio(zigzag, 10).iloc[-1] == pytest.approx(0.0)


def test_linreg_slope_and_r2():
    line = pd.Series(3.0 + 2.0 * np.arange(80))
    assert ind.linreg_slope(line, 50).iloc[-1] == pytest.approx(2.0)
    assert ind.linreg_r2(line, 50).iloc[-1] == pytest.approx(1.0)
    assert ind.linreg_r2(line, 50).iloc[:49].isna().all()
    assert ind.linreg_r2(pd.Series([1.0] * 10), 50).isna().all()


def test_rolling_percentile():
    s = pd.Series(np.arange(200, dtype=float))
    assert ind.rolling_percentile(s, 100).iloc[-1] == pytest.approx(100.0)
    assert ind.rolling_percentile(s[::-1].reset_index(drop=True), 100).iloc[-1] == pytest.approx(0.0)


def test_no_lookahead():
    """Mudar o futuro não pode alterar valores passados."""
    df = random_walk(300)
    changed = df.copy()
    changed.loc[250:, ["open", "high", "low", "close"]] *= 1.5
    for fn in (lambda d: ind.atr(d), lambda d: ind.adx(d)["adx"], lambda d: ind.rsi(d["close"]),
               lambda d: ind.bollinger(d["close"])["upper"], lambda d: ind.efficiency_ratio(d["close"]),
               lambda d: ind.linreg_r2(d["close"])):
        pd.testing.assert_series_equal(fn(df).iloc[:250], fn(changed).iloc[:250], check_names=False)
