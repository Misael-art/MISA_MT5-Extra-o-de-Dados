"""Séries OHLC sintéticas e determinísticas para testar estratégias e triagem."""
import numpy as np
import pandas as pd


def ohlc_from_close(close, start="2020-01-01", freq="D", wick=0.5, spread=10, volume=1000):
    close = np.asarray(close, dtype=float)
    open_ = np.concatenate([[close[0]], close[:-1]])
    high = np.maximum(open_, close) + wick
    low = np.minimum(open_, close) - wick
    times = pd.date_range(start, periods=len(close), freq=freq)
    return pd.DataFrame({"time": times, "open": open_, "high": high, "low": low, "close": close,
                         "tick_volume": np.full(len(close), volume), "spread": np.full(len(close), spread),
                         "real_volume": np.zeros(len(close))})


def random_walk(n=1500, seed=1, drift=0.0, vol=1.0, start_price=100.0, **kw):
    rng = np.random.default_rng(seed)
    close = start_price + np.cumsum(rng.normal(drift, vol, n))
    return ohlc_from_close(np.maximum(close, 1.0), **kw)


def trend(n=600, step=0.5, noise=0.2, seed=2, **kw):
    rng = np.random.default_rng(seed)
    close = 100 + step * np.arange(n) + rng.normal(0, noise, n)
    return ohlc_from_close(close, **kw)


def sine(n=600, amp=5.0, period=40, **kw):
    close = 100 + amp * np.sin(2 * np.pi * np.arange(n) / period)
    return ohlc_from_close(close, **kw)
