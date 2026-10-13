"""T6.2: seis estratégias com interface comum."""
import numpy as np
import pandas as pd
import pytest

from mt5_extracao.strategies import AVOID, REGISTRY, SIGNAL_COLUMNS, get_strategy, parse_keys
from tests.market_data import ohlc_from_close, random_walk, trend

KEYS = ["ema_atr", "donchian", "bb_rsi", "orb", "pullback", "squeeze"]


def intraday(days=30, seed=3, bars_per_day=60, minutes=5):
    """Dias de 5 min começando às 09:00, com tendência dentro de cada dia."""
    rng = np.random.default_rng(seed)
    frames = []
    price = 100.0
    for d in pd.bdate_range("2024-01-01", periods=days):
        close = price + np.cumsum(rng.normal(0.05 if d.day % 2 else -0.05, 0.3, bars_per_day))
        df = ohlc_from_close(close, start=d + pd.Timedelta(hours=9), freq=f"{minutes}min")
        frames.append(df)
        price = close[-1]
    return pd.concat(frames, ignore_index=True)


def test_registry_has_six_strategies_with_info():
    assert list(REGISTRY) == KEYS
    for key, cls in REGISTRY.items():
        s = cls()
        assert s.key == key
        assert s.info.name and s.info.regime and s.info.win_rate and s.info.reward_risk
        assert s.info.complexity in ("baixa", "média", "alta")
        assert set(s.param_grid) <= set(s.default_params)
        combos = np.prod([len(v) for v in s.param_grid.values()])
        assert combos <= 12
    assert len(AVOID) == 5


@pytest.mark.parametrize("key", KEYS)
def test_signal_contract(key):
    df = intraday() if key == "orb" else random_walk(800, seed=5)
    sig = get_strategy(key).gerar_sinais(df)
    assert list(sig.columns) == list(SIGNAL_COLUMNS)
    assert sig.index.equals(df.index)
    for col in ("entry_long", "entry_short", "exit_long", "exit_short"):
        assert sig[col].dtype == bool
    # Toda entrada tem stop do lado certo
    assert (sig.loc[sig["entry_long"], "stop_long"] < df.loc[sig["entry_long"], "close"]).all()
    assert (sig.loc[sig["entry_short"], "stop_short"] > df.loc[sig["entry_short"], "close"]).all()


@pytest.mark.parametrize("key", KEYS)
def test_no_lookahead(key):
    df = intraday() if key == "orb" else random_walk(800, seed=5)
    cut = len(df) - 100
    changed = df.copy()
    changed.loc[cut:, ["open", "high", "low", "close"]] *= 1.3
    a = get_strategy(key).gerar_sinais(df).iloc[:cut]
    b = get_strategy(key).gerar_sinais(changed).iloc[:cut]
    pd.testing.assert_frame_equal(a, b)


def test_unknown_param_and_strategy_messages():
    with pytest.raises(ValueError, match="Parâmetro"):
        get_strategy("donchian", periodo=3)
    with pytest.raises(ValueError, match="Estratégia desconhecida"):
        get_strategy("martingale")
    assert parse_keys("all") == KEYS
    assert parse_keys("orb, donchian") == ["orb", "donchian"]


def test_missing_columns_message():
    with pytest.raises(ValueError, match="Faltam colunas"):
        get_strategy("ema_atr").gerar_sinais(pd.DataFrame({"close": [1.0]}))


def test_trend_strategies_buy_in_uptrend():
    df = ohlc_from_close(np.concatenate([np.full(250, 100.0) + np.sin(np.arange(250)),
                                         100 + np.cumsum(np.full(200, 0.8))]))
    sig = get_strategy("donchian").gerar_sinais(df)
    assert sig["entry_long"].iloc[250:].any()
    assert not sig["entry_short"].iloc[260:].any()


def test_bb_rsi_only_in_range():
    noise = ohlc_from_close(100 + np.random.default_rng(4).normal(0, 1, 600))  # lateral: ADX baixo
    sig = get_strategy("bb_rsi").gerar_sinais(noise)
    assert sig["entry_long"].any() and sig["entry_short"].any()
    assert not get_strategy("bb_rsi").gerar_sinais(trend(600, step=1.0))["entry_long"].any()


def test_orb_one_entry_per_day_with_range_stop():
    df = intraday()
    sig = get_strategy("orb").gerar_sinais(df)
    entries = sig["entry_long"] | sig["entry_short"]
    per_day = entries.groupby(pd.to_datetime(df["time"]).dt.date).sum()
    assert entries.any() and per_day.max() == 1
    t = pd.to_datetime(df["time"])
    assert not entries[t.dt.time < pd.Timestamp("09:30").time()].any()
    first = df.index[entries][0]
    day_mask = (t.dt.date == t[first].date()) & (t.dt.time < pd.Timestamp("09:30").time())
    if sig.loc[first, "entry_long"]:
        assert sig.loc[first, "stop_long"] == df.loc[day_mask, "low"].min()
    assert get_strategy("orb").exit_rules().eod_exit


def test_pullback_exit_rules():
    rules = get_strategy("pullback").exit_rules()
    assert rules.target_r == 2.0 and rules.partial_r == 1.0 and rules.breakeven_after_partial


def test_squeeze_fires_after_compression():
    calm = 100 + 0.05 * np.sin(np.arange(200))
    breakout = calm[-1] + np.cumsum(np.full(40, 0.6))
    df = ohlc_from_close(np.concatenate([np.full(100, 100.0) + np.sin(np.arange(100)) * 3, calm, breakout]),
                         wick=0.3)
    sig = get_strategy("squeeze").gerar_sinais(df)
    assert sig["entry_long"].iloc[300:].any()
