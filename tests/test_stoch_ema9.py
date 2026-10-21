"""S10: oráculo Decimal independente da implementação e prefixos fechados."""
from decimal import Decimal, localcontext

import numpy as np
import pandas as pd
import pytest

from mt5_extracao.composition import Composition, base_composition, compatibility
from mt5_extracao.composition.registry import STRATEGY_PARAMETERS
from mt5_extracao.strategies import SIGNAL_COLUMNS, get_strategy
from mt5_extracao.strategies.stoch_ema9 import slow_stochastic

EMPTY = np.finfo(float).max


def market(n=700):
    rng = np.random.default_rng(38)
    close = 100 + np.cumsum(rng.normal(0, .7, n) + np.where((np.arange(n)//100) % 2 == 0, .3, -.3))
    return pd.DataFrame(dict(time=pd.date_range("2026-10-20", periods=n, freq="5min"), open=close,
        high=close+rng.uniform(.1, 1.5, n), low=close-rng.uniform(.1, 1.5, n), close=close, spread=0))


def decimal_reference(data, p):
    """Recalcula janelas por max/min e somas Decimal, sem helpers do produto."""
    with localcontext() as ctx:
        ctx.prec = 50
        h, l, c = ([Decimal(str(v)) for v in data[col]] for col in ("high", "low", "close"))
        k, d, ema = [None]*len(data), [None]*len(data), []
        alpha = Decimal(2) / Decimal(p["ema_period"]+1)
        for i, price in enumerate(c):
            ema.append(price if not ema else alpha*price+(1-alpha)*ema[-1])
            if i >= p["k_period"] + p["slowing"] - 2:
                numerator, denominator = Decimal(0), Decimal(0)
                for j in range(i-p["slowing"]+1, i+1):
                    lo, hi = min(l[j-p["k_period"]+1:j+1]), max(h[j-p["k_period"]+1:j+1])
                    numerator += c[j]-lo
                    denominator += hi-lo
                if denominator > 0:
                    k[i] = 100*numerator/denominator
            if i >= p["d_period"]-1 and all(v is not None for v in k[i-p["d_period"]+1:i+1]):
                d[i] = sum(k[i-p["d_period"]+1:i+1]) / p["d_period"]
        return k, d, ema


def test_ratio_of_sums_d_ema_seed_and_every_signal_match_decimal():
    data = market()
    strategy = get_strategy("stoch_ema9")
    p = strategy.params
    k, d = slow_stochastic(data)
    expected_k, expected_d, ema = decimal_reference(data, p)
    signals = strategy.gerar_sinais(data)
    for i in range(len(data)):
        assert np.isnan(k.iloc[i]) if expected_k[i] is None else k.iloc[i] == pytest.approx(float(expected_k[i]), abs=2e-12)
        assert np.isnan(d.iloc[i]) if expected_d[i] is None else d.iloc[i] == pytest.approx(float(expected_d[i]), abs=2e-12)
        ready = i >= strategy.min_bars-1 and expected_d[i-1] is not None and expected_d[i] is not None
        buy = ready and expected_k[i-1] <= expected_d[i-1] and expected_k[i] > expected_d[i] and expected_k[i] > 75 and Decimal(str(data.close.iloc[i])) > ema[i]
        sell = ready and expected_k[i-1] >= expected_d[i-1] and expected_k[i] < expected_d[i] and expected_k[i] < 25 and Decimal(str(data.close.iloc[i])) < ema[i]
        assert signals.entry_long.iloc[i] == bool(buy)
        assert signals.entry_short.iloc[i] == bool(sell)
        if i >= strategy.min_bars-1:
            assert signals.stop_long.iloc[i] == pytest.approx(float(ema[i]), abs=1e-12)
            assert signals.stop_short.iloc[i] == signals.stop_long.iloc[i]
    assert signals.entry_long.any() and signals.entry_short.any()
    assert list(signals) == list(SIGNAL_COLUMNS)
    assert not signals.exit_long.any() and not signals.exit_short.any()
    assert signals.target_long.isna().all() and signals.target_short.isna().all()
    # Na ausência de tick/spec a estratégia entrega a EMA em preço, sem ponto FX fabricado.
    assert strategy.exit_rules().signal_stop_offset_points == 50


def test_ratio_of_sums_is_not_average_of_raw_k_and_flat_range_is_undefined():
    data = pd.DataFrame(dict(high=[2., 4., 10., 8.], low=[0., 0., 0., 0.], close=[1., 1., 8., 2.]))
    k, d = slow_stochastic(data, 2, 2, 2)
    assert k.iloc[2] == pytest.approx(100*9/14)
    assert k.iloc[2] != pytest.approx((25+80)/2)
    assert d.iloc[3] == pytest.approx((100*9/14+100*10/20)/2)
    flat = data.assign(high=1., low=1., close=1.)
    k, d = slow_stochastic(flat, 2, 1, 1)
    assert k.isna().all() and d.isna().all()


def test_prefix_causality_preserves_all_columns_and_nondefault_index():
    data = market(510)
    data.index = pd.Index(np.arange(len(data))*3+17, name="source_bar")
    strategy = get_strategy("stoch_ema9", ema_period=37)
    full = strategy.gerar_sinais(data)
    for n in (0, 1, 17, 18, 19, 20, 299, 300, 338, 344, 427, 510):
        pd.testing.assert_frame_equal(strategy.gerar_sinais(data.iloc[:n]), full.iloc[:n])
    changed = data.copy()
    changed.iloc[430:, changed.columns.get_indexer(["open", "high", "low", "close"])] *= 10
    pd.testing.assert_frame_equal(strategy.gerar_sinais(changed).iloc[:430], full.iloc[:430])


@pytest.mark.parametrize("k_period,slowing,d_period,n", [(14, 3, 3, 300), (300, 3, 3, 305)])
def test_first_eligible_bar_has_no_extra_warmup_bar(k_period, slowing, d_period, n):
    data = market(n).assign(open=100., high=110., low=90., close=100.)
    data.loc[n-1, ["open", "close"]] = 101.
    strategy = get_strategy("stoch_ema9", k_period=k_period, slowing=slowing,
                            d_period=d_period, upper_level=51.)
    signals = strategy.gerar_sinais(data)
    assert strategy.min_bars == n
    assert not signals.entry_long.iloc[:-1].any()
    assert signals.stop_long.iloc[:-1].isna().all()
    assert signals.entry_long.iloc[-1]


@pytest.mark.parametrize("direction,level", [(1, 80.), (-1, 20.)])
def test_current_threshold_is_strict_previous_equality_allows_cross(direction, level):
    data = market(300).assign(open=100., high=110., low=90., close=100.)
    data.loc[299, ["open", "close"]] = 100+direction*6
    params = dict(k_period=2, slowing=1, d_period=2)
    key, column = ("upper_level", "entry_long") if direction > 0 else ("lower_level", "entry_short")
    equal = get_strategy("stoch_ema9", **params, **{key: level}).gerar_sinais(data)
    crossed = get_strategy("stoch_ema9", **params, **{key: level-direction*.1}).gerar_sinais(data)
    assert not equal[column].iloc[-1]
    assert crossed[column].iloc[-1]
    assert not crossed[column].iloc[:-1].any()
    # Mesmo cruzamento do oscilador, mas fechamento do lado errado da EMA: sem entrada.
    data.loc[:298, ["open", "close"]] = 109 if direction > 0 else 91
    assert not get_strategy("stoch_ema9", **params, **{key: level-direction*.1}).gerar_sinais(data)[column].iloc[-1]


@pytest.mark.parametrize("column", ["open", "high", "low", "close"])
@pytest.mark.parametrize("value", [np.nan, np.inf, -np.inf, EMPTY])
def test_bad_ohlc_invalidates_ema_prefix_without_changing_past(column, value):
    clean = market(420)
    data = clean.copy()
    data.loc[302, column] = value
    with np.errstate(over="ignore", invalid="ignore"):
        signals = get_strategy("stoch_ema9").gerar_sinais(data)
    pd.testing.assert_frame_equal(signals.iloc[:302], get_strategy("stoch_ema9").gerar_sinais(clean.iloc[:302]))
    assert signals.stop_long.iloc[302:].isna().all()
    assert signals.stop_short.iloc[302:].isna().all()
    assert not signals.entry_long.iloc[302:].any() and not signals.entry_short.iloc[302:].any()
    if column != "open":
        k, d = slow_stochastic(data)
        assert k.iloc[302:318].isna().all()
        assert k.iloc[318:].notna().all() and d.iloc[320:].notna().all()


def test_overflow_denominator_and_short_or_empty_frames_are_undefined():
    data = market(25).assign(high=1e308, low=-1e308, close=1.)
    k, d = slow_stochastic(data)
    assert k.isna().all() and d.isna().all()
    for n in (0, 1, 15, 17):
        sig = get_strategy("stoch_ema9").gerar_sinais(market(n))
        assert len(sig) == n and not sig.entry_long.any() and not sig.entry_short.any()


@pytest.mark.parametrize("name,value", [
    ("k_period", 1), ("k_period", 1001), ("slowing", 101), ("d_period", 0),
    ("ema_period", True), ("ema_period", 9.), ("ema_period", 1001),
    ("upper_level", np.nan), ("lower_level", np.inf), ("upper_level", True),
    ("upper_level", 25), ("lower_level", 75), ("lower_level", -1),
    ("stop_offset_points", -1), ("stop_offset_points", .5), ("stop_offset_points", True),
    ("target_points", 2147483648), ("target_points", "10"), ("upper_level", 10**400),
])
def test_typed_limits_and_cross_parameter_constraints(name, value):
    with pytest.raises(ValueError, match="S10 requer"):
        get_strategy("stoch_ema9", **{name: value})


def test_composition_registration_units_roundtrip_and_no_native_capability_granted():
    strategy = get_strategy("stoch_ema9", stop_offset_points=0, target_points=0)
    assert strategy.exit_rules().signal_target_points is None
    composition = base_composition("stoch_ema9", symbol="TEST", timeframe="M5")
    assert Composition(composition.to_json()).config_hash == composition.config_hash
    assert compatibility(composition) == []
    assert compatibility(composition, backend="native")
    params = STRATEGY_PARAMETERS["stoch_ema9"]
    assert params["stop_offset_points"].unit == params["target_points"].unit == "point"
    assert not params["stop_offset_points"].accepts(1.)
    assert not params["k_period"].accepts(True)
    invalid = composition.to_dict()
    invalid["strategy"]["params"]["target_points"] = -1
    with pytest.raises(ValueError):
        Composition(invalid)
