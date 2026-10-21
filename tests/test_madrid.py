"""T19.39: sementes Decimal independentes, prefixos fechados e candidatos."""
from decimal import Decimal, localcontext

import numpy as np
import pandas as pd
import pytest

from mt5_extracao.composition import Composition, base_composition, compatibility
from mt5_extracao.composition.registry import STRATEGY_PARAMETERS
from mt5_extracao.strategies import SIGNAL_COLUMNS, get_strategy
from mt5_extracao.strategies.madrid import madrid_averages

EMPTY = np.finfo(float).max


def market(n=800):
    x = np.arange(n)
    close = 100 + .008*x + 3*np.sin(x/35) + .35*np.sin(x/2)
    return pd.DataFrame(dict(time=pd.date_range("2026-10-19", periods=n, freq="5min"),
        open=close, high=close+.7, low=close-.7, close=close, spread=0))


def decimal_reference(data, first=5, step=5):
    with localcontext() as ctx:
        ctx.prec = 60
        means, atrs, previous_close = [], [], None
        for row in data.itertuples():
            h, l, c = (Decimal(str(v)) for v in (row.high, row.low, row.close))
            tr = h-l if previous_close is None else max(h-l, abs(h-previous_close), abs(l-previous_close))
            alpha = Decimal(1)/14
            atrs.append(tr if not atrs else alpha*tr+(1-alpha)*atrs[-1])
            means.append([c if not means else Decimal(2)/Decimal(first+j*step+1)*c
                +(1-Decimal(2)/Decimal(first+j*step+1))*means[-1][j] for j in range(10)])
            previous_close = c
        return means, atrs


@pytest.mark.parametrize("first,step", [(5, 5), (100, 100), (1, 1)])
def test_ten_emas_atr_seed_and_gate_match_independent_decimal(first, step):
    data = market()
    strategy = get_strategy("madrid", first_period=first, period_step=step)
    means, atrs = decimal_reference(data, first, step)
    actual = madrid_averages(data, first, step)
    np.testing.assert_allclose(actual.iloc[:, :10], np.array(means, dtype=float), rtol=1e-13)
    np.testing.assert_allclose(actual.atr, np.array(atrs, dtype=float), rtol=1e-13)
    assert list(actual.iloc[0, :10]) == [data.close.iloc[0]]*10
    assert actual.atr.iloc[0] == data.high.iloc[0]-data.low.iloc[0]
    signals = strategy.gerar_sinais(data)
    buys = sells = 0
    for i, m in enumerate(means):
        c = Decimal(str(data.close.iloc[i]))
        up = all(a>b for a, b in zip(m, m[1:]))
        down = all(a<b for a, b in zip(m, m[1:]))
        ready = i >= 299
        buy = ready and up and Decimal(str(data.close.iloc[i-1])) <= means[i-1][0] and c>m[0] and c-m[0]<=atrs[i]
        sell = ready and down and Decimal(str(data.close.iloc[i-1])) >= means[i-1][0] and c<m[0] and m[0]-c<=atrs[i]
        assert signals.entry_long.iloc[i] == bool(buy)
        assert signals.entry_short.iloc[i] == bool(sell)
        buys += bool(buy); sells += bool(sell)
        if ready:
            assert signals.stop_long.iloc[i] == pytest.approx(float(m[-1]-Decimal('.25')*atrs[i]), abs=1e-11)
            assert signals.target_short.iloc[i] == pytest.approx(float(m[0]-atrs[i]), abs=1e-11)
    if first == 5:
        assert buys and sells
    if first == 1:
        assert not buys and not sells  # EMA1=C: configuração válida, sem cruzamento estrito
    assert strategy.min_bars == 300 and signals.stop_long.iloc[299:].notna().all()
    assert list(signals) == list(SIGNAL_COLUMNS) and not signals.exit_long.any() and not signals.exit_short.any()


def test_prefix_causality_nondefault_index_and_future_changes():
    data = market(550)
    data.index = pd.Index(np.arange(len(data))*3+13, name="source")
    strategy = get_strategy("madrid", first_period=100, period_step=100)
    full = strategy.gerar_sinais(data)
    for n in (0, 1, 2, 14, 299, 300, 427, 550):
        pd.testing.assert_frame_equal(strategy.gerar_sinais(data.iloc[:n]), full.iloc[:n])
    changed = data.copy()
    changed.loc[data.index[430:], ["high", "low", "close"]] *= 10
    pd.testing.assert_frame_equal(strategy.gerar_sinais(changed).iloc[:430], full.iloc[:430])


@pytest.mark.parametrize("column", ["high", "low", "close"])
@pytest.mark.parametrize("value", [np.nan, np.inf, -np.inf, EMPTY, 0., -1.])
def test_invalid_hlc_poisons_whole_remaining_prefix_without_reseed(column, value):
    data = market(400);clean = data.copy();data.loc[302, column] = value
    strategy = get_strategy("madrid")
    signals = strategy.gerar_sinais(data)
    pd.testing.assert_frame_equal(signals.iloc[:302], strategy.gerar_sinais(clean.iloc[:302]))
    assert signals.iloc[302:][["atr", "stop_long", "stop_short", "target_long", "target_short"]].isna().all().all()
    assert not signals.entry_long.iloc[302:].any() and not signals.entry_short.iloc[302:].any()


@pytest.mark.parametrize("params", [dict(distance_atr=1e308), dict(offset_atr=1e308)])
def test_derived_overflow_never_generates_operable_candidates_or_guard(params):
    data = market().assign(high=110., low=90.)
    signals = get_strategy("madrid", **params).gerar_sinais(data)
    assert not signals.entry_long.any() and not signals.entry_short.any()
    assert signals.stop_long.isna().all() and signals.stop_short.isna().all()


def test_points_publish_m1_reference_and_unfiltered_distance_candidates():
    data = market();strategy = get_strategy("madrid", distance_points=1)
    signals = strategy.gerar_sinais(data);maths = madrid_averages(data)
    np.testing.assert_allclose(signals.target_long.iloc[299:], maths.m1.iloc[299:])
    pd.testing.assert_series_equal(signals.target_long, signals.target_short, check_names=False)
    assert signals.entry_long.any() and signals.entry_short.any()
    assert strategy.exit_rules().signal_reference_distance_points == 1
    assert not strategy.exit_rules().absolute_signal_target
    assert strategy.exit_rules().move_guard_from_signal_stop


def crossing(direction=1):
    # EMA3 alpha=.5: salto90→110 dáM1=100; retornoC100 mantémigualdade.
    # H-L=64 e todososclosesdentro: TR/ATR64 exatos, distância1=1/64×ATR.
    data = market(300).assign(open=90., high=128., low=64., close=90.)
    data.loc[297:, "close"] = [110., 100., 102.]
    data["open"] = data.close
    if direction < 0:
        data[["open", "high", "low", "close"]] = 200-data[["open", "low", "high", "close"]].to_numpy()
    return data


@pytest.mark.parametrize("direction", [1, -1])
@pytest.mark.parametrize("distance,allowed", [(.99, False), (1., True), (1.01, True)])
def test_real_first_bar_previous_equality_and_inclusive_atr_distance(direction, distance, allowed):
    data = crossing(direction)
    context = madrid_averages(data, 3, 1)
    assert context.m1.iloc[298] == data.close.iloc[298] == 100.
    assert context.atr.eq(64.).all()
    signals = get_strategy("madrid", first_period=3, period_step=1, distance_atr=distance/64).gerar_sinais(data)
    assert not signals.entry_long.iloc[:299].any() and not signals.entry_short.iloc[:299].any()
    assert signals["entry_long" if direction > 0 else "entry_short"].iloc[-1] == allowed


@pytest.mark.parametrize("direction", [1, -1])
@pytest.mark.parametrize("fault", ["current_equal", "previous_already_beyond"])
def test_current_equality_or_previous_wrong_side_do_not_cross(direction, fault):
    data = crossing(direction)
    if fault == "current_equal":
        data.loc[299, "close"] = 100.
    else:
        data.loc[298, "close"] = 100+direction*.5
    signals = get_strategy("madrid", first_period=3, period_step=1).gerar_sinais(data)
    assert not signals.entry_long.any() and not signals.entry_short.any()


def test_constant_price_no_alignment_no_new_cycles_and_flat_atr_recovers():
    data = market(305).assign(high=100., low=100., close=100.)
    data.loc[300:, ["high", "close"]] = 101.
    actual = madrid_averages(data)
    assert actual.atr.iloc[:300].eq(0).all() and actual.atr.iloc[300:].gt(0).all()
    signals = get_strategy("madrid").gerar_sinais(data)
    assert not signals.entry_long.iloc[:300].any() and not signals.entry_short.any()


@pytest.mark.parametrize("name,value", [("first_period", 0), ("first_period", 1001), ("first_period", True),
    ("first_period", 1.), ("period_step", 0), ("period_step", 112), ("period_step", 2147483647),
    ("period_step", 10**400), ("trail_line", 0), ("trail_line", 11), ("trail_line", 1.),
    ("distance_points", -1), ("distance_points", 1.), ("distance_points", True), ("distance_points", 2147483648),
    ("distance_atr", 0), ("distance_atr", np.inf), ("distance_atr", np.nan), ("distance_atr", 10**400),
    ("offset_atr", -1), ("offset_atr", np.inf), ("offset_atr", True),
    ("distance_atr", EMPTY), ("offset_atr", EMPTY)])
def test_parameter_types_limits_and_no_period_overflow(name, value):
    with pytest.raises(ValueError, match="Madrid requer"):
        get_strategy("madrid", **{name: value})


def test_registration_typed_roundtrip_does_not_grant_native_capabilities():
    value = base_composition("madrid", symbol="TEST", timeframe="M5")
    assert Composition(value.to_json()).config_hash == value.config_hash
    assert not compatibility(value) and compatibility(value, backend="mql5")
    assert set(STRATEGY_PARAMETERS["madrid"]) == set(get_strategy("madrid").params)
    assert STRATEGY_PARAMETERS["madrid"]["distance_points"].unit == "point"
    assert get_strategy("madrid").exit_rules().absolute_signal_target
