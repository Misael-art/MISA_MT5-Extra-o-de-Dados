"""T19.39: motor/adapter reais; referência em pontos continua sob overrides."""
from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from mt5_extracao.composition import BaseAdapter, Composition, base_composition
from mt5_extracao.strategies import ExitRules, Strategy, get_strategy
from mt5_extracao.strategies.backtester import BacktestConfig, SymbolSpec, run_backtest
from tests.test_backtester import INFO, bars
from tests.test_madrid import market

SPEC = SymbolSpec(symbol="TEST", point=.1, tick_size=.25, tick_value=.25, volume_min=.01, volume_step=.01)
CFG = BacktestConfig(initial_equity=10_000, risk_per_trade=.01, swap=False)


class ReferenceLevels(Strategy):
    key, info = "fixture_reference_levels", INFO

    def __init__(self, direction=1, reference=99.5, levels=None, rules=None):
        super().__init__()
        self.direction, self.reference, self.levels = direction, reference, levels
        self.rules = rules or ExitRules(move_guard_from_signal_stop=True, signal_reference_distance_points=10)

    def _signals(self, df, out):
        side = "long" if self.direction > 0 else "short"
        out["entry_"+side] = df.index == 0
        out["stop_"+side] = (90 if self.direction > 0 else 110) if self.levels is None else self.levels
        out["target_"+side] = self.reference

    def exit_rules(self):
        return self.rules


def directed(direction, opening=100., close=100.):
    data = bars([[100, 101, 99, close], [opening, max(opening, 100.3), min(opening, 99.8), opening]], freq="5min")
    if direction < 0:
        data[["open", "high", "low", "close"]] = 200-data[["open", "low", "high", "close"]].to_numpy()
    return data


@pytest.mark.parametrize("direction", [1, -1])
def test_reference_point_conversion_tick_normalization_and_signal_anchor(direction):
    reference = 99.63 if direction > 0 else 100.37
    trade = run_backtest(directed(direction), ReferenceLevels(direction, reference), SPEC, CFG).trades[0]
    assert trade.target_price == (100.75 if direction > 0 else 99.25)
    assert trade.stop_price == (90 if direction > 0 else 110)
    assert trade.volume == 10 and trade.exit_reason == "fim dos dados"


@pytest.mark.parametrize("direction", [1, -1])
def test_distance_boundary_inclusive_with_favorable_fill_but_equality_target_fill_is_refused(direction):
    strategy = ReferenceLevels(direction, 99 if direction > 0 else 101)
    trade = run_backtest(directed(direction, opening=99.75), strategy, SPEC, CFG).trades[0]
    assert trade.target_price == 100 and trade.entry_price == (99.75 if direction > 0 else 100.25)
    result = run_backtest(directed(direction), strategy, SPEC, CFG)
    assert result.skipped_gap == 1 and not result.trades


@pytest.mark.parametrize("direction", [1, -1])
@pytest.mark.parametrize("opening", [89., 90., 100.5, 101.])
def test_gaps_through_normalized_reference_target_or_stop_refuse_before_sizing(direction, opening, monkeypatch):
    monkeypatch.setattr(SPEC, "lots_for_risk", lambda *args: pytest.fail("não dimensionar bracket inválido"))
    result = run_backtest(directed(direction, opening), ReferenceLevels(direction, 99.5 if direction > 0 else 100.5), SPEC, CFG)
    assert result.skipped_gap == 1 and not result.trades


@pytest.mark.parametrize("direction", [1, -1])
@pytest.mark.parametrize("mode", ["fixed_points", "fixed_r"])
@pytest.mark.parametrize("reference", [np.nan, np.inf, -np.inf, np.finfo(float).max, 0., -1., 98.5])
def test_real_adapter_keeps_reference_filter_even_with_valid_profit_and_stop_overrides(direction, mode, reference, monkeypatch):
    # O adapter real não pode apagar a referência e nem transformá-la em alvo.
    value = base_composition("madrid", symbol="TEST", timeframe="M5", params=dict(distance_points=10), risk_fraction=.01).to_dict()
    value["profit_exit"] = dict(mode=mode, params=dict(target_points=30) if mode == "fixed_points" else dict(target_r=2))
    value["protection"] = dict(mode="fixed_points", params=dict(stop_points=20))
    strategy = ReferenceLevels(direction, reference if direction > 0 or not np.isfinite(reference) or reference <= 0 or reference == np.finfo(float).max else 200-reference)
    monkeypatch.setattr(BaseAdapter, "strategy", lambda self: strategy)
    monkeypatch.setattr(SPEC, "lots_for_risk", lambda *args: pytest.fail("referência inválida chegou ao lote"))
    result = BaseAdapter(Composition(value)).run(directed(direction), SPEC, CFG, timeframe="M5")
    assert result.skipped_gap == 1 and not result.trades


@pytest.mark.parametrize("direction", [1, -1])
@pytest.mark.parametrize("mode", ["fixed_points", "fixed_r"])
def test_real_adapter_keeps_valid_reference_and_override_anchor(direction, mode, monkeypatch):
    value = base_composition("madrid", symbol="TEST", timeframe="M5", params=dict(distance_points=10), risk_fraction=.01).to_dict()
    value["profit_exit"] = dict(mode=mode, params=dict(target_points=30) if mode == "fixed_points" else dict(target_r=2))
    value["protection"] = dict(mode="fixed_points", params=dict(stop_points=20))
    strategy = ReferenceLevels(direction, 99.5 if direction > 0 else 100.5)
    monkeypatch.setattr(BaseAdapter, "strategy", lambda self: strategy)
    trade = BaseAdapter(Composition(value)).run(directed(direction, opening=100.25), SPEC, CFG, timeframe="M5").trades[0]
    assert trade.stop_price == trade.entry_price-direction*2
    assert trade.target_price == trade.entry_price+direction*(3 if mode == "fixed_points" else 4)


@pytest.mark.parametrize("direction", [1, -1])
def test_reference_rule_reuses_one_monotonic_guard_without_retroactive_same_bar_exit(direction):
    data = bars([[100, 101, 99, 100], [100, 110, 95, 108], [108, 112, 106, 110], [110, 111, 103, 105]], freq="5min")
    if direction < 0:
        data[["open", "high", "low", "close"]] = 200-data[["open", "low", "high", "close"]].to_numpy()
    levels = [90, 104, 102, 99] if direction > 0 else [110, 96, 98, 101]
    rules = ExitRules(move_guard_from_signal_stop=True, signal_reference_distance_points=500)
    result = run_backtest(data, ReferenceLevels(direction, 100, levels, rules), SPEC, CFG)
    trade = result.trades[0]
    assert len(result.trades) == 1 and trade.exit_time == data.time.iloc[3]
    assert trade.exit_price == (104 if direction > 0 else 96)
    assert trade.target_price == (150 if direction > 0 else 50)


@pytest.mark.parametrize("points", [0, 100])
@pytest.mark.parametrize("mode", ["strategy_default", "fixed_points", "fixed_r"])
def test_real_madrid_composition_defaults_and_both_profit_overrides(points, mode):
    data = market()
    spec = replace(SPEC, point=.01, tick_size=.01, tick_value=.01)
    value = base_composition("madrid", symbol="TEST", timeframe="M5", params=dict(distance_points=points), risk_fraction=.01).to_dict()
    if mode != "strategy_default":
        value["profit_exit"] = dict(mode=mode, params=dict(target_points=300) if mode == "fixed_points" else dict(target_r=2))
        value["protection"] = dict(mode="fixed_points", params=dict(stop_points=200))
    result = BaseAdapter(Composition(value)).run(data, spec, CFG, timeframe="M5")
    assert result.trades and {trade.direction for trade in result.trades} == {1, -1}
    raw = get_strategy("madrid", distance_points=points).gerar_sinais(data)
    for trade in result.trades:
        i = data.index[data.time == trade.entry_time][0]-1
        if points:
            assert abs(data.close.iloc[i]-raw.target_long.iloc[i]) <= points*spec.point
        if mode == "strategy_default":
            target = raw["target_long" if trade.direction > 0 else "target_short"].iloc[i] + (trade.direction*points*spec.point if points else 0)
            assert trade.target_price == pytest.approx(round(target/spec.tick)*spec.tick)
        else:
            assert trade.stop_price == pytest.approx(trade.entry_price-trade.direction*2)
            assert trade.target_price == pytest.approx(trade.entry_price+trade.direction*(3 if mode == "fixed_points" else 4))


def test_direct_r_override_priority_and_none_preserves_legacy():
    rules = ExitRules(signal_reference_distance_points=10, target_r=2)
    trade = run_backtest(directed(1), ReferenceLevels(rules=rules), SPEC, CFG).trades[0]
    assert trade.target_price == 120
    legacy = ExitRules()
    a = run_backtest(directed(1), ReferenceLevels(reference=105, rules=legacy), SPEC, CFG)
    b = run_backtest(directed(1), ReferenceLevels(reference=105, rules=replace(legacy, signal_reference_distance_points=None)), SPEC, CFG)
    assert a.trades == b.trades and a.metrics == b.metrics
    pd.testing.assert_series_equal(a.equity, b.equity)


@pytest.mark.parametrize("mode", ["fixed_points", "fixed_r"])
def test_atr_nonpositive_default_short_tp_is_refused_but_explicit_profit_override_has_priority(mode):
    data = market();spec = replace(SPEC, point=.01, tick_size=.01, tick_value=.01)
    value = base_composition("madrid", symbol="TEST", timeframe="M5", params=dict(distance_atr=100), risk_fraction=.01).to_dict()
    default = BaseAdapter(Composition(value)).run(data, spec, CFG, timeframe="M5")
    assert default.skipped_gap and not any(t.direction < 0 for t in default.trades)
    value["profit_exit"] = dict(mode=mode, params=dict(target_points=300) if mode == "fixed_points" else dict(target_r=2))
    actual = BaseAdapter(Composition(value)).run(data, spec, CFG, timeframe="M5")
    assert any(t.direction < 0 for t in actual.trades)
    assert all(t.target_price > 0 for t in actual.trades)


def test_reference_distance_need_not_be_tick_multiple():
    rules = ExitRules(signal_reference_distance_points=3)
    trade = run_backtest(directed(1), ReferenceLevels(reference=99.9, rules=rules), SPEC, CFG).trades[0]
    assert trade.target_price == 100.25


@pytest.mark.parametrize("spec", [replace(SPEC, known=False), replace(SPEC, point=0), replace(SPEC, symbol=""), replace(SPEC, tick_size=np.inf)])
def test_reference_conversion_requires_known_point_tick_even_under_profit_override(spec):
    with pytest.raises(ValueError, match="especificação conhecida"):
        run_backtest(directed(1), ReferenceLevels(rules=ExitRules(signal_reference_distance_points=10, target_r=2)), spec, CFG)


@pytest.mark.parametrize("params", [dict(signal_reference_distance_points=v) for v in (False, 0, -1, np.nan, np.inf, 10**400)] +
    [dict(signal_reference_distance_points=1, absolute_signal_target=True), dict(signal_reference_distance_points=1, signal_target_points=2)])
def test_reference_rule_positive_finite_and_single_default_target_source(params):
    with pytest.raises(ValueError):
        ExitRules(**params)
