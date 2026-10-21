"""T19.36: execução pelo Backtester/adapter reais; nenhum executor alternativo."""
from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from mt5_extracao.composition import BaseAdapter, Composition, base_composition
from mt5_extracao.strategies import ExitRules, Strategy, get_strategy
from mt5_extracao.strategies.backtester import BacktestConfig, SymbolSpec, run_backtest
from tests.test_backtester import INFO, bars
from tests.test_vwap_bands import market

SPEC = SymbolSpec(symbol="TEST", point=.01, tick_size=.01, tick_value=.01,
                  volume_min=.01, volume_step=.01)
CFG = BacktestConfig(initial_equity=10_000, risk_per_trade=.01, swap=False)


def band_market(direction=1, opening=98.5, signal_close=98.5, n=301):
    data = market(n).assign(open=100., high=100., low=100., close=100., tick_volume=1., real_volume=0.)
    data.loc[299, ["open", "high", "low", "close", "tick_volume"]] = [100., 100., min(98., signal_close), signal_close, 0.]
    data.loc[300:, ["open", "high", "low", "close", "tick_volume"]] = [opening, max(99., opening), min(98., opening), opening, 0.]
    if direction < 0:
        data[["open", "high", "low", "close"]] = 200-data[["open", "low", "high", "close"]].to_numpy()
    return data


class AbsoluteLevels(Strategy):
    """Fornece só sinais/níveis para isolar as regras compartilhadas reais."""
    key, info = "fixture_absolute_levels", INFO

    def __init__(self, direction=1, target=105., rules=None, levels=None):
        super().__init__()
        self.direction, self.target = direction, target
        self.rules = rules if rules is not None else ExitRules(absolute_signal_target=True)
        self.levels = levels

    def _signals(self, df, out):
        side = "long" if self.direction > 0 else "short"
        out["entry_"+side] = df.index == 0
        out["stop_"+side] = self.levels if self.levels is not None else (90. if self.direction > 0 else 110.)
        out["target_"+side] = self.target

    def exit_rules(self):
        return self.rules


@pytest.mark.parametrize("direction", [1, -1])
def test_raw_band_wrong_side_of_close_is_allowed_only_after_valid_point_offset(direction):
    data = band_market(direction)
    strategy = get_strategy("vwap_bands", volume_source=1)
    signals = strategy.gerar_sinais(data)
    assert signals["entry_long" if direction > 0 else "entry_short"].iloc[299]
    trade = run_backtest(data, strategy, SPEC, CFG).trades[0]
    assert trade.entry_time == data.time.iloc[300]
    assert trade.entry_price == (98.5 if direction > 0 else 101.5)
    assert trade.stop_price == (97. if direction > 0 else 103.)
    assert trade.target_price == (99.5 if direction > 0 else 100.5)
    assert trade.volume == 66.66  # risco100 / distância real1,5; não banda99 sem offset
    assert trade.exit_reason == "fim dos dados"


@pytest.mark.parametrize("direction", [1, -1])
def test_final_stop_must_also_be_valid_at_signal_close_before_rebound(direction):
    data = band_market(direction, signal_close=96.)
    result = run_backtest(data, get_strategy("vwap_bands", volume_source=1), SPEC, CFG)
    assert result.skipped_gap == 1 and not result.trades


@pytest.mark.parametrize("direction", [1, -1])
@pytest.mark.parametrize("opening", [96., 97., 99.5, 100.])
def test_gap_at_or_through_stop_or_target_refuses_entire_entry_before_sizing(direction, opening, monkeypatch):
    monkeypatch.setattr(SPEC, "lots_for_risk", lambda *args: pytest.fail("lote não deve ser calculado"))
    data = band_market(direction, opening=opening)
    result = run_backtest(data, get_strategy("vwap_bands", volume_source=1), SPEC, CFG)
    assert not result.trades and result.skipped_gap == 1 and result.skipped_min_lot == 0


@pytest.mark.parametrize("direction", [1, -1])
def test_target_band_zero_means_central_vwap_with_tp(direction):
    strategy = get_strategy("vwap_bands", target_band_pct=0, volume_source=1)
    trade = run_backtest(band_market(direction), strategy, SPEC, CFG).trades[0]
    assert trade.target_price == 100. and strategy.exit_rules().absolute_signal_target


@pytest.mark.parametrize("target", [np.nan, np.inf, -np.inf, np.finfo(float).max, 0., -1., 100.])
def test_invalid_absolute_targets_refuse_before_lot_even_without_offset(target, monkeypatch):
    monkeypatch.setattr(SPEC, "lots_for_risk", lambda *args: pytest.fail("alvo inválido chegou ao lote"))
    data = bars([[100, 101, 99, 100]]*2, freq="5min")
    result = run_backtest(data, AbsoluteLevels(target=target), SPEC, CFG)
    assert not result.trades and result.skipped_gap == 1


@pytest.mark.parametrize("direction,target,expected", [(1, 105.25, 105.5), (-1, 94.75, 95.)])
def test_absolute_target_tick_normalization_precedes_validation_and_fill_anchor(direction, target, expected):
    spec = replace(SPEC, tick_size=.5, tick_value=.5)
    data = bars([[100, 101, 99, 100], [102 if direction > 0 else 98, 103, 97, 100]], freq="5min")
    trade = run_backtest(data, AbsoluteLevels(direction, target), spec, CFG).trades[0]
    assert trade.target_price == expected
    assert trade.target_price != trade.entry_price+direction*5.25
    # Arredondar um alvo para o fill torna a entrada inválida.
    flat = bars([[100, 101, 99, 100]]*2, freq="5min")
    result = run_backtest(flat, AbsoluteLevels(direction, 100+direction*.1), spec, CFG)
    assert result.skipped_gap == 1 and not result.trades


@pytest.mark.parametrize("direction", [1, -1])
def test_offset_does_not_enable_move_guard_or_move_original_target(direction):
    # Níveis das barras posteriores apertariam o SL se houvesse guard habilitado.
    data = bars([[100, 101, 99, 100], [100, 104, 99, 103], [100, 102, 97, 100]], freq="5min")
    if direction < 0:
        data[["open", "high", "low", "close"]] = 200-data[["open", "low", "high", "close"]].to_numpy()
    rules = get_strategy("vwap_bands").exit_rules()
    strategy = AbsoluteLevels(direction, 110 if direction > 0 else 90, rules,
                              levels=[95, 104, 104] if direction > 0 else [105, 96, 96])
    trade = run_backtest(data, strategy, SPEC, CFG).trades[0]
    assert trade.stop_price == (93 if direction > 0 else 107)
    assert trade.target_price == (110 if direction > 0 else 90)
    assert trade.exit_reason == "fim dos dados" and trade.exit_time == data.time.iloc[-1]


@pytest.mark.parametrize("direction", [1, -1])
@pytest.mark.parametrize("mode", ["fixed_points", "fixed_r"])
def test_actual_composition_overrides_have_fill_r_priority_without_double_stop_offset(direction, mode):
    data = band_market(direction)
    value = base_composition("vwap_bands", symbol="TEST", timeframe="M5",
                             params=dict(volume_source=1), risk_fraction=.01).to_dict()
    value["protection"] = dict(mode="fixed_points", params=dict(stop_points=100))
    value["profit_exit"] = (dict(mode=mode, params=dict(target_points=300)) if mode == "fixed_points"
                            else dict(mode=mode, params=dict(target_r=2)))
    trade = BaseAdapter(Composition(value)).run(data, SPEC, CFG, timeframe="M5").trades[0]
    assert trade.stop_price == pytest.approx(trade.entry_price-direction*1.)
    assert trade.target_price == pytest.approx(trade.entry_price+direction*(3. if mode == "fixed_points" else 2.))
    assert trade.volume == 100.


@pytest.mark.parametrize("direction", [1, -1])
def test_direct_typed_r_override_also_replaces_finite_absolute_raw_target(direction):
    rules = ExitRules(absolute_signal_target=True, target_r=2.)
    data = bars([[100, 101, 99, 100]]*2, freq="5min")
    strategy = AbsoluteLevels(direction, target=105 if direction > 0 else 95, rules=rules)
    trade = run_backtest(data, strategy, SPEC, CFG).trades[0]
    assert trade.target_price == (120. if direction > 0 else 80.)


def test_default_absolute_flag_off_keeps_legacy_target_gap_semantics_and_unknown_spec():
    data = bars([[100, 101, 99, 100], [106, 107, 100, 106]], freq="5min")
    legacy = ExitRules()
    spec = replace(SPEC, known=False, symbol="")
    old = run_backtest(data, AbsoluteLevels(rules=legacy), spec, CFG)
    off = run_backtest(data, AbsoluteLevels(rules=replace(legacy, absolute_signal_target=False)), spec, CFG)
    assert old.trades == off.trades and len(old.trades) == 1
    assert old.trades[0].target_price == 105 and old.trades[0].exit_reason == "alvo"
    for name in ("equity", "account_balance", "floating_equity", "performance_equity"):
        pd.testing.assert_series_equal(getattr(old, name), getattr(off, name))
    on = run_backtest(data, AbsoluteLevels(), SPEC, CFG)
    assert on.skipped_gap == 1 and not on.trades


@pytest.mark.parametrize("rules", [dict(absolute_signal_target=1), dict(absolute_signal_target="false"),
    dict(absolute_signal_target=True, signal_target_points=5)])
def test_absolute_target_flag_is_boolean_and_has_one_default_source(rules):
    with pytest.raises(ValueError):
        ExitRules(**rules)


@pytest.mark.parametrize("changes", [dict(known=False), dict(symbol=""), dict(point=0.), dict(tick_size=np.inf)])
def test_absolute_target_requires_known_symbol_units(changes):
    with pytest.raises(ValueError, match="especificação conhecida"):
        run_backtest(bars([[100, 101, 99, 100]]*2), AbsoluteLevels(), replace(SPEC, **changes), CFG)
