"""T19.38: exercícios do motor real, sem executor nativo/broker simulado."""
from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from mt5_extracao.composition import BaseAdapter, Composition, base_composition
from mt5_extracao.strategies import ExitRules, Strategy, get_strategy
from mt5_extracao.strategies.backtester import BacktestConfig, SymbolSpec, run_backtest
from tests.test_backtester import INFO, bars
from tests.test_stoch_ema9 import market

SPEC = SymbolSpec(symbol="TEST", point=1., tick_size=1., tick_value=1., volume_min=1., volume_step=1.)
CFG = BacktestConfig(initial_equity=10_000, risk_per_trade=.01, swap=False)
ROWS = [[100, 101, 99, 100], [100, 110, 95, 108], [108, 112, 106, 110], [110, 111, 103, 105]]


class SignalLevels(Strategy):
    """Só fornece o contrato de sinais; não possui gestor de posição próprio."""
    key = "fixture_signal_levels"
    info = INFO

    def __init__(self, levels=(90, 104, 102, 99), direction=1, rules=None):
        super().__init__()
        self.levels, self.direction = levels, direction
        self.rules = rules if rules is not None else ExitRules(move_guard_from_signal_stop=True)

    def _signals(self, df, out):
        out["entry_long" if self.direction > 0 else "entry_short"] = df.index == 0
        out["stop_long" if self.direction > 0 else "stop_short"] = self.levels
        out["atr"] = 1.

    def exit_rules(self):
        return self.rules


def directed(direction, rows=ROWS):
    data = bars(rows, freq="5min")
    if direction < 0:
        data[["open", "high", "low", "close"]] = 200-data[["open", "low", "high", "close"]].to_numpy()
    return data


@pytest.mark.parametrize("direction", [1, -1])
def test_guard_tightens_monotonically_at_close_without_retroactive_touch_or_extra_order(direction):
    levels = [90, 104, 102, 99] if direction > 0 else [110, 96, 98, 101]
    data = directed(direction)
    result = run_backtest(data, SignalLevels(levels, direction), SPEC, CFG)
    assert len(result.trades) == 1
    trade = result.trades[0]
    # Low/high da barra 1 já atravessou o novo guard, mas ocorreu antes da decisão no close.
    assert trade.exit_time == data.time.iloc[3]
    assert trade.stop_price == (90 if direction > 0 else 110)
    assert trade.exit_price == (104 if direction > 0 else 96)
    assert trade.volume == 10 and trade.target_price is None
    assert trade.exit_reason == "stop móvel"


@pytest.mark.parametrize("direction", [1, -1])
def test_offset_uses_symbol_point_before_initial_risk_sizing_and_on_each_guard(direction):
    spec = replace(SPEC, point=.5, tick_size=.5, tick_value=.5)
    rows = [*ROWS[:3], [110, 111, 102, 105]]
    levels = [90, 104, 102, 99] if direction > 0 else [110, 96, 98, 101]
    rules = ExitRules(move_guard_from_signal_stop=True, signal_stop_offset_points=2)
    trade = run_backtest(directed(direction, rows), SignalLevels(levels, direction, rules), spec, CFG).trades[0]
    assert trade.stop_price == (89 if direction > 0 else 111)
    assert trade.volume == 9  # risco 100 / distância 11; nenhum lote baseado na EMA sem offset
    assert trade.exit_price == (103 if direction > 0 else 97)


@pytest.mark.parametrize("direction", [1, -1])
def test_guard_cannot_loosen_atr_or_confirmed_breakeven(direction):
    levels = [90, 104, 102, 99] if direction > 0 else [110, 96, 98, 101]
    rules = ExitRules(move_guard_from_signal_stop=True, trailing_atr=2)
    trade = run_backtest(directed(direction), SignalLevels(levels, direction, rules), SPEC, CFG).trades[0]
    assert trade.exit_price == (106 if direction > 0 else 94)  # ATR mais apertado que o guard
    rows = [[100, 101, 99, 100], [100, 111, 95, 108], [108, 109, 99, 102]]
    levels = [90, 98, 97] if direction > 0 else [110, 102, 103]
    rules = ExitRules(move_guard_from_signal_stop=True, partial_r=1, partial_fraction=.5)
    trade = run_backtest(directed(direction, rows), SignalLevels(levels, direction, rules), SPEC, CFG).trades[0]
    assert trade.exit_reason == "stop no zero a zero" and trade.exit_price == 100
    assert trade.pnl == 50 and trade.volume == 10


@pytest.mark.parametrize("direction", [1, -1])
@pytest.mark.parametrize("raw", [np.nan, np.inf, -np.inf, np.finfo(float).max, 0., -1., 90., 89., 108., 120.])
def test_invalid_equal_retreating_or_wrong_side_guard_preserves_stop(direction, raw):
    # O sentinela precisa ser recusado ANTES da aritmética de offset.
    candidate = raw if direction > 0 or not np.isfinite(raw) or raw == np.finfo(float).max else 200-raw
    rows = [[100, 101, 99, 100], [100, 110, 95, 108], [100, 101, 89, 95]]
    levels = [90, candidate, 90] if direction > 0 else [110, candidate, 110]
    if direction < 0 and raw in (0., -1.):
        levels[1] = raw
    result = run_backtest(directed(direction, rows), SignalLevels(levels, direction), SPEC, CFG)
    trade = result.trades[0]
    assert trade.exit_price == (90 if direction > 0 else 110)
    assert trade.exit_reason == "stop" and trade.stop_price == trade.exit_price


@pytest.mark.parametrize("direction", [1, -1])
def test_tick_rounding_precedes_validity_and_monotonicity(direction):
    spec = replace(SPEC, point=.1, tick_size=.5, tick_value=.5)
    levels = [90, 104.25, 102, 99] if direction > 0 else [110, 95.75, 98, 101]
    trade = run_backtest(directed(direction), SignalLevels(levels, direction), spec, CFG).trades[0]
    assert trade.exit_price == (104.5 if direction > 0 else 96.)
    # Um guard que arredonda para o preço atual deve ser recusado.
    rows = [[100, 101, 99, 100], [100, 110, 95, 108], [100, 101, 89, 95]]
    levels = [90, 107.9, 90] if direction > 0 else [110, 92.1, 110]
    trade = run_backtest(directed(direction, rows), SignalLevels(levels, direction), spec, CFG).trades[0]
    assert trade.exit_price == (90 if direction > 0 else 110)


def test_short_guard_checks_ask_instead_of_bid_and_does_not_emit_fill_on_rejection():
    data = directed(-1, [[100, 101, 99, 100], [100, 110, 95, 108], [100, 101, 89, 95]])
    data["spread"] = 2.
    result = run_backtest(data, SignalLevels([110, 93, 110], -1), SPEC, CFG)
    assert len(result.trades) == 1
    assert result.trades[0].exit_price == 110  # guard93 é menor que ask94, apesar de maior que bid92
    assert result.trades[0].exit_reason == "stop"


@pytest.mark.parametrize("direction", [1, -1])
def test_signal_close_target_survives_guard_and_is_not_anchored_at_fill(direction):
    rows = [[100, 101, 99, 100], [102, 110, 95, 108], [108, 131, 106, 130]]
    rules = ExitRules(move_guard_from_signal_stop=True, signal_target_points=30)
    levels = [90, 104, 102] if direction > 0 else [110, 96, 98]
    trade = run_backtest(directed(direction, rows), SignalLevels(levels, direction, rules), SPEC, CFG).trades[0]
    assert trade.entry_price == (102 if direction > 0 else 98)
    assert trade.target_price == trade.exit_price == (130 if direction > 0 else 70)
    assert trade.exit_reason == "alvo" and trade.stop_price == (90 if direction > 0 else 110)
    assert trade.volume == 8


@pytest.mark.parametrize("direction", [1, -1])
def test_signal_target_or_stop_crossed_by_entry_gap_refuses_whole_entry(direction):
    rules = ExitRules(move_guard_from_signal_stop=True, signal_target_points=5)
    for opening in (106, 89):
        rows = [[100, 101, 99, 100], [opening, opening+1, opening-1, opening]]
        levels = [90, 90] if direction > 0 else [110, 110]
        result = run_backtest(directed(direction, rows), SignalLevels(levels, direction, rules), SPEC, CFG)
        assert not result.trades and result.skipped_gap == 1


def test_offsets_need_not_be_tick_multiples_but_absolute_prices_are_quantized():
    spec = replace(SPEC, point=.1, tick_size=.25, tick_value=.25)
    rules = ExitRules(move_guard_from_signal_stop=True, signal_stop_offset_points=3, signal_target_points=6)
    data = bars([[100, 100.1, 99.9, 100]]*2, freq="5min")
    trade = run_backtest(data, SignalLevels([95.13]*2, rules=rules), spec, CFG).trades[0]
    assert trade.stop_price == 94.75 and trade.target_price == 100.5


@pytest.mark.parametrize("direction", [1, -1])
def test_initial_signal_sentinel_and_nonpositive_fixed_override_never_size_a_position(direction):
    data = directed(direction, [[100, 101, 99, 100]]*2)
    strategy = SignalLevels([np.finfo(float).max]*2, direction)
    signals = strategy.gerar_sinais(data)
    # Exercita inclusive um sinal fornecido externamente, sem o filtro do produtor.
    signals.loc[0, "entry_long" if direction > 0 else "entry_short"] = True
    result = run_backtest(data, strategy, SPEC, CFG, signals=signals)
    assert result.skipped_gap == 1 and not result.trades
    if direction > 0:
        rules = ExitRules(stop_points=100, move_guard_from_signal_stop=True)
        result = run_backtest(data, SignalLevels([90]*2, rules=rules), SPEC, CFG)
        assert result.skipped_gap == 1 and not result.trades


@pytest.mark.parametrize("direction", [1, -1])
def test_zero_offset_is_enabled_and_false_flag_equals_legacy_with_no_known_spec_required(direction):
    data = directed(direction)
    levels = [90, 104, 102, 99] if direction > 0 else [110, 96, 98, 101]
    legacy = ExitRules()
    explicit_off = replace(legacy, move_guard_from_signal_stop=False, signal_stop_offset_points=0,
                           signal_target_points=None)
    unknown = replace(SPEC, known=False, symbol="")
    a = run_backtest(data, SignalLevels(levels, direction, legacy), unknown, CFG)
    b = run_backtest(data, SignalLevels(levels, direction, explicit_off), unknown, CFG)
    assert a.trades == b.trades and a.metrics == b.metrics
    for name in ("equity", "account_balance", "floating_equity", "performance_equity"):
        pd.testing.assert_series_equal(getattr(a, name), getattr(b, name))
    assert a.trades[0].exit_reason == "fim dos dados"
    on = run_backtest(data, SignalLevels(levels, direction), SPEC, CFG)
    assert on.trades[0].exit_reason == "stop móvel"


@pytest.mark.parametrize("spec", [replace(SPEC, known=False), replace(SPEC, symbol=""),
    replace(SPEC, point=0), replace(SPEC, point=np.nan), replace(SPEC, tick_size=np.inf)])
def test_opt_in_guard_requires_known_finite_symbol_units_even_with_zero_offset(spec):
    with pytest.raises(ValueError, match="especificação conhecida"):
        run_backtest(directed(1), SignalLevels(), spec, CFG)


@pytest.mark.parametrize("params", [dict(move_guard_from_signal_stop=1), dict(move_guard_from_signal_stop="false"),
    dict(signal_stop_offset_points=-1), dict(signal_stop_offset_points=True), dict(signal_stop_offset_points=np.inf),
    dict(signal_target_points=0), dict(signal_target_points=False), dict(signal_target_points=np.nan)])
def test_additive_rules_are_typed_and_finite(params):
    with pytest.raises(ValueError):
        ExitRules(**params)


def test_s10_default_composition_backtest_export_and_explicit_overrides():
    data = market()
    spec = replace(SPEC, point=.01, tick_size=.01, tick_value=.01, volume_min=.01, volume_step=.01)
    strategy = get_strategy("stoch_ema9", stop_offset_points=50, target_points=250)
    composition = base_composition("stoch_ema9", symbol="TEST", timeframe="M5").to_dict()
    composition["strategy"]["params"] = strategy.params
    direct = run_backtest(data, strategy, spec, CFG)
    default = BaseAdapter(Composition(composition)).run(data, spec, CFG, timeframe="M5")
    assert direct.trades == default.trades and len(default.trades) > 0
    for trade in default.trades:
        signal_bar = data.index[data.time == trade.entry_time][0]-1
        expected = float(round((data.close.iloc[signal_bar]+trade.direction*2.5)/spec.tick)*spec.tick)
        assert trade.target_price == pytest.approx(expected)
    exported = direct.trades_frame()
    assert exported.stop_price.notna().all() and exported.target_price.notna().all()
    # Override explícito usa o fill, mesmo com signal_target_points ainda presente na regra padrão.
    composition["profit_exit"] = dict(mode="fixed_points", params=dict(target_points=300))
    composition["protection"] = dict(mode="fixed_points", params=dict(stop_points=200))
    points = BaseAdapter(Composition(composition)).run(data, spec, CFG, timeframe="M5")
    assert points.trades
    for trade in points.trades:
        assert trade.target_price == pytest.approx(trade.entry_price+trade.direction*3)
        assert trade.stop_price == pytest.approx(trade.entry_price-trade.direction*2)
    composition["profit_exit"] = dict(mode="fixed_r", params=dict(target_r=2))
    fixed_r = BaseAdapter(Composition(composition)).run(data, spec, CFG, timeframe="M5")
    assert fixed_r.trades
    for trade in fixed_r.trades:
        assert trade.target_price == pytest.approx(trade.entry_price+trade.direction*4)
