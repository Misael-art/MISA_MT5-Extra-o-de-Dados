"""T19.40: HaHiLo/adapter/motor reais, sem executor ou broker simulados como aceitos."""
from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from mt5_extracao.composition import BaseAdapter, Composition, base_composition
from mt5_extracao.strategies import get_strategy
from mt5_extracao.strategies.backtester import BacktestConfig, SymbolSpec, run_backtest
from mt5_extracao.strategies.ha_hilo import ha_hilo_context
from mt5_extracao.trade_analytics.execution import engine
from tests.test_ha_hilo import market

SPEC = SymbolSpec(symbol="TEST", point=.1, tick_size=.25, tick_value=.25,
                  volume_min=.01, volume_step=.01)
CFG = BacktestConfig(initial_equity=10_000, risk_per_trade=.01, swap=False)


def reflected(data):
    data = data.copy()
    data[["open", "high", "low", "close"]] = 200-data[["open", "low", "high", "close"]].to_numpy()
    return data


@pytest.mark.parametrize("direction", [1, -1])
def test_real_pending_exit_then_opposite_entry_uses_next_real_open_and_fixed_stop(direction):
    data = market(650)
    if direction < 0:
        data = reflected(data)
    strategy = get_strategy("ha_hilo", stop_buffer_points=100)
    result = run_backtest(data, strategy, SPEC, CFG)
    assert len(result.trades) >= 3 and result.trades[0].direction == direction
    signals = strategy.gerar_sinais(data)
    context = ha_hilo_context(data)
    for before, after in zip(result.trades, result.trades[1:]):
        assert before.exit_reason == "sinal de saída"
        assert after.direction == -before.direction and after.entry_time == before.exit_time
        i = data.index[data.time == after.entry_time][0]
        assert signals["exit_long" if before.direction > 0 else "exit_short"].iloc[i-1]
    for trade in result.trades:
        i = data.index[data.time == trade.entry_time][0]
        assert i >= 300 and trade.entry_price == data.open.iloc[i]
        assert trade.entry_price != pytest.approx(context.ha_close.iloc[i-1], abs=1e-8)
        raw = signals.stop_long.iloc[i-1]
        expected = round((raw-trade.direction*100*SPEC.point)/SPEC.tick)*SPEC.tick
        assert trade.stop_price == expected and trade.target_price is None
    assert result.trades[-1].exit_reason == "fim dos dados"


@pytest.mark.parametrize("direction", [1, -1])
def test_real_reversal_gap_refuses_new_position_after_signal_close_before_sizing(direction, monkeypatch):
    data = market(650)
    if direction < 0:
        data = reflected(data)
    strategy = get_strategy("ha_hilo", stop_buffer_points=100)
    baseline = run_backtest(data, strategy, SPEC, CFG)
    first = baseline.trades[0]
    i = data.index[data.time == first.exit_time][0]
    data = data.iloc[:i+1].copy()
    line = strategy.gerar_sinais(data).stop_long.iloc[i-1]
    new_direction = -direction
    stop = round((line-new_direction*100*SPEC.point)/SPEC.tick)*SPEC.tick
    opening = stop-new_direction  # além do SL da mão inversa
    data.loc[i, ["open", "high", "low", "close"]] = [opening, opening+.1, opening-.1, opening]
    original = SPEC.lots_for_risk
    sizing = []

    def size(*args):
        sizing.append(args)
        return original(*args)

    monkeypatch.setattr(SPEC, "lots_for_risk", size)
    result = run_backtest(data, strategy, SPEC, CFG)
    assert len(result.trades) == 1 and result.trades[0].direction == direction
    assert result.trades[0].exit_reason == "sinal de saída" and result.trades[0].exit_time == data.time.iloc[-1]
    assert result.skipped_gap == 1 and len(sizing) == 1  # a perna recusada nunca chega ao lote
    assert result.floating_equity.iloc[-1] == result.account_balance.iloc[-1]


def wrong_side_market(direction=1):
    data = market(302).assign(open=8., high=8., low=8., close=8.)
    data.loc[298, ["open", "high", "low", "close"]] = [8, 10, 6, 10]
    data.loc[299, ["open", "high", "low", "close"]] = [10, 12, 10, 12]
    data.loc[300, ["open", "high", "low", "close"]] = [12, 18, 8, 8]
    data.loc[301, ["open", "high", "low", "close"]] = [8, 8.2, 7.8, 8]
    return data if direction > 0 else reflected(data)


@pytest.mark.parametrize("direction", [1, -1])
def test_real_ha_condition_can_close_opposite_without_operable_entry_and_offset_defers_validation(direction):
    data = wrong_side_market(direction)
    side, opposite = ("long", "short") if direction > 0 else ("short", "long")
    strategy = get_strategy("ha_hilo", hilo_period=2)
    signals = strategy.gerar_sinais(data)
    assert signals["exit_"+opposite].iloc[300] and not signals["entry_"+side].iloc[300]
    assert not run_backtest(data, strategy, SPEC, CFG, start_index=301).trades
    buffered = get_strategy("ha_hilo", hilo_period=2, stop_buffer_points=20)
    candidate = buffered.gerar_sinais(data)
    assert candidate["entry_"+side].iloc[300] and candidate["exit_"+opposite].iloc[300]
    assert candidate["stop_"+side].iloc[300] == signals["stop_"+side].iloc[300]
    trade = run_backtest(data, buffered, SPEC, CFG, start_index=301).trades[0]
    assert trade.entry_time == data.time.iloc[301] and trade.entry_price == data.open.iloc[301]
    assert trade.stop_price == (7 if direction > 0 else 193)  # point .1, tick .25; offset uma vez


@pytest.mark.parametrize("direction", [1, -1])
def test_favorable_gap_does_not_cure_invalid_normalized_stop_at_signal_close(direction, monkeypatch):
    data = wrong_side_market(direction)
    opening = 12 if direction > 0 else 188
    data.loc[301, ["open", "high", "low", "close"]] = [opening, opening+.2, opening-.2, opening]
    strategy = get_strategy("ha_hilo", hilo_period=2, stop_buffer_points=1)
    assert strategy.gerar_sinais(data)["entry_long" if direction > 0 else "entry_short"].iloc[300]
    monkeypatch.setattr(SPEC, "lots_for_risk", lambda *args: pytest.fail("SL inválido chegou ao lote"))
    result = run_backtest(data, strategy, SPEC, CFG, start_index=301)
    assert not result.trades and result.skipped_gap == 1


@pytest.mark.parametrize("buffer", [0, 3])
@pytest.mark.parametrize("profit", ["strategy_default", "fixed_points", "fixed_r"])
@pytest.mark.parametrize("protection", ["strategy_default", "fixed_points"])
def test_real_adapter_defaults_and_overrides_keep_initial_stop_target_anchor_and_exit_flags(buffer, profit, protection):
    data = market(650)
    value = base_composition("ha_hilo", symbol="TEST", timeframe="M5",
                             params=dict(stop_buffer_points=buffer), risk_fraction=.01).to_dict()
    if profit != "strategy_default":
        value["profit_exit"] = dict(mode=profit, params=dict(target_points=30) if profit == "fixed_points" else dict(target_r=2))
    if protection == "fixed_points":
        value["protection"] = dict(mode=protection, params=dict(stop_points=20))
    strategy = get_strategy("ha_hilo", stop_buffer_points=buffer)
    rules = replace(strategy.exit_rules(), stop_points=20) if protection == "fixed_points" else strategy.exit_rules()
    signals = strategy.gerar_sinais(data, exit_rules_override=rules)
    base_signals = strategy.gerar_sinais(data)
    for col in ("exit_long", "exit_short"):
        pd.testing.assert_series_equal(signals[col], base_signals[col])
    result = BaseAdapter(Composition(value)).run(data, SPEC, CFG, timeframe="M5")
    assert result.trades and {t.direction for t in result.trades} == {1, -1}
    for trade in result.trades:
        i = data.index[data.time == trade.entry_time][0]-1
        if protection == "fixed_points":
            expected_stop = trade.entry_price-trade.direction*2
        else:
            expected_stop = signals.stop_long.iloc[i]-trade.direction*buffer*SPEC.point
            if buffer:
                expected_stop = round(expected_stop/SPEC.tick)*SPEC.tick
        assert trade.stop_price == pytest.approx(expected_stop)
        expected_target = None if profit == "strategy_default" else trade.entry_price+trade.direction*(
            3 if profit == "fixed_points" else 2*abs(trade.entry_price-trade.stop_price))
        if expected_target is None:
            assert trade.target_price is None
        else:
            assert trade.target_price == pytest.approx(expected_target)
    if profit == protection == "strategy_default":
        direct = run_backtest(data, strategy, SPEC, CFG)
        pd.testing.assert_frame_equal(result.trades_frame(), direct.trades_frame())
        assert result.metrics == direct.metrics


@pytest.mark.parametrize("direction", [1, -1])
def test_fixed_protection_overrides_wrong_side_raw_stop_without_double_offset_and_keeps_signal_exit(direction):
    data = wrong_side_market(direction)
    strategy = get_strategy("ha_hilo", hilo_period=2, stop_buffer_points=30)
    rules = replace(strategy.exit_rules(), stop_points=20, target_points=40)
    result = run_backtest(data, strategy, SPEC, CFG, start_index=301, exit_rules_override=rules)
    trade = result.trades[0]
    assert trade.stop_price == trade.entry_price-direction*2
    assert trade.target_price == trade.entry_price+direction*4


@pytest.mark.parametrize("spec", [replace(SPEC, known=False), replace(SPEC, point=0), replace(SPEC, tick_size=np.inf)])
def test_nonzero_buffer_requires_known_valid_point_tick(spec):
    with pytest.raises(ValueError, match="especificação conhecida"):
        run_backtest(market(), get_strategy("ha_hilo", stop_buffer_points=1), spec, CFG)


def test_static_resimulation_reuses_original_signal_exits_but_cannot_generate_new_reverse_entries():
    data = market(650);strategy = get_strategy("ha_hilo", stop_buffer_points=100)
    original = run_backtest(data, strategy, SPEC, CFG)
    rows = pd.DataFrame([dict(direction=t.direction, entry_time=t.entry_time, exit_time=t.exit_time,
        entry_price=t.entry_price, stop_price=t.stop_price, target_price=t.target_price,
        volume=t.volume, exit_reason=t.exit_reason) for t in original.trades])
    policy = engine.ExitPolicy.from_rules(strategy.exit_rules())
    again = engine.simulate(rows, data, SPEC, CFG, policy, strategy.gerar_sinais(data).atr)
    assert again.skipped == again.approximate == 0 and len(again.trades) == len(original.trades)
    for a, b in zip(original.trades, again.trades):
        assert a.exit_time == b.exit_time and a.exit_price == b.exit_price and a.pnl == b.pnl
