"""P05: sinais causais passam pelo backtester existente, sem executar ordens."""

from dataclasses import replace

import pandas as pd
import pytest

from mt5_extracao.analytics import pattern_engine
from mt5_extracao.strategies.backtester import SymbolSpec
from mt5_extracao.strategies.pattern_adapter import (
    PatternCostModel, PatternExecutionPolicy, run_pattern_simulation,
)
from tests.test_pattern_engine import bar, definition


SPEC = SymbolSpec(symbol="SYN", point=1, tick_size=1, tick_value=1,
                  volume_min=1, volume_step=1, currency_profit="USD", known=True)
POLICY = PatternExecutionPolicy(order_type="market_next_open", position_policy="single_position",
                                stop_points=10, target_r=1, target_points=None, max_bars=4,
                                initial_equity=10_000, risk_per_trade=0.01,
                                compound=True, overnight_policy="eod_utc")
COSTS = PatternCostModel(model_id="qa_cost", source="synthetic_fixture", scenario="hypothetical",
                         price_basis="bid_ohlc", spread_basis="bar_points", spread_points=None,
                         commission_per_lot_side=0.5, slippage_points=0,
                         swap_policy="zero_hypothesis", account_currency="USD",
                         valid_from_utc="2025-01-06T09:00:00Z",
                         valid_until_utc="2025-01-06T10:00:00Z",
                         calibrated_at_utc="2025-01-05T00:00:00Z")


def sample(rows, *, spread=0):
    observations = []
    for minute, prices in enumerate(rows):
        item = bar(minute, prices[3])
        item.update(open=str(prices[0]), high=str(prices[1]), low=str(prices[2]))
        observations.append(item)
    data = pd.DataFrame(observations)
    data["time"] = data["bar_start_utc"]
    data["spread"] = spread
    return observations, data


def run(observations, data, *, side="long", policy=POLICY, costs=COSTS):
    compiled = definition(side=side, cost_model_id=costs.model_id)
    _, events = pattern_engine.evaluate_batch(compiled, "db-a", "SYN", observations)
    return run_pattern_simulation(data, events, compiled, source_namespace="db-a",
                                  symbol_spec=SPEC, policy=policy, costs=costs)


def test_long_entry_next_open_spread_commission_and_equity():
    observations, data = sample([[100, 101, 99, 100], [98, 99, 97, 98],
                                 [98, 100, 97, 99], [102, 103, 101, 102]], spread=2)
    report = run(observations, data)
    trade = report["backtest"].trades[0]
    assert report["events"][0]["status"] == "simulated_fill"
    assert trade.entry_time == pd.Timestamp("2025-01-06T09:02:00Z")
    assert trade.entry_price == 100 and trade.exit_price == 102
    assert trade.volume == 10 and trade.pnl == pytest.approx(10)
    assert report["backtest"].account_balance.iloc[-1] == pytest.approx(10_010)
    assert report["backtest"].floating_equity.iloc[-1] == pytest.approx(10_010)
    assert report["cost_status"] == "hypothetical" and not report["economic_approval"]


def test_stop_first_ambiguity_is_reported_without_repricing_engine():
    observations, data = sample([[100, 101, 99, 100], [98, 99, 97, 98],
                                 [100, 115, 85, 100], [100, 101, 99, 100]])
    report = run(observations, data, costs=replace(COSTS, commission_per_lot_side=0))
    trade = report["backtest"].trades[0]
    assert trade.exit_reason == "stop" and trade.exit_price == 90
    assert trade.pnl == pytest.approx(-100)
    assert report["ambiguous_stop_first_bars"] == ["2025-01-06T09:02:00+00:00"]


def test_short_uses_bid_entry_ask_exit_and_costs():
    observations, data = sample([[100, 101, 99, 100], [98, 99, 97, 98],
                                 [98, 99, 97, 98], [94, 95, 93, 94]], spread=1)
    report = run(observations, data, side="short")
    trade = report["backtest"].trades[0]
    assert trade.direction == -1 and trade.entry_price == 98 and trade.exit_price == 95
    assert trade.pnl == pytest.approx(20)


def test_delayed_event_waits_for_next_closed_bar_before_open():
    observations, data = sample([[100, 101, 99, 100], [98, 99, 97, 98],
                                 [98, 99, 97, 98], [98, 99, 97, 98], [98, 99, 97, 98]])
    compiled = definition(cost_model_id=COSTS.model_id)
    _, events = pattern_engine.evaluate_batch(compiled, "db-a", "SYN", observations[:2])
    events[0]["decision_at_utc"] = "2025-01-06T09:03:30Z"
    report = run_pattern_simulation(data, events, compiled, source_namespace="db-a",
                                    symbol_spec=SPEC, policy=POLICY, costs=COSTS)
    assert report["events"][0]["signal_bar_start_utc"] == "2025-01-06T09:03:00+00:00"
    assert report["events"][0]["execution_bar_start_utc"] == "2025-01-06T09:04:00+00:00"


def test_unsupported_policy_missing_spread_and_wrong_source_are_refused():
    with pytest.raises(ValueError, match="limitada"):
        replace(POLICY, order_type="limit")
    observations, data = sample([[100, 101, 99, 100], [98, 99, 97, 98], [98, 99, 97, 98]])
    data.loc[1, "spread"] = None
    with pytest.raises(ValueError, match="Spread"):
        run(observations, data)
    data["spread"] = 0
    data.loc[1, "source_namespace"] = "foreign"
    with pytest.raises(ValueError, match="misturam"):
        run(observations, data)


def test_revised_bar_and_tampered_event_hash_are_refused():
    observations, data = sample([[100, 101, 99, 100], [98, 99, 97, 98], [98, 99, 97, 98]])
    compiled = definition(cost_model_id=COSTS.model_id)
    _, events = pattern_engine.evaluate_batch(compiled, "db-a", "SYN", observations)
    revised = data.copy()
    revised.loc[1, "revision"] = 2
    with pytest.raises(ValueError, match="barra de origem"):
        run_pattern_simulation(revised, events, compiled, source_namespace="db-a",
                               symbol_spec=SPEC, policy=POLICY, costs=COSTS)
    events[0]["event_id"] = "0" * 64
    with pytest.raises(ValueError, match="hash"):
        run_pattern_simulation(data, events, compiled, source_namespace="db-a",
                               symbol_spec=SPEC, policy=POLICY, costs=COSTS)


def test_tick_value_over_point_controls_lot_and_no_contract_double_count():
    observations, data = sample([[100, 101, 99, 100], [98, 99, 97, 98],
                                 [100, 101, 100, 100.5], [100.5, 101, 100, 100.5]])
    compiled = definition(cost_model_id=COSTS.model_id)
    _, events = pattern_engine.evaluate_batch(compiled, "db-a", "SYN", observations)
    spec = replace(SPEC, point=0.1, tick_size=0.5, tick_value=2, contract_size=1000)
    policy = replace(POLICY, stop_points=5)
    costs = replace(COSTS, commission_per_lot_side=0)
    report = run_pattern_simulation(data, events, compiled, source_namespace="db-a",
                                    symbol_spec=spec, policy=policy, costs=costs)
    trade = report["backtest"].trades[0]
    assert trade.volume == 50 and trade.pnl == pytest.approx(100)
    with pytest.raises(ValueError, match="múltiplo do tick"):
        run_pattern_simulation(data, events, compiled, source_namespace="db-a",
                               symbol_spec=spec, policy=replace(policy, stop_points=1), costs=costs)


def test_gap_through_stop_and_last_bar_without_next_open():
    observations, data = sample([[100, 101, 99, 100], [98, 99, 97, 98],
                                 [100, 101, 99, 100], [80, 82, 79, 81]])
    report = run(observations, data, costs=replace(COSTS, commission_per_lot_side=0))
    trade = report["backtest"].trades[0]
    assert trade.exit_reason == "stop" and trade.exit_price == 80
    assert trade.pnl == pytest.approx(-200)
    observations, data = sample([[100, 101, 99, 100], [100, 101, 99, 100],
                                 [98, 99, 97, 98]])
    report = run(observations, data)
    assert report["backtest"].trades == []
    assert report["events"][0]["status"] == "censored_no_next_open"
