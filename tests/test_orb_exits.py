"""Bancada WIN da F19-PESQ-06 com mesmas entradas e risco por cenário."""
import pandas as pd
import pytest

from mt5_extracao.b3_public import spec_for
from mt5_extracao.composition import BaseAdapter, base_composition
from mt5_extracao.strategies.backtester import BacktestConfig
from mt5_extracao.strategies.specs import to_symbol_spec
from tests.test_backtester import bars


@pytest.mark.parametrize(("partial", "risk_fraction", "scenario", "expected", "fills"), [
    (True, .02, "target", 240, [2, 1, 1]),
    (False, .02, "target", 280, [2, 2]),
    (True, .01, "target", 140, [1, 1]),
    (True, .02, "stop_conflict", -200, [2, 2]),
    (True, .02, "breakeven", 100, [2, 1, 1]),
])
def test_orb_win_same_signal_cash_by_exit(partial, risk_fraction, scenario, expected, fills):
    frame = bars([[100000, 100250, 99750, 100000]] * 30 + [[100000, 100255, 100000, 100255],
                 [100255, 100755, 100250, 100755], [100755, 100955, 100750, 100955]],
                 start="2026-10-27 09:00", freq="min")
    symbol = "WINZ26"
    spec = to_symbol_spec(symbol, spec_for(symbol))
    if scenario == "stop_conflict":
        frame.loc[31, ["high", "low"]] = [100955, 99755]
    if scenario == "breakeven":
        frame.loc[32, ["high", "low", "close"]] = [100800, 100250, 100260]
    composition = base_composition("orb", symbol=symbol, timeframe="M1", risk_fraction=risk_fraction).to_dict()
    composition["protection"] = {"mode": "fixed_points", "params": {"stop_points": 500 / spec.point}}
    composition["profit_exit"] = {"mode": "fixed_points", "params": {"target_points": 700 / spec.point}}
    if partial:
        composition["partial_exit_plan"] = {"mode": "fixed_r", "params": {
            "trigger_r": 1, "fraction": .5, "breakeven_after_confirmation": scenario == "breakeven"}}
    result = BaseAdapter(composition).run(frame, spec, BacktestConfig(initial_equity=10000),
                                          timeframe="M1", observation_timeframe="M1")
    trade, = result.trades
    assert trade.entry_time == pd.Timestamp("2026-10-27 09:31")
    assert trade.entry_price - trade.stop_price == 500
    assert trade.target_price - trade.entry_price == 700
    assert trade.pnl == pytest.approx(expected)
    assert result.account_balance.iloc[-1] == pytest.approx(10000 + expected)
    assert [event["fill_volume"] for event in result.trajectory["records"] if event["fill_volume"] is not None] == fills


@pytest.mark.parametrize("direction", [1, -1])
def test_stepped_trailing_uses_closed_bars_in_both_directions(direction):
    reference = 100255
    movements = [[0, 700, -20, 595], [590, 610, 20, 600], [600, 730, 300, 695],
                 [695, 725, 200, 700], [310, 340, 205, 220], [150, 190, 120, 150]]
    values = [[100000, 100250, 99750, 100000]] * 30 + [[100000, 100255, 100000, 100255]]
    values += [[reference + price for price in row] for row in movements]
    if direction == -1:
        values = [[200000 - op, 200000 - low, 200000 - high, 200000 - close] for op, high, low, close in values]
    frame = bars(values, start="2026-10-27 09:00", freq="min")
    spec = to_symbol_spec("WINZ26", spec_for("WINZ26"))
    composition = base_composition("orb", symbol="WINZ26", timeframe="M1", risk_fraction=.02).to_dict()
    composition["protection"] = {"mode": "stepped_points_v1", "params": {
        "stop_points": 100, "trigger_points": 120, "distance_points": 100, "step_points": 20}}
    composition["profit_exit"] = {"mode": "fixed_points", "params": {"target_points": 400}}
    result = BaseAdapter(composition).run(frame, spec, BacktestConfig(initial_equity=10000),
                                          timeframe="M1", observation_timeframe="M1")
    trade, = result.trades
    changes = result.trajectory["trailing_changes"]
    assert [change["available_at"] for change in changes] == ["2026-10-27T09:33:00", "2026-10-27T09:35:00"]
    assert [direction * (change["stop"] - trade.entry_price) for change in changes] == [100, 200]
    assert direction * (trade.exit_price - trade.entry_price) == 150
    assert trade.exit_reason == "stop móvel"
    assert trade.pnl == pytest.approx(60)
