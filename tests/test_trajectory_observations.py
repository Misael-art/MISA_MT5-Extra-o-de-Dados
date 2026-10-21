"""T19.14: observar os motores não muda a execução nem duplica parciais/ciclos."""
import json

import pandas as pd
import pytest

from mt5_extracao.strategies import ExitRules
from mt5_extracao.strategies.backtester import BacktestConfig, SymbolSpec, run_backtest
from mt5_extracao.trade_analytics.execution.engine import ExitPolicy, simulate
from tests.test_backtester import Manual, bars

SPEC = SymbolSpec(point=1, tick_size=1, tick_value=1)


def test_python_partial_swap_and_final_close_are_one_cycle_with_canonical_cash():
    data = bars([[100, 101, 99, 100], [100, 101, 99, 100],
                 [100, 111, 100, 110], [105, 106, 104, 105]], start="2026-01-05")
    spec = SymbolSpec(point=1, tick_size=1, tick_value=1, swap_mode=1, swap_long=-1)
    cfg = BacktestConfig(initial_equity=10000, risk_per_trade=0.01, commission_per_lot=0.5)
    strategy = Manual(long=[0], stop={0: 90}, rules=ExitRules(partial_r=1, partial_fraction=0.5,
                                                          breakeven_after_partial=False))
    plain = run_backtest(data, strategy, spec, cfg)
    result = run_backtest(data, strategy, spec, cfg, observation_timeframe="D1")
    pd.testing.assert_frame_equal(plain.trades_frame(), result.trades_frame())
    for key in ("equity", "account_balance", "floating_equity", "performance_equity", "account_cashflows"):
        pd.testing.assert_series_equal(getattr(plain, key), getattr(result, key))
    assert result.metrics == plain.metrics and plain.trajectory == {}
    events = result.trajectory["records"]
    assert {e["cycle_seq"] for e in events} == {0}
    assert [e["event_seq"] for e in events] == list(range(len(events)))
    entry = next(e for e in events if e["kind"] == "entry")
    partial = next(e for e in events if e["kind"] == "partial")
    assert entry["volume_open"] == 10 and entry["realized_net"] == -5
    assert entry["net_pnl"] is None
    assert partial["volume_open"] == 5 and partial["realized_net"] == 37.5
    assert partial["phase"] == "intrabar" and partial["start_time"] < partial["end_time"]
    assert [e["net_pnl"] for e in events if e["kind"] == "mark"] == [-5, 82.5, 42.5]
    exit_ = events[-1]
    assert exit_["kind"] == "exit" and exit_["volume_open"] == 0 and exit_["net_pnl"] == 40
    # O trade bruto conserva abertura da barra; o novo observador dá o fechamento nominal modelado.
    assert result.trades[0].exit_time == pd.Timestamp("2026-01-08")
    assert exit_["start_time"] == exit_["end_time"] == "2026-01-09T00:00:00"
    assert result.trajectory["timestamp_basis"] == "source_clock"
    assert result.trajectory["sequence_basis"] == "modeled_execution_order"
    json.dumps(result.trajectory, allow_nan=False)


def test_resimulation_additions_preserve_source_identity_and_fifo_cash():
    data = bars([[100, 100, 100, 100], [100, 100, 98, 99], [100, 100, 100, 100]],
                start="2026-01-05 10:00", freq="min")
    source = pd.DataFrame([dict(seq=7, direction=1, entry_time=data.time.iloc[0], entry_price=100,
                                stop_price=90, target_price=None, volume=1, exit_time=None, exit_reason=None)])
    cfg = BacktestConfig(initial_equity=1000, commission_per_lot=0.1, swap=False)
    policy = ExitPolicy(grid_levels=1, grid_spacing_points=1, grid_max_volume=2)
    plain = simulate(source, data, SPEC, cfg, policy)
    result = simulate(source, data, SPEC, cfg, policy, observation_timeframe="M1")
    assert result.trades[0].pnl == plain.trades[0].pnl == pytest.approx(0.6)
    assert result.adds == plain.adds == 1 and result.max_volume == plain.max_volume == 2
    assert result.source_seqs == plain.source_seqs == [7]
    for key in ("account_balance", "floating_equity", "performance_equity", "account_cashflows"):
        pd.testing.assert_series_equal(getattr(plain, key), getattr(result, key))
    events = result.trajectory["records"]
    assert {e["cycle_seq"] for e in events} == {0} and {e["source_seq"] for e in events} == {7}
    addition = next(e for e in events if e["kind"] == "add")
    assert addition["volume_open"] == 2 and addition["realized_net"] == -0.2
    assert addition["net_pnl"] is None
    assert [e["net_pnl"] for e in events if e["kind"] == "mark"] == pytest.approx([-0.1, -1.2])
    assert events[-1]["volume_open"] == 0 and events[-1]["net_pnl"] == pytest.approx(0.6)


def test_overlapping_cycles_are_not_derived_by_splitting_the_account_curve():
    data = bars([[100, 101, 99, 100], [100, 101, 89, 90], [90, 111, 89, 110]], freq="h")
    source = pd.DataFrame([dict(seq=seq, direction=1, entry_time=data.time.iloc[0], exit_time=None,
                                entry_price=100, stop_price=70, target_price=None, volume=volume,
                                exit_reason=None) for seq, volume in [(7, 1), (11, 2)]])
    result = simulate(source, data, SPEC, BacktestConfig(initial_equity=1000), ExitPolicy(),
                      observation_timeframe="H1")
    records = result.trajectory["records"]
    marks = [e for e in records if e["kind"] == "mark"]
    assert [e["net_pnl"] for e in marks if e["cycle_seq"] == 0] == [0, -10]
    assert [e["net_pnl"] for e in marks if e["cycle_seq"] == 1] == [0, -20]
    assert [e["net_pnl"] for e in records if e["kind"] == "exit"] == [10, 20]
    assert [e["source_seq"] for e in records if e["kind"] == "entry"] == [7, 11]


def test_signal_exit_is_at_next_open_and_stop_has_model_interval():
    data = bars([[100, 101, 99, 100]] * 4, start="2026-01-05 10:00", freq="min")
    result = run_backtest(data, Manual(long=[0], exit_long=[1], stop={0: 90}), SPEC,
                          BacktestConfig(), observation_timeframe="M1")
    exit_ = result.trajectory["records"][-1]
    assert exit_["kind"] == "exit" and exit_["phase"] == "open"
    assert exit_["start_time"] == exit_["end_time"] == "2026-01-05T10:02:00"
    data.loc[1, "low"] = 89
    stopped = run_backtest(data, Manual(long=[0], stop={0: 90}), SPEC, BacktestConfig(),
                           observation_timeframe="M1")
    exit_ = stopped.trajectory["records"][-1]
    assert exit_["phase"] == "intrabar"
    assert exit_["start_time"] == "2026-01-05T10:01:00" and exit_["end_time"] == "2026-01-05T10:02:00"


def test_gap_exit_time_is_known_at_open_without_changing_legacy_fill():
    data = bars([[100, 101, 99, 100], [100, 101, 99, 100], [85, 86, 84, 85]],
                start="2026-01-05 10:00", freq="min")
    result = run_backtest(data, Manual(long=[0], stop={0: 90}), SPEC, BacktestConfig(), observation_timeframe="M1")
    assert result.trades[0].exit_price == 85
    exit_ = result.trajectory["records"][-1]
    assert exit_["phase"] == "open" and exit_["start_time"] == exit_["end_time"] == "2026-01-05T10:02:00"
    source = result.trades_frame().assign(seq=7)
    again = simulate(source, data, SPEC, BacktestConfig(), ExitPolicy(), observation_timeframe="M1")
    assert again.trades[0].pnl == result.trades[0].pnl
    assert again.trajectory["records"][-1]["phase"] == "open"
