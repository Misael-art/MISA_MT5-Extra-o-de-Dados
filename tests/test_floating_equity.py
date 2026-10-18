"""T20.3: patrimônio marcado, distinto do saldo de operações encerradas."""
import pandas as pd
import pytest
from sqlalchemy import text

from mt5_extracao.strategies import ExitRules
from mt5_extracao.strategies.backtester import BacktestConfig, SymbolSpec, run_backtest
from mt5_extracao.trade_analytics.execution.engine import ExitPolicy, simulate
from tests.test_backtester import Manual, bars


SPEC = SymbolSpec(point=1, tick_size=1, tick_value=1)
CFG = BacktestConfig(initial_equity=1000, risk_per_trade=0.1)


def test_open_loss_recovery_is_visible_without_changing_closed_drawdown():
    data = bars([[100, 101, 99, 100], [100, 101, 89, 90], [90, 111, 89, 110]], freq="h")
    result = run_backtest(data, Manual(long=[0], stop={0: 80}), SPEC, CFG)
    # 5 lotes: -50 ainda aberto; +50 no fechamento final.
    assert result.floating_equity.tolist() == [1000, 950, 1050]
    assert result.account_balance.tolist() == [1000, 1000, 1050]
    assert result.metrics["max_drawdown_pct"] == 0
    assert result.metrics["max_floating_drawdown_pct"] == 0.05
    assert result.metrics["max_floating_drawdown_money"] == 50
    assert result.valuation["resolution"] == "bar_close"
    assert not result.valuation["intrabar_observed"]
    assert result.valuation["status"] == "modeled"


@pytest.mark.parametrize("spec", [SymbolSpec(known=False), SymbolSpec(swap_mode=7, swap_long=1)])
def test_generic_spec_or_unsupported_swap_is_explicitly_limited(spec):
    data = bars([[100, 101, 99, 100]] * 3, freq="h")
    result = run_backtest(data, Manual(), spec, CFG)
    assert result.valuation["status"] == "limited"


def test_open_short_uses_ask_without_hypothetical_exit_costs():
    data = bars([[100, 101, 99, 100], [100, 105, 99, 104], [104, 105, 99, 100]],
                freq="h", spread=2)
    cfg = BacktestConfig(initial_equity=1000, risk_per_trade=0.1, commission_per_lot=1,
                         slippage_points=1)
    # Entrada 99; stop 119; 5 lotes; comissão de entrada 5.
    result = run_backtest(data, Manual(short=[0], stop={0: 119}), SPEC, cfg)
    assert result.account_balance.iloc[1] == 995
    assert result.floating_equity.iloc[1] == 960  # ASK 106: -35, sem saída hipotética
    assert result.floating_equity.iloc[-1] == 970  # recompra 103: -20; comissão total 10


def test_partial_and_swap_are_not_counted_twice_in_marks():
    data = bars([[100, 101, 99, 100], [100, 101, 99, 100],
                 [100, 111, 100, 110], [105, 106, 104, 105]], start="2026-01-05")
    spec = SymbolSpec(point=1, tick_size=1, tick_value=1, swap_mode=1, swap_long=-1)
    cfg = BacktestConfig(initial_equity=10000, risk_per_trade=0.01, commission_per_lot=0.5)
    rules = ExitRules(partial_r=1, partial_fraction=0.5, breakeven_after_partial=False)
    result = run_backtest(data, Manual(long=[0], stop={0: 90}, rules=rules), spec, cfg)
    assert result.account_balance.tolist() == [10000, 9995, 10037.5, 10040]
    assert result.floating_equity.tolist() == [10000, 9995, 10082.5, 10040]
    assert result.floating_equity.iloc[-1] == result.equity.iloc[-1]


def test_overlapping_positions_share_one_initial_capital():
    data = bars([[100, 101, 99, 100], [100, 101, 89, 90], [90, 111, 89, 110]], freq="h")
    source = pd.DataFrame([dict(direction=1, entry_time=data.time[0], exit_time=data.time[2],
                               entry_price=100, stop_price=70, target_price=None, volume=v,
                               exit_reason="fim dos dados") for v in (1, 2)])
    result = simulate(source, data, SPEC, CFG, ExitPolicy())
    assert result.floating_equity.tolist() == [1000, 970, 1030]
    assert result.account_balance.tolist() == [1000, 1000, 1030]
    assert result.metrics["max_floating_drawdown_money"] == 30


def test_gap_can_exceed_initial_capital_and_one_hundred_percent_drawdown():
    data = bars([[100, 101, 99, 100], [100, 101, 99, 100], [-200, -199, -201, -200]], freq="h")
    result = run_backtest(data, Manual(long=[0], stop={0: 80}), SPEC, CFG)
    assert result.floating_equity.iloc[-1] == -500
    assert result.metrics["max_floating_drawdown_pct"] == 1.5


@pytest.mark.parametrize("capital", [0, -100])
def test_nonpositive_peak_does_not_fabricate_a_percentage(capital):
    data = bars([[100, 101, 99, 100]] * 3, freq="h")
    result = run_backtest(data, Manual(), SPEC, BacktestConfig(initial_equity=capital))
    assert result.metrics["max_floating_drawdown_pct"] is None
    assert result.metrics["floating_drawdown_status"] == "nonpositive_peak"


def test_external_cashflows_change_cash_without_creating_profit_or_drawdown():
    data = bars([[100, 101, 99, 100]] * 3, freq="h")
    flows = pd.Series([200, -300, 100])
    result = run_backtest(data, Manual(), SPEC, CFG, cashflows=flows)
    assert result.account_balance.tolist() == [1200, 900, 1000]
    assert result.floating_equity.tolist() == [1200, 900, 1000]
    assert result.performance_equity.tolist() == [1000, 1000, 1000]
    assert result.metrics["net_profit"] == 0
    assert result.metrics["max_floating_drawdown_pct"] == 0
    assert result.metrics["time_weighted_return_pct"] == 0
    replay = simulate(pd.DataFrame(columns=["entry_time"]), data, SPEC, CFG, ExitPolicy(), cashflows=flows)
    assert replay.performance_equity.tolist() == [1000, 1000, 1000]
    assert replay.account_cashflows.tolist() == [200, -300, 100]


@pytest.mark.parametrize("flow,expected_cash,expected_final_performance", [
    (950, [1000, 1950, 2000], 1000),
    (-450, [1000, 550, 600], 1140),
])
def test_cashflow_at_close_is_unitized_at_preflow_equity(flow, expected_cash, expected_final_performance):
    data = bars([[100, 101, 99, 100], [100, 101, 89, 90], [90, 111, 89, 110]], freq="h")
    flows = [0, flow, 0]
    result = run_backtest(data, Manual(long=[0], stop={0: 80}), SPEC, CFG, cashflows=flows)
    # Antes do fluxo há 950 de equity / 1000 cotas = 0,95 por cota.
    # Aporte 950 cria 1000 cotas; retirada 450 resgata 450/0,95 cotas.
    assert result.account_balance.tolist() == expected_cash
    assert result.performance_equity.iloc[1] == pytest.approx(950, abs=1e-10, rel=0)
    assert result.performance_equity.iloc[-1] == pytest.approx(expected_final_performance, abs=1e-10, rel=0)
    assert result.metrics["max_floating_drawdown_pct"] == pytest.approx(0.05)
    assert result.metrics["net_profit"] == 50 and result.equity.iloc[-1] == 1050
    replay = simulate(result.trades_frame(), data, SPEC, CFG, ExitPolicy(), cashflows=flows)
    assert replay.account_balance.tolist() == expected_cash
    assert replay.performance_equity.iloc[-1] == pytest.approx(expected_final_performance, abs=1e-10, rel=0)


def test_deposit_at_close_funds_next_bar_risk_without_becoming_return():
    data = bars([[100, 101, 99, 100], [100, 101, 89, 90], [90, 111, 89, 110]], freq="h")
    result = run_backtest(data, Manual(long=[0], stop={0: 80}), SPEC, CFG, cashflows=[1000, 0, 0])
    assert result.trades[0].volume == 10  # 2000 de capital × 10% / stop 20
    assert result.trades[0].equity_before == 2000
    assert result.metrics["net_profit"] == 100
    assert result.floating_equity.iloc[-1] == 2100
    assert result.metrics["time_weighted_return_pct"] == pytest.approx(0.05)


@pytest.mark.parametrize("flows", [[0, float("nan"), 0], [0, True, 0], [0, 1], [0, -1000, 0]])
def test_invalid_or_exhausting_cashflow_is_rejected(flows):
    data = bars([[100, 101, 99, 100]] * 3, freq="h")
    with pytest.raises(ValueError, match="Fluxos externos"):
        run_backtest(data, Manual(), SPEC, CFG, cashflows=flows)


def test_cashflow_timestamp_must_match_its_valuation_bar():
    data = bars([[100, 101, 99, 100]] * 3, freq="h")
    flows = pd.Series([0, 100, 0], index=pd.DatetimeIndex(data.time) + pd.Timedelta(minutes=1))
    with pytest.raises(ValueError, match="Fluxos externos"):
        run_backtest(data, Manual(), SPEC, CFG, cashflows=flows)


def test_cashflow_cannot_unitize_an_unfunded_initial_portfolio():
    data = bars([[100, 101, 99, 100]] * 3, freq="h")
    with pytest.raises(ValueError, match="Fluxos externos"):
        run_backtest(data, Manual(), SPEC, BacktestConfig(initial_equity=0), cashflows=[100, 0, 0])


def test_legacy_read_does_not_add_columns_or_invent_open_equity(tmp_path):
    from mt5_extracao.database_manager import DatabaseManager
    from mt5_extracao.robots import store
    db = DatabaseManager(db_path=str(tmp_path / "legacy.sqlite"))
    try:
        with db.engine.begin() as conn:
            conn.execute(text(store._CREATE_RUNS))
            conn.execute(text("INSERT INTO _robot_runs (run_id, robot_key, source, created_at, equity_json, "
                              "metrics_json) VALUES ('old', 'python:x', 'backtest', '2026-01-01', "
                              ":curve, :metrics)"), {"curve": '[["t",1000]]', "metrics": '{"trades":0}'})
        with db.engine.connect() as conn:
            schema = conn.execute(text("SELECT sql FROM sqlite_master WHERE name='_robot_runs'")).scalar()
        loaded = store.load_runs(db)[0]
        assert loaded["equity"] == [["t", 1000]]
        assert loaded["account_balance"] is None and loaded["floating_equity"] is None
        assert loaded["performance_equity"] is None and loaded["account_cashflows"] is None
        assert loaded["valuation"]["status"] == "unavailable"
        with db.engine.connect() as conn:
            assert conn.execute(text("SELECT sql FROM sqlite_master WHERE name='_robot_runs'")).scalar() == schema
            assert conn.execute(text("SELECT equity_json FROM _robot_runs")).scalar() == '[["t",1000]]'
    finally:
        db.close()


def test_python_run_persists_complete_valuation_with_separate_closed_curve(tmp_path):
    from configparser import ConfigParser
    from types import SimpleNamespace
    from mt5_extracao.database_manager import DatabaseManager
    from mt5_extracao.robots import backtest, catalog, store
    from mt5_extracao.strategies.specs import spec_from_symbol_info
    db = DatabaseManager(db_path=str(tmp_path / "new.sqlite"))
    try:
        data = bars([[100, 101, 99, 100], [100, 101, 89, 90], [90, 111, 89, 110]], freq="h")
        data["tick_volume"], data["real_volume"] = 1, 0
        db.save_ohlcv_data("X", "1 hora", data)
        db.save_symbol_spec("X", spec_from_symbol_info(SimpleNamespace(point=1, trade_tick_size=1,
                                                                       trade_tick_value=1)))
        cfg = ConfigParser()
        cfg.read_dict({"STRATEGY": {"initial_equity": "1000", "risk_per_trade": "0.1"}})
        strategy = Manual(long=[0], stop={0: 80})
        strategy.min_bars = 0
        robot = SimpleNamespace(kind=catalog.PYTHON, key="python:manual")
        run = backtest.run_python(db, cfg, robot, "X", "H1", strategy=strategy, cashflows=[0, 950, 0])
        loaded = store.load_runs(db)[0]
        for key in ("account_balance", "floating_equity", "performance_equity", "account_cashflows", "valuation", "equity"):
            assert loaded[key] == run[key]
        assert [p[1] for p in loaded["floating_equity"]] == [1000, 1900, 2000]
        assert [p[1] for p in loaded["performance_equity"]] == [1000, 950, 1000]
        assert loaded["account_cashflows"] == [[str(data.time[1]), 950]]
        assert [p[1] for p in loaded["equity"]] == [1000, 1050]
        assert loaded["metrics"]["max_floating_drawdown_pct"] == 0.05
    finally:
        db.close()
