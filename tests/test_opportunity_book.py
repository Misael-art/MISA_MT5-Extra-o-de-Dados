import json

from mt5_extracao import cli
from mt5_extracao.analytics import datasets
from mt5_extracao.strategies import ExitRules
from mt5_extracao.strategies.backtester import BacktestConfig, SymbolSpec, run_backtest
from mt5_extracao.trade_analytics import importers
from mt5_extracao.trade_analytics.trajectories import opportunities
from tests.test_backtester import Manual, bars
from tests.test_opening_coverage import OCTOBER, market


def save_run(db, run_id, snapshot, result):
    run = {"run_id": run_id, "robot_key": "python:manual", "source": "fixture",
           "symbol": OCTOBER, "timeframe": "M1", "params": {}}
    datasets.bind(db, run_id, snapshot.manifest_id)
    return importers.record_python_run(db, run, result.trades, trajectory=result.trajectory)


def test_opportunities_keep_veto_and_expiration_and_link_partial_fills(market):
    db, _, _, schedule = market
    data = bars([[100, 101, 99, 100], [100, 101, 99, 100], [100, 111, 100, 110],
                 [105, 106, 104, 105], [100, 101, 99, 100]], start="2026-10-05 09:00", freq="min")
    db.save_ohlcv_data(OCTOBER, "1 minuto", data)
    snap = datasets.capture(db, OCTOBER, "M1", "2026-10-05 09:00", "2026-10-05 09:04")
    strategy = Manual(long=[0, 2, 4], stop={0: 90, 2: 90, 4: 90},
                      rules=ExitRules(partial_r=1, partial_fraction=0.5, breakeven_after_partial=False))
    model = run_backtest(snap.bars, strategy, SymbolSpec(point=1, tick_size=1, tick_value=1),
                         BacktestConfig(initial_equity=10000, risk_per_trade=0.01, commission_per_lot=0.5),
                         observation_timeframe="M1")
    assert save_run(db, "signal-ledger", snap, model) == 1
    result = opportunities.query(db, "signal-ledger", calendar={**schedule, "sessions": schedule["sessions"][:1]})
    assert result["counts"] == dict(opportunities=3, vetoed=1, expired=1, executed=1,
                                    closed_cycles=1, fills=3, broker_rejections=None)
    first, blocked, expired = result["opportunities"]
    assert [item["volume_open"] for item in first["fills"]] == [10, 5, 0]
    assert [item["commission"] for item in first["fills"]] == [5, 2.5, 2.5]
    assert first["fill_delay_seconds"] == 0 and first["requested_price"] == 100
    assert model.trades[0].pnl == first["net_pnl"] == result["net_pnl"]
    assert first["mae_observed"] == 5 and first["mfe_observed"] > 0
    assert blocked["reason"] == "position_open" and expired["reason"] == "no_following_bar"
    assert result["sessions"][0]["opportunity_seqs"] == [0, 1, 2]
    assert save_run(db, "signal-ledger", snap, model) == 1
    assert result == opportunities.query(db, "signal-ledger", calendar={**schedule, "sessions": schedule["sessions"][:1]})


def test_cli_keeps_eligible_session_without_opportunity(market, tmp_path, capsys):
    db, path, ids, schedule = market
    snapshot = datasets.load(db, ids[0])
    model = run_backtest(snapshot.bars, Manual(), observation_timeframe="M1")
    assert save_run(db, "no-signals", snapshot, model) == 0
    calendar = tmp_path / "sessions.json"
    calendar.write_text(json.dumps(schedule), encoding="utf-8")
    db.close()
    assert cli.main(["--db", str(path), "--json", "trades", "opportunities", "--run", "no-signals",
                     "--calendar", str(calendar)]) == 0
    result = json.loads(capsys.readouterr().out)["data"]["opportunities"]
    assert result["counts"]["opportunities"] == 0
    assert result["sessions"][0]["status"] == "eligible"
    assert result["sessions"][0]["opportunity_seqs"] == []
    assert result["sessions"][2]["status"] == "excluded"


def test_absence_of_legacy_opportunities_is_not_a_zero_count(market):
    db, _, _, _ = market
    result = opportunities.query(db, "known-to-old-format")
    assert result["status"] == "unavailable" and result["opportunities"] is None
