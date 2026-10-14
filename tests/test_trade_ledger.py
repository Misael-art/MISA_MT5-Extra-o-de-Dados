"""T16.1: livro de operações (_trade_ledger) do Laboratório."""
import pandas as pd
import pytest
from sqlalchemy import inspect, text

from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.strategies.backtester import Trade
from mt5_extracao.trade_analytics import ledger


@pytest.fixture
def db(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "base.db"))
    yield db
    db.close()


def _trade(day, direction=1, pnl=10.0):
    entry = pd.Timestamp("2026-01-05 10:00") + pd.Timedelta(days=day)
    return Trade(direction=direction, entry_time=entry, exit_time=entry + pd.Timedelta(minutes=30),
                 entry_price=100.0, exit_price=101.0, volume=1.0, pnl=pnl, r_multiple=pnl / 10, bars=6,
                 exit_reason="alvo", equity_before=100_000.0)


def test_round_trip_from_backtester_trades(db):
    assert ledger.save_trades(db, "r1", [_trade(0), _trade(1, -1, -5.0)], robot_key="python:donchian",
                              symbol="WIN$N", timeframe="M5", params_hash="abc") == 2
    df = ledger.load(db, run_id="r1")
    assert list(df["seq"]) == [0, 1] and list(df["direction"]) == [1, -1]
    assert list(df["pnl"]) == [10.0, -5.0] and set(df["completeness"]) == {ledger.COMPLETE}
    assert df["entry_time"].iloc[0] == pd.Timestamp("2026-01-05 10:00")
    assert df["params_hash"].iloc[0] == "abc" and df["stop_price"].isna().all()


def test_saving_again_replaces_the_run(db):
    ledger.save_trades(db, "r1", [_trade(0), _trade(1)], robot_key="a", symbol="X")
    ledger.save_trades(db, "r1", [_trade(5)], robot_key="a", symbol="X")
    assert len(ledger.load(db, run_id="r1")) == 1


def test_partial_rows_from_exit_only_records(db):
    ledger.save_trades(db, "mql", [{"exit_time": "2026-01-05 11:00:00", "pnl": -3.0, "volume": 0.1}],
                       robot_key="mql5:hdk", symbol="EURUSD", source="mql5_tester")
    df = ledger.load(db, robot_key="mql5:hdk")
    assert df["completeness"].tolist() == [ledger.PARTIAL] and pd.isna(df["entry_time"].iloc[0])
    assert ledger.load(db, start="2026-01-05", end="2026-01-06").shape[0] == 1   # filtra pela saída quando falta a entrada


def test_two_lenses_and_period_filters(db):
    ledger.save_trades(db, "r1", [_trade(d) for d in range(5)], robot_key="python:a", symbol="WIN$N")
    ledger.save_trades(db, "r2", [_trade(d) for d in range(3)], robot_key="python:b", symbol="WIN$N")
    ledger.save_trades(db, "r3", [_trade(d) for d in range(2)], robot_key="python:a", symbol="WDO$N")
    assert len(ledger.load(db, robot_key="python:a")) == 7               # por robô: dois ativos
    assert set(ledger.load(db, symbol="WIN$N")["robot_key"]) == {"python:a", "python:b"}   # por ativo
    assert len(ledger.load(db, robot_key="python:a", start="2026-01-06", end="2026-01-08")) == 3
    summary = {r["run_id"]: r for r in ledger.runs(db)}
    assert summary["r1"]["trades"] == 5 and summary["r1"]["completeness"] == ledger.COMPLETE


def test_mixed_run_is_reported_partial(db):
    ledger.save_trades(db, "m", [_trade(0), {"exit_time": "2026-01-09 10:00", "pnl": 1.0}], robot_key="x")
    assert ledger.runs(db)[0]["completeness"] == ledger.PARTIAL


def test_empty_base_and_old_table_get_new_columns(db):
    assert ledger.load(db).empty and ledger.runs(db) == []
    with db.engine.begin() as conn:      # tabela de uma versão mais velha, sem algumas colunas
        conn.execute(text("CREATE TABLE _trade_ledger (run_id TEXT NOT NULL, seq INTEGER NOT NULL, source TEXT NOT "
                          "NULL, completeness TEXT NOT NULL, pnl REAL, PRIMARY KEY (run_id, seq))"))
    ledger.save_trades(db, "r", [_trade(0)], robot_key="a")
    cols = {c["name"] for c in inspect(db.engine).get_columns("_trade_ledger")}
    assert set(ledger.COLUMNS) <= cols


def test_delete_run(db):
    ledger.save_trades(db, "r", [_trade(0)])
    assert ledger.delete_run(db, "r") == 1 and ledger.load(db).empty
