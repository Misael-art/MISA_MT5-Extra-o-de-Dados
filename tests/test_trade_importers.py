"""T16.2: importadores do livro de operações (backtest Python, CSV do HADOUKEN nos dois formatos, runs antigos)."""
import json

import pandas as pd
import pytest

from mt5_extracao import cli, robots
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.robots import backtest, store
from mt5_extracao.trade_analytics import importers, ledger
from tests.market_data import trend


@pytest.fixture
def db(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "base.db"))
    db.save_ohlcv_data("BBB", "1 dia", trend(1300, step=0.2, noise=1.0, freq="D", spread=5, volume=3000))
    yield db
    db.close()


def _donchian(db):
    return next(r for r in robots.load_catalog(None, db) if r.key == "python:donchian")


def test_python_backtest_writes_complete_trades_to_the_ledger(db):
    run = backtest.run_python(db, None, _donchian(db), "BBB", "1 dia")
    df = ledger.load(db, run_id=run["run_id"])
    assert len(df) == run["metrics"]["trades"] > 0
    assert set(df["completeness"]) == {ledger.COMPLETE} and set(df["source"]) == {importers.PYTHON}
    assert df["pnl"].sum() == pytest.approx(run["metrics"]["net_profit"])
    assert df["params_hash"].nunique() == 1 and df["robot_key"].iloc[0] == "python:donchian"
    assert set(df["direction"]) <= {1, -1} and df["entry_time"].notna().all()


def test_ledger_failure_never_breaks_the_backtest(db, monkeypatch):
    monkeypatch.setattr(ledger, "save_trades", lambda *a, **k: 1 / 0)
    run = backtest.run_python(db, None, _donchian(db), "BBB", "1 dia")
    assert store.load_runs(db)[0]["run_id"] == run["run_id"]


def test_hidden_runs_do_not_enter(db):
    from mt5_extracao.robots import catalog
    run = {"run_id": "x", "robot_key": "python:a", "symbol": "BBB", "timeframe": "D1",
           "source": catalog.HIDDEN_RUN_SOURCES[0]}
    assert importers.record_python_run(db, run, []) == 0


OLD_CSV = "time;symbol;type;volume;result\n2026.01.05 10:30;EURUSD;buy;0.10;12.50\n2026.01.05 14:00;EURUSD;sell;0.10;-4,00\n"
NEW_CSV = ("time;symbol;type;volume;result;entry_time;entry_price;exit_price;sl;tp;direction;exit_reason\n"
           "2026.01.05 10:30;EURUSD;buy;0.10;12.50;2026.01.05 10:00;1.1000;1.1012;1.0990;0;buy;tp\n"
           "2026.01.05 14:00;EURUSD;sell;0.10;-4.00;2026.01.05 13:00;1.1010;1.1014;1.1014;1.0990;sell;sl\n")


def test_hdk_csv_old_and_extended_formats(tmp_path):
    old, new = tmp_path / "old.csv", tmp_path / "new.csv"
    old.write_text(OLD_CSV, encoding="utf-8")
    new.write_text(NEW_CSV, encoding="utf-8")
    a = importers.read_hdk_trades_full(old)
    assert [r["pnl"] for r in a] == [12.5, -4.0] and a[0]["entry_time"] is None
    b = importers.read_hdk_trades_full(new)
    assert [r["direction"] for r in b] == [1, -1] and b[0]["target_price"] is None and b[1]["stop_price"] == 1.1014
    assert b[1]["exit_reason"] == "sl" and b[0]["entry_time"] == "2026.01.05 10:00"
    # a leitura antiga (do card) continua igual para os dois formatos
    assert backtest.read_hdk_trades(new) == backtest.read_hdk_trades(old)


def test_mql5_run_from_csv_and_from_record(db, tmp_path):
    csv = tmp_path / "t.csv"
    csv.write_text(NEW_CSV, encoding="utf-8")
    record = {"run_id": "m1", "robot_key": "mql5:hdk", "symbol": "EURUSD", "timeframe": "H1", "source": "teste",
              "params": {"a": 1}, "trades": [["2026-01-05 10:30:00", 12.5, 0.1]]}
    assert importers.record_mql5_run(db, record, csv) == 2
    assert set(ledger.load(db, run_id="m1")["completeness"]) == {ledger.COMPLETE}
    record["run_id"] = "m2"
    importers.record_mql5_run(db, record)          # sem CSV: o que o run guardou, parcial
    assert ledger.load(db, run_id="m2")["completeness"].tolist() == [ledger.PARTIAL]


def test_import_existing_runs_is_idempotent(db):
    store.save_run(db, {"run_id": "old", "robot_key": "mql5:x", "source": "teste", "symbol": "BBB",
                        "timeframe": "D1", "data_start": "", "data_end": "", "bars": 0, "metrics": {}, "equity": [],
                        "trades": [["2026-01-05 10:00:00", 3.0, 1.0], ["2026-01-06 10:00:00", -1.0, 1.0]],
                        "created_at": "2026-01-07 00:00:00"})
    first = importers.import_existing_runs(db)
    assert first["runs"] == 1 and first["trades"] == 2
    assert importers.import_existing_runs(db)["runs"] == 0
    assert ledger.load(db, run_id="old")["source"].iloc[0] == importers.MQL5


def test_params_hash_is_stable():
    assert importers.params_hash({"b": 1, "a": 2}) == importers.params_hash({"a": 2, "b": 1})
    assert importers.params_hash({"a": 1}) != importers.params_hash({"a": 2})


def test_cli_trades_import(db, tmp_path, capsys):
    backtest.run_python(db, None, _donchian(db), "BBB", "1 dia")
    ledger.delete_run(db, store.load_runs(db)[0]["run_id"])
    cfg = tmp_path / "config.ini"
    cfg.write_text(f"[DATABASE]\ntype = sqlite\npath = {db.db_path}\n", encoding="utf-8")
    assert cli.main(["--config", str(cfg), "--json", "trades", "import"]) == cli.EXIT_OK
    data = json.loads(capsys.readouterr().out.strip().splitlines()[-1])["data"]
    assert data["runs"] == 1 and data["trades"] > 0


def test_cli_as_module_keeps_json_data(db, tmp_path):
    """`python -m mt5_extracao.cli` (run.sh --cli): subcomando de outro módulo também devolve os dados no --json."""
    import subprocess
    import sys
    cfg = tmp_path / "config.ini"
    cfg.write_text(f"[DATABASE]\ntype = sqlite\npath = {db.db_path}\n", encoding="utf-8")
    out = subprocess.run([sys.executable, "-m", "mt5_extracao.cli", "--config", str(cfg), "--json", "trades", "runs"],
                         capture_output=True, text=True, timeout=120)
    assert out.returncode == 0, out.stderr
    assert "runs" in json.loads(out.stdout.strip().splitlines()[-1])["data"]
