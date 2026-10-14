"""T16.12: mt5x trades report | compare | insights | simulate (com --json)."""
import json

import pytest

from mt5_extracao import cli, robots
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.robots import backtest
from tests.market_data import random_walk


@pytest.fixture
def setup(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "b.db"))
    db.save_ohlcv_data("BBB", "15 minutos", random_walk(3000, seed=4, vol=0.6, freq="15min", spread=10))
    robot = next(r for r in robots.load_catalog(None, db) if r.key == "python:donchian")
    run = backtest.run_python(db, None, robot, "BBB", "M15")
    cfg = tmp_path / "config.ini"
    cfg.write_text(f"[DATABASE]\ntype = sqlite\npath = {db.db_path}\n", encoding="utf-8")
    yield db, run, str(cfg)
    db.close()


def _call(cfg, capsys, *args):
    code = cli.main(["--config", cfg, "--json", "trades", *args])
    return code, json.loads(capsys.readouterr().out.strip().splitlines()[-1])


def test_report_both_lenses(setup, capsys):
    db, run, cfg = setup
    code, out = _call(cfg, capsys, "report", "--robot", "python:donchian")
    assert code == cli.EXIT_OK and out["data"]["total"]["n"] == run["metrics"]["trades"]
    assert "day_of_week" in out["data"]["by"] and out["data"]["lens"][0]["symbol"] == "BBB"
    code, out = _call(cfg, capsys, "report", "--symbol", "BBB", "--by", "hour")
    assert out["data"]["lens"][0]["robot_key"] == "python:donchian"


def test_compare_insights_simulate_and_export(setup, capsys, tmp_path, monkeypatch):
    from mt5_extracao.trade_analytics import export
    db, run, cfg = setup
    code, out = _call(cfg, capsys, "compare", "--run", run["run_id"], "--modes", "ponto_fixo,trailing_atr,gradiente_fixo")
    assert code == cli.EXIT_OK and set(out["data"]["modes"]) == {"ponto_fixo", "trailing_atr", "gradiente_fixo"}
    assert out["data"]["modes"]["ponto_fixo"]["metrics"]["net"] == pytest.approx(run["metrics"]["net_profit"])
    assert cli.main(["--config", cfg, "trades", "compare", "--run", run["run_id"], "--modes", "martingale"]) \
        == cli.EXIT_USAGE                                                  # Python: sem martingale
    capsys.readouterr()
    code, out = _call(cfg, capsys, "insights", "--robot", "python:donchian", "--save")
    assert code == cli.EXIT_OK and "insights" in out["data"]
    monkeypatch.setattr(export, "_folder", lambda: tmp_path / "propostas")
    code, out = _call(cfg, capsys, "simulate", "--robot", "python:donchian", "--exclude-days", "4", "--export")
    assert code == cli.EXIT_OK and out["data"]["evaluation"]["rules"] == ["Não operar Sex"]
    assert out["data"]["proposal"] and list((tmp_path / "propostas").glob("*.json"))
    assert cli.main(["--config", cfg, "trades", "simulate", "--robot", "python:donchian"]) == cli.EXIT_USAGE
