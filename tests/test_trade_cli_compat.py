"""T16.12 compatibility for the CLI commands already published by the earlier branch."""
import json

import pytest

from mt5_extracao import cli, robots
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.robots import backtest
from mt5_extracao.trade_analytics import insights
from tests.market_data import random_walk


@pytest.fixture
def setup(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "b.db"))
    db.save_ohlcv_data("BBB", "15 minutos", random_walk(3000, seed=4, vol=0.6, freq="15min", spread=10))
    robot = next(r for r in robots.load_catalog(None, db) if r.key == "python:donchian")
    run = backtest.run_python(db, None, robot, "BBB", "M15")
    config = tmp_path / "config.ini"
    config.write_text(f"[DATABASE]\ntype = sqlite\npath = {db.db_path}\n", encoding="utf-8")
    yield db, run, str(config)
    db.close()


def _call(config, capsys, *args):
    code = cli.main(["--config", config, "--json", "trades", *args])
    output = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    return code, output


def test_report_compare_keep_legacy_json_keys_and_dimension_filter(setup, capsys):
    db, run, config = setup
    code, output = _call(config, capsys, "report", "--robot", "python:donchian")
    assert code == cli.EXIT_OK and output["data"]["total"]["n"] == run["metrics"]["trades"]
    assert "day_of_week" in output["data"]["by"] and output["data"]["lens"][0]["symbol"] == "BBB"

    code, output = _call(config, capsys, "report", "--symbol", "BBB", "--by", "hour")
    assert code == cli.EXIT_OK and output["data"]["lens"][0]["robot_key"] == "python:donchian"
    assert list(output["data"]["by"]) == ["hour"]

    code, output = _call(config, capsys, "compare", "--run", run["run_id"],
                         "--modes", "ponto_fixo,trailing_atr,gradiente_fixo")
    assert code == cli.EXIT_OK
    assert set(output["data"]["modes"]) == {"ponto_fixo", "trailing_atr", "gradiente_fixo"}
    assert output["data"]["modes"]["ponto_fixo"]["metrics"]["net"] == pytest.approx(
        run["metrics"]["net_profit"])

    code = cli.main(["--config", config, "trades", "compare", "--run", run["run_id"], "--modes", "martingale"])
    assert code == cli.EXIT_USAGE
    capsys.readouterr()


def test_filter_simulation_and_insight_persistence_remain_explicit(setup, capsys, tmp_path, monkeypatch):
    from mt5_extracao.trade_analytics import export

    db, run, config = setup
    scope = "robot:" + run["robot_key"]
    code, output = _call(config, capsys, "insights", "--robot", run["robot_key"])
    assert code == cli.EXIT_OK and output["data"]["saved"] == 0
    assert insights.load(db, scope) == []

    code, output = _call(config, capsys, "insights", "--robot", run["robot_key"], "--save")
    assert code == cli.EXIT_OK and "insights" in output["data"]

    monkeypatch.setattr(export, "_folder", lambda: tmp_path / "propostas")
    code, output = _call(config, capsys, "simulate", "--robot", run["robot_key"], "--exclude-days", "4", "--export")
    assert code == cli.EXIT_OK and output["data"]["evaluation"]["rules"] == ["Não operar Sex"]
    assert output["data"]["proposal"] and list((tmp_path / "propostas").glob("*.json"))

    code = cli.main(["--config", config, "trades", "simulate", "--robot", run["robot_key"]])
    assert code == cli.EXIT_USAGE
    capsys.readouterr()
