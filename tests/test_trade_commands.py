"""T16.12: comandos CLI do Laboratório, com base de teste e contrato JSON."""
import configparser
import json
from pathlib import Path

import pytest

from mt5_extracao import cli, robots
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.robots import backtest
from mt5_extracao.trade_analytics import aggregate, ledger
from tests.market_data import trend


@pytest.fixture
def study(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "trades.db"))
    db.save_ohlcv_data("BBB", "1 dia", trend(1300, step=0.2, noise=1.0, freq="D", spread=5, volume=3000))
    robot = next(r for r in robots.load_catalog(None, db) if r.key == "python:donchian")
    run = backtest.run_python(db, None, robot, "BBB", "1 dia")
    config = tmp_path / "config.ini"
    config.write_text(f"[DATABASE]\ntype = sqlite\npath = {db.db_path}\n", encoding="utf-8")
    yield db, config, run
    db.close()


def _json_call(args, capsys):
    assert cli.main(args) == cli.EXIT_OK
    return json.loads(capsys.readouterr().out.strip().splitlines()[-1])


def test_enrich_and_report_json_match_laboratory_metrics(study, capsys):
    db, config, run = study
    scope = run["robot_key"]
    enriched = _json_call(["--config", str(config), "--json", "trades", "enrich", "--robot", scope], capsys)
    assert enriched["data"]["enrichment"]["operations"] == run["metrics"]["trades"]
    assert enriched["data"]["enrichment"]["partial"] == 0

    report = _json_call(["--config", str(config), "--json", "trades", "report", "--robot", scope], capsys)
    payload = report["data"]["report"]
    lab = aggregate.frame(db, configparser.ConfigParser(), robot_key=scope)
    expected = aggregate.summarize(lab)
    assert payload["summary"]["n"] == expected["n"]
    assert payload["summary"]["expectancy"] == pytest.approx(expected["expectancy"])
    assert payload["summary"]["ci_low"] == pytest.approx(expected["ci_low"])
    assert payload["scope_breakdown"]
    assert "time_by_day" in payload["heatmaps"]
    by_symbol = _json_call(["--config", str(config), "--json", "trades", "report", "--symbol", "BBB"], capsys)
    assert by_symbol["data"]["report"]["scope"] == "symbol:BBB"
    assert by_symbol["data"]["report"]["summary"]["n"] == expected["n"]


def test_simulate_json_and_explicit_save(study, capsys):
    db, config, run = study
    result = _json_call(["--config", str(config), "--json", "trades", "simulate", "--run-id", run["run_id"],
                         "--mode", "ponto_fixo", "--save"], capsys)
    simulation = result["data"]["simulations"][0]
    assert simulation["metrics"]["n"] > 0 and simulation["saved"] is True
    saved = ledger.load(db, run_id=run["run_id"] + "#ponto_fixo")
    assert len(saved) == simulation["metrics"]["n"]
    assert set(saved["source"]) == {"resim:ponto_fixo"}


def test_insights_and_export_json(study, capsys, tmp_path):
    _db, config, run = study
    scope = run["robot_key"]
    found = _json_call(["--config", str(config), "--json", "trades", "insights", "--robot", scope,
                        "--mode", "trailing_atr"], capsys)
    assert found["data"]["scope"] == "robot:" + scope
    assert isinstance(found["data"]["insights"], list)

    rules_file = tmp_path / "rules.json"
    rules_file.write_text(json.dumps({"rules": [{"tipo": "excluir_dia", "dias": [4]}], "tested": 8}),
                          encoding="utf-8")
    exported = _json_call(["--config", str(config), "--json", "trades", "export", "--robot", scope,
                           "--rules-file", str(rules_file), "--out-dir", str(tmp_path / "propostas")], capsys)
    proposal = exported["data"]["proposal"]
    saved = json.loads(Path(proposal["path"]).read_text(encoding="utf-8"))
    assert proposal["tested"] == saved["alternativas_testadas"] == 8
    assert saved["regras"] == [{"tipo": "excluir_dia", "dias": [4]}]
