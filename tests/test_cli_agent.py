"""T9.1: a CLI pode ser operada por um agente sem tela: saída JSON, catálogo de comandos e retrato do projeto."""
import json
from pathlib import Path

import pytest

from mt5_extracao import cli
from mt5_extracao.database_manager import DatabaseManager
from tests.market_data import random_walk, trend

LIBRARY = Path(__file__).parent / "robots_library"


@pytest.fixture
def env(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "db.sqlite"))
    db.save_ohlcv_data("BBB", "1 dia", trend(1300, step=0.2, noise=1.0, freq="D", spread=5, volume=3000))
    db.save_ohlcv_data("AAA", "1 dia", random_walk(400, seed=1, spread=5, volume=4000))
    cfg = tmp_path / "config.ini"
    cfg.write_text(f"[DATABASE]\ntype = sqlite\npath = {tmp_path / 'db.sqlite'}\n[STRATEGY]\nmin_trades = 5\n"
                   f"[ROBOTS]\nlibrary_dirs = {LIBRARY}\n", encoding="utf-8")
    return ["--config", str(cfg), "--json"]


def run(capsys, argv):
    code = cli.main(argv)
    out = capsys.readouterr()
    lines = [line for line in out.out.splitlines() if line.strip()]
    assert len(lines) == 1, out.out                      # um único objeto JSON, nada mais na saída padrão
    payload = json.loads(lines[0])
    assert payload["exit_code"] == code and payload["ok"] == (code == 0)
    return payload


def test_every_listed_command_exists_and_has_arguments(env, capsys):
    payload = run(capsys, env + ["commands"])
    commands = {c["command"]: c for c in payload["data"]["commands"]}
    assert {"extract", "update", "validate", "screen", "robots", "analyze", "status", "commands"} <= set(commands)
    flags = [a["flags"][0] for a in commands["extract"]["arguments"]]
    assert "--symbols" in flags and "--tf" in flags
    assert payload["data"]["exit_codes"] == {"0": "sucesso", "1": "falha", "2": "uso incorreto"}


def test_errors_come_inside_the_json_with_the_exit_code(env, capsys):
    payload = run(capsys, env + ["analyze", "--symbol", "ZZZ", "--tf", "D1"])
    assert payload["exit_code"] == cli.EXIT_USAGE and "mt5x extract" in payload["errors"][0]
    payload = run(capsys, env + ["robots", "backtest", "donchian"])
    assert payload["exit_code"] == cli.EXIT_USAGE and "--symbol" in payload["errors"][0]


def test_tables_and_analyze_give_structured_numbers(env, capsys):
    series = run(capsys, env + ["tables"])["data"]["series"]
    assert {(s["symbol"], s["rows"]) for s in series} == {("BBB", 1300), ("AAA", 400)}
    data = run(capsys, env + ["analyze", "--symbol", "BBB", "--tf", "D1", "--period", "Tudo"])["data"]
    assert data["bars"] == 1300 and isinstance(data["summary"]["return_pct"], float)
    assert data["seasonality"]["weekday"] and data["freshness"]


def test_robots_list_and_backtest_as_json(env, capsys):
    robots = {r["key"]: r for r in run(capsys, env + ["robots"])["data"]["robots"]}
    assert robots["mql5:demoguard"]["risk"]["level"] == "Moderado" and "inputs" not in robots["mql5:demoguard"]
    assert robots["mql5:demoguard"]["preset_risks"]["Demo_Conservador"]["level"] == "Baixo"
    detail = run(capsys, env + ["robots", "list", "demoguard"])["data"]["robots"][0]
    assert any(i["name"] == "InpRiskPct" for i in detail["inputs"])

    run_data = run(capsys, env + ["robots", "backtest", "donchian", "--symbol", "BBB", "--tf", "D1"])["data"]["run"]
    assert run_data["metrics"]["trades"] > 0 and run_data["equity"][0][1] == 100000.0
    after = {r["key"]: r for r in run(capsys, env + ["robots"])["data"]["robots"]}
    assert after["python:donchian"]["stage"] == "testado" and after["python:donchian"]["headline"]["kind"] == "backtest"
    assert "equity" not in after["python:donchian"]["runs"][0]         # a lista fica leve


def test_validate_and_status_point_to_the_next_action(env, capsys):
    state = run(capsys, env + ["status"])["data"]
    assert state["base"]["series"] == 2 and state["robots"]["python"] == 7 and state["robots"]["mql5"] == 2
    assert state["robots"]["high_risk"] == ["mql5:martingale"]
    ids = [a["id"] for a in state["next_actions"]]
    assert ids[0] == "atualizar" and "especificacoes" in ids and "validar" in ids      # séries de teste são antigas
    assert [a["priority"] for a in state["next_actions"]] == list(range(1, len(ids) + 1))
    human = [a for a in state["next_actions"] if a["requires_human"]]
    assert [a["id"] for a in human] == ["compilar"]
    validate = next(a for a in state["next_actions"] if a["id"] == "validar")
    assert validate["command"] == "mt5x validate --symbols AAA,BBB --tf D1 --strategies all"

    payload = run(capsys, env + ["validate", "--symbols", "BBB", "--tf", "D1", "--strategies", "donchian"])
    verdict = payload["data"]["verdicts"][0]
    assert verdict["symbol"] == "BBB" and isinstance(verdict["approved"], bool) and verdict["checks"]
    assert run(capsys, env + ["status"])["data"]["validations"]["total"] == 1


def test_json_never_has_nan_or_infinity():
    text = json.dumps(cli._jsonable({"a": float("inf"), "b": float("nan"), "c": [1.5, None]}))
    assert json.loads(text) == {"a": None, "b": None, "c": [1.5, None]}


def test_plain_text_mode_is_unchanged(env, capsys):
    assert cli.main([a for a in env if a != "--json"] + ["status"]) == 0
    out = capsys.readouterr().out
    assert "Próximas ações sugeridas:" in out and not out.lstrip().startswith("{")
