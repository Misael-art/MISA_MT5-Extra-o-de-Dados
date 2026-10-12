"""T10.1: contrato do robô (robot.json versão 1), validação e uso pela bateria e pelo backtest."""
import configparser
import json
from pathlib import Path

import pytest

from mt5_extracao import cli
from mt5_extracao.robots import backtest, battery, catalog, contract

LIBRARY = Path(__file__).parent / "robots_library"
# Nomes de parâmetro que a leitura por nomes prováveis NÃO reconhece: só o contrato diz o que cada um é
EA = ('#property version "1.00"\n#include <MT5X/Safety/Exec.mqh>\n'
      "input double Alfa = 0.5; // Fração do capital\n"
      "input double Beta = 2.0; // Teto do dia\n"
      "input double Gama = 10.0; // Teto geral\n"
      "input double Delta = 1.5; // Distância de proteção\n"
      "input long   Ident = 7001; // Identificador\n"
      "input bool   Gravar = false; // Gravar operações\n"
      "input int    Periodo = 20; // Período\n"
      'input string Rotulo = "x"; // Rótulo\n'
      "int OnInit(){return(0);}\nvoid OnTick(){ Entrar(); }\n")
MANIFEST = {"contract": 1, "name": "Contratado", "regime": "tendência", "timeframes": ["M5"],
            "inputs": {"magic": "Ident", "risk_pct": "Alfa", "daily_loss_pct": "Beta", "max_drawdown_pct": "Gama",
                       "stop": "Delta", "export_trades": "Gravar"},
            "tunable": ["Periodo"], "safety_lib": "1.0.0"}


def make_robot(tmp_path, manifest=MANIFEST, ea=EA):
    root = tmp_path / "lib"
    (root / "Include" / "MT5X" / "Safety").mkdir(parents=True, exist_ok=True)
    (root / "Include" / "MT5X" / "Safety" / "Exec.mqh").write_text("void Entrar(){ trade.Buy(0.1); }\n")
    (root / "Experts").mkdir(exist_ok=True)
    path = root / "Experts" / "Contratado.mq5"
    path.write_text(ea, encoding="utf-8")
    path.with_suffix(".ex5").write_bytes(b"ex5")
    path.with_suffix(".robot.json").unlink(missing_ok=True)
    if manifest is not None:
        path.with_suffix(".robot.json").write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
    return catalog.robot_from_file(path)


def test_valid_contract_and_declared_names(tmp_path):
    robot = make_robot(tmp_path)
    assert contract.declared(robot) and contract.validate(robot) == []
    assert contract.input_name(robot, "magic") == "Ident" and contract.input_name(robot, "nada") is None
    assert contract.facts(robot) == {"risk_pct": 0.5, "daily_loss_pct": 2.0, "max_drawdown_pct": 10.0,
                                     "mandatory_stop": True}
    assert contract.facts(robot, {"Alfa": "1.5", "Delta": "0"}) == {
        "risk_pct": 1.5, "daily_loss_pct": 2.0, "max_drawdown_pct": 10.0, "mandatory_stop": False}
    assert robot.name == "Contratado" and robot.regime == "tendência"       # o catálogo segue lendo os mesmos campos


@pytest.mark.parametrize("change, field, text", [
    ({"contract": 2}, "contract", "não suportada"),
    ({"contract": "1"}, "contract", "número inteiro"),
    ({"extra": 1}, "extra", "campo desconhecido"),
    ({"timeframes": "M5"}, "timeframes", "uma lista"),
    ({"inputs": {**MANIFEST["inputs"], "magic": "NaoExiste"}}, "inputs.magic", "não tem o parâmetro 'NaoExiste'"),
    ({"inputs": {**MANIFEST["inputs"], "risk_pct": "Rotulo"}}, "inputs.risk_pct", "numérico"),
    ({"inputs": {**MANIFEST["inputs"], "lucro": "Alfa"}}, "inputs.lucro", "papel desconhecido"),
    ({"inputs": {"magic": "Ident"}}, "inputs.stop", "obrigatório"),
    ({"inputs": "Ident"}, "inputs", "um objeto"),
    ({"tunable": ["Rotulo"]}, "tunable", "não é numérico"),
    ({"tunable": ["Sumiu"]}, "tunable", "não tem o parâmetro"),
])
def test_each_contract_problem_names_the_field_and_the_reason(tmp_path, change, field, text):
    problems = contract.validate(make_robot(tmp_path, {**MANIFEST, **change}))
    assert any(p.field == field and text in p.message for p in problems), [str(p) for p in problems]


def test_robot_without_contract_is_still_accepted():
    guard = next(r for r in catalog.scan_library(LIBRARY) if r.name == "DemoGuard")
    assert not contract.declared(guard) and contract.input_name(guard, "magic") is None
    assert [p.field for p in contract.validate(guard)] == ["contract"]
    assert battery.magic_number(guard) == "4242"                 # sem contrato, o magic segue vindo do nome provável


def test_schema_file_matches_the_validator():
    schema = json.loads((Path(contract.__file__).parent / "robot.schema.json").read_text(encoding="utf-8"))
    assert set(schema["properties"]) == set(contract.FIELDS)
    assert set(schema["properties"]["inputs"]["properties"]) == set(contract.ROLES)
    assert tuple(schema["properties"]["inputs"]["required"]) == contract.REQUIRED_ROLES
    assert schema["properties"]["contract"]["const"] == contract.CONTRACT_VERSION


def test_battery_reads_limits_from_the_contract_instead_of_guessing(tmp_path):
    with_contract = {i.id: i for i in battery.static_items(make_robot(tmp_path))}
    assert with_contract["contract"].status == battery.OK and with_contract["stop"].status == battery.OK
    assert with_contract["limits"].status == battery.OK and "0,50% por operação" in with_contract["limits"].detail
    assert with_contract["magic"].detail == "magic 7001"
    without = {i.id: i for i in battery.static_items(make_robot(tmp_path, None))}
    assert without["contract"].status == battery.WARN and without["limits"].status == battery.WARN
    assert without["stop"].status == battery.WARN and without["magic"].status == battery.WARN   # nomes não reconhecidos
    risky = {**MANIFEST}
    items = {i.id: i for i in battery.static_items(make_robot(tmp_path, risky, EA.replace("Alfa = 0.5", "Alfa = 3.0")
                                                              .replace("Delta = 1.5", "Delta = 0")))}
    assert items["limits"].status == battery.FAIL and items["stop"].status == battery.FAIL
    broken = {i.id: i for i in battery.static_items(make_robot(tmp_path, {**MANIFEST, "contract": 9}))}
    assert broken["contract"].status == battery.FAIL and "mt5x robots validate contratado" in broken["contract"].fix


def test_backtest_uses_the_declared_export_and_magic(tmp_path):
    robot = make_robot(tmp_path)
    assert backtest.tester_values(robot) == {"Gravar": "true"}
    assert backtest.tester_values(make_robot(tmp_path, None)) == {}


def test_cli_validate(tmp_path, capsys):
    make_robot(tmp_path, {**MANIFEST, "contract": 2})
    cfg = tmp_path / "config.ini"
    config = configparser.ConfigParser()
    config.read_dict({"DATABASE": {"type": "sqlite", "path": str(tmp_path / "db.sqlite")},
                      "ROBOTS": {"library_dirs": str(tmp_path / "lib")}})
    with open(cfg, "w", encoding="utf-8") as f:
        config.write(f)
    args = ["--config", str(cfg)]
    assert cli.main(args + ["robots", "validate", "contratado"]) == cli.EXIT_FAIL
    assert "✗ contract: versão 2 não suportada" in capsys.readouterr().out
    make_robot(tmp_path)
    assert cli.main(args + ["robots", "validate", "contratado"]) == cli.EXIT_OK
    assert "contrato 1 válido" in capsys.readouterr().out
    assert cli.main(args + ["robots", "validate"]) == cli.EXIT_USAGE
    assert cli.main(args + ["robots", "validate", "donchian"]) == cli.EXIT_USAGE
