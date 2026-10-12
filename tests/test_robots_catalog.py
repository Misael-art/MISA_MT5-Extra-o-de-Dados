"""T8.1: catálogo de robôs (leitura de MQL5 e presets, classe de risco, registro e CLI), sem MT5."""
import configparser
import re
import shutil
from pathlib import Path

import pytest

from mt5_extracao import cli, robots
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.robots import catalog, mql, risk, store

LIBRARY = Path(__file__).parent / "robots_library"
EA = LIBRARY / "Experts" / "Demo" / "DemoGuard.mq5"


def by_name(items, name):
    return next(r for r in items if r.name == name)


# --- mql.py ---------------------------------------------------------------------------

def test_parse_source_reads_properties_inputs_and_includes():
    src = mql.parse_source(mql.read_text(EA))
    assert src.version == "1.20"
    assert src.descriptions == ["Robô de exemplo com gestão de risco.", "Segunda linha da descrição."]
    assert src.groups == ["1. Risco", "2. Stop"]
    names = [i.name for i in src.inputs]
    assert "InpIgnored" not in names and "InpAlsoIgnored" not in names      # comentados
    risk_input = next(i for i in src.inputs if i.name == "InpRiskPct")
    assert (risk_input.type, risk_input.default, risk_input.group) == ("double", "1.0", "1. Risco")
    assert risk_input.comment == "Risco por trade (%) [Personalizado]"
    assert next(i for i in src.inputs if i.name == "InpStartTime").default == "09:15"   # sem as aspas
    assert next(i for i in src.inputs if i.name == "InpMagicNumber").hidden             # sinput
    assert src.includes == [("Demo/Types.mqh", True), ("Trade/Trade.mqh", True)]


@pytest.mark.parametrize("encoding", ["utf-8", "utf-8-sig", "utf-16"])
def test_read_text_handles_metaeditor_encodings(tmp_path, encoding):
    path = tmp_path / "a.mq5"
    path.write_text('#property description "Ação média"\n', encoding=encoding)
    assert mql.parse_source(mql.read_text(path)).descriptions == ["Ação média"]


def test_parse_set_ignores_comments_and_optimization_ranges():
    values = mql.parse_set("; comentário\r\nA=1\r\nB=1.0||0.5||0.5||3.0||Y\r\n\r\nsem_igual\r\n")
    assert values == {"A": "1", "B": "1.0"}


def test_enums_resolve_number_from_preset_and_name_from_source():
    enums = mql.parse_enums(mql.read_text(LIBRARY / "Include" / "Demo" / "Types.mqh"))
    assert enums["ENUM_DEMO_PROFILE"]["DEMO_PROFILE_AGGRESSIVE"] == 2
    assert mql.enum_member(enums, "ENUM_DEMO_PROFILE", "0") == "DEMO_PROFILE_CONSERVATIVE"
    assert mql.enum_member(enums, "ENUM_DEMO_PROFILE", "DEMO_PROFILE_CUSTOM") == "DEMO_PROFILE_CUSTOM"
    assert mql.enum_member(enums, "ENUM_DEMO_PROFILE", "9") == ""


# --- catálogo -------------------------------------------------------------------------

def test_scan_library_finds_experts_only():
    found = robots.scan_library(LIBRARY)
    assert sorted(r.name for r in found) == ["DemoGuard", "Martingale"]      # o indicador fica de fora


def test_robot_from_file_collects_files_presets_and_notes():
    robot = robots.robot_from_file(EA)
    assert (robot.key, robot.kind, robot.version, robot.stage) == ("mql5:demoguard", "mql5", "1.20", "importado")
    assert robot.description.startswith("Robô de exemplo")
    assert sorted(robot.presets) == ["Demo_Agressivo", "Demo_Conservador", "Demo_SemLimite"]   # 'Outro' é de outro robô
    assert robot.presets["Demo_Agressivo"]["InpStopAtrMult"] == "1.0"
    assert any(f.endswith("Types.mqh") for f in robot.files) and any(f.endswith(".set") for f in robot.files)
    assert robot.missing_includes == ["Trade/Trade.mqh"]                     # biblioteca padrão, não versionada
    assert any("não compilado" in n for n in robot.notes)


def test_presets_get_risk_matching_their_profile():
    robot = robots.robot_from_file(EA)
    assert robot.risk.level == risk.MODERATE and "perfil Moderado" in robot.risk.reasons
    assert robot.preset_risks["Demo_Conservador"].level == risk.LOW
    assert robot.preset_risks["Demo_Agressivo"].level == risk.HIGH
    custom = robot.preset_risks["Demo_SemLimite"]                             # 0,4%/trade, mas sem limite diário
    assert custom.level == risk.HIGH and "sem limite de perda diário" in custom.reasons
    assert "sem preço médio" in robot.risk.reasons                            # proteção não é confundida com martingale


def test_martingale_without_stop_is_high_risk_and_dll_is_flagged():
    robot = by_name(robots.scan_library(LIBRARY), "Martingale")
    assert robot.risk.level == risk.HIGH
    assert "sem stop" in robot.risk.reasons and any("InpLotMultiplier" in r for r in robot.risk.reasons)
    assert robot.dll_imports == ["helper.dll"] and any("DLL" in n for n in robot.notes)


def test_risk_thresholds_and_unknown():
    def one(**values):
        inputs = [mql.MqlInput("InpRiskPct", "double", "1.0", "Risco por trade (%)"),
                  mql.MqlInput("InpStopPoints", "int", "300", "Stop (pontos)")]
        return risk.rate(inputs, {k: str(v) for k, v in values.items()})
    assert one(InpRiskPct=0.5).level == risk.LOW
    assert one(InpRiskPct=1.0).level == risk.MODERATE
    assert one(InpRiskPct=1.5).level == risk.HIGH
    assert one(InpRiskPct=0.5, InpStopPoints=0).level == risk.HIGH
    no_risk = risk.rate([mql.MqlInput("InpPeriod", "int", "14", "Período")])
    assert no_risk.level == risk.UNKNOWN and no_risk.reasons
    only_risk = risk.rate([mql.MqlInput("InpRiskPct", "double", "0.3", "")])
    assert only_risk.level == risk.MODERATE and "stop não identificado nos parâmetros" in only_risk.reasons


def test_compiled_only_robot_is_not_rated(tmp_path):
    ex5 = tmp_path / "Fechado.ex5"
    ex5.write_bytes(b"\x00binario")
    robot = robots.robot_from_file(ex5)
    assert (robot.has_source, robot.risk.level, robot.stage) == (False, risk.UNKNOWN, "compilado")
    with pytest.raises(ValueError, match="mq5 ou .ex5"):
        robots.robot_from_file(LIBRARY / "Presets" / "Outro.set")
    with pytest.raises(ValueError, match="indicador ou script"):
        robots.robot_from_file(LIBRARY / "Indicators" / "NotARobot.mq5")


def test_compiled_next_to_source_and_manifest(tmp_path):
    ea = tmp_path / "Experts" / "X" / "DemoGuard.mq5"
    shutil.copytree(LIBRARY, tmp_path, dirs_exist_ok=True)
    shutil.move(str(tmp_path / "Experts" / "Demo"), str(ea.parent))
    ea.with_suffix(".ex5").write_bytes(b"\x00")
    ea.with_suffix(".robot.json").write_text(
        '{"name": "Guardião", "regime": "tendência", "timeframes": ["M5", "M15"], "risk": {"risk_pct": 0.25, '
        '"daily_loss_pct": 2, "mandatory_stop": true}}', encoding="utf-8")
    robot = robots.robot_from_file(ea)
    assert (robot.name, robot.regime, robot.timeframes, robot.stage) == ("Guardião", "tendência", ("M5", "M15"), "compilado")
    assert robot.risk.level == risk.LOW and robot.risk.facts["risk_pct"] == 0.25
    assert not any("não compilado" in n for n in robot.notes)


def test_python_strategies_are_in_the_catalog():
    config = configparser.ConfigParser()
    config.read_string("[STRATEGY]\nrisk_per_trade = 0.02\n")
    items = catalog.python_robots(config)
    assert len(items) == 6 and all(r.kind == "python" and r.regime and r.timeframes for r in items)
    donchian = robots.get_robot(items, "donchian")
    assert donchian.key == "python:donchian" and donchian.risk.level == risk.HIGH        # 2%/trade na configuração
    assert [i.name for i in donchian.inputs]
    assert catalog.python_robots()[0].risk.level == risk.MODERATE                        # padrão: 1%
    with pytest.raises(ValueError, match="Robô desconhecido"):
        robots.get_robot(items, "nao_existe")


def test_load_catalog_merges_python_library_and_imported(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "db.sqlite"))
    config = configparser.ConfigParser()
    config.read_string(f"[ROBOTS]\nlibrary_dirs = {LIBRARY} ; {tmp_path / 'nao_existe'}\n")
    assert store.load_records(db) == {}                                       # base antiga: sem a tabela, sem erro
    loose = tmp_path / "Avulso.mq5"
    loose.write_text('#property version "2.00"\ninput double InpRiskPct = 0.5; // Risco por trade (%)\n'
                     'input int InpStopPoints = 200; // Stop (pontos)\nvoid OnTick() { }\n', encoding="utf-8")
    assert robots.import_robot(db, loose).key == "mql5:avulso"
    robots.import_robot(db, loose)                                            # importar de novo não duplica
    items = robots.load_catalog(config, db)
    assert len(items) == 9 and len({r.key for r in items}) == 9               # 6 Python + 2 da pasta + 1 avulso
    assert robots.get_robot(items, "avulso").risk.level == risk.LOW

    store.save_record(db, "mql5:demoguard", "mql5", "DemoGuard", str(EA), stage="testado")
    assert robots.get_robot(robots.load_catalog(config, db), "demoguard").stage == "testado"
    loose.unlink()
    gone = robots.get_robot(robots.load_catalog(config, db), "avulso")
    assert gone.risk.level == risk.UNKNOWN and gone.notes
    store.delete_record(db, "mql5:avulso")
    assert len(robots.load_catalog(config, db)) == 8


# --- CLI ------------------------------------------------------------------------------

@pytest.fixture
def cli_args(tmp_path):
    cfg = tmp_path / "config.ini"
    cfg.write_text(f"[DATABASE]\ntype = sqlite\npath = {tmp_path / 'db.sqlite'}\n"
                   f"[ROBOTS]\nlibrary_dirs = {LIBRARY}\n", encoding="utf-8")
    return ["--config", str(cfg)]


def test_cli_robots_list_and_detail(cli_args, capsys):
    assert cli.main(cli_args + ["robots"]) == 0
    out = capsys.readouterr().out
    assert "mql5:demoguard" in out and "python:donchian" in out and "v1.20" in out
    assert "preset Demo_Conservador: Baixo" in out and "não é garantia" in out
    assert cli.main(cli_args + ["robots", "list", "demoguard"]) == 0
    out = capsys.readouterr().out
    assert "InpRiskPct = 1.0" in out and "1. Risco" in out and "python:" not in out
    assert cli.main(cli_args + ["robots", "list", "inexistente"]) == cli.EXIT_USAGE


def test_cli_robots_import(cli_args, tmp_path, capsys):
    assert cli.main(cli_args + ["robots", "import"]) == cli.EXIT_USAGE
    assert cli.main(cli_args + ["robots", "import", str(tmp_path / "nada.mq5")]) == cli.EXIT_FAIL
    ex5 = tmp_path / "Fechado.ex5"
    ex5.write_bytes(b"\x00")
    assert cli.main(cli_args + ["robots", "import", str(ex5)]) == 0
    assert "Não avaliado" in capsys.readouterr().out
    assert cli.main(cli_args + ["robots"]) == 0
    assert "mql5:fechado" in capsys.readouterr().out


def test_cli_without_library_explains_how_to_add(tmp_path, capsys):
    cfg = tmp_path / "config.ini"
    cfg.write_text(f"[DATABASE]\ntype = sqlite\npath = {tmp_path / 'db.sqlite'}\n", encoding="utf-8")
    assert cli.main(["--config", str(cfg), "robots"]) == 0
    assert "library_dirs" in capsys.readouterr().out


def test_robots_key_is_in_defaults_and_example():
    from mt5_extracao.bootstrap.config_builder import DEFAULTS
    assert DEFAULTS["ROBOTS"]["library_dirs"] == ""
    example = (Path(__file__).parent.parent / "config" / "config.ini.example").read_text(encoding="utf-8")
    assert re.search(r"^\[ROBOTS\]\n(#.*\n)*library_dirs", example, re.M)
