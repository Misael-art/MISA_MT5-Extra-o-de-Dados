"""T10.3: `mt5x robots new` cria um robô válido sobre a biblioteca de segurança, sem MT5."""
import configparser
import json
import random
from pathlib import Path

import pytest

from mt5_extracao import cli
from mt5_extracao.robots import battery, catalog, contract, mql, scaffold

LIBRARY = Path(__file__).parent / "robots_library"


def test_create_writes_a_robot_that_honours_the_contract(tmp_path):
    files = scaffold.create("MeuRobo", tmp_path, magic=123456)
    assert [p.relative_to(tmp_path).as_posix() for p in files] == [
        "Experts/MeuRobo/MeuRobo.mq5", "Experts/MeuRobo/MeuRobo.robot.json", "Experts/MeuRobo/README.md",
        "Presets/MeuRobo_Conservador.set"]
    assert not any("{{" in p.read_text(encoding="utf-8") for p in files)          # nenhum marcador sobrou
    robot = catalog.robot_from_file(files[0])
    assert contract.validate(robot) == [] and robot.name == "MeuRobo" and robot.version == "1.00"
    assert battery.magic_number(robot) == "123456" and robot.manifest["safety_lib"] == scaffold.DEFAULT_SAFETY_VERSION
    assert list(robot.presets) == ["MeuRobo_Conservador"] and robot.presets["MeuRobo_Conservador"]["InpRiskPct"] == "0.25"
    assert robot.dll_imports == [] and "mt5x robots compile meurobo" in files[2].read_text(encoding="utf-8")


def test_new_robot_has_no_failed_static_item_except_compilation(tmp_path):
    robot = catalog.robot_from_file(scaffold.create("MeuRobo", tmp_path)[0])
    items = {i.id: i for i in battery.static_items(robot, None, [robot])}
    failed = {i for i, item in items.items() if item.status == battery.FAIL}
    assert failed == {"compile", "safety_lib"}                 # ainda não compilado nem instalado: é o próximo passo
    assert items["contract"].status == battery.OK and items["limits"].status == battery.OK
    assert items["orders"].status == battery.OK and items["stop"].status == battery.OK
    assert items["averaging"].status == battery.WARN           # o bloqueio é fixo no código, não um parâmetro
    preset = {i.id: i for i in battery.static_items(robot, None, [robot], "MeuRobo_Conservador")}
    assert "0,25% por operação" in preset["limits"].detail


def test_skeleton_never_sends_orders_outside_the_library_and_has_no_strategy(tmp_path):
    source = mql.read_text(scaffold.create("MeuRobo", tmp_path)[0])
    code = mql._BLOCK_COMMENT.sub("", "\n".join(line.split("//")[0] for line in source.splitlines()))
    assert not mql.ORDER_CALL.search(code)                     # nem a função nativa de ordem nem CTrade
    assert "g_exec.Open(" in code and "#include <MT5X/Safety/HDK_SafeExecutor.mqh>" in source
    body = source.split("//--- MT5X:SINAL:INICIO")[1].split("//--- MT5X:SINAL:FIM")[0]
    assert body.strip() == "return 0;"                         # sem sinal, não opera


def test_set_section_fills_only_the_marked_block(tmp_path):
    source = mql.read_text(scaffold.create("MeuRobo", tmp_path)[0])
    changed = scaffold.set_section(source, "SINAL", "   return 1;")
    assert "//--- MT5X:SINAL:INICIO\n   return 1;\n//--- MT5X:SINAL:FIM" in changed
    assert changed.replace("   return 1;\n", "   return 0;\n") == source
    for section in scaffold.SECTIONS:
        assert scaffold.set_section(source, section, "// x") != source
    with pytest.raises(ValueError, match="Marcador"):
        scaffold.set_section(source, "OUTRO", "x")
    with pytest.raises(ValueError, match="Marcador"):
        scaffold.set_section("void OnTick(){}", "SINAL", "x")


def test_magic_is_unique_and_names_are_checked(tmp_path):
    existing = catalog.scan_library(LIBRARY)                    # o DemoGuard usa o magic 4242

    class Fixed(random.Random):
        values = iter([4242, 4242, 555555])

        def randint(self, a, b):
            return next(self.values)
    assert scaffold.new_magic(existing, Fixed()) == 555555
    for bad in ("", "ab", "1Robo", "Meu Robo", "Robô", "x" * 41):
        with pytest.raises(ValueError, match="Nome inválido"):
            scaffold.create(bad, tmp_path)
    with pytest.raises(ValueError, match="Já existe um robô"):
        scaffold.create("DemoGuard", tmp_path, existing)
    scaffold.create("MeuRobo", tmp_path)
    before = (tmp_path / "Experts" / "MeuRobo" / "MeuRobo.mq5").read_text(encoding="utf-8")
    with pytest.raises(FileExistsError, match="nada foi alterado"):
        scaffold.create("MeuRobo", tmp_path)
    assert (tmp_path / "Experts" / "MeuRobo" / "MeuRobo.mq5").read_text(encoding="utf-8") == before


def test_safety_version_follows_the_configured_clone(tmp_path):
    lib = tmp_path / "lib"
    lib.mkdir()
    (lib / "safety.json").write_text(json.dumps({"version": "1.4.0", "include_dir": "MT5X/Safety", "files": ["a.mqh"]}))
    config = configparser.ConfigParser()
    config.read_dict({"ROBOTS": {"safety_lib_dir": str(lib)}})
    robot = catalog.robot_from_file(scaffold.create("MeuRobo", tmp_path / "robos", config=config)[0])
    assert robot.manifest["safety_lib"] == "1.4.0"


def test_cli_new_registers_the_folder_in_the_catalog(tmp_path, capsys):
    cfg = tmp_path / "config.ini"
    cfg.write_text(f"[DATABASE]\ntype = sqlite\npath = {tmp_path / 'db.sqlite'}\n", encoding="utf-8")
    args = ["--config", str(cfg)]
    assert cli.main(args + ["robots", "new", "MeuRobo", "--dir", str(tmp_path / "robos")]) == cli.EXIT_OK
    out = capsys.readouterr().out
    assert "Robô MeuRobo criado" in out and "adicionada ao catálogo" in out and "mt5x robots compile meurobo" in out
    assert cli.main(args + ["robots", "validate", "meurobo"]) == cli.EXIT_OK
    capsys.readouterr()
    assert cli.main(args + ["robots", "new", "MeuRobo", "--dir", str(tmp_path / "robos")]) == cli.EXIT_USAGE
    assert cli.main(args + ["robots", "new", "Outro"]) == cli.EXIT_USAGE
    assert cli.main(args + ["robots", "new", "Outro", "--dir", str(tmp_path / "robos")]) == cli.EXIT_OK
    assert "adicionada ao catálogo" not in capsys.readouterr().out        # a pasta já estava registrada


def test_reading_a_windows_file_and_writing_it_back_does_not_double_the_lines(tmp_path):
    """No Windows os fontes têm CRLF; o texto lido volta com "\\n", senão regravar criaria linhas em branco."""
    source = tmp_path / "Robo.mq5"
    source.write_bytes("int a;\r\nint b;\r\n".encode("utf-8"))
    assert mql.read_text(source) == "int a;\nint b;\n"
    source.write_bytes("﻿int a;\r\nint b;\r\n".encode("utf-16"))
    assert mql.read_text(source) == "int a;\nint b;\n"
