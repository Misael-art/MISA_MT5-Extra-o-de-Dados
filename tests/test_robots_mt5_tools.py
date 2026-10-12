"""T8.4: ferramentas do MT5 para robôs (pasta de dados, instalação e compilação), sem MT5 nem Wine."""
import configparser
import shutil
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from mt5_extracao import cli, robots
from mt5_extracao.bootstrap import paths
from mt5_extracao.robots import catalog, mt5_tools

LIBRARY = Path(__file__).parent / "robots_library"
WINDOWS_DIR = "C:\\Program Files\\MetaTrader 5"

LOG_OK = ("MQL5\\Experts\\Demo\\DemoGuard.mq5 : information: compiling MQL5\\Experts\\Demo\\DemoGuard.mq5\n"
          " : information: generating code 100%\n : information: code generated\n"
          "Result: 0 errors, 0 warnings, 672 ms elapsed, cpu='X64 Regular'\n")
# Formato real do MetaEditor no Wine (prova manual da T8.4), com a linha de erro repetida
LOG_BAD = ("MQL5\\Experts\\Ruim.mq5 : information: compiling MQL5\\Experts\\Ruim.mq5\n"
           "MQL5\\Experts\\Ruim.mq5(2,53) : error 154: '}' - semicolon expected\n"
           "MQL5\\Experts\\Ruim.mq5(2,53) : error 154: '}' - semicolon expected\n"
           "MQL5\\Experts\\Ruim.mq5(2,20) : warning 31: variable 'naoUsada' not used\n"
           "MQL5\\Experts\\Ruim.mq5(3,17) : error 256: undeclared identifier 'indefinida'\n"
           "MQL5\\Experts\\Ruim.mq5(3,28) : error 157: ')' - expression expected\n"
           "Result: 4 errors, 1 warnings\n")


@pytest.fixture
def linux(monkeypatch):
    """Os testes exercitam o caminho do Linux (prefixo Wine) em qualquer sistema."""
    monkeypatch.setattr(paths, "is_windows", lambda: False)


@pytest.fixture
def mt5_home(tmp_path, linux):
    """Prefixo Wine falso com o MT5 em modo portátil (dados na pasta da instalação)."""
    prefix = tmp_path / "wine"
    home = prefix / "drive_c" / "Program Files" / "MetaTrader 5"
    (home / "MQL5" / "Experts").mkdir(parents=True)
    (home / "terminal64.exe").write_bytes(b"")
    (home / "MetaEditor64.exe").write_bytes(b"")
    config = configparser.ConfigParser()
    config.read_dict({"MT5": {"path": str(home), "wine_prefix": str(prefix)}, "ROBOTS": {"library_dirs": str(LIBRARY)}})
    return SimpleNamespace(prefix=prefix, home=home, config=config)


def robot(name="DemoGuard"):
    return next(r for r in catalog.scan_library(LIBRARY) if r.name == name)


def fake_metaeditor(log_text, write_ex5=True, calls=None):
    """Faz o papel do MetaEditor: lê a linha de comando e grava o log (UTF-16) e o .ex5."""
    def run(command_line, cwd, prefix, timeout):
        if calls is not None:
            calls.append((command_line, cwd, prefix))
        quoted = command_line.split('"')
        source, log_file = (paths.wine_to_linux_path(quoted[i], prefix) for i in (3, 5))
        log_file.write_bytes(log_text.encode("utf-16"))
        if write_ex5:
            source.with_suffix(".ex5").write_bytes(b"ex5")
        return 1 if write_ex5 else 0        # o MetaEditor devolve 1 quando compila
    return run


# --- pasta de dados e MetaEditor ------------------------------------------------------

def test_data_dir_prefers_the_connected_terminal(mt5_home):
    other = mt5_home.prefix / "drive_c" / "users" / "u" / "AppData" / "Roaming" / "MetaQuotes" / "Terminal" / "ABC"
    (other / "MQL5").mkdir(parents=True)
    mt5 = SimpleNamespace(terminal_info=lambda: SimpleNamespace(
        data_path="C:\\users\\u\\AppData\\Roaming\\MetaQuotes\\Terminal\\ABC"))
    assert mt5_tools.data_dir(mt5_home.config, mt5) == other
    broken = SimpleNamespace(terminal_info=lambda: None)          # MT5 desconectado: cai na configuração
    assert mt5_tools.data_dir(mt5_home.config, broken) == mt5_home.home


def test_data_dir_without_mt5_uses_origin_txt_then_the_installation(mt5_home):
    assert mt5_tools.data_dir(mt5_home.config) == mt5_home.home    # portátil
    user = mt5_home.prefix / "drive_c" / "users" / "u" / "AppData" / "Roaming" / "MetaQuotes" / "Terminal" / "ABC"
    user.mkdir(parents=True)
    (user / "origin.txt").write_bytes(WINDOWS_DIR.encode("utf-16"))
    assert mt5_tools.data_dir(mt5_home.config) == mt5_home.home    # origin.txt sem MQL5 ao lado não vale
    (user / "MQL5").mkdir()
    assert mt5_tools.data_dir(mt5_home.config) == user


def test_missing_data_dir_and_metaeditor_say_what_to_do(mt5_home):
    assert mt5_tools.find_metaeditor(mt5_home.config) == mt5_home.home / "MetaEditor64.exe"
    (mt5_home.home / "MetaEditor64.exe").unlink()
    with pytest.raises(FileNotFoundError, match="metaeditor_path"):
        mt5_tools.find_metaeditor(mt5_home.config)
    custom = mt5_home.prefix / "drive_c" / "Editor.exe"
    custom.write_bytes(b"")
    mt5_home.config["ROBOTS"]["metaeditor_path"] = str(custom)
    assert mt5_tools.find_metaeditor(mt5_home.config) == custom
    shutil.rmtree(mt5_home.home / "MQL5")
    with pytest.raises(FileNotFoundError, match="Abra o MetaTrader 5"):
        mt5_tools.data_dir(mt5_home.config)
    with pytest.raises(FileNotFoundError, match="configure"):
        mt5_tools.data_dir(configparser.ConfigParser())


# --- instalação -----------------------------------------------------------------------

def test_install_mirrors_the_mt5_layout_and_is_idempotent(mt5_home):
    items = mt5_tools.install(robot(), mt5_home.home)
    mql5 = mt5_home.home / "MQL5"
    targets = {i.target.relative_to(mql5).as_posix() for i in items}
    assert {"Experts/Demo/DemoGuard.mq5", "Include/Demo/Types.mqh", "Presets/Demo_Conservador.set"} <= targets
    assert all(i.status == mt5_tools.COPIED and i.target.read_bytes() == i.source.read_bytes() for i in items)
    assert {i.status for i in mt5_tools.install(robot(), mt5_home.home)} == {mt5_tools.SAME}


def test_install_keeps_a_preset_the_user_changed_but_updates_the_source(mt5_home):
    mt5_tools.install(robot(), mt5_home.home)
    preset = mt5_home.home / "MQL5" / "Presets" / "Demo_Conservador.set"
    source = mt5_home.home / "MQL5" / "Experts" / "Demo" / "DemoGuard.mq5"
    preset.write_text("InpRiskPct=0.1\n", encoding="utf-8")
    source.write_text("// versão antiga\n", encoding="utf-8")
    status = {i.target.name: i.status for i in mt5_tools.install(robot(), mt5_home.home)}
    assert status["Demo_Conservador.set"] == mt5_tools.KEPT and preset.read_text(encoding="utf-8") == "InpRiskPct=0.1\n"
    assert status["DemoGuard.mq5"] == mt5_tools.COPIED and "versão antiga" not in source.read_text(encoding="utf-8")
    status = {i.target.name: i.status for i in mt5_tools.install(robot(), mt5_home.home, overwrite_presets=True)}
    assert status["Demo_Conservador.set"] == mt5_tools.COPIED


def test_install_of_a_loose_file_and_of_a_python_strategy(mt5_home, tmp_path):
    loose = tmp_path / "solto" / "Solto.mq5"
    loose.parent.mkdir()
    shutil.copy(LIBRARY / "Experts" / "Demo" / "Martingale.mq5", loose)
    items = mt5_tools.install(catalog.robot_from_file(loose), mt5_home.home)
    assert items[0].target == mt5_home.home / "MQL5" / "Experts" / "Solto.mq5"
    python = next(r for r in robots.load_catalog() if r.kind == robots.PYTHON)
    with pytest.raises(ValueError, match="estratégia Python"):
        mt5_tools.install(python, mt5_home.home)
    with pytest.raises(ValueError, match="não há o que compilar"):
        mt5_tools.compile_robot(mt5_home.config, catalog.Robot("mql5:x", catalog.MQL5, "X", has_source=False))


# --- compilação -----------------------------------------------------------------------

def test_parse_compile_log_reads_errors_and_warnings():
    errors, warnings, messages = mt5_tools.parse_compile_log(LOG_BAD)
    assert (errors, warnings) == (4, 1)
    assert [(m.kind, m.line, m.column, m.code) for m in messages] == [
        ("error", 2, 53, 154), ("warning", 2, 20, 31), ("error", 3, 17, 256), ("error", 3, 28, 157)]
    assert str(messages[1]) == "MQL5\\Experts\\Ruim.mq5(2,20): aviso 31: variable 'naoUsada' not used"
    assert mt5_tools.parse_compile_log(LOG_OK) == (0, 0, [])
    assert mt5_tools.parse_compile_log("nada útil")[:2] == (None, None)


def test_compile_robot_installs_builds_the_quoted_command_and_reads_the_log(mt5_home):
    calls = []
    items, result = mt5_tools.compile_robot(mt5_home.config, robot(), run=fake_metaeditor(LOG_OK, calls=calls))
    source = mt5_home.home / "MQL5" / "Experts" / "Demo" / "DemoGuard.mq5"
    assert items[0].target == source and result.ok and (result.errors, result.warnings) == (0, 0)
    assert result.compiled == str(source.with_suffix(".ex5")) and "sem erros" in result.summary
    command_line, cwd, prefix = calls[0]
    assert command_line == (f'"{WINDOWS_DIR}\\MetaEditor64.exe" '
                            f'/compile:"{WINDOWS_DIR}\\MQL5\\Experts\\Demo\\DemoGuard.mq5" '
                            f'/log:"{WINDOWS_DIR}\\MQL5\\Experts\\Demo\\DemoGuard.log"')
    assert cwd == mt5_home.home and prefix == mt5_home.prefix


def test_compile_failures_are_reported_with_the_fix(mt5_home):
    source = mt5_tools.install(robot(), mt5_home.home)[0].target
    editor = mt5_home.home / "MetaEditor64.exe"
    bad = mt5_tools.compile_source(source, editor, mt5_home.prefix, fake_metaeditor(LOG_BAD, write_ex5=False))
    assert not bad.ok and (bad.errors, bad.warnings) == (4, 1) and "corrija o fonte" in bad.summary
    assert bad.to_dict()["messages"][0]["code"] == 154
    no_file = mt5_tools.compile_source(source, editor, mt5_home.prefix, fake_metaeditor(LOG_OK, write_ex5=False))
    assert not no_file.ok and "não foi gerado" in no_file.summary
    silent = mt5_tools.compile_source(source, editor, mt5_home.prefix, lambda *a: 0)
    assert not silent.ok and "não gerou o log" in silent.summary

    def slow(*a):
        raise subprocess.TimeoutExpired("wine", 1)
    assert "não terminou" in mt5_tools.compile_source(source, editor, mt5_home.prefix, slow).summary
    with pytest.raises(ValueError, match="fora do prefixo Wine"):
        mt5_tools.compile_source(LIBRARY / "Experts" / "Demo" / "DemoGuard.mq5", editor, mt5_home.prefix, lambda *a: 0)


def test_wine_runner_uses_a_bat_file_and_cleans_up(mt5_home, monkeypatch):
    seen = {}

    def fake_run(argv, cwd, env, timeout, stdout, stderr):
        bat = paths.wine_to_linux_path(argv[3], mt5_home.prefix)
        seen.update(argv=argv, script=bat.read_bytes().decode("cp1252"), prefix=env["WINEPREFIX"], bat=bat)
        return SimpleNamespace(returncode=1)
    monkeypatch.setattr(mt5_tools.shutil, "which", lambda name: "/usr/bin/wine")
    monkeypatch.setattr(mt5_tools.subprocess, "run", fake_run)
    code = mt5_tools.run_windows_command('"C:\\a b\\x.exe" /compile:"C:\\50%\\é.mq5"', mt5_home.home, mt5_home.prefix)
    assert code == 1 and seen["argv"][:3] == ["wine", "cmd", "/c"] and seen["prefix"] == str(mt5_home.prefix)
    assert seen["script"] == '@echo off\r\n"C:\\a b\\x.exe" /compile:"C:\\50%%\\é.mq5"\r\nexit /b %errorlevel%\r\n'
    assert not seen["bat"].exists()
    with pytest.raises(ValueError, match="letras comuns"):
        mt5_tools.run_windows_command('x.exe "C:\\日本.mq5"', mt5_home.home, mt5_home.prefix)
    assert not list((mt5_home.prefix / "drive_c" / "windows" / "temp").iterdir())
    with pytest.raises(RuntimeError, match="wine_prefix"):
        mt5_tools.run_windows_command("x.exe", mt5_home.home, None)
    monkeypatch.setattr(mt5_tools.shutil, "which", lambda name: None)
    with pytest.raises(RuntimeError, match="Wine não encontrado"):
        mt5_tools.run_windows_command("x.exe", mt5_home.home, mt5_home.prefix)


# --- CLI ------------------------------------------------------------------------------

@pytest.fixture
def cli_args(mt5_home, tmp_path):
    cfg = tmp_path / "config.ini"
    mt5_home.config.read_dict({"DATABASE": {"type": "sqlite", "path": str(tmp_path / "db.sqlite")}})
    with open(cfg, "w", encoding="utf-8") as f:
        mt5_home.config.write(f)
    return ["--config", str(cfg)]


def test_cli_compile_records_the_stage_and_failure_exits_1(cli_args, mt5_home, monkeypatch, capsys):
    monkeypatch.setattr(mt5_tools, "run_windows_command", fake_metaeditor(LOG_BAD, write_ex5=False))
    assert cli.main(cli_args + ["robots", "compile", "demoguard"]) == cli.EXIT_FAIL
    out = capsys.readouterr().out
    assert "erro 154" in out and "4 erro(s)" in out
    assert cli.main(cli_args + ["robots", "list", "demoguard"]) == cli.EXIT_OK
    assert "estágio: Importado" in capsys.readouterr().out

    monkeypatch.setattr(mt5_tools, "run_windows_command", fake_metaeditor(LOG_OK))
    assert cli.main(cli_args + ["robots", "compile", "demoguard"]) == cli.EXIT_OK
    assert "Compilado sem erros" in capsys.readouterr().out
    assert cli.main(cli_args + ["robots", "list", "demoguard"]) == cli.EXIT_OK
    out = capsys.readouterr().out
    assert "estágio: Compilado" in out and "Ainda não compilado" not in out
    assert not list((LIBRARY / "Experts").rglob("*.ex5"))          # a biblioteca do usuário não é alterada


def test_cli_install_copies_without_compiling_and_usage_errors(cli_args, mt5_home, capsys):
    assert cli.main(cli_args + ["robots", "install", "demoguard"]) == cli.EXIT_OK
    assert "copiado" in capsys.readouterr().out
    assert (mt5_home.home / "MQL5" / "Experts" / "Demo" / "DemoGuard.mq5").is_file()
    assert cli.main(cli_args + ["robots", "compile"]) == cli.EXIT_USAGE
    assert cli.main(cli_args + ["robots", "compile", "donchian"]) == cli.EXIT_USAGE
    (mt5_home.home / "MetaEditor64.exe").unlink()
    assert cli.main(cli_args + ["robots", "compile", "demoguard"]) == cli.EXIT_FAIL
    assert "metaeditor_path" in capsys.readouterr().err
