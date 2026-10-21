"""T10.2: biblioteca de segurança (mt5x-safety) instalada na versão que o robô declara, sem MT5."""
import configparser
import hashlib
import json
from pathlib import Path

import pytest

from mt5_extracao import cli
from mt5_extracao.bootstrap import paths
from mt5_extracao.robots import battery, catalog, mt5_tools, safety_lib

EA = ('#property version "1.00"\n#include <MT5X/Safety/Guard.mqh>\n'
      "input long InpMagicNumber = 8100; // Magic\nint OnInit(){return(0);}\nvoid OnTick(){ Entrar(); }\n")


def make_lib(folder, version="1.0.0", files=("Types.mqh", "Guard.mqh")):
    (folder / "Include" / "MT5X" / "Safety").mkdir(parents=True, exist_ok=True)
    for name in files:
        (folder / "Include" / "MT5X" / "Safety" / name).write_text(f"// {name} {version}\n", encoding="utf-8")
    (folder / "safety.json").write_text(json.dumps({"name": "mt5x-safety", "version": version,
                                                    "include_dir": "MT5X/Safety", "files": list(files)}))
    return folder


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setattr(paths, "is_windows", lambda: False)
    prefix = tmp_path / "wine"
    mt5 = prefix / "drive_c" / "Program Files" / "MetaTrader 5"
    (mt5 / "MQL5" / "Experts").mkdir(parents=True)
    for exe in ("terminal64.exe", "MetaEditor64.exe"):
        (mt5 / exe).write_bytes(b"")
    library = tmp_path / "robots"
    (library / "Experts").mkdir(parents=True)
    ea = library / "Experts" / "Usa.mq5"
    ea.write_text(EA, encoding="utf-8")
    lib = make_lib(tmp_path / "mt5x-safety")
    config = configparser.ConfigParser()
    config.read_dict({"MT5": {"path": str(mt5), "wine_prefix": str(prefix)},
                      "DATABASE": {"type": "sqlite", "path": str(tmp_path / "db.sqlite")},
                      "ROBOTS": {"library_dirs": str(library), "safety_lib_dir": str(lib)}})

    def robot(version="1.0.0"):
        manifest = ea.with_suffix(".robot.json")
        manifest.unlink(missing_ok=True)
        if version:
            manifest.write_text(json.dumps({"contract": 1, "safety_lib": version, "inputs": {}}))
        return catalog.robot_from_file(ea)

    def args():
        cfg = tmp_path / "config.ini"
        with open(cfg, "w", encoding="utf-8") as f:
            config.write(f)
        return ["--config", str(cfg)]
    return type("Env", (), {"mt5": mt5, "lib": lib, "config": config, "robot": staticmethod(robot),
                            "args": staticmethod(args), "tmp": tmp_path})


def test_install_copies_the_listed_files_and_is_idempotent(env):
    assert safety_lib.installed(env.mt5) is None and safety_lib.installed_version(env.mt5) == ""
    copied = safety_lib.install(env.lib, env.mt5)
    include = env.mt5 / "MQL5" / "Include"
    assert {p.relative_to(include).as_posix() for p in copied} == {"MT5X/Safety/Types.mqh", "MT5X/Safety/Guard.mqh"}
    assert safety_lib.installed_version(env.mt5) == "1.0.0" and safety_lib.install(env.lib, env.mt5) == []


def test_another_version_is_never_replaced_silently(env):
    safety_lib.install(env.lib, env.mt5)
    newer = make_lib(env.tmp / "newer", "1.1.0")
    with pytest.raises(safety_lib.SafetyLibError, match="--upgrade-safety"):
        safety_lib.install(newer, env.mt5)
    assert safety_lib.installed_version(env.mt5) == "1.0.0"
    assert "1.0.0" in (env.mt5 / "MQL5" / "Include" / "MT5X" / "Safety" / "Guard.mqh").read_text()
    safety_lib.install(newer, env.mt5, upgrade=True)
    assert safety_lib.installed_version(env.mt5) == "1.1.0"


def test_ensure_explains_each_way_it_can_fail(env):
    assert safety_lib.ensure(env.config, env.robot(None), env.mt5) == ""          # robô não declara: nada a fazer
    assert safety_lib.installed(env.mt5) is None
    with pytest.raises(safety_lib.SafetyLibError, match="git checkout v2.0.0"):
        safety_lib.ensure(env.config, env.robot("2.0.0"), env.mt5)
    env.config["ROBOTS"]["safety_lib_dir"] = ""
    with pytest.raises(safety_lib.SafetyLibError, match="safety_lib_dir está vazio"):
        safety_lib.ensure(env.config, env.robot(), env.mt5)
    env.config["ROBOTS"]["safety_lib_dir"] = str(env.tmp / "nada")
    with pytest.raises(safety_lib.SafetyLibError, match="clone de https://github.com"):
        safety_lib.ensure(env.config, env.robot(), env.mt5)
    broken = make_lib(env.tmp / "broken")
    (broken / "Include" / "MT5X" / "Safety" / "Guard.mqh").unlink()
    with pytest.raises(safety_lib.SafetyLibError, match="incompleta"):
        safety_lib.install(broken, env.mt5)
    (broken / "safety.json").write_text('{"version": "1.0.0"}')
    with pytest.raises(safety_lib.SafetyLibError, match="incompleto"):
        safety_lib.read_manifest(broken)


def test_install_robot_brings_the_library_first(env):
    items = mt5_tools.install_robot(env.config, env.robot())
    assert items[0].target == env.mt5 / "MQL5" / "Experts" / "Usa.mq5"
    assert (env.mt5 / "MQL5" / "Include" / "MT5X" / "Safety" / "Guard.mqh").is_file()
    assert safety_lib.installed_version(env.mt5) == "1.0.0"


def test_battery_checks_the_installed_version_and_the_order_path(env):
    robot = env.robot()
    items = {i.id: i for i in battery.static_items(robot, env.config, [robot])}
    assert items["safety_lib"].status == battery.FAIL and "mt5x robots install usa" in items["safety_lib"].fix
    assert items["orders"].status == battery.OK and "biblioteca de segurança" in items["orders"].detail
    mt5_tools.install_robot(env.config, robot)
    items = {i.id: i for i in battery.static_items(robot, env.config, [robot])}
    assert items["safety_lib"].status == battery.OK and items["safety_lib"].detail == "mt5x-safety 1.0.0"
    assert "safety_lib" not in {i.id for i in battery.static_items(env.robot(None), env.config)}


def test_cli_install_reports_the_library_and_its_errors(env, capsys):
    env.robot()
    assert cli.main(env.args() + ["robots", "install", "usa"]) == cli.EXIT_OK
    assert "biblioteca de segurança 1.0.0 instalada" in capsys.readouterr().out
    make_lib(env.lib, "1.1.0")
    env.robot("1.1.0")
    assert cli.main(env.args() + ["robots", "install", "usa"]) == cli.EXIT_FAIL
    assert "--upgrade-safety" in capsys.readouterr().err
    assert cli.main(env.args() + ["robots", "install", "usa", "--upgrade-safety"]) == cli.EXIT_OK
    assert safety_lib.installed_version(env.mt5) == "1.1.0"


def _add_hashes(folder):
    path = folder / "safety.json"
    manifest = json.loads(path.read_text())
    base = folder / "Include" / manifest["include_dir"]
    manifest["sha256"] = {name: hashlib.sha256((base / name).read_bytes()).hexdigest()
                          for name in manifest["files"]}
    path.write_text(json.dumps(manifest))
    return manifest


@pytest.mark.parametrize("windows", [False, True])
def test_hashed_distribution_is_verified_and_idempotent(env, monkeypatch, windows):
    monkeypatch.setattr(paths, "is_windows", lambda: windows)
    manifest = _add_hashes(env.lib)
    assert len(safety_lib.install(env.lib, env.mt5)) == 2
    assert safety_lib.installed(env.mt5)["sha256"] == manifest["sha256"]
    assert safety_lib.install(env.lib, env.mt5) == []


@pytest.mark.parametrize("failure", ["missing_last", "wrong_hash", "incomplete_hashes"])
def test_invalid_distribution_preserves_every_installed_file_and_marker(env, failure):
    safety_lib.install(env.lib, env.mt5)
    include = env.mt5 / "MQL5" / "Include"
    before = {str(path.relative_to(include)): path.read_bytes() for path in include.rglob("*") if path.is_file()}
    newer = make_lib(env.tmp / "newer", "1.8.0")
    manifest = _add_hashes(newer)
    last = newer / "Include" / "MT5X" / "Safety" / "Guard.mqh"
    if failure == "missing_last":
        last.unlink()
    elif failure == "wrong_hash":
        last.write_text("// fonte de outra revisão\n")
    else:
        del manifest["sha256"]["Guard.mqh"]
        (newer / "safety.json").write_text(json.dumps(manifest))
    with pytest.raises(safety_lib.SafetyLibError, match="incompleta|hash"):
        safety_lib.install(newer, env.mt5, upgrade=True)
    after = {str(path.relative_to(include)): path.read_bytes() for path in include.rglob("*") if path.is_file()}
    assert after == before
