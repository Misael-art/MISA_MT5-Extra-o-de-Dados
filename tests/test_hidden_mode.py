"""T11.3: MT5 oculto (tela virtual no Linux, janela minimizada no Windows), sem iniciar Xvfb nem Wine."""
import configparser
import subprocess
from types import SimpleNamespace

import pytest

from mt5_extracao import cli
from mt5_extracao.bootstrap import display, doctor, paths
from mt5_extracao.robots import mt5_tools


def config(hidden="true", **extra):
    c = configparser.ConfigParser()
    c.read_dict({"MT5": {"hidden": hidden, **extra}})
    return c


@pytest.fixture
def linux(monkeypatch):
    monkeypatch.setattr(paths, "is_windows", lambda: False)
    monkeypatch.delenv(display.ENV_SWITCH, raising=False)


def test_enabled_by_config_and_overridden_by_the_environment(linux):
    assert not display.enabled(None) and not display.enabled(config("false")) and display.enabled(config("true"))
    assert display.enabled(config("false"), {display.ENV_SWITCH: "1"})          # só nesta execução
    assert not display.enabled(config("true"), {display.ENV_SWITCH: "0"})
    assert display.enabled(config("true"), {display.ENV_SWITCH: "talvez"})      # valor estranho: vale a configuração
    assert display.display_name(config()) == ":99" and display.display_name(config(hidden_display="42")) == ":42"


def test_child_env_points_wine_at_the_virtual_display_only_when_hidden(linux, monkeypatch):
    started = []
    monkeypatch.setattr(display, "ensure", lambda name, **k: started.append(name) or name)
    real = {"DISPLAY": ":0", "WAYLAND_DISPLAY": "wayland-0", "HOME": "/h"}
    env = display.child_env(config(hidden_display=":77"), real)
    assert env == {"DISPLAY": ":77", "HOME": "/h"} and started == [":77"]       # nada vai para a tela real
    assert display.child_env(config("false"), real) == real and started == [":77"]
    assert real["DISPLAY"] == ":0"                                              # o ambiente original não muda
    monkeypatch.setattr(paths, "is_windows", lambda: True)
    assert display.child_env(config(), real) == real                           # no Windows não há tela virtual


def test_ensure_starts_xvfb_once_and_explains_failures(linux, monkeypatch):
    state = {"running": False, "commands": []}
    monkeypatch.setattr(display, "is_running", lambda name: state["running"])
    monkeypatch.setattr(display.shutil, "which", lambda name: "/usr/bin/Xvfb")

    def popen(command, **kwargs):
        state["commands"].append(command)
        state["running"] = True
        assert kwargs["start_new_session"]                                      # sobrevive ao comando que o iniciou
    assert display.ensure(":88", popen=popen) == ":88"
    assert state["commands"] == [["Xvfb", ":88", "-screen", "0", display.SCREEN, "-nolisten", "tcp"]]
    assert display.ensure(":88", popen=popen) == ":88" and len(state["commands"]) == 1    # já ativa: não inicia outra
    state["running"] = False
    with pytest.raises(RuntimeError, match="não ficou pronta"):
        display.ensure(":88", timeout=0.2, popen=lambda *a, **k: None)
    monkeypatch.setattr(display.shutil, "which", lambda name: None)
    with pytest.raises(RuntimeError, match="precisa do Xvfb"):
        display.ensure(":88")


def test_wine_commands_inherit_the_hidden_display(linux, monkeypatch, tmp_path):
    monkeypatch.setattr(display, "ensure", lambda name, **k: name)
    monkeypatch.setenv("DISPLAY", ":0")
    monkeypatch.setenv("WAYLAND_DISPLAY", "wayland-0")
    monkeypatch.setattr(mt5_tools.shutil, "which", lambda name: "/usr/bin/wine")
    seen = []
    monkeypatch.setattr(mt5_tools.subprocess, "run",
                        lambda argv, **kw: seen.append(kw.get("env")) or SimpleNamespace(returncode=0))
    prefix = tmp_path / "wine"
    (prefix / "drive_c").mkdir(parents=True)
    try:
        mt5_tools.use_config(config())
        mt5_tools.run_windows_command('"C:\\x.exe"', tmp_path, prefix)
        mt5_tools._bridge("status")
        mt5_tools.use_config(config("false"))
        mt5_tools.run_windows_command('"C:\\x.exe"', tmp_path, prefix)
        mt5_tools._bridge("status")
    finally:
        mt5_tools.use_config(None)
    hidden_run, hidden_bridge, visible_run, visible_bridge = seen
    assert hidden_run["DISPLAY"] == ":99" and "WAYLAND_DISPLAY" not in hidden_run
    assert hidden_run["WINEPREFIX"] == str(prefix) and hidden_bridge[display.ENV_SWITCH] == "1"
    assert visible_run["DISPLAY"] == ":0" and visible_run["WAYLAND_DISPLAY"] == "wayland-0"
    assert display.ENV_SWITCH not in visible_bridge


def test_windows_starts_the_terminal_minimised_without_focus(monkeypatch):
    monkeypatch.setattr(paths, "is_windows", lambda: True)
    monkeypatch.setattr(subprocess, "STARTUPINFO", lambda: SimpleNamespace(dwFlags=0, wShowWindow=0), raising=False)
    monkeypatch.setattr(subprocess, "STARTF_USESHOWWINDOW", 1, raising=False)
    info = display.windows_startup(True)
    assert info.dwFlags == 1 and info.wShowWindow == 7                       # SW_SHOWMINNOACTIVE
    assert display.windows_startup(False) is None
    monkeypatch.setattr(paths, "is_windows", lambda: False)
    assert display.windows_startup(True) is None


def test_doctor_and_status_say_when_the_terminal_is_out_of_sight(linux, monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(display, "is_running", lambda name: True)
    monkeypatch.setattr("shutil.which", lambda name: f"/usr/bin/{name}")        # o Xvfb não existe em toda máquina
    assert "desligado" in doctor.check_hidden_mode(config("false")).detail
    check = doctor.check_hidden_mode(config())
    assert check.status == doctor.OK and "mt5x mt5 show" in check.detail

    cfg = tmp_path / "config.ini"
    cfg.write_text(f"[MT5]\nhidden = true\n[DATABASE]\ntype = sqlite\npath = {tmp_path / 'db.sqlite'}\n", encoding="utf-8")
    monkeypatch.setattr(mt5_tools, "terminal_processes", lambda config=None: [(object(), mt5_tools.OURS)])
    assert cli.main(["--config", str(cfg), "mt5", "status"]) == cli.EXIT_OK
    out = capsys.readouterr().out
    assert "ABERTO" in out and "Modo oculto: LIGADO" in out and "Um robô pode estar operando nele" in out
    monkeypatch.setattr(display, "screenshot", lambda name, path: path)
    assert cli.main(["--config", str(cfg), "mt5", "show", "--out", str(tmp_path / "t.png")]) == cli.EXIT_OK
    cfg.write_text(f"[MT5]\nhidden = false\n[DATABASE]\ntype = sqlite\npath = {tmp_path / 'db.sqlite'}\n", encoding="utf-8")
    assert cli.main(["--config", str(cfg), "mt5", "show"]) == cli.EXIT_USAGE
    mt5_tools.use_config(None)

    import shutil
    monkeypatch.setattr(shutil, "which", lambda name: None)
    assert doctor.check_hidden_mode(config()).status == doctor.FAIL            # ligado sem Xvfb instalado
