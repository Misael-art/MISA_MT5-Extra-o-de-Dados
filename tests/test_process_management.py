"""T2.4: gestão do processo terminal64.exe sem tasklist/taskkill (psutil) e neutra no Linux."""
import os
import re
from pathlib import Path

import pytest

import mt5_extracao.mt5_connector as connector_module

REPO = Path(__file__).resolve().parents[1]


class FakeProc:
    def __init__(self, name, stubborn=False):
        self.info = {"name": name}
        self.stubborn = stubborn
        self.terminated = self.killed = False

    def terminate(self):
        self.terminated = True

    def kill(self):
        self.killed = True


class FakePsutil:
    NoSuchProcess = AccessDenied = ZombieProcess = Exception

    def __init__(self, procs):
        self.procs = procs

    def process_iter(self, attrs=None):
        return list(self.procs)

    def wait_procs(self, procs, timeout=None):
        alive = [p for p in procs if p.stubborn and not p.killed]
        gone = [p for p in procs if p not in alive]
        return gone, alive


def test_no_tasklist_or_taskkill_in_connector():
    src = (REPO / "mt5_extracao" / "mt5_connector.py").read_text(encoding="utf-8")
    assert not re.search(r"\[\s*['\"](tasklist|taskkill)['\"]", src)


def test_kill_terminal_processes_terminates_only_terminal(connector, monkeypatch):
    term, other = FakeProc("terminal64.exe"), FakeProc("python.exe")
    monkeypatch.setattr(connector_module, "psutil", FakePsutil([term, other]))
    assert connector._kill_terminal_processes(timeout=0) is True
    assert term.terminated and not other.terminated


def test_kill_terminal_processes_escalates_to_kill(connector, monkeypatch):
    stubborn = FakeProc("Terminal64.EXE", stubborn=True)
    monkeypatch.setattr(connector_module, "psutil", FakePsutil([stubborn]))
    assert connector._kill_terminal_processes(timeout=0) is True
    assert stubborn.terminated and stubborn.killed


def test_without_psutil_is_not_running(connector, monkeypatch):
    monkeypatch.setattr(connector_module, "psutil", None)
    assert connector._is_mt5_running() is False
    assert connector._kill_terminal_processes() is False


@pytest.mark.skipif(os.name == "nt", reason="comportamento específico do Linux")
def test_linux_never_launches_exe(connector, monkeypatch):
    monkeypatch.setattr(connector, "_is_mt5_running", lambda: False)
    connector.mt5_path = "/tmp"
    assert connector._start_mt5_if_not_running() is False
    assert connector.launch_mt5_as_admin(wait_for_user=False) is False
    assert connector.is_admin() == (os.geteuid() == 0)


# --- Wine: o terminal aparece com outro nome de processo ------------------------------

WINE_CMDLINE = ["C:\\Program Files\\MetaTrader 5\\terminal64.exe", ""]


class WineProc(FakeProc):
    """Como o psutil mostra o terminal no Wine 11.17: nome 'main', executável wine-preloader."""

    def __init__(self, cmdline, name="main"):
        super().__init__(name)
        self.info.update(cmdline=cmdline, exe="/usr/lib/wine/x86_64-unix/wine-preloader")


def test_is_terminal_process_by_name_or_command_line():
    from mt5_extracao.bootstrap import paths
    assert paths.is_terminal_process("terminal64.exe") and paths.is_terminal_process("Terminal64.EXE", [])
    assert paths.is_terminal_process("main", WINE_CMDLINE)
    assert paths.is_terminal_process("main", ["/opt/mt5/terminal64.exe", "/portable"])
    assert not paths.is_terminal_process("main", ["C:\\Python311\\python.exe", "Z:\\x\\terminal64.exe"])
    assert not paths.is_terminal_process("main", ["C:\\Program Files\\MetaTrader 5\\MetaEditor64.exe"])
    assert not paths.is_terminal_process(None, None) and not paths.is_terminal_process("python.exe", [])


def test_kill_terminal_processes_finds_the_wine_terminal(connector, monkeypatch):
    term, bridge = WineProc(WINE_CMDLINE), WineProc(["C:\\Python311\\python.exe", "mt5_bridge_server.py"])
    monkeypatch.setattr(connector_module, "psutil", FakePsutil([term, bridge]))
    assert connector._kill_terminal_processes(timeout=0) is True
    assert term.terminated and not bridge.terminated


def test_robot_tools_see_and_close_the_wine_terminal(monkeypatch):
    import psutil
    from mt5_extracao.robots import mt5_tools
    term, other = WineProc(WINE_CMDLINE), WineProc(["C:\\Python311\\python.exe"])
    state = {"procs": [term, other]}
    monkeypatch.setattr(psutil, "process_iter", lambda attrs=None: list(state["procs"]))
    monkeypatch.setattr(psutil, "wait_procs", lambda procs, timeout=None: (list(procs), []))
    assert mt5_tools.terminal_running()
    state["procs"] = [other]
    assert not mt5_tools.terminal_running()
    state["procs"] = [term, other]
    term.terminate = lambda: state.update(procs=[other])
    assert mt5_tools.close_terminal(timeout=0) and state["procs"] == [other]
