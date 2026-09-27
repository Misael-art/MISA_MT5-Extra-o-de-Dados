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
