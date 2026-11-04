"""Abertura por controlador, sem instalar navegador nem executar comandos livres."""
import configparser
from types import SimpleNamespace
import webbrowser

import pytest

from mt5_extracao.dashboard import opener


@pytest.mark.parametrize("windows", [False, True])
def test_default_opens_after_ready(monkeypatch, windows):
    calls = []
    monkeypatch.setattr(opener.paths, "is_windows", lambda: windows)
    controller = SimpleNamespace(open=lambda url, **kw: calls.append((url, kw)) or True)
    monkeypatch.setattr(opener.webbrowser, "get", lambda: controller)
    srv = SimpleNamespace(start=lambda: calls.append("ready"), url="http://127.0.0.1:5055/?k=qa")
    result = opener.open_dashboard(srv)
    assert result.opened and result.url == srv.url
    assert calls[0] == "ready" and calls[1][0] == srv.url


@pytest.mark.parametrize("failure", [False, webbrowser.Error("ausente"), OSError("ausente")])
def test_missing_browser_preserves_url(monkeypatch, failure):
    def attempt(*a, **kw):
        if isinstance(failure, Exception):
            raise failure
        return failure
    monkeypatch.setattr(opener.webbrowser, "get", lambda: SimpleNamespace(open=attempt))
    srv = SimpleNamespace(start=lambda: None, url="http://127.0.0.1:5055/?k=qa")
    result = opener.open_dashboard(srv)
    assert not result.opened and result.url == srv.url and "Copie a URL" in result.message


def test_explicit_unicode_path_and_spaces_is_one_executable(tmp_path, monkeypatch):
    executable = tmp_path / "Navegador Português" / "browser executable"
    executable.parent.mkdir()
    executable.touch()
    calls = []
    monkeypatch.setattr(opener.webbrowser, "BackgroundBrowser", lambda command: calls.append(command) or object())
    opener._controller(str(executable))
    assert calls == [[str(executable), "%s"]]


def test_unknown_browser_does_not_run_or_fallback(monkeypatch):
    monkeypatch.setattr(opener.webbrowser, "get", lambda *a: pytest.fail("não usar outro navegador"))
    cfg = configparser.ConfigParser()
    cfg.read_dict({"DASHBOARD": {"browser": "arbitrary --command"}})
    result = opener.open_dashboard(SimpleNamespace(start=lambda: None, url="local"), cfg)
    assert not result.opened


def test_windows_edge_searches_existing_program_files(tmp_path, monkeypatch):
    executable = tmp_path / "Microsoft/Edge/Application/msedge.exe"
    executable.parent.mkdir(parents=True)
    executable.touch()
    monkeypatch.setattr(opener.paths, "is_windows", lambda: True)
    monkeypatch.setattr(opener.shutil, "which", lambda _: None)
    monkeypatch.setenv("PROGRAMFILES", str(tmp_path))
    controller = opener._controller("edge")
    assert controller.name == str(executable)
