"""Isolamento do display sem herdar ou encerrar o servidor Wine da ponte."""
import configparser
import os
from functools import partial
from pathlib import Path
import subprocess

import pytest

from mt5_extracao.bootstrap import paths
from mt5_extracao.robots import session_wine, sessions, session_window
from tests.test_robot_controls import robot

# Prefixos/dosdevices existem só no executor Linux/Wine; Win32 tem testes próprios.
pytestmark = pytest.mark.skipif(os.name == "nt", reason="Runtime Wine exclusivo do Linux")


@pytest.fixture
def runtime(tmp_path):
    source = tmp_path / "main-wine"
    (source / "drive_c" / "windows" / "system32").mkdir(parents=True)
    (source / "drive_c" / "windows" / "system32" / "wine.dll").write_bytes(b"system")
    for name in ("system.reg", "user.reg", "userdef.reg", ".update-timestamp"):
        (source / name).write_bytes(b"original registry")
    common = source / "drive_c/users/owner/AppData/Roaming/MetaQuotes/Terminal/Common/Files"
    common.mkdir(parents=True)
    (common / "unrelated.txt").write_bytes(b"preserve other sessions")
    directory = tmp_path / "session"
    directory.mkdir()
    (directory / "terminal64.exe").write_bytes(b"portable")
    return session_wine.Runtime(source, directory)


def test_runtime_has_independent_writable_registry_and_system_and_scoped_common(runtime):
    charges = []
    runtime.prepare(partial(sessions._copy_file, check=lambda **kw: charges.append(kw)))
    (runtime.prefix / "user.reg").write_bytes(b"changed")
    (runtime.directory / "windows/system32/wine.dll").write_bytes(b"changed")
    assert (runtime.source / "user.reg").read_bytes() == b"original registry"
    assert (runtime.source / "drive_c/windows/system32/wine.dll").read_bytes() == b"system"
    assert paths.linux_to_wine_path(runtime.directory / "terminal64.exe", runtime.prefix) == r"C:\terminal64.exe"
    with pytest.raises(ValueError):
        paths.linux_to_wine_path(runtime.source / "drive_c/windows/wine.exe", runtime.prefix)
    assert charges and all(item["reserve_bytes"] > 0 for item in charges)
    # A remoção das cópias não segue o link de Common nem o C: circular.
    runtime.cleanup()
    assert list(runtime.directory.iterdir()) == [runtime.directory / "terminal64.exe"]
    common = runtime.source / "drive_c/users/owner/AppData/Roaming/MetaQuotes/Terminal/Common/Files"
    assert (common / "unrelated.txt").read_bytes() == b"preserve other sessions"


def test_cancel_during_system_copy_keeps_original_and_cleans_only_allocated_paths(runtime):
    def reject(**kwargs):
        raise sessions.Cancelled("budget/cancel")
    with pytest.raises(sessions.Cancelled):
        runtime.prepare(partial(sessions._copy_file, check=reject))
    runtime.cleanup()
    assert (runtime.source / "system.reg").exists()
    assert (runtime.directory / "terminal64.exe").read_bytes() == b"portable"


def test_existing_directory_is_never_claimed_or_removed(runtime):
    (runtime.directory / "windows").mkdir()
    (runtime.directory / "windows" / "foreign").write_bytes(b"foreign")
    with pytest.raises(FileExistsError):
        runtime.prepare(sessions._copy_file)
    runtime.cleanup()
    assert (runtime.directory / "windows" / "foreign").read_bytes() == b"foreign"


def test_server_shutdown_is_scoped_to_own_uuid_prefix_and_waited(runtime, monkeypatch):
    runtime.prepare(sessions._copy_file)
    called = []
    monkeypatch.setenv("WINEPREFIX", str(runtime.source))
    monkeypatch.setattr(session_wine.subprocess, "run", lambda args, **kw: called.append((args, kw)))
    runtime.stop()
    assert [args for args, _ in called] == [["wineserver", "-k"], ["wineserver", "-w"]]
    assert all(kw["env"]["WINEPREFIX"] == str(runtime.prefix) for _, kw in called)
    assert called[-1][1]["check"] is True
    assert session_wine.os.environ["WINEPREFIX"] == str(runtime.source)


@pytest.mark.parametrize("mode", ["play", "replay"])
def test_desktop_is_first_wine_command_and_uses_fresh_monitor(runtime, robot, monkeypatch, mode):
    runtime.prepare(sessions._copy_file)
    config = configparser.ConfigParser()
    config.read_dict({"MT5": {"wine_prefix": str(runtime.prefix)}})
    job = sessions.Session(None, config, robot, "EURUSD", "H1", mode=mode,
                           visible=True, root=runtime.directory.parent)
    job.directory = runtime.directory; job.child_config = config; job.visual_runtime = runtime
    seen, closed = [], []
    monkeypatch.setattr(session_window, "Placement", lambda *a: type("Placement", (), {
        "monitor": session_window.Monitor(1280, -50, 2560, 1440), "close": lambda _: closed.append(True)})())
    def launch(args, **kwargs):
        # O modo interno já deve existir quando o PRIMEIRO Wine nasce,
        # inclusive no compilador; acertar só a janela externa é insuficiente.
        registry = (runtime.prefix / "user.reg").read_text()
        assert r"[Software\\Wine\\Explorer\\Desktops]" in registry
        assert '"Default"="2560x1440"' in registry
        seen.append((args, kwargs))
        return type("Finished", (), {"poll": lambda _: 0, "returncode": 0})()
    monkeypatch.setattr(sessions.subprocess, "Popen", launch)
    monkeypatch.setattr(job, "_write_lease", lambda: None)
    monkeypatch.setattr(sessions.display, "child_env", lambda c, e: e)
    command = r'"C:\terminal64.exe" /portable /config:"C:\test.ini"'
    job._run_command(command, job.directory, runtime.prefix, 1, compiling=True)
    original = (runtime.prefix / "user.reg").read_bytes()
    job._run_command(command, job.directory, runtime.prefix, 1)
    assert (runtime.prefix / "user.reg").read_bytes() == original
    assert seen[0][0] == ["wine", "explorer", f"/desktop=MT5X_{job.id},2560x1440", "cmd", "/c", r"C:\launch.bat"]
    assert command in (job.directory / "launch.bat").read_text(encoding="cp1252")
    assert closed == [True, True]
    runtime.cleanup()


@pytest.mark.parametrize("size", [(1280, 800), (3456, 1458), (800, 1280)])
def test_private_desktop_modes_replace_only_default_preserving_owner_and_other_keys(runtime, size):
    source = (runtime.source / "user.reg")
    section = r"[Software\\Wine\\Explorer\\Desktops]"
    text = ('WINE REGISTRY Version 2\n\n' + section + ' 1792771000\n'
            '"Default"="640x480"\n"Other"="1920x1080"\n\n'
            '[Unrelated]\n"Default"="preserve"\n')
    source.write_text(text)
    runtime.prepare(sessions._copy_file)
    runtime.configure_desktop(*size)
    result = (runtime.prefix / "user.reg").read_text()
    assert result.count(section) == 1
    assert f'"Default"="{size[0]}x{size[1]}"' in result
    assert '"Default"="640x480"' not in result
    assert '"Other"="1920x1080"' in result and '[Unrelated]\n"Default"="preserve"' in result
    assert source.read_text() == text
    # Falha se dock/resolução mudar enquanto o servidor privado está vivo.
    with pytest.raises(RuntimeError, match="tamanho do monitor mudou"):
        runtime.configure_desktop(size[0] + 1, size[1])
    assert (runtime.prefix / "user.reg").read_text() == result
    runtime.cleanup()


def test_desktop_mode_refuses_unclaimed_runtime_and_invalid_dimensions(runtime):
    with pytest.raises(RuntimeError, match="privada preparada"):
        runtime.configure_desktop(1280, 800)
    for size in [(True, 800), (1280, 0), (-1, 800), (1280., 800), (1280, "800")]:
        with pytest.raises(ValueError):
            runtime.configure_desktop(*size)


def test_placement_closes_connection_when_monitor_was_disconnected():
    closed = []
    backend = type("Backend", (), {"monitors": lambda _: [], "close": lambda _: closed.append(True)})()
    with pytest.raises(OSError):
        session_window.Placement((2500, 500), backend)
    assert closed == [True]


def test_removed_dock_selects_remaining_handheld():
    handheld = session_window.Monitor(0, 0, 1280, 800)
    assert session_window.select_monitor([handheld], (2500, 500)) == handheld


@pytest.mark.parametrize("stop_fails", [False, True, "not_started"])
def test_failed_session_preserves_runtime_until_its_server_is_confirmed_stopped(runtime, robot, monkeypatch, stop_fails):
    import contextlib
    import json
    from mt5_extracao.robots import mt5_tools
    config = configparser.ConfigParser()
    config.read_dict({"MT5": {"wine_prefix": str(runtime.source)}})
    job = sessions.Session(None, config, robot, "EURUSD", "H1", root=runtime.directory.parent)
    job.directory = runtime.directory; job.report = runtime.directory / "relatorio.json"
    runtime.prepare(sessions._copy_file)
    job.visual_runtime = runtime
    job.child_config = configparser.ConfigParser()
    job.child_config.read_dict({"MT5": {"wine_prefix": str(runtime.prefix)}})
    monkeypatch.setattr(mt5_tools, "tester_lease", lambda *a: contextlib.nullcontext())
    monkeypatch.setattr(mt5_tools, "release_native_context", lambda _: True)
    monkeypatch.setattr(job, "_stop_owned", lambda: None)
    def stop():
        if stop_fails:
            raise subprocess.TimeoutExpired("own-server-wait", 15)
    if stop_fails == "not_started":
        # Cancelamento/preparação falha antes de Popen: -k1 não é servidor vivo.
        def no_server(args, **kwargs):
            code = 1 if args[-1] == "-k" else 0
            if kwargs.get("check") and code:
                raise subprocess.CalledProcessError(code, args)
            return subprocess.CompletedProcess(args, code)
        monkeypatch.setattr(session_wine.subprocess, "run", no_server)
        stop_fails = False
    else:
        monkeypatch.setattr(runtime, "stop", stop)
    def fail(**kwargs):
        raise RuntimeError("preparation failed")
    monkeypatch.setattr(job, "_prepare_expert", fail)
    job.run()
    payload = json.loads(job.report.read_text())
    assert payload["state"] == "failed" and payload["resources_released"] is not stop_fails
    assert runtime.prefix.exists() is stop_fails
    assert payload["wine_prefix"] == str(runtime.prefix if stop_fails else runtime.source)
    assert (runtime.directory / "terminal64.exe").read_bytes() == b"portable"
    if stop_fails:
        runtime.cleanup()


def test_launcher_ownership_requires_both_uuid_and_exact_prefix(runtime, monkeypatch):
    import psutil
    from mt5_extracao.robots import mt5_tools
    runtime.prepare(sessions._copy_file)
    config = configparser.ConfigParser()
    config.read_dict({"MT5": {"wine_prefix": str(runtime.prefix),
                              "path": str(runtime.directory / "terminal64.exe")}})
    class Process:
        def __init__(self, pid, argument, prefix):
            self.pid, self.argument, self.prefix = pid, argument, prefix
        def cmdline(self):return [r"C:\windows\explorer.exe", self.argument]
        def environ(self):return {"WINEPREFIX": str(self.prefix)}
    own = Process(10, "/desktop=MT5X_session,1920x1080", runtime.prefix)
    wrong_uuid = Process(11, "/desktop=MT5X_other,1920x1080", runtime.prefix)
    wrong_prefix = Process(12, "/desktop=MT5X_session,1920x1080", runtime.source)
    monkeypatch.setattr(psutil, "process_iter", lambda: [own, wrong_uuid, wrong_prefix])
    assert sessions.owned_processes(config, launchers=True) == [own]
    runtime.cleanup()


def test_unclaimed_existing_prefix_never_receives_a_server_signal(runtime, monkeypatch):
    runtime.prefix.mkdir()
    marker = runtime.prefix / "foreign.reg"
    marker.write_bytes(b"foreign")
    with pytest.raises(FileExistsError):
        runtime.prepare(sessions._copy_file)
    def forbidden(*args, **kwargs):
        raise AssertionError("Unclaimed prefix must not receive a signal")
    monkeypatch.setattr(session_wine.subprocess, "run", forbidden)
    runtime.stop(); runtime.cleanup()
    assert marker.read_bytes() == b"foreign"
