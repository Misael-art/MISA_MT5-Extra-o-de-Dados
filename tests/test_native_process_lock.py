"""A primitiva de posse segue o host, independentemente da simulação de caminhos."""
import sys
from types import SimpleNamespace

import pytest

from mt5_extracao.bootstrap import paths
from mt5_extracao.robots import mt5_tools


@pytest.mark.parametrize("host,path_windows,backend", [("nt", False, "msvcrt"), ("posix", True, "fcntl")])
@pytest.mark.parametrize("release", [False, True])
def test_native_lease_uses_host_api_even_when_path_platform_is_simulated(monkeypatch, host, path_windows, backend, release):
    events = []
    handle = SimpleNamespace(seek=lambda offset: events.append(("seek", offset)), fileno=lambda: 7)
    monkeypatch.setattr(mt5_tools, "os", SimpleNamespace(name=host))
    monkeypatch.setattr(paths, "is_windows", lambda: path_windows)
    monkeypatch.setitem(sys.modules, "msvcrt", SimpleNamespace(
        LK_UNLCK=0, LK_NBLCK=2, locking=lambda *args: events.append(("msvcrt", *args))))
    monkeypatch.setitem(sys.modules, "fcntl", SimpleNamespace(
        LOCK_UN=8, LOCK_EX=2, LOCK_NB=4, flock=lambda *args: events.append(("fcntl", *args))))
    mt5_tools._lock_tester_file(handle, release=release)
    assert events[0] == ("seek", 0)
    assert events[1][0] == backend
    assert events[1][1:] == ((7, 0 if release else 2, 1) if backend == "msvcrt" else
                              (7, 8 if release else 6))
