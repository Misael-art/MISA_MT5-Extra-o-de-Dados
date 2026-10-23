"""Monitores e janelas por posse; sem terminal ou display real obrigatório."""
from types import SimpleNamespace
import configparser
from pathlib import Path

import pytest

from mt5_extracao.robots import session_window as window, sessions
from tests.test_robot_controls import robot
from tests.test_manual_panel_play import manual


@pytest.mark.parametrize("point,expected", [
    ((0, 0), 0), ((1279, 799), 0), ((1280, 0), 1), ((4500, 1000), 1),
    ((-400, 100), 2), ((100, -500), 3), ((9000, 1000), 1),
])
def test_select_launch_monitor_including_rotated_negative_and_mixed_sizes(point, expected):
    monitors = [window.Monitor(0, 0, 1280, 800), window.Monitor(1280, 0, 3456, 1458),
                window.Monitor(-800, 0, 800, 1280), window.Monitor(0, -1080, 1920, 1080)]
    assert window.select_monitor(monitors, point) == monitors[expected]


def test_no_monitor_never_invents_a_small_fixed_resolution():
    with pytest.raises(OSError):
        window.select_monitor([], (10, 10))


@pytest.mark.parametrize("windows", [False, True])
def test_placement_observes_owned_pid_filter_and_only_adjusts_each_window_once(windows):
    monitor = window.Monitor(1280, 0, 2560, 1080)
    changes, filters, closed = [], [], []
    available = [(10, 123), (11, 123), (99, 456)]
    def windows_for(pids):
        filters.append(pids)
        return [entry for entry in available if entry[1] in pids]
    backend = SimpleNamespace(pointer=lambda: (1400, 500), windows=windows_for,
                              place=lambda *a: changes.append(a), close=lambda: closed.append(True))
    if windows:
        backend.monitor = lambda point: monitor
    else:
        backend.monitors = lambda: [window.Monitor(0, 0, 1280, 800), monitor]
    placement = window.Placement(backend=backend)
    placement.poll([123]); placement.poll([123])
    # O visualizador pode nascer depois do terminal principal.
    available.append((12, 123)); placement.poll([123]); placement.close()
    assert changes == [(10, monitor), (11, monitor), (12, monitor)]
    assert all(pids == {123} for pids in filters) and closed == [True]


def test_x11_repositions_then_maximizes_without_changing_global_resolution():
    backend = object.__new__(window._X11)
    sent = []
    backend.atom = lambda name: name
    backend.message = lambda *args: sent.append(args)
    monitor = window.Monitor(-1920, 30, 1920, 1050)
    backend.place(7, monitor)
    assert [row[1] for row in sent] == ["_NET_WM_STATE", "_NET_MOVERESIZE_WINDOW", "_NET_WM_STATE"]
    assert sent[1][2][1:] == [-1920, 30, 1920, 1050]
    assert sent[0][2][0] == 0 and sent[-1][2][0] == 1


@pytest.mark.parametrize("mode,requested,expected", [
    ("play", None, False), ("play", False, False), ("play", True, True),
    ("replay", None, True), ("replay", False, True), ("replay", True, True),
    ("backtest", None, False), ("backtest", True, False),
])
def test_automatic_visibility_default_and_explicit_choice(robot, mode, requested, expected):
    assert sessions.chart_visible(robot, mode, requested) is expected


def test_reviewed_manual_panel_defaults_visible_and_can_be_explicitly_hidden(manual):
    assert sessions.chart_visible(manual, "play")
    assert not sessions.chart_visible(manual, "play", False)
    assert sessions.chart_visible(manual, "replay", False)


@pytest.mark.parametrize("invalid", (1, "false", [], {}))
def test_visibility_does_not_accept_truthy_strings_or_numeric_values(robot, invalid):
    with pytest.raises(ValueError, match="booleana"):
        sessions.chart_visible(robot, "play", invalid)


def test_window_ini_preserves_native_docking_and_user_config(robot, tmp_path):
    config = configparser.ConfigParser()
    config.read_dict({"MT5": {"hidden": "true"}})
    session = sessions.Session(None, config, robot, "EURUSD", "H1", mode="replay", root=tmp_path)
    target = session.directory / "Tester" / "config" / "metatester.ini"
    target.parent.mkdir(parents=True)
    target.write_text("[Visualization]\nchartmode=1\n[Docking]\nDividers-0=feed1234\n", encoding="utf-16")
    session._window_ini(3456, 1458)
    native = configparser.ConfigParser()
    native.read(target, encoding="utf-16")
    assert native.getint("Window", "Right") == 3456 and native.getint("Window", "Bottom") == 1458
    assert native.get("Docking", "Dividers-0") == "feed1234" and native.getint("Visualization", "chartmode") == 1
    assert config.getboolean("MT5", "hidden")  # escolha da sessão não reconfigura a ponte
