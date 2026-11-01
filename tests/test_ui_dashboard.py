"""DASH-13: Dashboard aberto pelas abas Ativos e Robôs, fora da thread da UI e sem misturar seleções."""
import socket
import time
import urllib.request

import pytest

from tests.test_ui_main_window import ui  # noqa: F401  (fixture da janela principal)


@pytest.fixture
def dashboard_ui(ui, monkeypatch):  # noqa: F811
    # Diálogos modais bloqueariam a automação; o teste confere o resultado pelo callback.
    from mt5_extracao import ui_manager
    monkeypatch.setattr(ui_manager.messagebox, "showerror", lambda *a, **k: None)
    monkeypatch.setattr(ui_manager.messagebox, "showinfo", lambda *a, **k: None)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    if not ui.app.config.has_section("DASHBOARD"):
        ui.app.config.add_section("DASHBOARD")
    ui.app.config.set("DASHBOARD", "port", str(port))
    yield ui
    server = getattr(ui, "_dashboard_server", None)
    if server is not None:
        server.stop()


def wait(ui, outcomes, count=1, seconds=20):
    end = time.time() + seconds
    while len(outcomes) < count and time.time() < end:
        ui.pump(0.1)
    assert len(outcomes) >= count, "Dashboard não concluiu"


def read(url):
    client = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with client.open(url, timeout=5) as response:
        return response.read().decode("utf-8")


def test_buttons_exist_in_assets_and_robots(dashboard_ui):
    assert dashboard_ui.dashboard_button.cget("text") == "Dashboard"
    assert dashboard_ui.robots_panel.dashboard_button.cget("text") == "Dashboard"


def test_robot_dashboard_opens_from_catalog_without_running_anything(dashboard_ui):
    from mt5_extracao.robots import store
    from tests.test_robots_catalog import PYTHON_KEYS
    key = "python:" + PYTHON_KEYS[0]
    before = len(store.load_runs(dashboard_ui.app.db_manager))
    opened, outcomes = [], []
    dashboard_ui.open_dashboard_for({"mode": "robot", "robot_key": key}, opener=lambda s: opened.append(s.url) or s,
                                    on_done=lambda o, e: outcomes.append((o, e)))
    wait(dashboard_ui, outcomes)
    outcome, error = outcomes[0]
    assert error is None and not outcome["stale"]
    html = read(opened[0])
    assert "Sem resultado neste recorte" in html or "Execução" in html
    assert len(store.load_runs(dashboard_ui.app.db_manager)) == before


def test_asset_dashboard_reads_base_off_the_ui_thread(dashboard_ui, monkeypatch):
    pending = []
    monkeypatch.setattr(dashboard_ui, "run_bg", lambda work, done: pending.append((work, done)))
    dashboard_ui.open_dashboard_for({"mode": "asset", "symbol": "WIN$N", "timeframe": "M1"},
                                    opener=lambda s: s)
    assert len(pending) == 1 and getattr(dashboard_ui, "_dashboard_server", None) is None   # nada síncrono


def test_rapid_reselection_discards_stale_result_and_keeps_one_server(dashboard_ui):
    opened, outcomes = [], []
    record = lambda o, e: outcomes.append((o, e))
    dashboard_ui.open_dashboard_for({"mode": "asset", "symbol": "WDO$N", "timeframe": "M1"},
                                    opener=lambda s: opened.append(s.url) or s, on_done=record)
    dashboard_ui.open_dashboard_for({"mode": "asset", "symbol": "WIN$N", "timeframe": "M1"},
                                    opener=lambda s: opened.append(s.url) or s, on_done=record)
    wait(dashboard_ui, outcomes, count=2)
    assert sorted(o["stale"] for o, _ in outcomes) == [False, True]
    assert len(opened) == 1                                                  # só a seleção mais nova abriu
    html = read(opened[0])
    assert "WIN$N" in html and "WDO$N" not in html


def test_reopening_replaces_previous_server(dashboard_ui):
    opened, outcomes = [], []
    for symbol in ("WIN$N", "PETR4"):
        dashboard_ui.open_dashboard_for({"mode": "asset", "symbol": symbol, "timeframe": "M1"},
                                        opener=lambda s: opened.append(s.url) or s,
                                        on_done=lambda o, e: outcomes.append((o, e)))
        wait(dashboard_ui, outcomes, count=len(opened) + 1 if len(outcomes) < len(opened) + 1 else len(outcomes))
    assert len(opened) == 2 and "PETR4" in read(opened[1])
    with pytest.raises(OSError):
        read(opened[0])                                                      # servidor anterior encerrado
