"""Caminho Linux de ponta a ponta: app -> RemoteMT5 -> ponte RPyC -> MetaTrader5 (falso)."""
import datetime as dt
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest
from sqlalchemy import text

import mt5_extracao.mt5_connector as connector_module
from mt5_extracao import mt5_backend
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.historical_extractor import HistoricalExtractor
from mt5_extracao.indicator_calculator import IndicatorCalculator

REPO = Path(__file__).resolve().parents[1]


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def bridge_port():
    pytest.importorskip("rpyc")
    port = _free_port()
    proc = subprocess.Popen([sys.executable, str(REPO / "scripts" / "mt5_bridge_server.py"), "--port", str(port)],
                            env=dict(os.environ, PYTHONPATH=str(REPO / "tests" / "fake_mt5_package")),
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    deadline = time.time() + 20
    while True:
        try:
            socket.create_connection(("127.0.0.1", port), 0.5).close()
            break
        except OSError:
            assert proc.poll() is None, proc.stdout.read().decode(errors="replace")
            assert time.time() < deadline
            time.sleep(0.2)
    yield port
    proc.terminate()
    proc.wait(timeout=10)


def test_extraction_through_bridge(bridge_port, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    cfg = tmp_path / "config.ini"
    cfg.write_text(f"[BRIDGE]\nenabled = true\nhost = 127.0.0.1\nport = {bridge_port}\n", encoding="utf-8")
    mt5_backend.reset()
    monkeypatch.setattr(connector_module, "mt5", None)
    try:
        connector = connector_module.MT5Connector(config_path=str(cfg))
        assert isinstance(connector_module.mt5, mt5_backend.RemoteMT5)
        connector.is_initialized = True
        db = DatabaseManager(db_path=str(tmp_path / "t.db"))
        ext = HistoricalExtractor(connector, db, IndicatorCalculator(), chunk_config={"m1": 1, "m5_m15": 1, "default": 1})
        assert ext._process_symbol("WIN$N", 1, "1 minuto", dt.datetime(2024, 1, 2), dt.datetime(2024, 1, 4),
                                   include_indicators=True, overwrite=False)
        with db.engine.connect() as c:
            count, first, last = c.execute(text("SELECT COUNT(*), MIN(time), MAX(time) FROM win_n_1_minuto")).fetchone()
        assert count == 2 * 540
        assert first.startswith("2024-01-02 09:00:00") and last.startswith("2024-01-03 17:59:00")
        assert connector.last_error() == (1, "Success")
    finally:
        mt5_backend.reset()


def test_initialize_uses_windows_path_through_bridge(bridge_port, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    mt5_dir = tmp_path / "wine" / "drive_c" / "Program Files" / "MetaTrader 5"
    mt5_dir.mkdir(parents=True)
    (mt5_dir / "terminal64.exe").write_bytes(b"")
    cfg = tmp_path / "config.ini"
    cfg.write_text(f"[MT5]\npath = {mt5_dir}\nwindows_path = C:\\Program Files\\MetaTrader 5\n"
                   f"[BRIDGE]\nenabled = true\nhost = 127.0.0.1\nport = {bridge_port}\n", encoding="utf-8")
    mt5_backend.reset()
    monkeypatch.setattr(connector_module, "mt5", None)
    monkeypatch.setattr(connector_module.time, "sleep", lambda s: None)
    try:
        connector = connector_module.MT5Connector(config_path=str(cfg))
        # Sem terminal64.exe rodando no host: com a ponte, initialize não depende do processo local
        assert connector.initialize() is True
        assert connector.is_initialized
        assert connector.mt5_windows_path == r"C:\Program Files\MetaTrader 5"
        # O MT5 (no "Wine") recebeu o caminho no formato Windows, não o caminho do Linux
        init_calls = connector_module.mt5.calls_named("initialize")
        assert init_calls[-1][2]["path"] == r"C:\Program Files\MetaTrader 5"
    finally:
        mt5_backend.reset()


def test_ticks_through_bridge(bridge_port, tmp_path, monkeypatch):
    """T5.3: copy_ticks_range e a constante COPY_TICKS_ALL passam pela ponte."""
    from mt5_extracao.tick_extractor import TickExtractor
    monkeypatch.chdir(tmp_path)
    cfg = tmp_path / "config.ini"
    cfg.write_text(f"[BRIDGE]\nenabled = true\nhost = 127.0.0.1\nport = {bridge_port}\n", encoding="utf-8")
    mt5_backend.reset()
    monkeypatch.setattr(connector_module, "mt5", None)
    try:
        connector = connector_module.MT5Connector(config_path=str(cfg))
        connector.is_initialized = True
        db = DatabaseManager(db_path=str(tmp_path / "t.db"))
        r = TickExtractor(connector, db, chunk_hours=1).extract("WIN$N", dt.datetime(2024, 1, 2, 9),
                                                                dt.datetime(2024, 1, 2, 11))
        assert r["failed"] == 0 and r["rows"] > 0
        with db.engine.connect() as c:
            assert c.execute(text("SELECT COUNT(*) FROM win_n_ticks")).scalar() == 121 * 3
    finally:
        mt5_backend.reset()


def test_book_through_bridge(bridge_port, tmp_path, monkeypatch):
    """T5.4: market_book_* (tupla de namedtuples BookInfo) passa pela ponte."""
    cfg = tmp_path / "config.ini"
    cfg.write_text(f"[BRIDGE]\nenabled = true\nhost = 127.0.0.1\nport = {bridge_port}\n", encoding="utf-8")
    mt5_backend.reset()
    monkeypatch.setattr(connector_module, "mt5", None)
    try:
        connector = connector_module.MT5Connector(config_path=str(cfg))
        connector.is_initialized = True
        assert connector.book_subscribe("WIN$N")
        levels = connector.book_snapshot("WIN$N")
        assert len(levels) == 10 and {l["type"] for l in levels} == {1, 2}
        connector.book_release("WIN$N")
    finally:
        mt5_backend.reset()
