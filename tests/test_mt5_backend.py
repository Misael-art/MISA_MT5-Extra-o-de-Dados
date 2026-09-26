import datetime as dt
import os
import socket
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pandas as pd
import pytest

from mt5_extracao import mt5_backend

REPO = Path(__file__).resolve().parents[1]

FAKE_MT5_SOURCE = textwrap.dedent('''
    import collections
    import numpy as np
    __version__ = "5.0.teste"
    TIMEFRAME_M1 = 1
    TIMEFRAME_H1 = 16385
    SymbolInfo = collections.namedtuple("SymbolInfo", "name spread")
    DT = [("time", "<i8"), ("open", "<f8"), ("high", "<f8"), ("low", "<f8"), ("close", "<f8"),
          ("tick_volume", "<u8"), ("spread", "<i4"), ("real_volume", "<u8")]
    def initialize(path=None, **kw):
        return True
    def copy_rates_range(symbol, tf, a, b):
        assert isinstance(a, int) and isinstance(b, int), (type(a), type(b))
        return np.array([(a, 1.0, 2.0, 0.5, 1.5, 10, 1, 100), (a + 60, 1.5, 2.5, 1.0, 2.0, 11, 2, 90)], dtype=DT)
    def copy_rates_from_pos(symbol, tf, pos, count):
        return None
    def symbol_info(s):
        return SymbolInfo(s, 5)
    def symbols_get(group="*"):
        return (SymbolInfo("A", 1), SymbolInfo("B", 2))
    def last_error():
        return (1, "Success")
''')


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def bridge(tmp_path):
    """Sobe scripts/mt5_bridge_server.py com um MetaTrader5 falso e devolve a porta."""
    pytest.importorskip("rpyc")
    fake_dir = tmp_path / "fake"
    fake_dir.mkdir()
    (fake_dir / "MetaTrader5.py").write_text(FAKE_MT5_SOURCE, encoding="utf-8")
    port = _free_port()
    proc = subprocess.Popen([sys.executable, str(REPO / "scripts" / "mt5_bridge_server.py"), "--port", str(port)],
                            env=dict(os.environ, PYTHONPATH=str(fake_dir)),
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    deadline = time.time() + 20
    while True:
        try:
            socket.create_connection(("127.0.0.1", port), 0.5).close()
            break
        except OSError:
            assert proc.poll() is None, proc.stdout.read().decode(errors="replace")
            assert time.time() < deadline, "ponte não iniciou"
            time.sleep(0.2)
    yield port
    proc.terminate()
    proc.wait(timeout=10)


@pytest.fixture(autouse=True)
def reset_backend():
    mt5_backend.reset()
    yield
    mt5_backend.reset()


def test_to_epoch():
    assert mt5_backend._to_epoch(dt.datetime(1970, 1, 1, 0, 1)) == 60
    aware = dt.datetime(1970, 1, 1, 0, 1, tzinfo=dt.timezone(dt.timedelta(hours=-3)))
    assert mt5_backend._to_epoch(aware) == 60 + 3 * 3600
    assert mt5_backend._to_epoch("x") == "x"


def test_remote_mt5_api(bridge):
    m = mt5_backend.RemoteMT5("127.0.0.1", bridge)
    assert m.__version__ == "5.0.teste"
    assert m.TIMEFRAME_H1 == 16385
    assert m.initialize(path=r"C:\Program Files\MetaTrader 5") is True
    rates = m.copy_rates_range("WIN$N", m.TIMEFRAME_M1, dt.datetime(2024, 1, 2, 9), dt.datetime(2024, 1, 2, 18))
    df = pd.DataFrame(rates)
    assert list(df.columns) == ["time", "open", "high", "low", "close", "tick_volume", "spread", "real_volume"]
    assert str(df["tick_volume"].dtype) == "uint64" and str(df["spread"].dtype) == "int32"
    assert df["time"].iloc[0] == mt5_backend._to_epoch(dt.datetime(2024, 1, 2, 9))
    info = m.symbol_info("PETR4")
    assert info.spread == 5 and info._asdict() == {"name": "PETR4", "spread": 5}
    assert [s.name for s in m.symbols_get()] == ["A", "B"]
    assert m.last_error() == (1, "Success")
    assert m.copy_rates_from_pos("A", 1, 0, 10) is None
    assert not hasattr(m, "_privado")


def test_get_mt5_uses_bridge_when_enabled(bridge, tmp_path):
    cfg = tmp_path / "config.ini"
    cfg.write_text(f"[BRIDGE]\nenabled = true\nhost = 127.0.0.1\nport = {bridge}\n", encoding="utf-8")
    m = mt5_backend.get_mt5(str(cfg))
    assert isinstance(m, mt5_backend.RemoteMT5)
    assert mt5_backend.get_mt5(str(cfg)) is m  # instância única


def test_get_mt5_bridge_down_returns_none(tmp_path):
    pytest.importorskip("rpyc")
    cfg = tmp_path / "config.ini"
    cfg.write_text(f"[BRIDGE]\nenabled = true\nport = {_free_port()}\n", encoding="utf-8")
    assert mt5_backend.get_mt5(str(cfg)) is None
