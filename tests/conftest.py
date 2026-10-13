import os

import pytest

from fakes import FakeMT5


@pytest.fixture
def fake_mt5(monkeypatch):
    """FakeMT5 instalado no lugar do MetaTrader5 usado pelo MT5Connector."""
    fake = FakeMT5()
    import mt5_extracao.mt5_connector as connector_module
    monkeypatch.setattr(connector_module, "mt5", fake)
    return fake


@pytest.fixture
def connector(fake_mt5, tmp_path, monkeypatch):
    """MT5Connector inicializado sobre o FakeMT5 (sem ler config real)."""
    from mt5_extracao.mt5_connector import MT5Connector
    monkeypatch.chdir(tmp_path)
    conn = MT5Connector(config_path=os.path.join(str(tmp_path), "nao-existe.ini"))
    conn.is_initialized = True
    return conn
