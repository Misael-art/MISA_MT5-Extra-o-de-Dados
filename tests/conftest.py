import os
import sys
import types

import pytest

from fakes import TIMEFRAMES, FakeMT5

# No Linux o pacote MetaTrader5 não existe. Alguns módulos ainda o importam no topo
# (até a tarefa T2.2); um módulo com as constantes basta para importá-los nos testes.
try:
    import MetaTrader5  # noqa: F401
except ImportError:
    _stub = types.ModuleType("MetaTrader5")
    _stub.__dict__.update(TIMEFRAMES)
    sys.modules["MetaTrader5"] = _stub


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
