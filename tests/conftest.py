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


@pytest.fixture(autouse=True)
def _collect_tk_garbage_on_main_thread():
    """
    Depois de cada teste, coleta o lixo na thread principal. Objetos do Tk (variáveis, imagens) de janelas já fechadas
    só podem ser destruídos na thread principal; se sobrarem, o coletor pode rodar numa thread de segundo plano de um
    teste seguinte e o Tk derruba o processo ("Illegal instruction" ou "Segmentation fault").
    """
    yield
    import gc
    gc.enable()          # a janela principal desliga a coleta automática (ui_manager.setup_ui); religa entre testes
    gc.collect()


@pytest.fixture(autouse=True)
def _isolate_native_tester_state(tmp_path, monkeypatch):
    """Os terminais falsos nunca disputam a posse nativa do usuário real."""
    from mt5_extracao.bootstrap import paths
    monkeypatch.setattr(paths, "state_dir", lambda: tmp_path / "native-state")
