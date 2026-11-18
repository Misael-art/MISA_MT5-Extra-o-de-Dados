"""Ordem das tentativas de conexão: terminal64.exe primeiro, no Windows e na ponte Wine."""
import ntpath

from mt5_extracao import mt5_connector


def test_full_terminal_path_comes_first_on_windows_and_through_the_bridge():
    for folder, module in ((r"C:\Program Files\MetaTrader 5", ntpath),
                           (r"C:\Program Files\Corretora MT5", ntpath)):
        strategies = mt5_connector.connection_strategies_for(folder, module)
        first = strategies[0]
        assert first["description"] == "Terminal direto"
        assert first["params"]["path"] == ntpath.join(folder, "terminal64.exe")
        # A pasta continua como reserva; nunca é a 1ª tentativa (Process create failed -> correção de IPC).
        assert [s["params"].get("path") for s in strategies].index(folder) > 0
        assert len({s["description"] for s in strategies}) == len(strategies) == 6


def test_initialize_does_not_restart_terminal_when_folder_path_would_fail(tmp_path, monkeypatch):
    """Windows físico: a 1ª chamada já usa o executável; a correção de IPC (que encerra o terminal) não roda."""
    folder = tmp_path / "MetaTrader 5"
    folder.mkdir()
    (folder / "terminal64.exe").write_bytes(b"")
    calls, killed = [], []

    class FakeMT5:
        def initialize(self, **kwargs):
            calls.append(kwargs)
            return str(kwargs.get("path", "")).endswith("terminal64.exe")

        def last_error(self):
            return (-10003, "IPC initialize failed, Process create failed")

        def __getattr__(self, name):
            return lambda *a, **k: None

    monkeypatch.setattr(mt5_connector, "mt5", FakeMT5())
    monkeypatch.setattr(mt5_connector.MT5Connector, "_kill_terminal_processes",
                        lambda self, timeout=10: killed.append(True) or True)
    cfg = tmp_path / "config.ini"
    cfg.write_text(f"[MT5]\npath = {folder}\n[BRIDGE]\nenabled = false\n", encoding="utf-8")
    connector = mt5_connector.MT5Connector(config_path=str(cfg))
    connector.mt5_path = str(folder)
    monkeypatch.setattr(connector, "_is_mt5_running", lambda: True)
    monkeypatch.setattr(connector, "_connected_session", lambda *a, **k: None)
    connector.initialize()
    assert calls and str(calls[0]["path"]).endswith("terminal64.exe")
    assert killed == []
