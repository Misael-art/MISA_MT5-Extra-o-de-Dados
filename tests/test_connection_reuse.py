"""Reuso do backend já autenticado; nenhum teste abre terminal ou ponte reais."""
import configparser
from types import SimpleNamespace

import pytest

from mt5_extracao import mt5_connector as module
from mt5_extracao.mt5_backend import RemoteMT5


class API:
    def __init__(self, directory):
        self.directory, self.data_path, self.active = directory, directory, True
        self.connected, self.calls, self.reads = True, [], []
        self.account = SimpleNamespace(login=42, server="Synthetic-Demo", trade_mode=0)

    def terminal_info(self):
        self.reads.append("terminal_info")
        return (SimpleNamespace(connected=self.connected, path=self.directory, data_path=self.data_path)
                if self.active else None)

    def account_info(self):
        self.reads.append("account_info")
        return self.account if self.active else None

    def initialize(self, **kwargs):
        self.calls.append("initialize")
        self.active = self.connected = True
        return True

    def shutdown(self):
        self.calls.append("shutdown")
        self.active = False

    def login(self, login, **kwargs):
        self.calls.append("login")
        self.account = SimpleNamespace(login=login, server=kwargs.get("server", self.account.server), trade_mode=0)
        return True

    def symbols_get(self):
        return [SimpleNamespace(name="EURUSD")]

    def last_error(self):
        return (-1, "synthetic")


class RemoteAPI(API, RemoteMT5):
    pass  # API.__init__ evita abrir RPyC; o percurso Windows da ponte é real.


@pytest.fixture(params=[False, True], ids=["local", "remote-windows"])
def linked(request, monkeypatch, tmp_path):
    folder = tmp_path / "terminal"
    folder.mkdir()
    directory = r"C:\Synthetic MT5" if request.param else str(folder)
    api = (RemoteAPI if request.param else API)(directory)
    monkeypatch.setattr(module, "mt5", api)
    monkeypatch.setattr(module, "time", SimpleNamespace(sleep=lambda seconds: None))
    monkeypatch.setattr("dotenv.load_dotenv", lambda *args, **kwargs: False)
    for name in ("MT5_LOGIN", "MT5_PASSWORD", "MT5_SERVER"):
        monkeypatch.delenv(name, raising=False)
    config = configparser.ConfigParser(interpolation=None)
    config["MT5"] = {"path": str(folder), **({"windows_path": directory} if request.param else {})}
    path = tmp_path / "config.ini"
    with path.open("w") as handle:
        config.write(handle)
    connector = module.MT5Connector(str(path))
    monkeypatch.setattr(connector, "_is_mt5_running", lambda: True)
    prepared = []
    connector.prepare_terminal = lambda: prepared.append(True)
    return connector, api, prepared


def test_healthy_backend_is_reused_without_initializing_preparing_or_shutting_down(linked):
    connector, api, prepared = linked
    assert connector.initialize() and connector.is_initialized
    assert not api.calls and not prepared
    assert "existente" in connector.get_connection_status()["mode"]
    connector.shutdown()
    assert not connector.is_initialized and api.active and not api.calls


def test_backend_initially_missing_is_resolved_again_without_reinitializing_session(linked,monkeypatch):
    connector,api,prepared=linked;monkeypatch.setattr(module,'mt5',None);calls=[]
    def recover(path):calls.append(path);return api
    monkeypatch.setattr(module,'get_mt5',recover)
    assert connector.initialize() and len(calls)==1
    assert not api.calls and not prepared


def test_unavailable_backend_readiness_never_initializes_or_prepares_terminal(linked,monkeypatch):
    connector,api,prepared=linked;monkeypatch.setattr(module,'mt5',None)
    monkeypatch.setattr(module,'get_mt5',lambda path:None)
    assert not connector.backend_ready() and not connector.initialize()
    assert not api.calls and not prepared


def test_repeated_initialize_and_local_disconnect_keep_borrowed_session(linked):
    connector, api, prepared = linked
    assert connector.initialize() and connector.initialize()
    connector.shutdown()
    assert connector.initialize()
    assert not api.calls and not prepared


@pytest.mark.parametrize("change", ["disconnected", "account", "server", "directory", "data_path", "trade_mode", "missing_path"])
def test_changed_borrowed_session_is_refused_without_reinitializing(linked, change):
    connector, api, _ = linked
    assert connector.initialize()
    api.calls.clear()
    if change == "disconnected":
        api.connected = False
    elif change == "account":
        api.account.login = 77
    elif change == "server":
        api.account.server = "Other-Demo"
    elif change == "directory":
        api.directory += "-other"
    elif change == "data_path":
        api.data_path += "-other"
    elif change == "trade_mode":
        api.account.trade_mode = 2
    else:
        api.directory = ""
    assert not connector.initialize() and not connector.is_initialized
    connector.shutdown()
    assert api.calls == []


def test_explicit_force_preserves_legacy_reinitialization_and_ownership(linked):
    connector, api, prepared = linked
    assert connector.initialize()
    api.calls.clear()
    assert connector.initialize(force_restart=True)
    assert api.calls == ["shutdown", "initialize"] and prepared == [True]
    connector.shutdown()
    assert api.calls[-1] == "shutdown" and not api.active


def test_fresh_backend_keeps_legacy_connection_and_repeated_call_keeps_ownership(linked):
    connector, api, prepared = linked
    api.active = False
    assert connector.initialize()
    assert api.calls == ["shutdown", "initialize"]
    assert connector.initialize()
    assert api.calls == ["shutdown", "initialize"]
    connector.shutdown()
    assert api.calls == ["shutdown", "initialize", "shutdown"] and prepared == [True]


def test_configured_account_matches_without_login_or_secret_in_logs(linked, monkeypatch, caplog):
    connector, api, _ = linked
    monkeypatch.setenv("MT5_LOGIN", "42")
    monkeypatch.setenv("MT5_SERVER", "Synthetic-Demo")
    monkeypatch.setenv("MT5_PASSWORD", "synthetic-secret")
    assert connector.initialize() and not api.calls
    assert "synthetic-secret" not in caplog.text


def test_initial_configured_account_change_keeps_legacy_login_contract(linked, monkeypatch, caplog):
    connector, api, _ = linked
    monkeypatch.setenv("MT5_LOGIN", "55")
    monkeypatch.setenv("MT5_SERVER", "Configured-Demo")
    monkeypatch.setenv("MT5_PASSWORD", "synthetic-secret")
    assert connector.initialize()
    assert api.calls == ["shutdown", "initialize", "login"]
    assert api.account.login == 55 and api.account.server == "Configured-Demo"
    assert "synthetic-secret" not in caplog.text


def test_native_context_guard_precedes_backend_reads_or_changes(linked, monkeypatch):
    from mt5_extracao.robots import mt5_tools
    from mt5_extracao.bootstrap import paths
    connector, api, _ = linked
    folder = paths.state_dir()
    folder.mkdir(parents=True)
    marker = folder / "native-active.json"
    marker.write_text("{}")
    monkeypatch.setattr(mt5_tools, "_native_context_processes", lambda context: [object()])
    assert not connector.initialize() and not api.calls and not api.reads
    assert marker.exists()


def test_reuse_reenters_existing_lease_in_same_thread(linked):
    from mt5_extracao.robots import mt5_tools
    connector, api, _ = linked
    with mt5_tools.tester_lease():
        assert connector.initialize() and not api.calls


def test_borrowed_affinity_survives_local_disconnect_and_refuses_changed_credentials(linked, monkeypatch):
    connector, api, _ = linked
    assert connector.initialize()
    connector.shutdown()
    monkeypatch.setenv("MT5_LOGIN", "77")
    monkeypatch.setenv("MT5_PASSWORD", "synthetic-secret")
    assert not connector.initialize() and not api.calls


def test_backend_read_failure_after_borrow_does_not_reset_session(linked, monkeypatch):
    connector, api, _ = linked
    assert connector.initialize()
    def unavailable():
        raise RuntimeError("synthetic read failure")
    monkeypatch.setattr(api, "terminal_info", unavailable)
    assert not connector.initialize() and not api.calls


def test_full_executable_path_matches_terminal_directory(linked):
    connector, api, _ = linked
    if isinstance(api, RemoteMT5):
        connector.mt5_windows_path = api.directory.swapcase() + r"\terminal64.exe"
        requested = None
    else:
        executable = module.Path(api.directory) / "terminal64.exe"
        executable.touch()
        requested = str(executable)
    assert connector.initialize(mt5_path=requested) and not api.calls


def test_auto_login_false_does_not_switch_account(linked, monkeypatch):
    connector, api, _ = linked
    monkeypatch.setenv("MT5_LOGIN", "77")
    monkeypatch.setenv("MT5_PASSWORD", "synthetic-secret")
    assert connector.initialize(auto_login=False) and not api.calls
    assert connector.get_symbols_count() == 1 and connector.force_connection_check()
    assert not api.calls


@pytest.mark.parametrize("method", ["get_symbols_count", "force_connection_check"])
def test_collection_recovery_keeps_borrowed_session_when_backend_disappears(linked, method):
    connector, api, _ = linked
    assert connector.initialize()
    api.active = False
    assert not getattr(connector, method)() and not api.calls


def test_collection_preflight_reuses_healthy_session(linked):
    connector, api, _ = linked
    assert connector.initialize()
    assert connector.get_symbols_count() == 1 and connector.force_connection_check()
    assert not api.calls


def test_fresh_aggressive_recovery_keeps_owned_shutdown_contract(linked):
    connector, api, _ = linked
    api.active = False
    assert connector.force_connection_check()
    assert api.calls == ["shutdown", "initialize"]
    connector.shutdown()
    assert api.calls == ["shutdown", "initialize", "shutdown"]
