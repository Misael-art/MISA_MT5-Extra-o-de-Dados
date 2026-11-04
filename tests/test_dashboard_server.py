"""DASH-01: sockets reais locais, ciclo de vida e isolamento do servidor de leitura."""
import concurrent.futures
import configparser
import socket
import threading
import urllib.error
import urllib.request

import pytest

from mt5_extracao.dashboard import server as module


def read(url, *, headers=None, method="GET"):
    client = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    request = urllib.request.Request(url, headers=headers or {}, method=method)
    try:
        with client.open(request, timeout=3) as response:
            return response.status, response.read(), response.headers
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read(), exc.headers


def test_real_http_readiness_access_css_and_shutdown():
    instance = module.DashboardServer(port=0)
    with instance:
        assert instance._httpd.server_address[0] == "127.0.0.1"
        port = instance.port
        code, body, headers = read(instance.url)
        assert code == 200 and "Nenhuma fotografia selecionada".encode() in body
        assert headers["Cache-Control"] == "no-store"
        assert headers["Referrer-Policy"] == "no-referrer"
        import re
        css = re.search(rb'href="([^\"]*dashboard.css[^\"]*)"', body).group(1).decode()
        assert read(f"http://127.0.0.1:{port}" + css)[0] == 200
        assert read(instance.url.replace("/?", "/health?"))[0] == 200
        assert read(f"http://127.0.0.1:{port}/")[0] == 403
        assert read(instance.url, headers={"Host": "evil.example"})[0] in {400, 403}
        assert read(instance.url, headers={"Host": f"localhost:{port + 1}"})[0] == 403
        assert read(instance.url, headers={"Origin": "https://evil.example"})[0] == 403
        assert read(instance.url, method="POST")[0] == 405
        assert read(instance.url.replace("/?", "/api/backtest?"))[0] == 404
    instance.stop()
    with socket.socket() as probe:
        assert probe.connect_ex(("127.0.0.1", port)) != 0
    assert not any(t.name == "mt5x-dashboard" and t.is_alive() for t in threading.enumerate())


def test_repeated_concurrent_open_owns_one_instance():
    instance = module.DashboardServer(port=0)
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            returned = list(executor.map(lambda _: instance.start(), range(6)))
        assert all(result is instance for result in returned)
        assert sum(t.name == "mt5x-dashboard" for t in threading.enumerate()) == 1
        assert read(instance.url)[0] == 200
    finally:
        instance.stop()


def test_occupied_port_is_not_reassigned_or_closed():
    with module.DashboardServer(port=0) as owner:
        contender = module.DashboardServer(port=owner.port)
        with pytest.raises(RuntimeError, match=f"porta {owner.port}"):
            contender.start()
        contender.stop()
        assert read(owner.url)[0] == 200


def test_failed_readiness_closes_own_socket(monkeypatch):
    instance = module.DashboardServer(port=0)
    def fail(_):
        raise RuntimeError("não pronto")
    monkeypatch.setattr(instance, "_wait_ready", fail)
    with pytest.raises(RuntimeError, match="não pronto"):
        instance.start()
    with socket.socket() as probe:
        assert probe.connect_ex(("127.0.0.1", instance.port)) != 0


@pytest.mark.parametrize("port", ["abc", "0", "-1", "65536"])
def test_config_invalid_port(port):
    cfg = configparser.ConfigParser()
    cfg.read_dict({"DASHBOARD": {"port": port}})
    with pytest.raises(ValueError, match="porta"):
        module.configured_port(cfg)


def test_old_config_and_explicit_port():
    cfg = configparser.ConfigParser()
    assert module.configured_port(cfg) == 5055
    cfg.read_dict({"DASHBOARD": {"port": "5057"}})
    assert module.DashboardServer(cfg).port == 5057
    assert module.DashboardServer(cfg, port=5058).port == 5058


def test_cli_reads_without_database_or_terminal(monkeypatch):
    from mt5_extracao import cli
    monkeypatch.setattr(cli.services, "create_db_manager", lambda *a: pytest.fail("não abrir base"))
    monkeypatch.setattr(cli.Context, "connector", lambda *a: pytest.fail("não conectar MT5"))
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    assert cli.main(["dashboard", "--port", str(port), "--no-browser", "--seconds", "0.01"]) == 0
    with socket.socket() as probe:
        assert probe.connect_ex(("127.0.0.1", port)) != 0


@pytest.mark.parametrize("seconds", ["nan", "inf", "-1", "0"])
def test_cli_rejects_invalid_duration(seconds):
    from mt5_extracao import cli
    assert cli.main(["dashboard", "--no-browser", "--seconds", seconds]) == 2
