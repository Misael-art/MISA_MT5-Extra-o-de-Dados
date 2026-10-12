"""T11.9: painel vivo (servidor local com chave, atualização e ações de uma lista fechada), sem MT5."""
import json
import time
import urllib.error
import urllib.request

import pytest

from mt5_extracao import audit
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.robots import panel_server, store
from mt5_extracao.timeframes import Timeframe
from tests.market_data import trend


@pytest.fixture
def server(tmp_path, monkeypatch):
    from mt5_extracao.bootstrap import paths
    monkeypatch.setattr(paths, "is_windows", lambda: False)
    db = DatabaseManager(db_type="sqlite", db_path=str(tmp_path / "db.sqlite"))
    db.save_ohlcv_data("EURUSD", Timeframe.M15.label, trend(1200, step=0.02, noise=0.3, freq="15min", spread=2, volume=500))
    import configparser
    config = configparser.ConfigParser()
    prefix = tmp_path / "wine"
    (prefix / "drive_c" / "users" / "u" / "AppData" / "Roaming").mkdir(parents=True)
    config.read_dict({"MT5": {"wine_prefix": str(prefix)}})
    notices = []
    srv = panel_server.PanelServer(db, config, notifier=lambda *a: notices.append(a)).start()
    srv.db, srv.notices, srv.prefix = db, notices, prefix
    yield srv
    srv.stop()


def call(server, method, path, body=None, key=True, headers=None):
    """(status, json) de um pedido ao servidor; `key` False = sem chave, texto = chave dada."""
    request = urllib.request.Request(f"http://127.0.0.1:{server.port}{path}", method=method,
                                     data=json.dumps(body).encode() if body is not None else None)
    if key:
        request.add_header(panel_server.KEY_HEADER, server.app.key if key is True else key)
    if body is not None:
        request.add_header("Content-Type", "application/json")
    for name, value in (headers or {}).items():
        request.add_header(name, value)
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            raw, status = response.read(), response.status
    except urllib.error.HTTPError as e:
        raw, status = e.read(), e.code
    try:
        return status, json.loads(raw)
    except ValueError:
        return status, raw.decode("utf-8")


def wait(server, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        job = call(server, "GET", "/api/job")[1]["job"]
        if job and job["status"] != panel_server.RUNNING:
            return job
        time.sleep(0.05)
    raise AssertionError("o trabalho não terminou")


# --- regras de segurança ----------------------------------------------------------------

def test_rule_1_only_this_machine(server):
    assert server.httpd.server_address[0] == "127.0.0.1" and panel_server.HOST == "127.0.0.1"
    status, body = call(server, "GET", "/api/matrix", headers={"Host": f"192.168.0.10:{server.port}"})
    assert status == 403 and "própria máquina" in body["error"]
    assert call(server, "GET", "/api/matrix", headers={"Host": "evil.example"})[0] == 403
    assert call(server, "GET", "/api/matrix", headers={"Host": f"localhost:{server.port}"})[0] == 200
    assert call(server, "GET", "/api/matrix", headers={"Host": "127.0.0.1:1"})[0] == 403        # outra porta


def test_rule_2_nothing_without_the_session_key(server):
    for path in ("/", "/api/matrix", "/api/job"):
        assert call(server, "GET", path, key=False)[0] == 403
        assert call(server, "GET", path, key="chave-errada")[0] == 403
    assert call(server, "POST", "/api/action", {"action": "stop_all", "params": {}}, key=False)[0] == 403
    assert call(server, "GET", f"/?k={server.app.key}", key=False)[0] == 200            # a página abre pelo endereço
    # em POST a chave só vale no cabeçalho: um formulário de outra página não consegue mandá-la
    assert call(server, "POST", f"/api/action?k={server.app.key}", {"action": "stop_all", "params": {}}, key=False)[0] == 403
    status, body = call(server, "POST", "/api/action", {"action": "stop_all", "params": {}},
                        headers={"Origin": "http://outro.site"})
    assert status == 403 and "outra página" in body["error"]
    assert len(server.app.key) >= 30 and server.app.key != panel_server.PanelServer(server.db, None).app.key
    assert list((server.prefix / "drive_c").rglob("HDK_KILL.txt")) == []          # nenhum pedido recusado agiu


def test_rule_3_closed_list_of_actions_and_checked_parameters(server):
    post = lambda body: call(server, "POST", "/api/action", body)
    status, body = post({"action": "run", "params": {"cmd": "rm -rf /"}})
    assert status == 400 and "Ação desconhecida" in body["error"]
    assert set(panel_server.ACTIONS) == {"backtest", "resample", "battery", "stop_all"}      # nada de iniciar robô
    assert post({"action": "backtest", "params": {"robots": ["python:donchian"], "symbols": ["EURUSD"], "tfs": ["M15"],
                                                  "shell": "x"}})[0] == 400                  # parâmetro fora da lista
    for params, text in (
            ({"robots": ["mql5:nao_existe"], "symbols": ["EURUSD"], "tfs": ["M15"]}, "não está no catálogo"),
            ({"robots": ["python:donchian"], "symbols": ["../../etc/passwd"], "tfs": ["M15"]}, "Ativo sem base"),
            ({"robots": ["python:donchian"], "symbols": ["EURUSD"], "tfs": ["M15; ls"]}, "Timeframe desconhecido"),
            ({"robots": "python:donchian", "symbols": ["EURUSD"], "tfs": ["M15"]}, "Informe de 1 a")):
        assert post({"action": "backtest", "params": params})[0] == 202
        job = wait(server)
        assert job["status"] == panel_server.FAILED and text in job["result"], job
    assert call(server, "POST", "/api/action", key=True, headers={"Content-Type": "text/plain"})[0] in (413, 415)
    request = urllib.request.Request(f"http://127.0.0.1:{server.port}/api/action", method="POST", data=b"{nao e json",
                                     headers={panel_server.KEY_HEADER: server.app.key, "Content-Type": "application/json"})
    with pytest.raises(urllib.error.HTTPError) as error:
        urllib.request.urlopen(request, timeout=10)
    assert error.value.code == 400
    assert call(server, "GET", "/api/outra")[0] == 404 and store.load_runs(server.db) == []


def test_rule_4_one_job_at_a_time(server, monkeypatch):
    import threading
    gate = threading.Event()
    monkeypatch.setattr(panel_server.PanelApp, "_stop_all", lambda self, params, job: gate.wait(10) and "ok")
    assert call(server, "POST", "/api/action", {"action": "stop_all", "params": {}})[0] == 202
    status, body = call(server, "POST", "/api/action", {"action": "stop_all", "params": {}})
    assert status == 409 and "Já há um trabalho em andamento" in body["error"]
    gate.set()
    assert wait(server)["status"] == panel_server.DONE
    assert call(server, "POST", "/api/action", {"action": "stop_all", "params": {}})[0] == 202     # liberado de novo
    wait(server)


# --- o que o painel faz -----------------------------------------------------------------

def test_page_matrix_and_refresh_after_a_backtest(server):
    status, html = call(server, "GET", f"/?k={server.app.key}", key=False)
    assert status == 200 and "var LIVE = true" in html and "Testar o que falta" in html
    before = call(server, "GET", "/api/matrix")[1]
    assert before["cells"] == [] and before["symbols"] == ["EURUSD"] and before["actions"] == sorted(panel_server.ACTIONS)
    assert call(server, "GET", "/api/matrix")[1]["version"] == before["version"]             # sem mudança: mesma versão
    status, body = call(server, "POST", "/api/action", {"action": "backtest", "params": {
        "robots": ["python:donchian", "python:ema_atr"], "symbols": ["EURUSD"], "tfs": ["M15"], "resume": True}})
    assert status == 202 and body["job"]["status"] == panel_server.RUNNING
    job = wait(server)
    assert job["status"] == panel_server.DONE and job["result"].startswith("2 testado(s)") and len(job["messages"]) == 2
    after = call(server, "GET", "/api/matrix")[1]
    assert after["version"] != before["version"] and len(after["cells"]) == 2                # a página redesenha
    assert server.notices[-1][0] == "Painel: Backtest (concluído)"


def test_mql5_backtest_is_not_offered_here_yet(server, tmp_path):
    from mt5_extracao.robots import scaffold
    scaffold.create("Vivo", tmp_path / "robos", magic=730001)
    server.app.config["ROBOTS"] = {"library_dirs": str(tmp_path / "robos")}
    call(server, "POST", "/api/action", {"action": "backtest", "params": {"robots": ["mql5:vivo"], "symbols": ["EURUSD"],
                                                                         "tfs": ["M15"]}})
    job = wait(server)
    assert job["status"] == panel_server.FAILED and "fecha o MetaTrader 5" in job["result"]


def test_resample_battery_and_stop_all(server):
    call(server, "POST", "/api/action", {"action": "resample", "params": {"symbols": ["EURUSD"], "base": "M15",
                                                                          "tfs": ["H1", "H4"]}})
    job = wait(server)
    assert job["status"] == panel_server.DONE and job["result"] == "2 série(s) montada(s)"
    assert call(server, "GET", "/api/matrix")[1]["inventory"]["EURUSD"]["H1"]["built_from"] == "M15"
    call(server, "POST", "/api/action", {"action": "resample", "params": {"symbols": ["EURUSD"], "base": "H1", "tfs": ["M15"]}})
    assert "só dá para montar um timeframe maior" in wait(server)["result"]
    call(server, "POST", "/api/action", {"action": "battery", "params": {"robot": "python:donchian", "symbol": "EURUSD"}})
    assert "bateria REPROVADA" in wait(server)["result"]                                    # sem backtest: reprova
    call(server, "POST", "/api/action", {"action": "stop_all", "params": {}})
    job = wait(server)
    kill = list((server.prefix / "drive_c").rglob("HDK_KILL.txt"))
    assert job["status"] == panel_server.DONE and len(kill) == 1 and "NÃO foram fechadas" in " ".join(job["messages"])
    assert server.notices[-1][2] == "crítico"


def test_static_file_has_no_buttons_and_audit_checks_the_panel(server):
    from mt5_extracao.robots import panel
    html = panel.render(server.app.matrix())
    assert "var LIVE = false" in html                                   # arquivo aberto do disco: só a fotografia
    assert audit.check_panel().status == audit.OK
    import unittest.mock as mock
    with mock.patch.object(panel_server, "HOST", "0.0.0.0"):
        finding = audit.check_panel()
    assert finding.status == audit.FAIL and "qualquer máquina da rede" in finding.detail
