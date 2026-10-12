"""T9.2: auditoria de segurança do projeto, patrimonial e da gestão do capital (sem MT5 e sem ler segredos)."""
import configparser
import json
import os
import subprocess
from datetime import datetime
from pathlib import Path

import pytest

from mt5_extracao import audit, cli, robots
from mt5_extracao.database_manager import DatabaseManager

REPO = Path(__file__).resolve().parent.parent
LIBRARY = Path(__file__).parent / "robots_library"


def cfg(text=""):
    config = configparser.ConfigParser()
    config.read_string(text)
    return config


def by_id(findings, fid):
    return [f for f in findings if f.id == fid]


@pytest.fixture
def project(tmp_path):
    (tmp_path / "mt5_extracao").mkdir()
    (tmp_path / "scripts").mkdir()
    (tmp_path / "logs").mkdir()
    (tmp_path / "mt5_extracao" / "ok.py").write_text("x = 1\n", encoding="utf-8")
    return tmp_path


def test_clean_project_has_no_blocking_findings(project):
    config = cfg("[STRATEGY]\ncommission_per_lot = 1.5\nslippage_points = 2\n")
    findings = audit.run_audit(project, config, None, {"demo": True, "login": 1, "server": "Demo"}, catalog=[],
                               installed={"pillow": "12.3.0", "cryptography": "50.0.2"})
    assert audit.failures(findings, audit.LOW) == []
    assert by_id(findings, "A3")[0].status == audit.OK and by_id(findings, "P5")[0].status == audit.SKIPPED


def test_tracked_secrets_and_env_permissions(project):
    subprocess.run(["git", "init", "-q"], cwd=project, check=True)
    (project / ".env").write_text("MT5_PASSWORD=x\n", encoding="utf-8")
    (project / "base.db").write_bytes(b"")
    (project / ".env.example").write_text("MT5_PASSWORD=\n", encoding="utf-8")
    subprocess.run(["git", "add", "-f", ".env", "base.db", ".env.example"], cwd=project, check=True)
    tracked = audit.check_tracked_secrets(project)
    assert tracked.status == audit.FAIL and ".env" in tracked.detail and "base.db" in tracked.detail
    assert ".env.example" not in tracked.detail
    if os.name == "posix":
        os.chmod(project / ".env", 0o644)
        assert audit.check_env_permissions(project).status == audit.FAIL
        os.chmod(project / ".env", 0o600)
        assert audit.check_env_permissions(project).status == audit.OK


def test_secrets_in_config_and_logs(project):
    found = audit.check_config_secrets(cfg("[MT5]\npassword = abc\npath = C:/x\n[DATABASE]\n"
                                           "url = postgresql://u:segredo@host/db\n"))
    assert found.status == audit.FAIL and "[MT5] password" in found.detail and "senha na URL" in found.detail
    assert audit.check_config_secrets(cfg("[MT5]\npassword =\n[DATABASE]\nurl = postgresql://u@host/db\n")).status == audit.OK
    (project / "logs" / "app.log").write_text("ok\nlogin com password=hunter22\n", encoding="utf-8")
    (project / "logs" / "limpo.log").write_text("Senha: ****\nsenha não informada\n", encoding="utf-8")
    logs = audit.check_logs(project)
    assert logs.status == audit.FAIL and "app.log" in logs.detail and "limpo.log" not in logs.detail


def test_bridge_exposed_and_open_service(project):
    (project / "scripts" / "mt5_bridge_server.py").write_text("server = ThreadedServer(rpyc.SlaveService)\n")
    exposed, open_service = audit.check_bridge(project, cfg("[BRIDGE]\nenabled = true\nhost = 0.0.0.0\n"))
    assert (exposed.status, exposed.severity) == (audit.FAIL, audit.CRITICAL)
    assert (open_service.status, open_service.area) == (audit.FAIL, audit.ASSETS) and "enviar ordens" in open_service.detail
    exposed, open_service = audit.check_bridge(project, cfg("[BRIDGE]\nenabled = true\nhost = 127.0.0.1\n"))
    assert exposed.status == audit.OK and open_service.status == audit.FAIL
    (project / "scripts" / "mt5_bridge_server.py").write_text("class ReadOnly(rpyc.Service): pass\n")
    assert audit.check_bridge(project, cfg("[BRIDGE]\nenabled = true\n"))[1].status == audit.OK
    assert all(f.status == audit.SKIPPED for f in audit.check_bridge(project, cfg("[BRIDGE]\nenabled = false\n")))


def test_python_orders_dependencies_and_account(project):
    (project / "mt5_extracao" / "ruim.py").write_text("mt5." + "order" + "_send(request)\n", encoding="utf-8")
    orders = audit.check_python_orders(project)
    assert (orders.status, orders.severity) == (audit.FAIL, audit.CRITICAL) and "ruim.py" in orders.detail
    old = audit.check_dependencies({"pillow": "10.0.0", "requests": "2.34.2"})
    assert old.status == audit.FAIL and "pillow 10.0.0" in old.detail and "requests" not in old.detail
    assert audit.check_dependencies({"pillow": "12.3.0"}).status == audit.OK
    real, allow = audit.check_account({"demo": False, "login": 77, "server": "Corretora-Real"},
                                      cfg("[ROBOTS]\nallow_real = true\n"))
    assert real.status == audit.FAIL and "REAL" in real.detail and allow.status == audit.FAIL
    assert audit.check_account(None, cfg())[0].status == audit.SKIPPED


def test_robots_risk_dll_and_missing_source(tmp_path):
    ex5 = tmp_path / "Fechado.ex5"
    ex5.write_bytes(b"\x00")
    catalog = robots.scan_library(LIBRARY) + [robots.robot_from_file(ex5)]
    findings = audit.check_robots(catalog)
    titles = " | ".join(f.title for f in findings)
    assert "Robô usa DLL externa: Martingale" in titles and "Robô sem código-fonte: Fechado" in titles
    high = [f for f in findings if f.id == "C5"]
    assert {f.title for f in high} == {"Risco alto em Martingale (mql5:martingale), padrão do código",
                                       "Risco alto em DemoGuard (mql5:demoguard), Demo_Agressivo",
                                       "Risco alto em DemoGuard (mql5:demoguard), Demo_SemLimite"}
    assert audit.check_robots(robots.catalog.python_robots())[0].status == audit.OK


def test_order_outside_the_safety_layer(tmp_path):
    root = tmp_path / "lib"
    (root / "Experts").mkdir(parents=True)
    (root / "Include" / "Safety").mkdir(parents=True)
    (root / "Include" / "Safety" / "Exec.mqh").write_text("bool Send(MqlTradeRequest &r, MqlTradeResult &x) { return OrderSend(r, x); }\n")
    ea = root / "Experts" / "Bot.mq5"
    ea.write_text('#include <Safety\\Exec.mqh>\ninput double InpRiskPct = 0.5; // Risco por trade (%)\n'
                  'input int InpStopPoints = 100; // Stop (pontos)\n// OrderSend(r, x) só em comentário\nvoid OnTick() { }\n')
    assert not by_id(audit.check_robots([robots.robot_from_file(ea)]), "A7")
    ea.write_text(ea.read_text() + "void Cheat() { MqlTradeRequest r; MqlTradeResult x; OrderSend(r, x); }\n")
    loose = by_id(audit.check_robots([robots.robot_from_file(ea)]), "A7")
    assert loose and "Bot.mq5" in loose[0].detail


def test_capital_rules(tmp_path):
    default = {f.id: f for f in audit.check_capital(cfg())}
    assert default["C1"].status == audit.OK and default["C2"].status == audit.OK
    assert default["C3"].status == audit.FAIL                      # o padrão do projeto não tem corretagem nem slippage
    loose = {f.id: f for f in audit.check_capital(cfg(
        "[STRATEGY]\nrisk_per_trade = 0.05\nmax_drawdown = 0.4\nmin_trades = 20\nmin_profit_factor = 1.0\n"
        "commission_per_lot = 1\n"))}
    assert (loose["C1"].status, loose["C1"].severity) == (audit.FAIL, audit.HIGH) and "40%" in loose["C1"].detail
    assert loose["C2"].status == audit.FAIL and loose["C2"].detail.count(";") == 2 and loose["C3"].status == audit.OK

    from mt5_extracao.strategies import validation
    db = DatabaseManager(db_path=str(tmp_path / "db.sqlite"))
    assert audit.check_validations(db)[0].status == audit.OK
    validation.save_validation(db, "AAA", "D1", {
        "strategy": "donchian", "approved": True, "status": "ok", "params": {},
        "oos_metrics": {"profit_factor": 1.5, "trades": 150},
        "checks": [{"name": "Janelas walk-forward positivas", "value": 0.8}],
        "robustness": {"robust": True}, "monte_carlo": {"dd_percentile": 0.1},
        "data_start": "2020-01-01", "data_end": "2024-01-01"})
    stale = audit.check_validations(db, now=datetime(2099, 1, 1))[0]
    assert stale.status == audit.FAIL and "validada em" in stale.detail and "sem especificação" in stale.detail


def test_cli_audit_json_exit_code_and_order(tmp_path, capsys):
    cfg_file = tmp_path / "config.ini"
    cfg_file.write_text(f"[DATABASE]\ntype = sqlite\npath = {tmp_path / 'db.sqlite'}\n"
                        f"[STRATEGY]\nrisk_per_trade = 0.05\n[ROBOTS]\nlibrary_dirs = {LIBRARY}\n", encoding="utf-8")
    code = cli.main(["--config", str(cfg_file), "--json", "audit"])
    payload = json.loads(capsys.readouterr().out)
    assert code == cli.EXIT_FAIL and "C1" in payload["data"]["blocking"]
    statuses = [f["status"] for f in payload["data"]["findings"]]
    assert statuses == sorted(statuses, key=["falha", "não verificado", "ok"].index)       # falhas primeiro
    assert payload["data"]["summary"]["by_area"]["capital"] >= 2
    assert all(f["fix"] for f in payload["data"]["findings"] if f["status"] == "falha")
    assert cli.main(["--config", str(cfg_file), "audit", "--fail-on", "crítico"]) == cli.EXIT_OK
    assert "Como corrigir:" in capsys.readouterr().out


def test_this_repository_does_not_track_secrets_or_send_orders():
    assert audit.check_tracked_secrets(REPO).status in (audit.OK, audit.SKIPPED)
    assert audit.check_python_orders(REPO).status == audit.OK
