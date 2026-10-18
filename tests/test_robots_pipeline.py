"""T10.4: `mt5x robots check` (contrato, compilação, backtest e bateria em um comando), sem MT5."""
import configparser
import json
import xml.etree.ElementTree as ET
from types import SimpleNamespace

import pytest
from tests.native_process import configure_test_safety

from mt5_extracao import cli
from mt5_extracao.bootstrap import paths
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.robots import catalog, mt5_tools, pipeline, scaffold, store

LOG_OK = "Result: 0 errors, 0 warnings, 500 ms elapsed\n"
LOG_BAD = "X.mq5(3,17) : error 256: undeclared identifier 'indefinida'\nResult: 1 errors, 0 warnings\n"


def report_html(pnl):
    rows = ['<tr><td>2025.01.01 00:00:00</td><td>1</td><td></td><td>balance</td><td></td><td></td><td></td><td></td>'
            "<td>0.00</td><td>0.00</td><td>10 000.00</td><td>10 000.00</td><td></td></tr>"]
    for i, value in enumerate(pnl):
        rows.append(f"<tr><td>2025.01.02 {i % 24:02d}:00:00</td><td>{i + 2}</td><td>EURUSD</td><td>buy</td><td>out</td>"
                    f"<td>0.10</td><td>1.1</td><td>{i + 2}</td><td>0.00</td><td>0.00</td><td>{value:.2f}</td>"
                    "<td>0.00</td><td></td></tr>")
    return "<html><body><table>" + "\n".join(rows) + "</table></body></html>"


GOOD = [30.0, -10.0, 25.0, -10.0, 20.0] * 60


@pytest.fixture
def env(tmp_path, monkeypatch):
    from tests.native_process import fake_session_commands
    fake_session_commands(monkeypatch)
    monkeypatch.setattr(paths, "is_windows", lambda: False)
    prefix = tmp_path / "wine"
    mt5 = prefix / "drive_c" / "Program Files" / "MetaTrader 5"
    (mt5 / "MQL5" / "Experts").mkdir(parents=True)
    for exe in ("terminal64.exe", "MetaEditor64.exe"):
        (mt5 / exe).write_bytes(b"")
    lib = tmp_path / "safety"
    (lib / "Include" / "MT5X" / "Safety").mkdir(parents=True)
    names = ["HDK_SafetyTypes.mqh", "HDK_Logger.mqh", "HDK_RiskGuard.mqh", "HDK_SafeExecutor.mqh"]
    for name in names:
        (lib / "Include" / "MT5X" / "Safety" / name).write_text("// lib\n")
    (lib / "safety.json").write_text(json.dumps({"version": "1.0.0", "include_dir": "MT5X/Safety", "files": names}))
    robots_dir = tmp_path / "robos"
    config = configparser.ConfigParser()
    config.read_dict({"MT5": {"path": str(mt5), "wine_prefix": str(prefix)},
                      "DATABASE": {"type": "sqlite", "path": str(tmp_path / "db.sqlite")},
                      "STRATEGY": {"initial_equity": "10000"},
                      "ROBOTS": {"library_dirs": str(robots_dir), "safety_lib_dir": str(lib)}})
    configure_test_safety(config, tmp_path)
    scaffold.create("Ciclo", robots_dir, config=config, magic=777001)
    cfg = tmp_path / "config.ini"
    with open(cfg, "w", encoding="utf-8") as f:
        config.write(f)
    db = DatabaseManager(db_type="sqlite", db_path=str(tmp_path / "db.sqlite"))
    env = SimpleNamespace(config=config, db=db, mt5=mt5, robots_dir=robots_dir, args=["--config", str(cfg)], calls=[])
    env.catalog = lambda: catalog.load_catalog(config, db)
    env.robot = lambda: catalog.get_robot(env.catalog(), "ciclo")

    def fake(log=LOG_OK, pnl=GOOD):
        """Faz o papel do MetaEditor (/compile:) e do terminal (/config:) conforme a linha de comando."""
        def run(command_line, cwd, prefix, timeout):
            quoted = command_line.split('"')
            if "/compile:" in command_line:
                env.calls.append("compile")
                source, log_file = (paths.wine_to_linux_path(quoted[i], prefix) for i in (3, 5))
                log_file.write_bytes(log.encode("utf-16"))
                if "0 errors" in log:
                    source.with_suffix(".ex5").write_bytes(b"ex5")
                return 1
            env.calls.append("tester")
            ini_path = paths.wine_to_linux_path(quoted[3], prefix)
            ini = configparser.ConfigParser()
            ini.optionxform = str
            ini.read_string(ini_path.read_bytes().decode("utf-16"))
            (ini_path.parent / (ini["Tester"]["Report"] + ".htm")).write_bytes(report_html(pnl).encode("utf-16"))
            return 0
        return run
    env.fake = fake
    return env


def run_check(env, **kwargs):
    return pipeline.check(env.db, env.config, env.robot(), env.catalog(), "EURUSD", "H1",
                          run=env.fake(**kwargs), is_running=lambda: False)


def test_check_runs_the_four_steps_in_order(env):
    result = run_check(env)
    assert [(s.id, s.status) for s in result.steps] == [
        ("contract", pipeline.PASSED), ("compile", pipeline.PASSED), ("backtest", pipeline.PASSED),
        ("battery", pipeline.WARNED)]                                  # avisos da bateria não reprovam
    assert result.ok and env.calls == ["compile", "compile", "tester"] and result.report.approved
    assert "300 operações" in result.steps[2].detail and result.steps[2].data["run_id"]
    assert env.robot().stage == catalog.APPROVED and len(store.load_runs(env.db)) == 1
    assert result.to_dict()["battery"]["approved"] is True


def test_a_compile_error_stops_before_the_backtest(env):
    result = run_check(env, log=LOG_BAD)
    assert [s.status for s in result.steps] == [pipeline.PASSED, pipeline.FAILED, pipeline.SKIPPED, pipeline.SKIPPED]
    assert not result.ok and env.calls == ["compile"] and "undeclared identifier" in result.steps[1].detail
    assert store.load_runs(env.db) == [] and result.report is None


def test_each_later_step_can_fail_with_its_reason(env):
    empty = run_check(env, pnl=[])
    assert empty.steps[2].status == pipeline.FAILED and "nenhuma operação" in empty.steps[2].detail
    assert empty.steps[3].status == pipeline.SKIPPED
    losing = run_check(env, pnl=[10.0, -12.0] * 150)
    assert losing.steps[3].status == pipeline.FAILED and "Expectativa positiva" in losing.steps[3].detail
    assert not losing.ok and not losing.report.approved
    busy = pipeline.check(env.db, env.config, env.robot(), env.catalog(), "EURUSD", "H1", run=env.fake(),
                          is_running=lambda: True)
    assert busy.steps[2].status == pipeline.FAILED and "terminal informado pelo executor" in busy.steps[2].detail


def test_contract_step_fails_on_a_broken_robot_json_and_warns_without_one(env):
    manifest = env.robots_dir / "Experts" / "Ciclo" / "Ciclo.robot.json"
    data = json.loads(manifest.read_text(encoding="utf-8"))
    manifest.write_text(json.dumps({**data, "contract": 5}), encoding="utf-8")
    broken = run_check(env)
    assert broken.steps[0].status == pipeline.FAILED and env.calls == [] and "não suportada" in broken.steps[0].detail
    manifest.unlink()
    legacy = run_check(env)
    assert legacy.steps[0].status == pipeline.WARNED and legacy.steps[1].status == pipeline.PASSED
    python = next(r for r in env.catalog() if r.kind == catalog.PYTHON)
    with pytest.raises(ValueError, match="estratégia Python"):
        pipeline.check(env.db, env.config, python, env.catalog(), "EURUSD", "H1")


def test_junit_report_lists_steps_and_battery_items(env, tmp_path):
    result = run_check(env, pnl=[10.0, -12.0] * 150)
    path = tmp_path / "relatorio.xml"
    pipeline.write_junit(result, path)
    suite = ET.parse(path).getroot()
    cases = suite.findall("testcase")
    assert suite.tag == "testsuite" and int(suite.get("tests")) == len(cases) == 4 + len(result.report.items)
    failed = {c.get("name") for c in cases if c.find("failure") is not None}
    assert {"Bateria de segurança", "Expectativa positiva com custos"} <= failed
    assert int(suite.get("failures")) == len(failed)
    stopped = run_check(env, log=LOG_BAD)
    pipeline.write_junit(stopped, path)
    suite = ET.parse(path).getroot()
    assert (suite.get("tests"), suite.get("failures"), suite.get("skipped")) == ("4", "1", "2")


def test_cli_check_exit_code_json_and_report(env, monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(mt5_tools, "terminal_running", lambda *a: False)
    monkeypatch.setattr(mt5_tools, "run_windows_command", env.fake())
    report = tmp_path / "ci.xml"
    argv = env.args + ["robots", "check", "ciclo", "--symbol", "EURUSD", "--tf", "H1", "--report", str(report)]
    assert cli.main(argv) == cli.EXIT_OK
    out = capsys.readouterr().out
    assert "✓ Instalar e compilar" in out and "⚠ Bateria de segurança" in out and "Tudo passou" in out
    assert ET.parse(report).getroot().get("failures") == "0"
    monkeypatch.setattr(mt5_tools, "run_windows_command", env.fake(log=LOG_BAD))
    assert cli.main(["--config", env.args[1], "--json", "robots", "check", "ciclo", "--symbol", "EURUSD", "--tf", "H1"]) \
        == cli.EXIT_FAIL
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False and payload["data"]["check"]["steps"][1]["status"] == pipeline.FAILED
    assert cli.main(env.args + ["robots", "check", "ciclo"]) == cli.EXIT_USAGE
