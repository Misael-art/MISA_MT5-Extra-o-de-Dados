"""T11.5: lote de backtests (robôs x ativos x timeframes), em fila e com retomada, sem MT5."""
import configparser
import json
from types import SimpleNamespace

import pytest

from mt5_extracao import cli
from mt5_extracao.bootstrap import paths
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.robots import backtest, catalog, mt5_tools, scaffold, store
from mt5_extracao.timeframes import Timeframe
from tests.market_data import trend


def report_html(symbol):
    rows = ['<tr><td>2025.01.01 00:00:00</td><td>1</td><td></td><td>balance</td><td></td><td></td><td></td><td></td>'
            "<td>0.00</td><td>0.00</td><td>10 000.00</td><td>10 000.00</td><td></td></tr>"]
    for i in range(40):
        when = f"2025.01.02 {i % 24:02d}:00:00"
        rows.append(f"<tr><td>{when}</td><td>{i + 2}</td><td>{symbol}</td><td>sell</td><td>out</td><td>0.10</td>"
                    f"<td>1.1</td><td>{i + 2}</td><td>0.00</td><td>0.00</td><td>{(30 if i % 2 else -10):.2f}</td>"
                    "<td>0.00</td><td></td></tr>")
    return "<html><body><table>" + "\n".join(rows) + "</table></body></html>"


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setattr(paths, "is_windows", lambda: False)
    prefix = tmp_path / "wine"
    mt5 = prefix / "drive_c" / "Program Files" / "MetaTrader 5"
    (mt5 / "MQL5" / "Experts").mkdir(parents=True)
    (mt5 / "terminal64.exe").write_bytes(b"")
    robots_dir = tmp_path / "robos"
    config = configparser.ConfigParser()
    config.read_dict({"MT5": {"path": str(mt5), "wine_prefix": str(prefix)},
                      "DATABASE": {"type": "sqlite", "path": str(tmp_path / "db.sqlite")},
                      "ROBOTS": {"library_dirs": str(robots_dir)}})
    for name, magic in (("Alfa", 610001), ("Beta", 610002)):
        scaffold.create(name, robots_dir, magic=magic)
    cfg = tmp_path / "config.ini"
    with open(cfg, "w", encoding="utf-8") as f:
        config.write(f)
    db = DatabaseManager(db_type="sqlite", db_path=str(tmp_path / "db.sqlite"))
    env = SimpleNamespace(config=config, db=db, args=["--config", str(cfg)], tested=[], fail=set())
    env.catalog = lambda: catalog.load_catalog(config, db)
    env.robots = lambda *keys: [catalog.get_robot(env.catalog(), k) for k in keys]
    for robot in env.robots("alfa", "beta"):
        target = mt5_tools.install(robot, mt5)[0].target
        target.with_suffix(".ex5").write_bytes(b"ex5")

    def terminal(command_line, cwd, prefix_, timeout):
        ini_path = paths.wine_to_linux_path(command_line.split('"')[3], prefix_)
        ini = configparser.ConfigParser()
        ini.optionxform = str
        ini.read_string(ini_path.read_bytes().decode("utf-16"))
        t = ini["Tester"]
        env.tested.append((t["Expert"].split("\\")[-1], t["Symbol"], t["Period"]))
        if (t["Symbol"], t["Period"]) not in env.fail:
            (ini_path.parent / (t["Report"] + ".htm")).write_bytes(report_html(t["Symbol"]).encode("utf-16"))
        return 0
    env.terminal = terminal
    return env


def batch(env, robots, resume=False, progress=None):
    return backtest.run_batch(env.db, env.config, robots, ["EURUSD", "GBPUSD"], [Timeframe.M15, Timeframe.H1],
                              resume=resume, progress=progress, run=env.terminal, is_running=lambda: False)


def test_batch_runs_every_combination_in_order_and_reports_progress(env):
    seen = []
    results = batch(env, env.robots("alfa", "beta"), progress=seen.append)
    assert len(results) == 8 == len(env.tested) and seen == results
    assert env.tested[:4] == [("Alfa", "EURUSD", "M15"), ("Alfa", "GBPUSD", "M15"), ("Alfa", "EURUSD", "H1"),
                              ("Alfa", "GBPUSD", "H1")]
    assert all(r["run"] and not r["skipped"] and not r["error"] for r in results)
    assert len(store.load_runs(env.db)) == 8


def test_one_failure_does_not_stop_the_batch_and_resume_runs_only_what_is_missing(env):
    env.fail = {("GBPUSD", "H1")}
    first = batch(env, env.robots("alfa", "beta"))
    assert [bool(r["run"]) for r in first].count(False) == 2 and len(env.tested) == 8
    assert "não gerou o relatório" in next(r["error"] for r in first if not r["run"])
    env.fail, env.tested[:] = set(), []
    again = batch(env, env.robots("alfa", "beta"), resume=True)
    assert env.tested == [("Alfa", "GBPUSD", "H1"), ("Beta", "GBPUSD", "H1")]             # só o que faltava
    assert sum(r["skipped"] for r in again) == 6 and all(r["run"] for r in again)
    env.tested[:] = []
    batch(env, env.robots("alfa"))                                                        # sem --resume: roda tudo
    assert len(env.tested) == 4


def test_resume_reruns_when_code_preset_or_model_changed(env):
    alfa = env.robots("alfa")
    batch(env, alfa)
    record = lambda robot, **kw: backtest.already_tested(env.db, robot, "EURUSD", "H1", kw.get("preset"), kw.get("model", "4"))
    assert record(alfa[0]) is not None
    assert record(alfa[0], preset="Alfa_Conservador") is None and record(alfa[0], model="0") is None
    assert backtest.already_tested(env.db, alfa[0], "EURUSD", "D1", None, "4") is None
    source = alfa[0].path
    with open(source, "a", encoding="utf-8") as f:
        f.write("\n// mudou\n")
    assert record(env.robots("alfa")[0]) is None                                          # código novo: testa de novo


def test_python_strategies_in_the_same_batch(env):
    env.db.save_ohlcv_data("EURUSD", Timeframe.H1.label, trend(900, step=0.02, noise=0.3, freq="h", spread=2, volume=500))
    results = backtest.run_batch(env.db, env.config, env.robots("donchian", "ema_atr"), ["EURUSD", "GBPUSD"],
                                 [Timeframe.H1])
    assert [bool(r["run"]) for r in results] == [True, False, True, False] and env.tested == []
    assert "GBPUSD" in results[1]["error"]
    resumed = backtest.run_batch(env.db, env.config, env.robots("donchian"), ["EURUSD"], [Timeframe.H1], resume=True)
    assert resumed[0]["skipped"]


def test_cli_batch(env, monkeypatch, capsys):
    monkeypatch.setattr(mt5_tools, "terminal_running", lambda *a: False)
    monkeypatch.setattr(mt5_tools, "run_windows_command", env.terminal)
    argv = ["robots", "backtest", "alfa,beta", "--symbols", "EURUSD,GBPUSD", "--tfs", "M15,H1"]
    assert cli.main(env.args + argv) == cli.EXIT_OK
    out = capsys.readouterr().out
    assert "2 robô(s) × 2 ativo(s) × 2 timeframe(s) = 8 teste(s)" in out and "[8/8] Beta GBPUSD H1:" in out
    assert "Lote concluído: 8 testado(s), 0 pulado(s), 0 sem teste" in out and "mt5x panel" in out
    assert cli.main(["--config", env.args[1], "--json"] + argv + ["--resume"]) == cli.EXIT_OK
    summary = json.loads(capsys.readouterr().out)["data"]["summary"]
    assert summary == {"total": 8, "tested": 0, "skipped": 8, "failed": 0}
    assert cli.main(env.args + argv + ["--param", "InpRiskPct=2"]) == cli.EXIT_USAGE
    assert cli.main(env.args + ["robots", "backtest", "alfa,beta", "--tfs", "H1"]) == cli.EXIT_USAGE
    monkeypatch.setattr(mt5_tools, "terminal_running", lambda *a: True)
    env.tested[:] = []
    assert cli.main(env.args + argv) == cli.EXIT_FAIL and env.tested == []
    assert capsys.readouterr().err.count("--close-terminal") == 1
