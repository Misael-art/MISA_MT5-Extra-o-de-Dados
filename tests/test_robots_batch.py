"""T11.5: lote de backtests (robôs x ativos x timeframes), em fila e com retomada, sem MT5."""
import configparser
import json
from types import SimpleNamespace
from pathlib import Path

import pytest
from tests.native_process import compile_observer, logical_expert, configure_test_safety

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
    from tests.native_process import fake_session_commands
    fake_session_commands(monkeypatch)
    monkeypatch.setattr(paths, "is_windows", lambda: False)
    prefix = tmp_path / "wine"
    mt5 = prefix / "drive_c" / "Program Files" / "MetaTrader 5"
    (mt5 / "MQL5" / "Experts").mkdir(parents=True)
    (mt5 / "terminal64.exe").write_bytes(b"")
    (mt5 / "MetaEditor64.exe").write_bytes(b"")
    robots_dir = tmp_path / "robos"
    config = configparser.ConfigParser()
    config.read_dict({"MT5": {"path": str(mt5), "wine_prefix": str(prefix)},
                      "DATABASE": {"type": "sqlite", "path": str(tmp_path / "db.sqlite")},
                      "ROBOTS": {"library_dirs": str(robots_dir)}})
    configure_test_safety(config, tmp_path)
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
        compiled = compile_observer(command_line, prefix_)
        if compiled is not None:
            return compiled
        ini_path = paths.wine_to_linux_path(command_line.split('"')[3], prefix_)
        ini = configparser.ConfigParser()
        ini.optionxform = str
        ini.read_string(ini_path.read_bytes().decode("utf-16"))
        t = ini["Tester"]
        env.tested.append((logical_expert(ini_path.parent,t["Expert"]), t["Symbol"], t["Period"]))
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


def test_native_checkpoint_requires_same_code_preset_and_model(env):
    from mt5_extracao.robots import budget
    alfa = env.robots("alfa")
    first = budget.Budget(env.db, env.config)
    args = (env.db, env.config, alfa, ["EURUSD"], [Timeframe.H1])
    results = backtest.run_batch(*args, budget=first, run=env.terminal, is_running=lambda: False)
    assert results[0]["run"]["metrics"]["execution_identity"]["cache_eligible"] is False
    # Recuperar o experimento não certifica fonte nativa para um novo experimento.
    assert backtest.already_tested(env.db, alfa[0], "EURUSD", "H1", None, "4", config=env.config) is None
    env.tested.clear()
    recovered = backtest.run_batch(*args, batch_id=first.batch_id, run=env.terminal, is_running=lambda: False)
    assert recovered[0]["skipped"] and env.tested == []
    env.config.set("ROBOTS", "tester_model", "0")
    with pytest.raises(ValueError, match="outro plano"):
        backtest.run_batch(*args, batch_id=first.batch_id)
    env.config.set("ROBOTS", "tester_model", "4")
    with pytest.raises(ValueError, match="outro plano"):
        backtest.run_batch(*args, preset="Alfa_Conservador", batch_id=first.batch_id)
    source = Path(alfa[0].path)
    source.write_text(source.read_text(encoding="utf-8") + "\n// mudou\n", encoding="utf-8")
    with pytest.raises(ValueError, match="outro plano"):
        backtest.run_batch(env.db, env.config, env.robots("alfa"), ["EURUSD"], [Timeframe.H1], batch_id=first.batch_id)
    assert env.tested == []


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
    capsys.readouterr()
    monkeypatch.setattr(mt5_tools, "terminal_running", lambda *a: True)
    env.tested[:] = []
    assert cli.main(env.args + argv) == cli.EXIT_OK and len(env.tested) == 8
    assert not capsys.readouterr().err
