"""T11.2: o mesmo backtest em vários ativos; a bateria e a aprovação valem para um ativo e um timeframe."""
import configparser
import json
from types import SimpleNamespace

import pytest
from tests.native_process import compile_observer, logical_expert, configure_test_safety

from mt5_extracao import cli
from mt5_extracao.bootstrap import paths
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.robots import backtest, battery, catalog, live, mt5_tools, scaffold, store
from tests.market_data import trend

GOOD = [30.0, -10.0, 25.0, -10.0, 20.0] * 60
BAD = [10.0, -12.0] * 150


def report_html(pnl, symbol):
    rows = ['<tr><td>2025.01.01 00:00:00</td><td>1</td><td></td><td>balance</td><td></td><td></td><td></td><td></td>'
            "<td>0.00</td><td>0.00</td><td>10 000.00</td><td>10 000.00</td><td></td></tr>"]
    for i, value in enumerate(pnl):
        when = f"2025.01.02 {i % 24:02d}:00:00"
        rows.append(f'<tr><td>{when}</td><td>{2 * i + 2}</td><td>{symbol}</td><td>buy</td><td colspan="2">0.1 / 0.1</td>'
                    f'<td>0.0</td><td>1.10000</td><td></td><td colspan="2">{when}</td><td>filled</td><td></td></tr>')
        rows.append(f"<tr><td>{when}</td><td>{2 * i + 2}</td><td>{symbol}</td><td>buy</td><td>in</td><td>0.10</td>"
                    f"<td>1.1</td><td>{2 * i + 2}</td><td>0.00</td><td>0.00</td><td>0.00</td><td>0.00</td><td></td></tr>")
        rows.append(f"<tr><td>{when}</td><td>{2 * i + 3}</td><td>{symbol}</td><td>sell</td><td>out</td><td>0.10</td>"
                    f"<td>1.1</td><td>{2 * i + 3}</td><td>0.00</td><td>0.00</td><td>{value:.2f}</td><td>0.00</td><td></td></tr>")
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
                      "STRATEGY": {"initial_equity": "10000"}, "ROBOTS": {"library_dirs": str(robots_dir)}})
    configure_test_safety(config, tmp_path)
    scaffold.create("Varios", robots_dir, magic=550001)
    cfg = tmp_path / "config.ini"
    with open(cfg, "w", encoding="utf-8") as f:
        config.write(f)
    db = DatabaseManager(db_type="sqlite", db_path=str(tmp_path / "db.sqlite"))
    env = SimpleNamespace(config=config, db=db, args=["--config", str(cfg)], tested=[])
    env.robot = lambda: catalog.get_robot(catalog.load_catalog(config, db), "varios")
    target = mt5_tools.install(env.robot(), mt5)[0].target
    target.with_suffix(".ex5").write_bytes(b"ex5")
    store.save_record(db, "mql5:varios", catalog.MQL5, "Varios", env.robot().path, catalog.COMPILED)

    def terminal(command_line, cwd, prefix_, timeout):
        compiled = compile_observer(command_line, prefix_)
        if compiled is not None:
            return compiled
        """EURUSD ganha, GBPUSD perde, XAUUSD não existe na corretora (o Tester não gera relatório)."""
        ini_path = paths.wine_to_linux_path(command_line.split('"')[3], prefix_)
        ini = configparser.ConfigParser()
        ini.optionxform = str
        ini.read_string(ini_path.read_bytes().decode("utf-16"))
        symbol = ini["Tester"]["Symbol"]
        env.tested.append(symbol)
        if symbol != "XAUUSD":
            html = report_html(GOOD if symbol == "EURUSD" else BAD, symbol)
            (ini_path.parent / (ini["Tester"]["Report"] + ".htm")).write_bytes(html.encode("utf-16"))
        return 0
    env.terminal = terminal
    return env


def test_run_many_tests_each_symbol_and_keeps_going_after_a_failure(env):
    results = backtest.run_many(env.db, env.config, env.robot(), ["EURUSD", "XAUUSD", "GBPUSD"], "H1",
                                run=env.terminal, is_running=lambda: False)
    assert [r["symbol"] for r in results] == ["EURUSD", "XAUUSD", "GBPUSD"] and env.tested == ["EURUSD", "XAUUSD", "GBPUSD"]
    assert results[0]["run"]["metrics"]["profit_factor"] == pytest.approx(3.75) and not results[0]["error"]
    assert results[1]["run"] is None and "não gerou o relatório" in results[1]["error"]
    assert results[2]["run"]["metrics"]["net_profit"] < 0
    assert {r["symbol"] for r in store.load_runs(env.db, "mql5:varios")} == {"EURUSD", "GBPUSD"}


def test_python_strategy_in_several_symbols(env):
    env.db.save_ohlcv_data("AAA", "1 dia", trend(1300, step=0.2, noise=1.0, freq="D", spread=5, volume=3000))
    donchian = catalog.get_robot(catalog.load_catalog(env.config, env.db), "donchian")
    results = backtest.run_many(env.db, env.config, donchian, ["AAA", "ZZZ"], "1 dia")
    assert results[0]["run"]["symbol"] == "AAA" and results[1]["run"] is None and "ZZZ" in results[1]["error"]


def test_battery_evaluates_the_chosen_symbol_and_the_approval_is_bound_to_it(env, monkeypatch):
    monkeypatch.setattr(battery, "static_items", lambda *a, **k: [])
    backtest.run_many(env.db, env.config, env.robot(), ["EURUSD", "GBPUSD"], "H1", run=env.terminal,
                      is_running=lambda: False)
    robot = env.robot()
    latest = battery.run_battery(env.db, env.config, robot, [robot])
    assert (latest.symbol, latest.approved) == ("GBPUSD", False)             # sem escolher, vale o mais recente
    chosen = battery.run_battery(env.db, env.config, robot, [robot], symbol="eurusd")
    assert (chosen.symbol, chosen.timeframe, chosen.approved) == ("EURUSD", "H1", True)
    with pytest.raises(ValueError, match="Testados: EURUSD, GBPUSD"):
        battery.run_battery(env.db, env.config, robot, [robot], symbol="USDJPY")
    saved = store.load_checks(env.db, robot.key)[0]
    assert (saved["symbol"], saved["timeframe"]) == ("EURUSD", "H1")

    account = {"login": 1, "server": "Demo", "demo": True}
    name = "Bateria de segurança aprovada"
    robot = env.robot()
    assert {g.name: g for g in live.demo_gates(env.config, robot, account, None, "EURUSD", "H1")}[name].ok
    other = {g.name: g for g in live.demo_gates(env.config, robot, account, None, "GBPUSD", "H1")}[name]
    assert not other.ok and "a aprovação é de EURUSD H1, não de GBPUSD H1" in other.detail
    assert "mt5x robots backtest varios --symbol GBPUSD --tf H1" in other.fix
    assert not {g.name: g for g in live.demo_gates(env.config, robot, account, None, "EURUSD", "M5")}[name].ok
    with pytest.raises(live.GateError, match="não de GBPUSD"):
        live.start_demo(env.db, env.config, robot, account, "GBPUSD", "H1", spawn=lambda *a: None,
                        is_running=lambda: False)


def test_cli_table_exit_code_and_json(env, monkeypatch, capsys):
    monkeypatch.setattr(mt5_tools, "terminal_running", lambda *a: False)
    monkeypatch.setattr(mt5_tools, "run_windows_command", env.terminal)
    argv = ["robots", "backtest", "varios", "--symbols", "EURUSD,GBPUSD", "--tf", "H1"]
    assert cli.main(env.args + argv) == cli.EXIT_OK
    out = capsys.readouterr().out
    assert "Varios em H1, 2 ativo(s):" in out and "Profit factor" in out and "teste independente" in out
    eur = next(line for line in out.splitlines() if line.strip().startswith("EURUSD"))
    assert "300" in eur and "3,75" in eur
    assert cli.main(env.args + ["robots", "backtest", "varios", "--symbols", "EURUSD,XAUUSD", "--tf", "H1"]) == cli.EXIT_FAIL
    out = capsys.readouterr().out
    assert "XAUUSD      não testado:" in out and "Sem teste: XAUUSD." in out
    assert cli.main(["--config", env.args[1], "--json"] + argv) == cli.EXIT_OK
    data = json.loads(capsys.readouterr().out)["data"]
    assert data["summary"] == {"tested": 2, "failed": 0} and "trades" not in data["runs"][0]["run"]
    assert cli.main(env.args + ["robots", "backtest", "varios", "--symbol", "EURUSD", "--symbols", "GBPUSD", "--tf",
                                "H1"]) == cli.EXIT_USAGE
    capsys.readouterr()
    monkeypatch.setattr(mt5_tools, "terminal_running", lambda *a: True)
    env.tested.clear()
    assert cli.main(env.args + argv) == cli.EXIT_OK
    assert not capsys.readouterr().err and len(env.tested) == 2
    assert cli.main(env.args + ["robots", "battery", "varios", "--symbol", "EURUSD"]) in (cli.EXIT_OK, cli.EXIT_FAIL)
    assert "sobre o backtest de EURUSD H1" in capsys.readouterr().out
