"""T10.5: bateria completa para MQL5 (fora da amostra e robustez ±20% repetindo o Strategy Tester), sem MT5."""
import configparser
import json
from types import SimpleNamespace

import pytest

from mt5_extracao import cli
from mt5_extracao.bootstrap import paths
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.robots import backtest, battery, catalog, mt5_tools, scaffold, store

GOOD = [30.0, -10.0, 25.0, -10.0, 20.0] * 60
BAD = [10.0, -12.0] * 150


def report_html(pnl):
    rows = ['<tr><td>2025.01.01 00:00:00</td><td>1</td><td></td><td>balance</td><td></td><td></td><td></td><td></td>'
            "<td>0.00</td><td>0.00</td><td>10 000.00</td><td>10 000.00</td><td></td></tr>"]
    for i, value in enumerate(pnl):
        rows.append(f"<tr><td>2025.01.02 {i % 24:02d}:00:00</td><td>{i + 2}</td><td>EURUSD</td><td>buy</td><td>out</td>"
                    f"<td>0.10</td><td>1.1</td><td>{i + 2}</td><td>0.00</td><td>0.00</td><td>{value:.2f}</td>"
                    "<td>0.00</td><td></td></tr>")
    return "<html><body><table>" + "\n".join(rows) + "</table></body></html>"


@pytest.fixture
def env(tmp_path, monkeypatch):
    """Robô criado por `robots new`, 'compilado', com um backtest gravado; o terminal é simulado."""
    monkeypatch.setattr(paths, "is_windows", lambda: False)
    prefix = tmp_path / "wine"
    mt5 = prefix / "drive_c" / "Program Files" / "MetaTrader 5"
    (mt5 / "MQL5" / "Experts").mkdir(parents=True)
    (mt5 / "terminal64.exe").write_bytes(b"")
    robots_dir = tmp_path / "robos"
    config = configparser.ConfigParser()
    config.read_dict({"MT5": {"path": str(mt5), "wine_prefix": str(prefix)},
                      "DATABASE": {"type": "sqlite", "path": str(tmp_path / "db.sqlite")},
                      "STRATEGY": {"initial_equity": "10000"}, "ROBOTS": {"library_dirs": str(robots_dir)}})
    files = scaffold.create("Fundo", robots_dir, magic=880001)
    manifest = json.loads(files[1].read_text(encoding="utf-8"))
    files[1].write_text(json.dumps({**manifest, "tunable": ["InpStopAtrMult", "InpAtrPeriod"]}), encoding="utf-8")
    cfg = tmp_path / "config.ini"
    with open(cfg, "w", encoding="utf-8") as f:
        config.write(f)
    db = DatabaseManager(db_type="sqlite", db_path=str(tmp_path / "db.sqlite"))
    env = SimpleNamespace(config=config, db=db, args=["--config", str(cfg)], tests=[], manifest=files[1])
    env.robot = lambda: catalog.get_robot(catalog.load_catalog(config, db), "fundo")
    target = mt5_tools.install(env.robot(), mt5)[0].target
    target.with_suffix(".ex5").write_bytes(b"ex5")

    def terminal(pnl_for=lambda ini, values: GOOD):
        def run(command_line, cwd, prefix_, timeout):
            ini_path = paths.wine_to_linux_path(command_line.split('"')[3], prefix_)
            ini = configparser.ConfigParser()
            ini.optionxform = str
            ini.read_string(ini_path.read_bytes().decode("utf-16"))
            section = dict(ini["Tester"])
            values = {}
            if section.get("ExpertParameters"):
                text = (mt5 / "MQL5" / "Profiles" / "Tester" / section["ExpertParameters"]).read_bytes().decode("utf-16")
                values = dict(line.split("=", 1) for line in text.splitlines() if "=" in line)
            env.tests.append((section["FromDate"], section["ToDate"], values))
            html = report_html(pnl_for(section, values))
            (ini_path.parent / (section["Report"] + ".htm")).write_bytes(html.encode("utf-16"))
            return 0
        return run
    env.terminal = terminal
    env.base = lambda pnl=GOOD: backtest.run_mql5(db, config, env.robot(), "EURUSD", "H1",
                                                  backtest.datetime(2025, 1, 1), backtest.datetime(2025, 11, 1),
                                                  run=terminal(lambda ini, values: pnl), is_running=lambda: False)
    return env


def deep(env, pnl_for=lambda ini, values: GOOD):
    robot = env.robot()
    return battery.run_battery(env.db, env.config, robot, [robot], deep=True, runner=env.terminal(pnl_for),
                               is_running=lambda: False)


def test_deep_plan_splits_the_period_and_varies_each_tunable(env):
    run = env.base()
    plan = battery.deep_plan(env.robot(), run)
    assert [t["label"] for t in plan] == ["parte inicial", "parte final", "InpStopAtrMult = 1.2",
                                          "InpStopAtrMult = 1.8", "InpAtrPeriod = 11", "InpAtrPeriod = 17"]
    assert len(plan) == 2 + 2 * 2
    first, last = plan[0], plan[1]
    assert first["start"] == backtest.datetime(2025, 1, 1) and last["end"] == backtest.datetime(2025, 11, 1)
    assert first["end"] == last["start"] and str(first["end"])[:10] == "2025-08-01"      # 70% de 304 dias
    assert plan[2]["params"]["InpStopAtrMult"] == "1.2" and plan[2]["params"]["InpExportTrades"] == "true"


def test_deep_battery_runs_every_test_and_measures_both_items(env):
    env.base()
    env.tests.clear()
    report = deep(env)
    assert len(env.tests) == 6
    assert [(a, b) for a, b, _ in env.tests[:2]] == [("2025.01.01", "2025.08.01"), ("2025.08.01", "2025.11.01")]
    assert env.tests[2][2]["InpStopAtrMult"] == "1.2" and env.tests[5][2]["InpAtrPeriod"] == "17"
    items = {i.id: i for i in report.items}
    assert items["out_of_sample"].status == battery.OK and "parte final (300 operações)" in items["out_of_sample"].detail
    assert items["robustness"].status == battery.OK and "4 variações" in items["robustness"].detail
    assert report.statistical_status == battery.APPROVED       # (a estática reprova: o robô de teste não foi compilado)
    assert len(report.items) == len({i.id for i in report.items})                      # itens trocados, não somados


def test_fragile_parameter_and_weak_final_period_fail(env):
    env.base()
    fragile = deep(env, lambda ini, values: BAD if values.get("InpAtrPeriod") == "17" else GOOD)
    item = {i.id: i for i in fragile.items}["robustness"]
    assert item.status == battery.FAIL and "InpAtrPeriod = 17 (profit factor 0,83)" in item.detail
    assert fragile.statistical_status == battery.REJECTED
    fading = deep(env, lambda ini, values: BAD if ini["FromDate"] == "2025.08.01" else GOOD)
    item = {i.id: i for i in fading.items}["out_of_sample"]
    assert item.status == battery.FAIL and "0,83 na parte final" in item.detail
    few = deep(env, lambda ini, values: GOOD[:10] if ini["FromDate"] == "2025.08.01" else GOOD)
    assert {i.id: i for i in few.items}["out_of_sample"].status == battery.WARN


def test_extra_runs_do_not_become_the_card_backtest(env):
    base = env.base()
    deep(env)
    runs = store.load_runs(env.db, "mql5:fundo")
    assert len(runs) == 7 and sum(r["source"] == backtest.DEEP_SOURCE for r in runs) == 6
    robot = env.robot()
    assert [r["run_id"] for r in robot.runs] == [base["run_id"]]
    again = battery.run_battery(env.db, env.config, robot, [robot])           # a bateria comum segue no backtest do card
    assert again.run_id == base["run_id"]
    assert {i.id: i for i in again.items}["robustness"].status == battery.WARN


def test_deep_needs_a_backtest_tunables_and_an_mql5_robot(env):
    with pytest.raises(ValueError, match="Rode antes um backtest"):
        deep(env)
    env.base()
    data = json.loads(env.manifest.read_text(encoding="utf-8"))
    env.manifest.write_text(json.dumps({**data, "tunable": []}), encoding="utf-8")
    env.tests.clear()
    item = {i.id: i for i in deep(env).items}["robustness"]
    assert len(env.tests) == 2 and item.status == battery.WARN and "tunable" in item.detail
    python = next(r for r in catalog.load_catalog(env.config, env.db) if r.kind == catalog.PYTHON)
    with pytest.raises(ValueError, match="robôs MQL5"):
        battery.run_battery(env.db, env.config, python, [], deep=True)


def test_cli_battery_deep_announces_the_cost_first(env, monkeypatch, capsys):
    env.base()
    monkeypatch.setattr(mt5_tools, "terminal_running", lambda *a: False)
    monkeypatch.setattr(mt5_tools, "run_windows_command", env.terminal())
    cli.main(env.args + ["robots", "battery", "fundo", "--deep"])
    out = capsys.readouterr().out
    assert "6 testes no Strategy Tester, cerca de 2 min" in out and "✓ Robustez com parâmetros ±20%" in out
    assert "Estatística: aprovada" in out and out.index("6 testes") < out.index("Bateria de segurança de")
    monkeypatch.setattr(mt5_tools, "terminal_running", lambda *a: True)
    assert cli.main(env.args + ["robots", "battery", "fundo", "--deep"]) == cli.EXIT_FAIL
    assert "--close-terminal" in capsys.readouterr().err
