"""T8.6: bateria de segurança (itens de código/preset e estatística), sem MT5."""
import configparser
import uuid
from pathlib import Path

import pytest

from mt5_extracao import cli
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.robots import battery, catalog, store

LIBRARY = Path(__file__).parent / "robots_library"
SPEC = {"point": 0.00001, "digits": 5, "tick_size": 0.00001, "tick_value": 1.0, "volume_min": 0.01,
        "volume_step": 0.01, "volume_max": 100.0, "contract_size": 100000.0, "currency_profit": "USD", "spread": 10}


@pytest.fixture
def db(tmp_path):
    return DatabaseManager(db_type="sqlite", db_path=str(tmp_path / "db.sqlite"))


def library():
    return catalog.scan_library(LIBRARY)


def by_id(items):
    return {i.id: i for i in items}


def save_run(db, key, pnl, symbol="EURUSD", volume=0.1, initial=10000.0, trades=True):
    run = {"run_id": uuid.uuid4().hex, "robot_key": key, "source": "strategy tester", "symbol": symbol,
           "timeframe": "H1", "data_start": "2025-01-01 00:00:00", "data_end": "2026-01-01 00:00:00", "bars": 0,
           "params": {}, "metrics": {"trades": len(pnl), "initial_equity": initial}, "equity": [],
           "trades": [[f"2025-01-01 {i % 24:02d}:00:00", float(p), volume] for i, p in enumerate(pnl)] if trades else [],
           "created_at": "2026-01-02 10:00:00"}
    store.save_run(db, run)
    return run


GOOD = [30.0, -10.0, 25.0, -10.0, 20.0] * 60          # 300 operações, profit factor 3,75
BAD = [10.0, -12.0] * 150                             # perde na média


def mql_file(tmp_path, name, body, folder="Experts"):
    path = tmp_path / "lib" / folder / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    return path


SAFE_EA = ('#property version "1.00"\n#include <Framework/Safety/Exec.mqh>\n'
           "input double InpRiskPct = 0.5; // Risco por trade (%)\n"
           "input double InpDailyLossPct = 2.0; // Perda máxima diária (%)\n"
           "input double InpMaxDrawdownPct = 10.0; // Drawdown máximo (%)\n"
           "input double InpStopAtrMult = 1.5; // Stop (x ATR)\n"
           "input bool InpBlockAveraging = true; // Proibir preço médio\n"
           "input long InpMagicNumber = 9001; // Magic\n"
           "int OnInit(){return(0);}\nvoid OnTick(){ TryEnter(); }\n")


def safe_robot(tmp_path, ea=SAFE_EA, exec_body="void TryEnter(){ trade.Buy(0.1); }\n"):
    mql_file(tmp_path, "Framework/Safety/Exec.mqh", exec_body, folder="Include")
    path = mql_file(tmp_path, "Seguro.mq5", ea)
    path.with_suffix(".ex5").write_bytes(b"ex5")
    return catalog.robot_from_file(path)


# --- estática -------------------------------------------------------------------------

def test_static_items_of_a_robot_inside_the_safety_layer(tmp_path):
    robot = safe_robot(tmp_path)
    items = by_id(battery.static_items(robot, None, [robot]))
    assert {i: items[i].status for i in items} == {
        "compile": battery.OK, "encoding": battery.OK, "stop": battery.OK, "averaging": battery.OK,
        "orders": battery.OK, "dll": battery.OK, "magic": battery.OK, "limits": battery.OK}
    assert "0,50% por operação (teto 1,00%)" in items["limits"].detail and "magic 9001" in items["magic"].detail


def test_static_items_catch_each_problem(tmp_path):
    loose = SAFE_EA.replace("void OnTick(){ TryEnter(); }", "void OnTick(){ OrderSend(req, res); } // direto")
    robot = safe_robot(tmp_path, loose.replace("InpRiskPct = 0.5", "InpRiskPct = 2.5")
                       .replace("InpDailyLossPct = 2.0", "InpDailyLossPct = 0")
                       .replace("InpBlockAveraging = true", "InpBlockAveraging = false")
                       .replace("InpStopAtrMult = 1.5", "InpStopAtrMult = 0")
                       .replace("InpMagicNumber = 9001", "InpMagicNumber = 4242"))
    Path(robot.path).with_suffix(".ex5").unlink()
    Path(robot.path).write_bytes(Path(robot.path).read_bytes() + "// çã\n".encode("cp1252"))
    robot = catalog.robot_from_file(robot.path)
    items = by_id(battery.static_items(robot, None, [robot] + library()))      # o DemoGuard também usa o magic 4242
    failed = {i for i, item in items.items() if item.status == battery.FAIL}
    assert failed == {"compile", "stop", "averaging", "orders", "magic", "limits"}
    assert items["encoding"].status == battery.WARN and "Seguro.mq5" in items["encoding"].detail
    assert "Seguro.mq5" in items["orders"].detail and "DemoGuard" in items["magic"].detail
    assert "2,50% por operação" in items["limits"].detail and "sem limite de perda diário" in items["limits"].detail
    assert all(item.fix for item in items.values() if item.status != battery.OK)


def test_static_items_of_the_library_robots_and_presets():
    robots = library()
    guard = next(r for r in robots if r.name == "DemoGuard")
    items = by_id(battery.static_items(guard, None, robots, "Demo_Conservador"))
    assert items["orders"].status == battery.WARN                 # o exemplo não envia ordens
    assert items["limits"].status == battery.WARN and "perfil" in items["limits"].detail      # valores no código
    assert by_id(battery.static_items(guard, None, robots, "Demo_Agressivo"))["limits"].status == battery.FAIL
    with pytest.raises(ValueError, match="Preset desconhecido"):
        battery.static_items(guard, None, robots, "Nenhum")
    martingale = by_id(battery.static_items(next(r for r in robots if r.name == "Martingale"), None, robots))
    assert {i for i, item in martingale.items() if item.status == battery.FAIL} >= {"stop", "averaging", "dll"}
    assert martingale["magic"].status == battery.WARN


def test_comments_do_not_count_as_orders_and_a_robot_without_safety_fails(tmp_path):
    commented = SAFE_EA.replace("void OnTick(){ TryEnter(); }",
                                "/* OrderSend(a, b); */\nvoid OnTick(){ TryEnter(); } // trade.Buy(1)")
    assert by_id(battery.static_items(safe_robot(tmp_path, commented)))["orders"].status == battery.OK
    path = mql_file(tmp_path, "Solto/Direto.mq5", "input int InpStopLoss = 100; // Stop (pontos)\n"
                                                  "void OnTick(){ trade.Sell(0.1); }\n")
    item = by_id(battery.static_items(catalog.robot_from_file(path)))["orders"]
    assert item.status == battery.FAIL and "não tem camada de segurança" in item.detail


def test_compiled_only_robot_passes_demo_but_never_real(tmp_path, db):
    path = mql_file(tmp_path, "Caixa.ex5", "")
    robot = catalog.robot_from_file(path)
    save_run(db, robot.key, GOOD)
    report = battery.run_battery(db, None, robot, [robot])
    assert report.static_status == battery.NA and report.statistical_status == battery.APPROVED
    assert report.approved and not report.allows_real


# --- estatística ----------------------------------------------------------------------

def test_statistical_items_of_a_good_and_a_bad_run(tmp_path, db):
    robot = safe_robot(tmp_path)
    good = by_id(battery.statistical_items(robot, save_run(db, robot.key, GOOD), None, db))
    assert {i for i, item in good.items() if item.status == battery.FAIL} == set()
    assert good["trades"].detail == "300 operações" and "3,75" in good["profit_factor"].detail
    assert good["out_of_sample"].status == battery.OK and "90 operações" in good["out_of_sample"].detail
    assert good["robustness"].status == battery.WARN and good["cost_stress"].status == battery.WARN   # sem specs
    bad = by_id(battery.statistical_items(robot, save_run(db, robot.key, BAD), None, db))
    assert {"profit_factor", "expectancy", "out_of_sample"} <= {i for i, x in bad.items() if x.status == battery.FAIL}
    few = by_id(battery.statistical_items(robot, save_run(db, robot.key, GOOD[:50]), None, db))
    assert few["trades"].status == battery.FAIL and few["out_of_sample"].status == battery.WARN


def test_drawdown_ruin_and_loss_streak(tmp_path, db):
    robot = safe_robot(tmp_path)
    wild = [3000.0, -2500.0] * 150                       # ganha na média, mas com quedas enormes
    items = by_id(battery.statistical_items(robot, save_run(db, robot.key, wild), None, db))
    assert items["drawdown"].status == battery.FAIL and items["ruin"].status == battery.FAIL
    streak = ([-5.0] * 12 + [200.0]) * 25
    items = by_id(battery.statistical_items(robot, save_run(db, robot.key, streak), None, db))
    assert items["loss_streak"].status == battery.WARN and "12 perdas seguidas" in items["loss_streak"].detail


def test_cost_stress_uses_the_symbol_spec(tmp_path, db):
    robot = safe_robot(tmp_path)
    db.save_symbol_spec("EURUSD", SPEC)                   # 10 pontos a 1,00 por ponto por lote
    thin = [1.5, -1.0] * 150                              # 0,1 lote: um spread a mais custa 1,00 por operação
    item = by_id(battery.statistical_items(robot, save_run(db, robot.key, thin), None, db))["cost_stress"]
    assert item.status == battery.FAIL and "10 pontos" in item.detail
    item = by_id(battery.statistical_items(robot, save_run(db, robot.key, GOOD), None, db))["cost_stress"]
    assert item.status == battery.OK


def test_missing_run_and_old_run_without_trades(tmp_path, db):
    robot = safe_robot(tmp_path)
    items = battery.statistical_items(robot, None, None, db)
    assert [i.status for i in items] == [battery.FAIL] and "mt5x robots backtest seguro" in items[0].fix
    old = save_run(db, robot.key, GOOD, trades=False)
    items = battery.statistical_items(robot, store.load_runs(db, robot.key)[0], None, db)
    assert old["metrics"]["trades"] == 300 and "versão anterior" in items[0].detail


def test_runs_table_from_before_this_task_gains_the_trades_column(db):
    from sqlalchemy import text
    with db.engine.begin() as conn:                       # tabela como a T8.3 criava, com uma execução antiga
        conn.execute(text(store._CREATE_RUNS))
        conn.execute(text("INSERT INTO _robot_runs (run_id, robot_key, source, metrics_json, equity_json, created_at) "
                          "VALUES ('old', 'mql5:x', 'backtest', '{\"trades\": 5}', '[]', '2025-01-01 00:00:00')"))
    assert store.load_runs(db)[0]["trades"] == []
    save_run(db, "mql5:x", [1.0, 2.0])
    runs = store.load_runs(db, "mql5:x")
    assert [len(r["trades"]) for r in runs] == [2, 0] and runs[1]["run_id"] == "old"


def test_python_strategy_uses_the_saved_validation(db):
    donchian = catalog.get_robot(catalog.load_catalog(None, db), "donchian")
    run = save_run(db, donchian.key, GOOD, symbol="BBB")
    items = by_id(battery.static_items(donchian) + battery.statistical_items(donchian, run, None, db))
    assert items["stop"].status == battery.OK and items["risk"].status == battery.OK
    assert items["out_of_sample"].status == battery.FAIL and "mt5x validate --symbols BBB" in items["out_of_sample"].fix
    donchian.results = [{"symbol": "BBB", "timeframe": "H1", "approved": True, "status": "✅ aprovada",
                         "profit_factor": 1.8, "trades": 120, "positive_windows": 0.8, "robust": False}]
    items = by_id(battery.statistical_items(donchian, run, None, db))
    assert items["out_of_sample"].status == battery.OK and items["robustness"].status == battery.FAIL


# --- gravação, estágio e CLI ----------------------------------------------------------

def test_latest_battery_decides_the_stage(tmp_path, db):
    robot = safe_robot(tmp_path)
    store.save_record(db, robot.key, robot.kind, robot.name, robot.path, robot.stage)
    save_run(db, robot.key, GOOD)
    report = battery.run_battery(db, None, robot, [robot])
    assert report.approved and report.allows_real and report.run_id
    loaded = catalog.get_robot(catalog.load_catalog(None, db), "seguro")
    assert loaded.stage == catalog.APPROVED and loaded.checks[0]["check_id"] == report.check_id
    assert loaded.checks[0]["items"][0]["icon"] == "✓"

    run = save_run(db, robot.key, BAD)                    # backtest mais novo e ruim: a aprovação é revogada
    run_time = "2026-01-03 10:00:00"
    from sqlalchemy import text
    with db.engine.begin() as conn:
        conn.execute(text("UPDATE _robot_runs SET created_at = :t WHERE run_id = :r"), {"t": run_time, "r": run["run_id"]})
    second = battery.run_battery(db, None, robot, [robot])
    assert not second.approved and second.statistical_status == battery.REJECTED
    with db.engine.begin() as conn:                       # garante a ordem: a segunda bateria é a mais recente
        conn.execute(text("UPDATE _robot_checks SET created_at = '2099-01-01 00:00:00' WHERE check_id = :c"),
                     {"c": second.check_id})
    loaded = catalog.get_robot(catalog.load_catalog(None, db), "seguro")
    assert loaded.stage == catalog.TESTED and len(loaded.checks) == 2


def test_cli_battery_prints_items_and_exit_code(tmp_path, capsys):
    cfg = tmp_path / "config.ini"
    config = configparser.ConfigParser()
    config.read_dict({"DATABASE": {"type": "sqlite", "path": str(tmp_path / "db.sqlite")},
                      "ROBOTS": {"library_dirs": str(LIBRARY)}})
    with open(cfg, "w", encoding="utf-8") as f:
        config.write(f)
    args = ["--config", str(cfg)]
    assert cli.main(args + ["robots", "battery", "martingale"]) == cli.EXIT_FAIL
    out = capsys.readouterr().out
    assert "✗ Sem DLL externa: importa helper.dll" in out and "O que fazer:" in out and "Reprovada" in out
    assert "mt5x robots backtest martingale" in out
    assert cli.main(args + ["robots", "battery"]) == cli.EXIT_USAGE
    assert cli.main(args + ["robots", "battery", "demoguard", "--preset", "Nenhum"]) == cli.EXIT_USAGE


def test_panel_text_shows_the_latest_battery(tmp_path, db):
    pytest.importorskip("tkinter")
    from mt5_extracao.ui import robot_detail
    robot = safe_robot(tmp_path)
    assert "mt5x robots battery seguro" in robot_detail.battery_text(robot)
    save_run(db, robot.key, BAD)
    robot.checks = [battery.run_battery(db, None, robot, [robot]).to_dict()]
    text = robot_detail.battery_text(robot)
    assert text.startswith("✗ Reprovada") and "✗ Expectativa positiva com custos" in text
