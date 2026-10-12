"""T8.7: tempo real em demo (travas, início, acompanhamento e PARAR TUDO), só com terminal e conta simulados."""
import configparser
import datetime as dt
from pathlib import Path
from types import SimpleNamespace

import pytest

from mt5_extracao import cli, mt5_connector
from mt5_extracao.bootstrap import paths
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.robots import battery, catalog, live, mt5_tools, store

WINDOWS_DIR = "C:\\Program Files\\MetaTrader 5"
DEMO = {"login": 5001, "server": "Corretora-Demo", "demo": True}
REAL = {"login": 9009, "server": "Corretora-Real", "demo": False}
EA = ('#property version "1.00"\n#include <Framework/Safety/Exec.mqh>\n'
      "input double InpRiskPct = 0.5; // Risco por trade (%)\n"
      "input double InpDailyLossPct = 2.0; // Perda máxima diária (%)\n"
      "input double InpMaxDrawdownPct = 10.0; // Drawdown máximo (%)\n"
      "input double InpStopAtrMult = 1.5; // Stop (x ATR)\n"
      "input bool InpBlockAveraging = true; // Proibir preço médio\n"
      "input long InpMagicNumber = 9001; // Magic\n"
      "int OnInit(){return(0);}\nvoid OnTick(){ TryEnter(); }\n")
GOOD = [30.0, -10.0, 25.0, -10.0, 20.0] * 60


@pytest.fixture
def home(tmp_path, monkeypatch):
    """MT5 falso num prefixo Wine falso, com um robô seguro instalado, 'compilado', testado e aprovado na bateria."""
    monkeypatch.setattr(paths, "is_windows", lambda: False)
    prefix = tmp_path / "wine"
    mt5 = prefix / "drive_c" / "Program Files" / "MetaTrader 5"
    (mt5 / "MQL5" / "Experts").mkdir(parents=True)
    (mt5 / "terminal64.exe").write_bytes(b"")
    common = prefix / "drive_c" / "users" / "u" / "AppData" / "Roaming" / "MetaQuotes" / "Terminal" / "Common" / "Files"
    common.mkdir(parents=True)
    library = tmp_path / "lib"
    (library / "Include" / "Framework" / "Safety").mkdir(parents=True)
    (library / "Include" / "Framework" / "Safety" / "Exec.mqh").write_text("void TryEnter(){ trade.Buy(0.1); }\n")
    (library / "Experts").mkdir()
    (library / "Experts" / "Seguro.mq5").write_text(EA, encoding="utf-8")
    (library / "Experts" / "Seguro.ex5").write_bytes(b"ex5")
    (library / "Presets").mkdir()
    (library / "Presets" / "Calmo.set").write_text("InpRiskPct=0.25\nInpMagicNumber=9002\n", encoding="utf-8")
    config = configparser.ConfigParser()
    config.read_dict({"MT5": {"path": str(mt5), "wine_prefix": str(prefix)},
                      "DATABASE": {"type": "sqlite", "path": str(tmp_path / "db.sqlite")},
                      "ROBOTS": {"library_dirs": str(library)}})
    cfg = tmp_path / "config.ini"
    with open(cfg, "w", encoding="utf-8") as f:
        config.write(f)
    db = DatabaseManager(db_type="sqlite", db_path=str(tmp_path / "db.sqlite"))
    robot = catalog.scan_library(library)[0]
    target = mt5_tools.install(robot, mt5)[0].target
    target.with_suffix(".ex5").write_bytes(b"ex5")
    store.save_run(db, {"run_id": "r1", "robot_key": robot.key, "source": "strategy tester", "symbol": "EURUSD",
                        "timeframe": "H1", "data_start": "2025-01-01 00:00:00", "data_end": "2026-01-01 00:00:00",
                        "bars": 0, "params": {}, "metrics": {"trades": len(GOOD), "initial_equity": 10000.0},
                        "equity": [], "trades": [["2025-01-01 00:00:00", p, 0.1] for p in GOOD],
                        "created_at": "2026-01-02 10:00:00"})
    home = SimpleNamespace(prefix=prefix, mt5=mt5, common=common, config=config, db=db, args=["--config", str(cfg)])
    home.robot = lambda: catalog.get_robot(catalog.load_catalog(config, db), "seguro")
    home.approve = lambda preset=None: battery.run_battery(db, config, home.robot(), [home.robot()], preset)
    return home


def gate_map(gates):
    return {g.name: g for g in gates}


def fake_spawn(calls):
    def spawn(command_line, cwd, prefix):
        ini_path = paths.wine_to_linux_path(command_line.split('"')[3], prefix)
        ini = configparser.ConfigParser()
        ini.optionxform = str
        ini.read_string(ini_path.read_bytes().decode("utf-16"))
        calls.append((command_line, {s: dict(ini[s]) for s in ini.sections()}))
    return spawn


# --- travas ---------------------------------------------------------------------------

def test_demo_gates_all_green_only_after_the_battery(home):
    gates = gate_map(live.demo_gates(home.config, home.robot(), DEMO))
    assert not gates["Bateria de segurança aprovada"].ok and "mt5x robots battery seguro" in gates["Bateria de segurança aprovada"].fix
    assert home.approve().approved
    gates = live.demo_gates(home.config, home.robot(), DEMO)
    assert all(g.ok for g in gates) and len(gates) == 5


def test_real_account_is_refused_with_a_clear_message(home):
    home.approve()
    gate = gate_map(live.demo_gates(home.config, home.robot(), REAL))["Conta de demonstração conectada"]
    assert not gate.ok and "9009" in gate.detail and "REAL" in gate.detail and "não está disponível" in gate.fix
    calls = []
    with pytest.raises(live.GateError, match="é REAL") as error:
        live.start_demo(home.db, home.config, home.robot(), REAL, "EURUSD", "H1", spawn=fake_spawn(calls),
                        is_running=lambda: False)
    assert calls == [] and len(error.value.gates) == 5 and live.live_state(home.db, home.robot()) is None
    assert not gate_map(live.demo_gates(home.config, home.robot(), None))["Conta de demonstração conectada"].ok


def test_each_gate_blocks_the_start(home):
    home.approve()
    robot = home.robot()
    name = "Bateria de segurança aprovada"
    assert "padrão do código" in gate_map(live.demo_gates(home.config, robot, DEMO, "Calmo"))[name].detail   # outro preset
    compiled = home.mt5 / "MQL5" / "Experts" / "Seguro.ex5"
    compiled.unlink()
    assert not gate_map(live.demo_gates(home.config, robot, DEMO))["Compilado na pasta do MT5"].ok
    compiled.write_bytes(b"ex5")
    live.stop_all(home.config)
    gate = gate_map(live.demo_gates(home.config, robot, DEMO))["PARAR TUDO desligado"]
    assert not gate.ok and "--release" in gate.fix
    python = next(r for r in catalog.load_catalog(home.config, home.db) if r.kind == catalog.PYTHON)
    gates = live.demo_gates(home.config, python, DEMO)
    assert len(gates) == 1 and not gates[0].ok and "não opera" in gates[0].detail


def test_a_newer_failed_battery_closes_the_gate(home):
    from sqlalchemy import text
    home.approve()
    store.save_run(home.db, {"run_id": "r2", "robot_key": "mql5:seguro", "source": "strategy tester", "symbol": "EURUSD",
                             "timeframe": "H1", "data_start": "2025-01-01 00:00:00", "data_end": "2026-01-01 00:00:00",
                             "bars": 0, "params": {}, "metrics": {"trades": 300, "initial_equity": 10000.0}, "equity": [],
                             "trades": [["2025-01-01 00:00:00", p, 0.1] for p in [10.0, -12.0] * 150],
                             "created_at": "2099-01-01 00:00:00"})
    failed = home.approve()
    with home.db.engine.begin() as conn:
        conn.execute(text("UPDATE _robot_checks SET created_at = '2099-01-02 00:00:00' WHERE check_id = :c"),
                     {"c": failed.check_id})
    gate = gate_map(live.demo_gates(home.config, home.robot(), DEMO))["Bateria de segurança aprovada"]
    assert not failed.approved and not gate.ok and "reprovada" in gate.detail


# --- início ---------------------------------------------------------------------------

def test_start_demo_pins_the_checked_account_and_records_the_state(home):
    home.approve("Calmo")
    calls = []
    state = live.start_demo(home.db, home.config, home.robot(), DEMO, "EURUSD", "H1", "Calmo", fake_spawn(calls),
                            is_running=lambda: False)
    command_line, ini = calls[0]
    assert command_line == f'"{WINDOWS_DIR}\\terminal64.exe" /portable /config:"{WINDOWS_DIR}\\mt5x_startup.ini"'
    assert ini["Common"] == {"Login": "5001", "Server": "Corretora-Demo"}
    assert ini["Experts"] == {"Enabled": "1", "AllowLiveTrading": "1", "AllowDllImport": "0"}
    assert ini["StartUp"] == {"Expert": "Seguro", "Symbol": "EURUSD", "Period": "H1", "ExpertParameters": "Calmo.set"}
    assert (state["account"], state["magic"], state["preset"], state["stopped_at"]) == (5001, "9002", "Calmo", "")
    robot = home.robot()
    assert robot.stage == catalog.DEMO and live.live_state(home.db, robot)["started_at"] == state["started_at"]


def test_start_demo_needs_the_terminal_closed(home):
    home.approve()
    with pytest.raises(RuntimeError, match="--close-terminal"):
        live.start_demo(home.db, home.config, home.robot(), DEMO, "EURUSD", "H1", spawn=fake_spawn([]),
                        is_running=lambda: True)
    assert live.live_state(home.db, home.robot()) is None and home.robot().stage == catalog.APPROVED
    with pytest.raises(ValueError, match="Timeframe inválido"):
        live.start_demo(home.db, home.config, home.robot(), DEMO, "EURUSD", "XX", is_running=lambda: False)


def test_wine_spawn_does_not_wait_for_the_terminal(home, monkeypatch):
    seen = []
    monkeypatch.setattr(mt5_tools, "run_windows_command", lambda *a, **k: seen.append((a, k)) or 0)
    mt5_tools.spawn_windows_command('"C:\\t.exe" /config:"C:\\x.ini"', home.mt5, home.prefix)
    assert seen[0][0][0] == 'start "" "C:\\t.exe" /config:"C:\\x.ini"' and seen[0][1] == {"timeout": 60}


# --- acompanhamento e PARAR TUDO ------------------------------------------------------

def fake_mt5():
    position = lambda magic, profit, sl: SimpleNamespace(magic=magic, volume=0.1, profit=profit, swap=-0.5, sl=sl)
    deal = lambda magic, entry, profit: SimpleNamespace(magic=magic, entry=entry, profit=profit, swap=0.0, commission=-1.0)
    calls = []

    def history(start, end):
        calls.append((start, end))
        return (deal(9001, 0, 0.0), deal(9001, 1, 50.0), deal(9001, 1, -20.0), deal(7, 1, 999.0))
    mt5 = SimpleNamespace(positions_get=lambda: (position(9001, 12.0, 1.1), position(9001, -3.0, 0.0), position(7, 500.0, 1.0)),
                          history_deals_get=history, calls=calls)
    return mt5


def test_status_counts_only_this_robots_magic(home):
    mt5 = fake_mt5()
    since = dt.datetime(2026, 1, 1, 9, 0)
    numbers = live.status(mt5, "9001", since)
    assert (numbers["open_positions"], numbers["positions_without_stop"]) == (2, 1)
    assert numbers["open_volume"] == pytest.approx(0.2) and numbers["open_profit"] == pytest.approx(8.0)
    assert (numbers["closed_trades"], numbers["wins"], numbers["losses"]) == (2, 1, 1)
    assert numbers["closed_result"] == pytest.approx(28.0) and mt5.calls[0][0] == since
    empty = SimpleNamespace(positions_get=lambda: None, history_deals_get=lambda a, b: None)
    assert live.status(empty, 9001)["open_positions"] == 0


def test_stop_all_writes_the_kill_file_and_release_removes_it(home):
    assert not live.stop_active(home.config)
    path = Path(live.stop_all(home.config, "teste"))
    assert path == home.common / "HDK_KILL.txt" and "PARAR TUDO" in path.read_text(encoding="cp1252")
    assert live.stop_active(home.config) and live.release_stop(home.config) and not path.exists()
    assert not live.release_stop(home.config)


def test_recompiling_keeps_the_live_state(home, monkeypatch, capsys):
    home.approve()
    live.start_demo(home.db, home.config, home.robot(), DEMO, "EURUSD", "H1", spawn=fake_spawn([]), is_running=lambda: False)
    (home.mt5 / "MetaEditor64.exe").write_bytes(b"")

    def metaeditor(command_line, cwd, prefix, timeout):
        quoted = command_line.split('"')
        paths.wine_to_linux_path(quoted[5], prefix).write_bytes("Result: 0 errors, 0 warnings\n".encode("utf-16"))
        return 1
    monkeypatch.setattr(mt5_tools, "run_windows_command", metaeditor)
    assert cli.main(home.args + ["robots", "compile", "seguro"]) == cli.EXIT_OK
    capsys.readouterr()
    assert home.robot().stage == catalog.DEMO and live.live_state(home.db, home.robot())["account"] == 5001


# --- CLI ------------------------------------------------------------------------------

@pytest.fixture
def connected(home, monkeypatch):
    """A CLI 'conecta' a uma conta simulada; nada abre o MetaTrader 5 de verdade."""
    state = SimpleNamespace(account=DEMO, events=[], spawned=[], running=True)
    monkeypatch.setattr(cli.Context, "connector", lambda self: SimpleNamespace(account_summary=lambda: state.account))
    monkeypatch.setattr(mt5_connector, "mt5", fake_mt5())
    monkeypatch.setattr(mt5_tools, "terminal_running", lambda: state.running)

    def release():
        state.events.append("closed")
        state.running = False
        return True
    monkeypatch.setattr(mt5_tools, "release_terminal", release)
    monkeypatch.setattr(mt5_tools, "restart_bridge", lambda: state.events.append("bridge"))
    monkeypatch.setattr(mt5_tools, "spawn_windows_command", fake_spawn(state.spawned))
    return state


def test_cli_start_shows_the_gates_and_only_starts_with_consent(home, connected, capsys):
    argv = home.args + ["robots", "start", "seguro", "--symbol", "EURUSD", "--tf", "H1"]
    assert cli.main(argv) == cli.EXIT_FAIL
    out = capsys.readouterr().out
    assert "✗ Bateria de segurança aprovada" in out and "O robô não foi iniciado" in out and connected.spawned == []
    home.approve()
    assert cli.main(argv) == cli.EXIT_FAIL                         # tudo verde, mas sem autorização para reiniciar o MT5
    assert "--close-terminal" in capsys.readouterr().out and connected.spawned == [] and connected.events == []
    assert cli.main(argv + ["--close-terminal"]) == cli.EXIT_OK
    assert "iniciado na conta demo 5001" in capsys.readouterr().out
    assert connected.events == ["closed", "bridge"] and len(connected.spawned) == 1


def test_cli_start_refuses_a_real_account(home, connected, capsys):
    home.approve()
    connected.account = REAL
    argv = home.args + ["robots", "start", "seguro", "--symbol", "EURUSD", "--tf", "H1", "--close-terminal"]
    assert cli.main(argv) == cli.EXIT_FAIL
    assert "é REAL" in capsys.readouterr().out and connected.spawned == [] and connected.events == []
    assert cli.main(home.args + ["robots", "start", "seguro"]) == cli.EXIT_USAGE


def test_cli_monitor_and_stop(home, connected, capsys):
    assert cli.main(home.args + ["robots", "monitor", "seguro"]) == cli.EXIT_USAGE     # nunca iniciado
    home.approve()
    connected.running = False
    live.start_demo(home.db, home.config, home.robot(), DEMO, "EURUSD", "H1", spawn=fake_spawn([]), is_running=lambda: False)
    capsys.readouterr()
    connected.account = dict(DEMO, login=5002)
    assert cli.main(home.args + ["robots", "monitor", "seguro"]) == cli.EXIT_OK
    out = capsys.readouterr().out
    assert "Posições abertas: 2" in out and "1 posição(ões) sem stop" in out and "resultado 28,00" in out
    assert "não é a do início" in out
    connected.running = True
    assert cli.main(home.args + ["robots", "stop"]) == cli.EXIT_OK
    out = capsys.readouterr().out
    assert "PARAR TUDO acionado" in out and "NÃO são fechadas" in out and connected.events == []
    assert (home.common / "HDK_KILL.txt").is_file() and home.robot().stage == catalog.APPROVED
    assert live.live_state(home.db, home.robot())["stopped_at"]
    assert cli.main(home.args + ["robots", "stop", "--close-terminal"]) == cli.EXIT_OK
    assert "MetaTrader 5 fechado" in capsys.readouterr().out and connected.events == ["closed"]
    assert cli.main(home.args + ["robots", "stop", "--release"]) == cli.EXIT_OK
    assert "liberado" in capsys.readouterr().out and not (home.common / "HDK_KILL.txt").exists()
