"""T8.5: backtest de robô MQL5 pelo Strategy Tester (ini, relatório, CSV de trades e CLI), sem MT5 nem Wine."""
import configparser
import datetime as dt
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from mt5_extracao import cli
from mt5_extracao.bootstrap import paths
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.robots import backtest, catalog, mt5_tools, store

LIBRARY = Path(__file__).parent / "robots_library"
WINDOWS_DIR = "C:\\Program Files\\MetaTrader 5"

# Linhas no formato real do relatório (terminal em português, build 6257): títulos traduzidos, tipo e direção
# em inglês, milhar com espaço. A tabela de ordens tem 11 células por linha; a de negócios, 13.
DEALS = [("2025.01.01 00:00:00", "", "balance", "", "0.00", "0.00", "10 000.00", "10 000.00"),
         ("2025.01.02 12:00:00", "EURUSD", "sell", "in", "0.00", "0.00", "0.00", "10 000.00"),
         ("2025.01.07 16:00:00", "EURUSD", "buy", "out", "0.00", "-0.57", "-85.69", "9 913.74"),
         ("2025.01.09 05:00:00", "EURUSD", "buy", "in", "0.00", "0.00", "0.00", "9 913.74"),
         ("2025.01.09 06:00:00", "EURUSD", "sell", "out", "-1.00", "0.00", "1 201.00", "11 113.74"),
         ("2025.01.10 07:00:00", "EURUSD", "sell", "in", "0.00", "0.00", "0.00", "11 113.74"),
         ("2025.01.10 11:00:00", "EURUSD", "buy", "out", "0.00", "0.00", "-300.00", "10 813.74")]


def report_html(deals=DEALS):
    rows = "".join(
        f'<tr bgcolor="#FFFFFF" align=right><td>{t}</td><td>{n}</td><td>{sym}</td><td>{kind}</td><td>{way}</td>'
        f"<td>0.19</td><td>1.03516</td><td>{n}</td><td>{com}</td><td>{swap}</td><td>{profit}</td><td>{bal}</td>"
        "<td></td></tr>\n" for n, (t, sym, kind, way, com, swap, profit, bal) in enumerate(deals, 1))
    return ('<html><body><table>\n<tr><td colspan="3">Lucro Líquido Total:</td><td><b>814.74</b></td></tr>\n'
            '<tr align="center"><th colspan="13"><div><b>Ordens</b></div></th></tr>\n'
            '<tr bgcolor="#FFFFFF" align=right><td>2025.01.02 12:00:00</td><td>2</td><td>EURUSD</td><td>sell</td>'
            '<td colspan="2">0.19 / 0.19</td><td>0.00000</td><td></td><td></td>'
            '<td colspan="2">2025.01.02 12:00:00</td><td>filled</td><td></td></tr>\n'
            '<tr align="center"><th colspan="13"><div><b>Transações</b></div></th></tr>\n'
            '<tr align="center"><td nowrap><b>Horário</b></td>' + "<td nowrap><b>x</b></td>" * 12 + "</tr>\n" + rows +
            '<tr align="right"><td nowrap colspan="8"></td><td nowrap><b>-1.00</b></td><td nowrap><b>-0.57</b></td>'
            "<td nowrap><b>815.31</b></td><td nowrap><b>10 813.74</b></td><td nowrap></td></tr>\n</table></body></html>")


@pytest.fixture
def home(tmp_path, monkeypatch):
    """Prefixo Wine falso: MT5 portátil com o DemoGuard já instalado e 'compilado', e a pasta Common\\Files."""
    monkeypatch.setattr(paths, "is_windows", lambda: False)
    prefix = tmp_path / "wine"
    mt5 = prefix / "drive_c" / "Program Files" / "MetaTrader 5"
    (mt5 / "MQL5" / "Experts").mkdir(parents=True)
    for exe in ("terminal64.exe", "MetaEditor64.exe"):
        (mt5 / exe).write_bytes(b"")
    common = prefix / "drive_c" / "users" / "u" / "AppData" / "Roaming" / "MetaQuotes" / "Terminal" / "Common" / "Files"
    common.mkdir(parents=True)
    config = configparser.ConfigParser()
    config.read_dict({"MT5": {"path": str(mt5), "wine_prefix": str(prefix)},
                      "DATABASE": {"type": "sqlite", "path": str(tmp_path / "db.sqlite")},
                      "STRATEGY": {"initial_equity": "10000"}, "ROBOTS": {"library_dirs": str(LIBRARY)}})
    robot = next(r for r in catalog.scan_library(LIBRARY) if r.name == "DemoGuard")
    source = mt5_tools.install(robot, mt5)[0].target
    source.with_suffix(".ex5").write_bytes(b"ex5")
    cfg = tmp_path / "config.ini"
    with open(cfg, "w", encoding="utf-8") as f:
        config.write(f)
    return SimpleNamespace(prefix=prefix, mt5=mt5, common=common, config=config, robot=robot, source=source,
                           db=DatabaseManager(db_type="sqlite", db_path=str(tmp_path / "db.sqlite")),
                           args=["--config", str(cfg)])


def fake_terminal(html=None, calls=None, csv=None):
    """Faz o papel do terminal: lê o .ini indicado na linha de comando e grava o relatório (UTF-16)."""
    def run(command_line, cwd, prefix, timeout):
        ini_path = paths.wine_to_linux_path(command_line.split('"')[3], prefix)
        ini = configparser.ConfigParser()
        ini.optionxform = str
        ini.read_string(ini_path.read_bytes().decode("utf-16"))
        if calls is not None:
            calls.append((command_line, dict(ini["Tester"])))
        if html is not None:
            (ini_path.parent / (ini["Tester"]["Report"] + ".htm")).write_bytes(html.encode("utf-16"))
        if csv is not None:
            csv[0].write_text(csv[1], encoding="cp1252")
        return 0
    return run


# --- leitura do relatório e do CSV ----------------------------------------------------

def test_parse_report_reads_closed_deals_whatever_the_language():
    deposit, results = backtest.parse_report(report_html())
    assert deposit == 10000.0
    assert [r[0] for r in results] == ["2025.01.07 16:00:00", "2025.01.09 06:00:00", "2025.01.10 11:00:00"]
    assert [round(r[1], 2) for r in results] == [-86.26, 1200.0, -300.0]        # lucro + swap + comissão
    assert backtest.parse_report("<html><body>sem tabela</body></html>") == (None, [])


def test_metrics_from_results_match_the_card_keys():
    m = backtest.metrics_from_results(backtest.parse_report(report_html())[1], 10000.0)
    assert m["trades"] == 3 and m["win_rate"] == pytest.approx(1 / 3)
    assert m["profit_factor"] == pytest.approx(1200 / 386.26) and m["net_profit"] == pytest.approx(813.74)
    assert m["return_pct"] == pytest.approx(0.081374) and m["expectancy_r"] is None
    assert m["max_drawdown_pct"] == pytest.approx(300 / 11113.74)                # do pico até o fim
    empty = backtest.metrics_from_results([], 10000.0)
    assert (empty["trades"], empty["profit_factor"], empty["max_drawdown_pct"]) == (0, 0.0, 0.0)
    assert backtest.metrics_from_results([["t", 5.0, 1.0]], 100.0)["profit_factor"] == backtest.PF_CAP


def test_read_hdk_trades_follows_the_hadouken_contract(tmp_path):
    path = tmp_path / "HDK_trades_1.csv"
    path.write_text("time;symbol;type;volume;result\n2026.01.05 00:00;EURUSD;close_sell;0.10;19.90\n"
                    "2026.01.06 01:00;EURUSD;close_buy;0.10;-9.00\n\n", encoding="cp1252")
    assert backtest.read_hdk_trades(path) == [["2026.01.05 00:00", 19.9, 0.1], ["2026.01.06 01:00", -9.0, 0.1]]
    path.write_text("a;b\n1;2\n", encoding="cp1252")
    with pytest.raises(ValueError, match="time;symbol;type;volume;result"):
        backtest.read_hdk_trades(path)


# --- ini e execução -------------------------------------------------------------------

def test_tester_ini_and_set_files(home):
    text = mt5_tools.tester_ini("Demo\\DemoGuard", "WIN$N", "M5", dt.date(2025, 1, 2), dt.date(2025, 3, 4), 5000,
                                "mt5x_demoguard", "4", "mt5x_demoguard.set")
    ini = configparser.ConfigParser()
    ini.optionxform = str
    ini.read_string(text)
    assert dict(ini["Tester"]) == {
        "Expert": "Demo\\DemoGuard", "ExpertParameters": "mt5x_demoguard.set", "Symbol": "WIN$N", "Period": "M5",
        "Optimization": "0", "Model": "4", "FromDate": "2025.01.02", "ToDate": "2025.03.04", "ForwardMode": "0",
        "Deposit": "5000.00", "Report": "mt5x_demoguard", "ReplaceReport": "1", "ShutdownTerminal": "1"}
    assert "ExpertParameters" not in mt5_tools.tester_ini("A", "X", "H1", dt.date(2025, 1, 1), dt.date(2025, 2, 1), 1, "r")
    assert mt5_tools.write_tester_set(home.mt5, "t", {"InpRiskPct": "0.5", "InpOn": "true"}) == "t.set"
    saved = (home.mt5 / "MQL5" / "Profiles" / "Tester" / "t.set").read_bytes().decode("utf-16")
    assert saved == "InpRiskPct=0.5\r\nInpOn=true\r\n"
    assert mt5_tools.expert_name(home.source.with_suffix(".ex5"), home.mt5) == "Demo\\DemoGuard"
    assert mt5_tools.common_files_dir(home.config) == home.common
    home.config["ROBOTS"]["tester_model"] = "9"
    with pytest.raises(ValueError, match="tester_model"):
        mt5_tools.tester_model(home.config)


def test_run_tester_command_line_and_failures(home):
    calls = []
    report = mt5_tools.run_tester(home.config, home.mt5, "[Tester]\r\nReport=r1\r\n", "r1",
                                  fake_terminal("<html></html>", calls), is_running=lambda: False)
    assert report == home.mt5 / "r1.htm" and not (home.mt5 / "r1.ini").exists()
    assert calls[0][0] == f'"{WINDOWS_DIR}\\terminal64.exe" /portable /config:"{WINDOWS_DIR}\\r1.ini"'
    with pytest.raises(RuntimeError, match="--close-terminal"):
        mt5_tools.run_tester(home.config, home.mt5, "[Tester]\r\nReport=r1\r\n", "r1", is_running=lambda: True)
    with pytest.raises(RuntimeError, match="não gerou o relatório"):             # o relatório antigo não vale
        mt5_tools.run_tester(home.config, home.mt5, "[Tester]\r\nReport=r1\r\n", "r1", fake_terminal(None),
                             is_running=lambda: False)

    def slow(*a):
        raise subprocess.TimeoutExpired("wine", 1)
    with pytest.raises(RuntimeError, match="não terminou"):
        mt5_tools.run_tester(home.config, home.mt5, "[Tester]\r\nReport=r1\r\n", "r1", slow, is_running=lambda: False)
    assert not (home.mt5 / "r1.ini").exists()


def test_run_mql5_records_the_run_from_the_report(home):
    calls = []
    run = backtest.run_mql5(home.db, home.config, home.robot, "EURUSD", "H1", dt.datetime(2025, 1, 1),
                            dt.datetime(2025, 2, 1), preset="Demo_Conservador", params={"InpRiskPct": "0.25"},
                            run=fake_terminal(report_html(), calls), is_running=lambda: False)
    ini = calls[0][1]
    assert (ini["Expert"], ini["Symbol"], ini["Period"], ini["Model"]) == ("Demo\\DemoGuard", "EURUSD", "H1", "1")
    assert (ini["FromDate"], ini["ToDate"], ini["Deposit"]) == ("2025.01.01", "2025.02.01", "10000.00")
    preset = (home.mt5 / "MQL5" / "Profiles" / "Tester" / ini["ExpertParameters"]).read_bytes().decode("utf-16")
    assert "InpRiskPct=0.25\r\n" in preset                                       # --param vence o preset
    m = run["metrics"]
    assert (m["trades"], m["trades_source"], m["tester_model"]) == (3, backtest.REPORT_SOURCE, "OHLC de 1 minuto")
    assert run["source"] == "strategy tester" and run["params"]["preset"] == "Demo_Conservador"
    assert run["equity"][0] == ["2025-01-01 00:00:00", 10000.0]
    assert run["equity"][-1] == ["2025-01-10 11:00:00", pytest.approx(10813.74)]
    saved = store.load_runs(home.db, home.robot.key)[0]
    assert saved["run_id"] == run["run_id"] and saved["metrics"]["expectancy_r"] is None
    assert saved["trades"][0] == ["2025-01-07 16:00:00", pytest.approx(-86.26), 0.19] and len(saved["trades"]) == 3
    robot = catalog.get_robot(catalog.load_catalog(home.config, home.db), "demoguard")
    assert robot.stage == catalog.TESTED and robot.headline["trades"] == 3 and robot.headline["approved"] is None


def test_run_mql5_prefers_the_hadouken_csv_and_drops_a_stale_one(home, tmp_path):
    library = tmp_path / "lib"
    ea = library / "Experts" / "Hdk.mq5"
    ea.parent.mkdir(parents=True)
    ea.write_text('#property version "7.00"\ninput long InpMagicNumber = 4242; // Magic\n'
                  "input bool InpExportTrades = false; // Exportar\nint OnInit(){return(0);}\nvoid OnTick(){}\n",
                  encoding="utf-8")
    robot = catalog.robot_from_file(ea)
    target = mt5_tools.install(robot, home.mt5)[0].target
    target.with_suffix(".ex5").write_bytes(b"ex5")
    stale = home.common / "HDK_trades_4242.csv"
    stale.write_text("time;symbol;type;volume;result\n2020.01.01 00:00;X;close_buy;1;999.00\n", encoding="cp1252")
    kwargs = dict(start=dt.datetime(2025, 1, 1), end=dt.datetime(2025, 2, 1), is_running=lambda: False)
    run = backtest.run_mql5(home.db, home.config, robot, "EURUSD", "H1", run=fake_terminal(report_html()), **kwargs)
    assert run["metrics"]["trades_source"] == backtest.REPORT_SOURCE and not stale.exists()
    assert run["params"] == {"InpExportTrades": "true"}                          # ligado sozinho para o HADOUKEN
    fresh = "time;symbol;type;volume;result\n2025.01.05 00:00;EURUSD;close_sell;0.10;19.90\n"
    run = backtest.run_mql5(home.db, home.config, robot, "EURUSD", "H1",
                            run=fake_terminal(report_html(), csv=(stale, fresh)), **kwargs)
    assert (run["metrics"]["trades_source"], run["metrics"]["trades"]) == (backtest.CSV_SOURCE, 1)
    assert run["equity"][-1] == ["2025-01-05 00:00:00", pytest.approx(10019.9)]


def test_run_mql5_refuses_what_cannot_run(home):
    ok = dict(run=fake_terminal(report_html()), is_running=lambda: False)
    with pytest.raises(ValueError, match="Preset desconhecido"):
        backtest.run_mql5(home.db, home.config, home.robot, "EURUSD", "H1", preset="Nenhum", **ok)
    with pytest.raises(ValueError, match="não tem o parâmetro"):
        backtest.run_mql5(home.db, home.config, home.robot, "EURUSD", "H1", params={"Inexistente": "1"}, **ok)
    with pytest.raises(ValueError, match="anterior ao fim"):
        backtest.run_mql5(home.db, home.config, home.robot, "EURUSD", "H1", dt.datetime(2025, 2, 1),
                          dt.datetime(2025, 1, 1), **ok)
    python = next(r for r in catalog.load_catalog() if r.kind == catalog.PYTHON)
    with pytest.raises(ValueError, match="run_python"):
        backtest.run_mql5(home.db, home.config, python, "EURUSD", "H1", **ok)
    home.source.with_suffix(".ex5").unlink()
    with pytest.raises(ValueError, match="mt5x robots compile demoguard"):
        backtest.run_mql5(home.db, home.config, home.robot, "EURUSD", "H1", **ok)
    assert store.load_runs(home.db) == []


# --- CLI ------------------------------------------------------------------------------

def test_cli_backtest_of_an_mql5_robot(home, monkeypatch, capsys):
    monkeypatch.setattr(mt5_tools, "terminal_running", lambda: False)
    monkeypatch.setattr(mt5_tools, "run_windows_command", fake_terminal(report_html()))
    argv = home.args + ["robots", "backtest", "demoguard", "--symbol", "EURUSD", "--tf", "H1",
                        "--from", "2025-01-01", "--to", "2025-02-01", "--preset", "Demo_Conservador"]
    assert cli.main(argv) == cli.EXIT_OK
    out = capsys.readouterr().out
    assert "pelo Strategy Tester" in out and "Operações: 3" in out and "relatório do Strategy Tester" in out
    assert cli.main(home.args + ["robots", "list", "demoguard"]) == cli.EXIT_OK
    assert "estágio: Testado" in capsys.readouterr().out


def test_cli_backtest_with_the_terminal_open(home, monkeypatch, capsys):
    events = []
    monkeypatch.setattr(mt5_tools, "terminal_running", lambda: "closed" not in events)
    monkeypatch.setattr(mt5_tools, "run_windows_command", fake_terminal(report_html()))
    argv = home.args + ["robots", "backtest", "demoguard", "--symbol", "EURUSD", "--tf", "H1"]
    assert cli.main(argv) == cli.EXIT_FAIL
    assert "--close-terminal" in capsys.readouterr().err and store.load_runs(home.db) == []
    monkeypatch.setattr(mt5_tools, "release_terminal", lambda: events.append("closed") or True)
    monkeypatch.setattr(mt5_tools, "restart_bridge", lambda: events.append("bridge"))
    assert cli.main(argv + ["--close-terminal"]) == cli.EXIT_OK
    assert events == ["closed", "bridge"] and len(store.load_runs(home.db)) == 1   # fecha, testa e religa a ponte
