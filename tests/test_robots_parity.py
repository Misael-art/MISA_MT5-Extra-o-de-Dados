"""T10.6: paridade Python x MQL5 (comparação de operações), com listas sintéticas e terminal simulado."""
import configparser
from types import SimpleNamespace

import pytest
from tests.native_process import compile_observer, logical_expert, configure_test_safety

from mt5_extracao import cli
from mt5_extracao.bootstrap import paths
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.robots import catalog, mt5_tools, parity, scaffold, store
from tests.market_data import trend

PY = [["2025-01-02 10:00:00", 50.0, 1.0], ["2025-01-03 10:00:00", -20.0, 1.0], ["2025-01-06 10:00:00", 30.0, 1.0]]


def shifted(trades, minutes=0, factor=1.0):
    from datetime import datetime, timedelta
    return [[(datetime.strptime(t[0], "%Y-%m-%d %H:%M:%S") + timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S"),
             t[1] * factor, t[2]] for t in trades]


def test_identical_lists_are_full_parity():
    report = parity.compare(PY, PY)
    assert report.ok and report.matched == 3 and report.matched_share == 1.0 and report.same_sign == 3
    assert report.result_difference == 0.0 and report.only_python == [] and report.only_mql5 == []


def test_small_time_shift_matches_inside_the_tolerance_only():
    assert parity.compare(PY, shifted(PY, minutes=45), tolerance_minutes=60).matched == 3
    late = parity.compare(PY, shifted(PY, minutes=90), tolerance_minutes=60)
    assert late.matched == 0 and len(late.only_python) == 3 and len(late.only_mql5) == 3 and not late.ok


def test_an_extra_trade_on_one_side_is_named(tmp_path):
    extra = PY + [["2025-01-07 15:00:00", 10.0, 1.0]]
    report = parity.compare(PY, extra)
    assert report.matched == 3 and report.only_mql5 == [["2025-01-07 15:00:00", 10.0, 1.0]] and report.only_python == []
    assert report.matched_share == pytest.approx(0.75) and not report.ok
    report = parity.compare(extra, PY)
    assert [t[0] for t in report.only_python] == ["2025-01-07 15:00:00"]


def test_each_trade_is_matched_at_most_once_and_to_the_nearest():
    python = [["2025-01-02 10:00:00", 5.0, 1.0], ["2025-01-02 10:30:00", 5.0, 1.0]]
    mql5 = [["2025-01-02 10:25:00", 5.0, 1.0]]
    report = parity.compare(python, mql5, tolerance_minutes=60)
    assert report.matched == 1 and [t[0] for t in report.only_python] == ["2025-01-02 10:30:00"]


def test_result_difference_and_sign_agreement_decide():
    close = parity.compare(PY, shifted(PY, factor=1.01))
    assert close.ok and close.result_difference == pytest.approx(0.01) and close.median_ratio == pytest.approx(1.01)
    biased = parity.compare(PY, shifted(PY, factor=0.96))        # todo par 4% menor: viés sistemático (lote, capital)
    assert biased.pairs_close_share == 1.0 and biased.median_ratio == pytest.approx(0.96) and not biased.ok
    far = parity.compare(PY, shifted(PY, factor=1.2))
    assert not far.ok and far.matched_share == 1.0 and far.result_difference == pytest.approx(0.2)
    flipped = parity.compare(PY, [[t[0], -t[1], t[2]] for t in PY])
    assert flipped.same_sign == 0 and not flipped.ok
    empty = parity.compare([], [])
    assert not empty.ok and empty.matched_share == 0.0 and empty.result_difference == 0.0
    assert parity.compare([["2025-01-02 10:00:00", 0.0, 1.0]], PY).result_difference is None
    assert set(close.to_dict()) >= {"ok", "matched_share", "result_difference", "only_python", "only_mql5"}


# --- ponta a ponta com o terminal simulado ----------------------------------------------

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
    scaffold.create("Gemeo", robots_dir, magic=990001)
    cfg = tmp_path / "config.ini"
    with open(cfg, "w", encoding="utf-8") as f:
        config.write(f)
    db = DatabaseManager(db_type="sqlite", db_path=str(tmp_path / "db.sqlite"))
    db.save_ohlcv_data("BBB", "1 dia", trend(1300, step=0.2, noise=1.0, freq="D", spread=5, volume=3000))
    robots = catalog.load_catalog(config, db)
    target = mt5_tools.install(catalog.get_robot(robots, "gemeo"), mt5)[0].target
    target.with_suffix(".ex5").write_bytes(b"ex5")
    env = SimpleNamespace(config=config, db=db, args=["--config", str(cfg)], inis=[])

    def terminal(trades):
        """Devolve, como relatório do Strategy Tester, as operações dadas (as do próprio Python, nos testes)."""
        def run(command_line, cwd, prefix_, timeout):
            compiled = compile_observer(command_line, prefix_)
            if compiled is not None:
                return compiled
            ini_path = paths.wine_to_linux_path(command_line.split('"')[3], prefix_)
            ini = configparser.ConfigParser()
            ini.optionxform = str
            ini.read_string(ini_path.read_bytes().decode("utf-16"))
            env.inis.append(dict(ini["Tester"]))
            rows = ['<tr><td>2000.01.01 00:00:00</td><td>1</td><td></td><td>balance</td><td></td><td></td><td></td>'
                    "<td></td><td>0.00</td><td>0.00</td><td>100 000.00</td><td>100 000.00</td><td></td></tr>"]
            for i, (when, pnl, volume) in enumerate(trades()):
                stamp = when[:19].replace("-", ".")
                rows.append(f"<tr><td>{stamp}</td><td>{i + 2}</td><td>BBB</td><td>sell</td><td>out</td><td>{volume}</td>"
                            f"<td>1.0</td><td>{i + 2}</td><td>0.00</td><td>0.00</td><td>{pnl:.2f}</td><td>0.00</td>"
                            "<td></td></tr>")
            html = "<html><body><table>" + "\n".join(rows) + "</table></body></html>"
            (ini_path.parent / (ini["Tester"]["Report"] + ".htm")).write_bytes(html.encode("utf-16"))
            return 0
        return run
    env.terminal = terminal
    env.robots = lambda: catalog.load_catalog(config, db)
    env.python_trades = lambda: store.load_runs(db, "python:donchian")[0]["trades"]
    return env


def test_run_compares_both_backtests_over_the_same_period(env):
    robots = env.robots()
    outcome = parity.run(env.db, env.config, catalog.get_robot(robots, "gemeo"), catalog.get_robot(robots, "donchian"),
                         "BBB", "1 dia", runner=env.terminal(env.python_trades), is_running=lambda: False)
    report = outcome["report"]
    assert report.ok and report.python_trades == report.mql5_trades == report.matched > 0
    # os dois lados começam juntos, depois do aquecimento dos indicadores do Python (não no início dos dados)
    assert env.inis[0]["FromDate"] == report.warmup_until[:10].replace("-", ".")
    assert report.warmup_until[:10] > outcome["python_run"]["data_start"][:10]
    assert all(t[0] >= report.warmup_until for t in outcome["python_run"]["trades"])
    assert env.inis[0]["Symbol"] == "BBB" and env.inis[0]["Period"] == "D1"


def test_cli_parity_reports_and_sets_the_exit_code(env, monkeypatch, capsys):
    monkeypatch.setattr(mt5_tools, "terminal_running", lambda *a: False)
    monkeypatch.setattr(mt5_tools, "run_windows_command", env.terminal(env.python_trades))
    argv = env.args + ["robots", "parity", "gemeo", "--strategy", "donchian", "--symbol", "BBB", "--tf", "D1"]
    assert cli.main(argv) == cli.EXIT_OK
    out = capsys.readouterr().out
    assert "Paridade dentro do critério" in out and "casadas (100,0%" in out
    monkeypatch.setattr(mt5_tools, "run_windows_command", env.terminal(lambda: env.python_trades()[:-1]))
    assert cli.main(argv) == cli.EXIT_FAIL
    out = capsys.readouterr().out
    assert "Só no Python (1)" in out and "Fora do critério" in out
    assert cli.main(env.args + ["robots", "parity", "gemeo", "--symbol", "BBB", "--tf", "D1"]) == cli.EXIT_USAGE
    assert cli.main(env.args + ["robots", "parity", "donchian", "--strategy", "gemeo", "--symbol", "BBB",
                                "--tf", "D1"]) == cli.EXIT_USAGE
