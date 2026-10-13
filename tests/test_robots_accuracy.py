"""
Exatidão do que leva a uma decisão de operar: o que foi testado é o que está no disco, com a configuração
avaliada, em modelo por tick, com stop em toda entrada; e só o terminal desta instalação é tocado.
"""
import configparser
import json
from types import SimpleNamespace

import pytest

from mt5_extracao.bootstrap import paths
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.robots import backtest, battery, catalog, live, mt5_tools, scaffold, store

GOOD = [30.0, -10.0, 25.0, -10.0, 20.0] * 60


def report_html(pnl, stop="1.10000"):
    """Relatório com a tabela de ordens (11 células) e a de negócios (13): uma entrada e uma saída por operação."""
    orders, deals = [], ['<tr><td>2025.01.01 00:00:00</td><td>1</td><td></td><td>balance</td><td></td><td></td><td></td>'
                         "<td></td><td>0.00</td><td>0.00</td><td>10 000.00</td><td>10 000.00</td><td></td></tr>"]
    for i, value in enumerate(pnl):
        entry, exit_ = 2 * i + 2, 2 * i + 3
        when = f"2025.01.02 {i % 24:02d}:00:00"
        orders.append(f'<tr><td>{when}</td><td>{entry}</td><td>EURUSD</td><td>buy</td><td colspan="2">0.10 / 0.10</td>'
                      f'<td>0.00000</td><td>{stop}</td><td>1.12000</td><td colspan="2">{when}</td><td>filled</td><td></td></tr>')
        orders.append(f'<tr><td>{when}</td><td>{exit_}</td><td>EURUSD</td><td>sell</td><td colspan="2">0.10 / 0.10</td>'
                      f'<td>0.00000</td><td></td><td></td><td colspan="2">{when}</td><td>filled</td><td>sl</td></tr>')
        deals.append(f"<tr><td>{when}</td><td>{entry}</td><td>EURUSD</td><td>buy</td><td>in</td><td>0.10</td><td>1.1</td>"
                     f"<td>{entry}</td><td>0.00</td><td>0.00</td><td>0.00</td><td>0.00</td><td></td></tr>")
        deals.append(f"<tr><td>{when}</td><td>{exit_}</td><td>EURUSD</td><td>sell</td><td>out</td><td>0.10</td><td>1.1</td>"
                     f"<td>{exit_}</td><td>0.00</td><td>0.00</td><td>{value:.2f}</td><td>0.00</td><td></td></tr>")
    return "<html><body><table>" + "\n".join(orders + deals) + "</table></body></html>"


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
                      "STRATEGY": {"initial_equity": "10000"}, "ROBOTS": {"library_dirs": str(robots_dir)}})
    files = scaffold.create("Exato", robots_dir, magic=440001)
    db = DatabaseManager(db_type="sqlite", db_path=str(tmp_path / "db.sqlite"))
    env = SimpleNamespace(config=config, db=db, mt5=mt5, source=files[0], preset_file=files[3], prefix=prefix)
    env.robot = lambda: catalog.get_robot(catalog.load_catalog(config, db), "exato")

    def compile_():
        target = mt5_tools.install(env.robot(), mt5)[0].target
        target.with_suffix(".ex5").write_bytes(b"ex5")
        store.save_record(db, "mql5:exato", catalog.MQL5, "Exato", str(files[0]), catalog.COMPILED)
    env.compile = compile_

    def terminal(html):
        def run(command_line, cwd, prefix_, timeout):
            ini_path = paths.wine_to_linux_path(command_line.split('"')[3], prefix_)
            ini = configparser.ConfigParser()
            ini.optionxform = str
            ini.read_string(ini_path.read_bytes().decode("utf-16"))
            (ini_path.parent / (ini["Tester"]["Report"] + ".htm")).write_bytes(html.encode("utf-16"))
            return 0
        return run

    def test_run(html=None, **kwargs):
        return backtest.run_mql5(db, config, env.robot(), "EURUSD", "H1", run=terminal(html or report_html(GOOD)),
                                 is_running=lambda: False, **kwargs)
    env.backtest = test_run
    env.battery = lambda preset=None: battery.run_battery(db, config, env.robot(), [env.robot()], preset)
    env.items = lambda preset=None: {i.id: i for i in env.battery(preset).items}
    compile_()
    return env


# --- o que foi testado é o que está no disco -------------------------------------------

def test_backtest_refuses_code_changed_since_the_last_compile(env):
    env.backtest()
    env.source.write_text(env.source.read_text(encoding="utf-8") + "\n// mudança\n", encoding="utf-8")
    with pytest.raises(ValueError, match="mudou desde a última compilação"):
        env.backtest()
    env.compile()
    assert env.backtest()["metrics"]["source_fingerprint"] == catalog.fingerprint(env.robot())


def test_fingerprint_follows_the_code_not_the_presets(env):
    before = catalog.fingerprint(env.robot())
    env.preset_file.write_text(env.preset_file.read_text(encoding="utf-8") + "InpAtrPeriod=20\n", encoding="utf-8")
    assert catalog.fingerprint(env.robot()) == before and len(before) == 16
    env.source.write_text(env.source.read_text(encoding="utf-8").replace("0.5;   // Risco", "0.6;   // Risco"),
                          encoding="utf-8")
    assert catalog.fingerprint(env.robot()) != before


def test_battery_and_stage_reject_a_backtest_of_older_code(env):
    env.backtest()
    assert env.items()["code"].status == battery.OK
    approved = env.battery()
    assert approved.statistical_status == battery.APPROVED and approved.fingerprint == catalog.fingerprint(env.robot())
    env.source.write_text(env.source.read_text(encoding="utf-8") + "\n// outra lógica\n", encoding="utf-8")
    item = env.items()["code"]
    assert item.status == battery.FAIL and "mudou depois do backtest" in item.detail and "robots compile exato" in item.fix
    old = dict(store.load_runs(env.db)[0])
    old["metrics"] = {k: v for k, v in old["metrics"].items() if k != "source_fingerprint"}
    store.save_run(env.db, dict(old, run_id="antigo", created_at="2099-01-01 00:00:00"))
    assert "versão anterior do programa" in env.items()["code"].detail


def test_approval_stops_counting_when_the_code_changes(env, monkeypatch):
    monkeypatch.setattr(battery, "static_items", lambda *a, **k: [])       # isola a parte estatística
    env.backtest()
    assert env.battery().approved and env.robot().stage == catalog.APPROVED
    account = {"login": 1, "server": "Demo", "demo": True}
    gate = {g.name: g for g in live.demo_gates(env.config, env.robot(), account)}["Bateria de segurança aprovada"]
    assert gate.ok
    env.source.write_text(env.source.read_text(encoding="utf-8") + "\n// mudou\n", encoding="utf-8")
    robot = env.robot()
    assert robot.stage != catalog.APPROVED and any("mudou depois da última bateria" in n for n in robot.notes)
    gate = {g.name: g for g in live.demo_gates(env.config, robot, account)}["Bateria de segurança aprovada"]
    assert not gate.ok and "o código mudou depois da aprovação" in gate.detail


# --- a configuração avaliada é a testada ------------------------------------------------

def test_battery_requires_the_backtest_of_the_same_preset_without_manual_changes(env):
    env.backtest()
    assert env.items()["config"].status == battery.OK
    other = env.items("Exato_Conservador")["config"]
    assert other.status == battery.FAIL and "o backtest usou o padrão do código" in other.detail
    assert "--preset Exato_Conservador" in other.fix
    env.backtest(preset="Exato_Conservador")
    assert env.items("Exato_Conservador")["config"].status == battery.OK
    assert env.items()["config"].status == battery.FAIL
    env.backtest(params={"InpRiskPct": "5"})
    changed = env.items()["config"]
    assert changed.status == battery.FAIL and "InpRiskPct=5" in changed.detail and "sem --param" in changed.fix


# --- modelo do Strategy Tester ----------------------------------------------------------

def test_default_model_is_real_ticks_and_bar_models_fail_the_battery(env):
    assert mt5_tools.tester_model(env.config) == "4"
    assert env.backtest()["metrics"]["tester_model_code"] == "4" and env.items()["tester_model"].status == battery.OK
    for code, status in (("0", battery.OK), ("1", battery.FAIL), ("2", battery.FAIL)):
        env.config["ROBOTS"]["tester_model"] = code
        env.backtest()
        item = env.items()["tester_model"]
        assert item.status == status, code
    assert "gap" in item.detail and "tester_model = 4" in item.fix


# --- stop comprovado pelo teste ---------------------------------------------------------

def test_entries_without_stop_are_counted_from_the_report_and_fail_the_battery(env):
    assert backtest.entries_without_stop(report_html(GOOD)) == (300, 0)
    assert backtest.entries_without_stop(report_html(GOOD[:10], stop="")) == (10, 10)
    assert backtest.entries_without_stop(report_html(GOOD[:10], stop="0.00000")) == (10, 10)
    assert backtest.entries_without_stop("<html></html>") == (0, 0)
    assert env.backtest()["metrics"]["entries_without_stop"] == 0 and env.items()["stop_tested"].status == battery.OK
    env.backtest(report_html(GOOD, stop=""))
    item = env.items()["stop_tested"]
    assert item.status == battery.FAIL and "300 de 300 entradas foram enviadas sem stop" in item.detail
    assert env.battery().statistical_status == battery.REJECTED


@pytest.mark.parametrize("text, value", [("10 000.00", 10000.0), ("1 234,56", 1234.56), ("1,234.56", 1234.56),
                                         ("1.234,56", 1234.56), ("-85.69", -85.69), ("-0,57", -0.57), ("", 0.0),
                                         ("12", 12.0)])
def test_report_numbers_in_every_separator_style(text, value):
    assert backtest._number(text) == pytest.approx(value)


# --- só o terminal desta instalação -----------------------------------------------------

class Proc:
    def __init__(self, prefix, cmdline=("C:\\Program Files\\MetaTrader 5\\terminal64.exe",), readable=True):
        self.info = {"name": "main", "cmdline": list(cmdline)}
        self.prefix, self.readable, self.terminated = prefix, readable, False

    def environ(self):
        if not self.readable:
            raise PermissionError("negado")
        return {"WINEPREFIX": self.prefix} if self.prefix else {}

    def terminate(self):
        self.terminated = True


def test_only_this_installations_terminal_counts_and_is_closed(env, monkeypatch):
    import psutil
    ours, theirs = Proc(str(env.prefix)), Proc("/outra/conta/real")
    bridge = Proc(str(env.prefix), cmdline=("C:\\Python311\\python.exe", "mt5_bridge_server.py"))
    state = {"procs": [theirs, bridge]}
    monkeypatch.setattr(psutil, "process_iter", lambda attrs=None: list(state["procs"]))
    monkeypatch.setattr(psutil, "wait_procs", lambda procs, timeout=None: (list(procs), []))
    assert not mt5_tools.terminal_running(env.config)            # o terminal de outra instalação não bloqueia
    assert mt5_tools.terminal_running()                           # sem config: qualquer terminal (uso antigo)
    state["procs"] = [ours, theirs, bridge]
    assert mt5_tools.terminal_running(env.config)
    ours.terminate = lambda: state.update(procs=[theirs, bridge])
    assert mt5_tools.close_terminal(timeout=0, config=env.config)
    assert not theirs.terminated and not bridge.terminated and state["procs"] == [theirs, bridge]


def test_an_unidentified_terminal_blocks_the_test_and_is_never_closed(env, monkeypatch):
    import psutil
    unknown = Proc(None, readable=False)
    monkeypatch.setattr(psutil, "process_iter", lambda attrs=None: [unknown])
    monkeypatch.setattr(psutil, "wait_procs", lambda procs, timeout=None: (list(procs), []))
    assert mt5_tools.terminal_running(env.config)                # na dúvida, conta como aberto
    with pytest.raises(RuntimeError, match="Não vou fechá-lo"):
        mt5_tools.close_terminal(timeout=0, config=env.config)
    monkeypatch.setattr(mt5_tools, "_bridge", lambda action: pytest.fail("a ponte não pode ser tocada"))
    with pytest.raises(RuntimeError, match="Não vou fechá-lo"):
        mt5_tools.release_terminal(env.config)
    assert not unknown.terminated


# --- paridade: medidas que não explodem com resultado líquido pequeno -------------------

def test_parity_reports_pair_agreement_and_difference_of_gross():
    from mt5_extracao.robots import parity
    python = [["2025-01-02 10:00:00", 1000.0, 1.0], ["2025-01-03 10:00:00", -990.0, 1.0]]
    mql5 = [["2025-01-02 10:05:00", 1010.0, 1.0], ["2025-01-03 10:05:00", -990.0, 1.0]]
    report = parity.compare(python, mql5)
    assert report.matched == 2 and report.pairs_close == 2 and report.pairs_close_share == 1.0
    assert report.result_difference == pytest.approx(1.0)        # 10 de diferença sobre um líquido de 10: 100%
    assert report.difference_of_gross == pytest.approx(10 / 1990) and report.median_ratio == pytest.approx(1.005)
    assert report.ok                 # o líquido pequeno não reprova mais: valem os pares, o giro bruto e o viés
    one_off = parity.compare(python + [["2025-01-06 10:00:00", -1000.0, 1.0]],
                             mql5 + [["2025-01-06 10:05:00", -1660.0, 1.0]])           # um stop preenchido em gap
    assert not one_off.ok and one_off.pairs_close_share == pytest.approx(2 / 3)
    assert set(report.to_dict()) >= {"pairs_close_share", "difference_of_gross", "median_ratio"}
