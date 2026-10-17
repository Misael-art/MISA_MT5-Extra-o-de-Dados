"""Consentimento, identidade e cenários dos cards; nenhum terminal/conta real."""
import configparser
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from mt5_extracao.robots import battery, catalog, controls, mql, sessions, mt5_tools, risk, store, live


@pytest.fixture
def robot(tmp_path):
    path = tmp_path / "Expert.mq5"
    path.write_text("input double Risk=0.5;\nint OnInit(){return 0;}\nvoid OnTick(){}\n")
    robot = catalog.Robot("mql5:expert", catalog.MQL5, "Expert", path=str(path), files=[str(path)],
                          inputs=[mql.MqlInput("Risk", "double", "0.5"),
                                  mql.MqlInput("InpExportTrades", "bool", "false")],
                          presets={"A": {"Risk": "0.25", "__file__": "private"}, "B": {"Risk": "0.1"}})
    robot.runs = [{"run_id": "one", "source": "strategy tester", "symbol": "EURUSD", "timeframe": "H1",
                   "created_at": "2026-01-01", "params": {"preset": "A", "Risk": "0.250", "InpExportTrades": "true"},
                   "metrics": {"source_fingerprint": catalog.fingerprint(robot), "trades": 20,
                               "net_profit": -50, "profit_factor": .8, "max_drawdown_pct": .1, "win_rate": .3},
                   "equity": []}]
    return robot


def test_selection_uses_exact_robot_preset_symbol_timeframe_and_values(robot):
    assert controls.selection(robot, "EURUSD", "H1", "A")["headline"]["profit_factor"] == .8
    for symbol, timeframe, preset in (("USDJPY", "H1", "A"), ("EURUSD", "M5", "A"), ("EURUSD", "H1", "B"),
                                      ("EURUSD", "H1", None)):
        assert controls.selection(robot, symbol, timeframe, preset)["headline"] is None
    robot.presets["A"]["Risk"] = ".2"
    assert not controls.matching_runs(robot, "EURUSD", "H1", "A")


def test_legacy_test_with_missing_secondary_metrics_preserves_unknown_values(robot):
    for name in ("profit_factor", "max_drawdown_pct", "win_rate"):
        robot.runs[0]["metrics"].pop(name)
    context = controls.selection(robot, "EURUSD", "H1", "A")
    assert context["run"] and context["headline"]["trades"] == 20
    assert all(context["headline"][k] is None for k in ("profit_factor", "max_drawdown", "extra"))


def test_native_defaults_stay_in_compiler_and_enum_presets_use_numeric_values(robot):
    robot.inputs.extend([mql.MqlInput("Profile", "ENUM_PROFILE", "MODERATE"),
                         mql.MqlInput("Color", "color", "clrWhite"),
                         mql.MqlInput("Frame", "ENUM_TIMEFRAMES", "PERIOD_H1")])
    robot.enums = {"ENUM_PROFILE": {"CONSERVATIVE": 0, "MODERATE": 1}}
    robot.presets["A"]["Profile"] = "CONSERVATIVE"
    assert controls.effective_values(robot)["Profile"] == "1"
    assert controls.effective_values(robot)["Frame"] == "16385"
    assert controls.native_values(robot) == {}
    assert controls.native_values(robot, "A") == {"Risk": "0.25", "Profile": "0"}
    robot.runs[0]["params"]["Profile"] = "0"
    assert controls.matching_runs(robot, "EURUSD", "H1", "A")


@pytest.mark.parametrize("mutation", ["source", "fingerprint", "cancelled", "extra", "code"])
def test_stale_hidden_cancelled_and_changed_code_do_not_enable_play(robot, mutation):
    run = robot.runs[0]
    if mutation == "source":
        run["source"] = "strategy tester (bateria)"
    elif mutation == "fingerprint":
        run["metrics"]["source_fingerprint"] = "old"
    elif mutation == "cancelled":
        run["metrics"]["session_state"] = "cancelled"
    elif mutation == "extra":
        run["params"]["Unknown"] = 1
    else:
        with open(robot.path, "a") as handle:
            handle.write("// code changed")
    assert not controls.matching_runs(robot, "EURUSD", "H1", "A")


def test_loss_scenario_keeps_test_basis_and_never_invents_week_or_currency(robot):
    scenario = controls.loss_scenario(robot.runs[0])
    assert scenario["loss_100"] == 250 and scenario["per_trade"] == -2.5
    assert "teste" in scenario["currency"] and "Não é previsão" in scenario["basis"]
    assert "week" not in scenario and "weekly_loss" not in scenario


@pytest.mark.parametrize("net,trades", [(0, 10), (2, 10), (-2, 0), (float("nan"), 10), (-2, 1.5), (-2, None)])
def test_loss_scenario_does_not_fabricate_missing_invalid_or_nonnegative_results(net, trades):
    assert controls.loss_scenario({"metrics": {"net_profit": net, "trades": trades}}) is None


def test_consent_is_bound_to_context_and_requires_two_steps_and_extra_negative(robot):
    account = {"login": 1, "server": "Demo", "demo": True}
    digest = controls.context_digest(robot, "EURUSD", "H1", "A", account, robot.runs[0])
    consent = controls.Consent(digest, 100)
    with pytest.raises(ValueError, match="incompletas"):
        consent.validate(digest, now=101)
    consent.reviewed = consent.confirmed = True
    with pytest.raises(ValueError, match="incompletas"):
        consent.validate(digest, negative=True, now=101)
    consent.negative_confirmed = True
    consent.validate(digest, negative=True, now=101)
    with pytest.raises(ValueError, match="REAL"):
        consent.validate(digest, real=True, now=101)
    consent.real_authorized = True
    consent.validate(digest, real=True, now=101)
    for changed in (dict(account, login=2), dict(account, demo=False), dict(account, server="Other")):
        with pytest.raises(ValueError, match="mudou"):
            consent.validate(controls.context_digest(robot, "EURUSD", "H1", "A", changed, robot.runs[0]), now=101)
    with pytest.raises(ValueError, match="expirou"):
        consent.validate(digest, now=281)


@pytest.mark.parametrize("active,count,profit,fresh,expected", [
    (False, 1, -1, True, "idle"), (True, 0, 0, True, "waiting"), (True, 1, -1, True, "loss"),
    (True, 1, 1, True, "profit"), (True, 1, 0, True, "waiting"), (True, 1, 2, False, "unknown"),
    (True, None, None, True, "unknown"), (True, 1, float("nan"), True, "unknown")])
def test_activity_is_position_scoped_and_stale_never_shows_profit(active, count, profit, fresh, expected):
    assert controls.activity_state(active, count, profit, fresh)[0] == expected


def test_telemetry_wraps_existing_events_without_changing_timer_or_orders(robot):
    source = sessions.telemetry_source(robot, "test.txt", {"login": 123, "server": "Demo", "demo": True})
    assert "#define OnInit MT5X_OnInit" in source and "#define OnTick MT5X_OnTick" in source
    assert "ACCOUNT_TRADE_MODE_DEMO" in source and "ACCOUNT_LOGIN)==123" in source
    assert "INIT_FAILED" in source and "ExpertRemove()" in source
    assert "EventSetTimer(1)" in source and "order_send" not in source and "OrderSend" not in source
    assert "POSITION_SYMBOL)==_Symbol" in source


def test_observer_never_replaces_strategy_timer_cadence(robot):
    with open(robot.path, "a") as handle:
        handle.write("\nvoid MyTimer(){EventSetTimer(15);}\nvoid OnTimer(){}")
    source = sessions.telemetry_source(robot, "test.txt")
    assert "#define OnTimer MT5X_OnTimer" in source
    assert "EventSetTimer(1)" not in source and "EventKillTimer" not in source


def test_cancel_before_preparation_never_copies_credentials_or_starts_terminal(robot, tmp_path):
    session = sessions.Session(None, configparser.ConfigParser(), robot, "EURUSD", "H1", root=tmp_path / "sessions")
    session.cancel()
    assert session.run().state == "cancelled"
    assert not session.directory.exists()


def test_play_without_account_reader_never_prepares_native_terminal(robot, tmp_path):
    session = sessions.Session(None, configparser.ConfigParser(), robot, "EURUSD", "H1", mode="play", root=tmp_path)
    assert session.run().state == "failed"
    assert "revalidação" in session.error
    assert not session.directory.exists()


@pytest.mark.parametrize("windows", [False, True])
def test_owned_processes_requires_executable_and_prefix_and_stop_never_touches_main(robot, tmp_path, monkeypatch, windows):
    prefix = tmp_path / "wine"
    directory = prefix / "drive_c" / "mt5x_sessions" / "own"
    directory.mkdir(parents=True)
    config = configparser.ConfigParser()
    config.read_dict({"MT5": {"path": str(directory / "terminal64.exe"), "wine_prefix": str(prefix)}})
    monkeypatch.setattr(sessions.paths, "is_windows", lambda: windows)
    expected = str(directory / "terminal64.exe") if windows else r"C:\mt5x_sessions\own\terminal64.exe"
    killed = []

    def proc(name, exe, own_prefix=prefix):
        return SimpleNamespace(exe=lambda: exe, cmdline=lambda: [exe],
                               environ=lambda: {"WINEPREFIX": str(own_prefix)}, create_time=lambda: time.time(),
                               terminate=lambda: killed.append(name))
    main = proc("main", str(tmp_path / "mt5" / "terminal64.exe") if windows else r"C:\Program Files\MT5\terminal64.exe")
    ours = proc("ours", expected)
    unknown = proc("different-prefix", expected, tmp_path / "other")
    import psutil
    monkeypatch.setattr(psutil, "process_iter", lambda: iter((main, ours, unknown)))
    found = sessions.owned_processes(config)
    assert main not in found and ours in found
    if not windows:
        assert unknown not in found
    session = sessions.Session(None, config, robot, "EURUSD", "H1", root=tmp_path / "test")
    session.child_config = config
    session._stop_owned()
    assert "main" not in killed and "ours" in killed


def test_prepare_preserves_originals_and_case_and_uses_catalog_includes_last(robot, tmp_path, monkeypatch):
    source = tmp_path / "native"
    (source / "Config").mkdir(parents=True)
    (source / "Config" / "accounts.dat").write_bytes(b"opaque encrypted cache")
    (source / "Config" / "common.ini").write_text("[Common]\nLogin=1\nServer=Demo\nPassword=must-not-copy\n[Experts]\nEnabled=1")
    (source / "Bases" / "Demo" / "history" / "EURUSD").mkdir(parents=True)
    (source / "Bases" / "alerts.dat").write_bytes(b"not-a-directory")
    (source / "Bases" / "Demo" / "history" / "EURUSD" / "2026.hcc").write_bytes(b"history")
    (source / "terminal64.exe").write_bytes(b"terminal")
    (source / "MQL5" / "Include").mkdir(parents=True)
    config = configparser.ConfigParser()
    config.read_dict({"MT5": {"path": str(source)}, "ROBOTS": {"metaeditor_path": "keep-original-config"}})
    monkeypatch.setattr(mt5_tools, "data_dir", lambda config: source)
    from mt5_extracao.robots import safety_lib
    monkeypatch.setattr(safety_lib, "ensure", lambda *a: None)
    before = {str(p.relative_to(source)): p.read_bytes() for p in source.rglob("*") if p.is_file()}
    destination = tmp_path / "session"
    child = sessions.prepare(config, robot, destination, "EURUSD")
    assert child.get("ROBOTS", "metaeditor_path") == "" and config.get("ROBOTS", "metaeditor_path") == "keep-original-config"
    assert (destination / "Bases" / "Demo" / "history" / "EURUSD" / "2026.hcc").read_bytes() == b"history"
    assert "Password" not in (destination / "Config" / "common.ini").read_text(encoding="utf-16")
    assert "Experts" not in (destination / "Config" / "common.ini").read_text(encoding="utf-16")
    visual = configparser.ConfigParser()
    visual.read(destination / "Tester" / "config" / "metatester.ini", encoding="utf-16")
    assert visual.has_option("Docking", "Dividers-0")
    assert all(visual.getint(f"Pane-{pane}", "IsVisible") == 0 for pane in (32793, 32794, 32795))
    assert visual.getint("Pane-32792", "IsVisible") == 1  # controles de pausa/stop acessíveis
    assert before == {str(p.relative_to(source)): p.read_bytes() for p in source.rglob("*") if p.is_file()}


def test_demo_negative_can_proceed_after_static_safety_but_real_keeps_operational_gates(robot, monkeypatch):
    config = configparser.ConfigParser()
    config.read_dict({"ROBOTS": {"min_demo_days": "7"}})
    facts = {"mandatory_stop": True, "risk_pct": .25, "daily_loss_pct": 2, "max_drawdown_pct": 10}
    robot.preset_risks["A"] = risk.RiskRating(risk.LOW, ["stop obrigatório"], facts)
    seen = []
    monkeypatch.setattr(catalog, "load_catalog", lambda config: [robot])
    monkeypatch.setattr(battery, "static_items", lambda robot, config, robots, preset: seen.append(preset) or [])
    monkeypatch.setattr(live, "stop_active", lambda config: False)
    demo = {"login": 1, "server": "Demo", "demo": True}
    assert controls.play_issues(config, robot, "EURUSD", "H1", "A", demo) == []
    assert seen == ["A"]
    real = dict(demo, demo=False)
    issues = controls.play_issues(config, robot, "EURUSD", "H1", "A", real)
    assert any("bateria completa" in x for x in issues) and any("7 dias" in x for x in issues)
    robot.checks = [{"fingerprint": catalog.fingerprint(robot), "preset": "A", "allows_real": True,
                     "symbol": "EURUSD", "timeframe": "H1"}]
    identity = controls.context_digest(robot, "EURUSD", "H1", "A", {}, None)
    monkeypatch.setattr(store, "load_records", lambda db: {robot.key: {"meta": {"demo_sessions": [
        {"identity": identity, "verified_demo": True, "observed_seconds": 7*86400}]}}})
    assert controls.play_issues(config, robot, "EURUSD", "H1", "A", real, db=object()) == []
    robot.presets["A"]["Risk"] = ".3"
    assert controls.play_issues(config, robot, "EURUSD", "H1", "A", real, db=object())


def test_historical_observer_exports_only_own_position_costs_and_preserves_user_tester(robot):
    with open(robot.path, "a") as handle:
        handle.write("\ndouble OnTester(){return 12.5;}")
    source = sessions.telemetry_source(robot, "state.txt", trades_filename="unique.csv")
    assert "#define OnTester MT5X_OnTester" in source and "double result=MT5X_OnTester()" in source
    assert 'MT5X_Export("unique.csv"' in source
    assert "DEAL_COMMISSION" in source and "DEAL_SWAP" in source and "DEAL_FEE" in source
    assert "DEAL_POSITION_ID" in source and "DEAL_ENTRY_IN" in source
    assert "OrderSend" not in source and "trade.Buy" not in source and "FileDelete" not in source


def test_account_identity_rejects_contest_disconnect_and_incomplete_portfolio_and_binds_exposure():
    account = SimpleNamespace(login=123, server="Demo", trade_mode=0, balance=10000, currency="USD", trade_allowed=True)
    position = SimpleNamespace(ticket=1, symbol="EURUSD", magic=2, volume=.1, price_open=1.1, sl=1, tp=1.2, profit=3)
    state = SimpleNamespace(connected=True, positions=[position])
    provider = SimpleNamespace(account_info=lambda: account, terminal_info=lambda: state,
                               positions_get=lambda: state.positions, orders_get=lambda: ())
    first = controls.account_identity(provider)
    position.profit = -3
    assert controls.account_identity(provider) == first  # cotação/PnL não invalidam a revisão
    position.sl = .9
    assert controls.account_identity(provider)["portfolio_signature"] != first["portfolio_signature"]
    for mode in (None, 1):
        account.trade_mode = mode
        with pytest.raises(ValueError, match="Tipo de conta"):
            controls.account_identity(provider)
    account.trade_mode = 0
    state.positions = None
    with pytest.raises(ValueError, match="carteira"):
        controls.account_identity(provider)
    state.positions = []
    state.connected = False
    with pytest.raises(ValueError, match="conectada"):
        controls.account_identity(provider)


def test_risk_budget_uses_selected_preset_and_current_balance_without_inventing_currency(robot):
    facts = {"risk_pct": .25, "daily_loss_pct": 2, "max_drawdown_pct": 10}
    robot.preset_risks["A"] = risk.RiskRating(risk.LOW, facts=facts)
    assert controls.risk_budget(robot, "A", {"balance": 10000, "currency": "USD"})["amounts"] == {
        "risk_pct": 25, "daily_loss_pct": 200, "max_drawdown_pct": 1000}
    assert controls.risk_budget(robot, "A", {"balance": 10000})["amounts"] == {}


def test_account_observer_lease_and_historical_common_namespaces_have_separate_scopes(robot):
    account = {"login": 123, "server": "Demo", "demo": True}
    source = sessions.telemetry_source(robot, "state.txt", account, lease_filename="lease.txt")
    assert "age<=15" in source and "TimeGMT()" in source and "MT5X_Lease()" in source
    assert "#define FileOpen" not in source  # Safety de conta continua compartilhada na conta
    historical = sessions.telemetry_source(robot, "state.txt", trades_filename="trades.csv", common_namespace="sessions\\uuid\\")
    assert "#define FileOpen MT5X_CommonOpen" in historical
    assert "#define FileDelete MT5X_CommonDelete" in historical
    assert historical.index("#undef FileOpen") < historical.index("bool MT5X_Export")
    assert "#define OrderSend" not in historical and "OrderSend(" not in historical


def test_native_stop_and_download_cancellation_are_not_success_or_trade_stop(tmp_path):
    folder = tmp_path / "Tester" / "logs"
    folder.mkdir(parents=True)
    log = folder / "20261017.log"
    log.write_text("stop loss triggered #3\nlast test passed with result successfully finished", encoding="utf-16")
    assert not sessions.native_interrupted(tmp_path)
    log.write_text("Tester\tEURUSD: preliminary downloading of history ticks canceled", encoding="utf-16")
    assert sessions.native_interrupted(tmp_path)
    log.write_text("Tester\ttesting stopped by user", encoding="utf-8")
    assert sessions.native_interrupted(tmp_path)


def test_native_visual_stop_in_local_agent_journal_is_cancelled_not_completed(tmp_path):
    agent = tmp_path / "Tester" / "Agent-127.0.0.1-40651" / "logs" / "20261017.log"
    agent.parent.mkdir(parents=True)
    agent.write_text("CS\t0\t09:28:19\tTester\tposition closed by stop loss\n", encoding="utf-16")
    assert not sessions.native_interrupted(tmp_path)
    with agent.open("a", encoding="utf-16") as handle:
        handle.write("CS\t3\t09:29:14.716\tTester\tstopped by user\n")
    assert sessions.native_interrupted(tmp_path)


def test_generated_experts_refuse_the_wrong_execution_environment(robot):
    historical = sessions.telemetry_source(robot, "observer.txt")
    assert "int OnInit() { if(!(MQLInfoInteger(MQL_TESTER))) return INIT_FAILED;" in historical
    account = {"login": 123, "server": "FakeDemo", "demo": True}
    active = sessions.telemetry_source(robot, "observer.txt", account=account, lease_filename="lease.txt")
    assert "int OnInit() { if(!(!MQLInfoInteger(MQL_TESTER) && AccountInfoInteger(ACCOUNT_LOGIN)==123" in active
