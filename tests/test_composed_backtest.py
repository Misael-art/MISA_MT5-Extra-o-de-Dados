"""T19.24: composições executam a sequência completa e persistem pelo caminho atual.

O sinal controlado é só uma fixture; políticas, motor, persistência e journal são reais.
"""
import configparser
from dataclasses import asdict
from types import SimpleNamespace

import pytest

from mt5_extracao import experiments
from mt5_extracao.analytics import datasets
from mt5_extracao.composition import BaseAdapter, Composition, base_composition
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.robots import backtest, budget, catalog, store
from mt5_extracao.strategies import ExitRules
from mt5_extracao.strategies.ema_atr import EmaAtrTrend
from mt5_extracao.strategies.session_entry import SessionEntryPolicy
from mt5_extracao.strategies.specs import spec_from_symbol_info
from mt5_extracao.strategies.temporal import TemporalPolicy
from mt5_extracao.trade_analytics import ledger
from tests.test_backtester import bars


@pytest.fixture
def source(tmp_path, monkeypatch):
    def signals(self, frame, out):
        for index in (299, 301, 303, 305):
            if index < len(frame):
                out.loc[out.index[index], "entry_long"] = True
        if len(frame) > 320:
            out.loc[out.index[320], "exit_long"] = True
        out["stop_long"] = 90.0
        out["stop_short"] = 110.0

    monkeypatch.setattr(EmaAtrTrend, "_signals", signals)
    monkeypatch.setattr(EmaAtrTrend, "exit_rules", lambda self: ExitRules())
    db = DatabaseManager(db_path=str(tmp_path / "composed.sqlite"))
    data = bars([[100, 101, 99, 100]] * 330, start="2026-01-05 04:00", freq="min")
    data["tick_volume"] = 100
    data["real_volume"] = 0
    db.save_ohlcv_data("QA", "1 minuto", data)
    db.save_symbol_spec("QA", spec_from_symbol_info(SimpleNamespace(
        point=1, trade_tick_size=1, trade_tick_value=1, volume_min=.01, volume_step=.01)))
    cfg = configparser.ConfigParser()
    cfg["STRATEGY"] = {"initial_equity": "10000", "risk_per_trade": ".01"}
    composition = base_composition("ema_atr", symbol="QA", timeframe="M1", risk_fraction=.0025)
    snapshot = datasets.capture(db, "QA", "M1")
    definition = dict(schema_version=1, name="composed", revision=1,
                      criteria=dict(id="functional_fixture", version=1),
                      objective=dict(id="no_selection", version=1), grid=[], seed=42,
                      budget=dict(scope="evaluation_request", limits=asdict(budget.Limits())),
                      roles=dict(train=dict(data_manifest_id=snapshot.manifest_id,
                                            evaluation_start=data.time.iloc[0].isoformat())))
    protocol = experiments.create(db, definition)["protocol_id"]
    yield db, cfg, data, composition, snapshot, protocol
    db.close()


def changed(composition, *, temporal=False, points=False, compound=True, quota=None):
    value = composition.to_dict()
    value["sizing"]["params"]["compound"] = compound
    if temporal:
        value["conditional_policy"] = dict(mode="temporal_v1", params=TemporalPolicy(max_seconds=60).to_dict())
    if points:
        value["profit_exit"] = dict(mode="fixed_points", params=dict(target_points=10))
        value["protection"] = dict(mode="fixed_points", params=dict(stop_points=10))
    if quota is not None:
        value["filters"] = [dict(mode="session_v1", params=SessionEntryPolicy(max_cycles=quota).to_dict())]
    return Composition(value)


def evaluate(source, composition, key="one", *, executor="composition_v1"):
    db, cfg, _, _, _, protocol = source
    item = experiments.request(db, protocol, key, composition, config=cfg, executor=executor, use_cache=False)
    assert item["state"] == "requested", item
    result = experiments.evaluate(db, item["request_id"], config=cfg)
    assert result["request"]["state"] == "run_recorded", result
    return result


def test_early_exit_releases_later_signals_and_composition_survives_reopen(source):
    db, _, data, composition, snapshot, _ = source
    baseline = evaluate(source, composition, "baseline", executor="base_v1")
    selected = changed(composition, temporal=True)
    result = evaluate(source, selected)
    assert baseline["run"]["metrics"]["trades"] == 1
    assert result["run"]["metrics"]["trades"] == 4
    book = ledger.load(db, run_id=result["run"]["run_id"])
    assert list(book.entry_time) == list(data.time.iloc[[300, 302, 304, 306]])
    assert set(book.exit_reason) == {"prazo absoluto"}
    saved = store.load_runs(db, run_id=result["run"]["run_id"])[0]
    assert saved["metrics"]["composition"] == selected.to_dict()
    assert saved["metrics"]["composition_hash"] == selected.config_hash
    assert saved["data_manifest_id"] == snapshot.manifest_id
    assert saved["metrics"]["risk_per_trade"] == .0025
    assert not result["ledger_qualified"] and not result["statistical_approval"]


def test_session_quota_prevents_new_orders_after_early_exits(source):
    db, _, _, composition, _, _ = source
    unlimited = evaluate(source, changed(composition, temporal=True), "unlimited")
    limited = evaluate(source, changed(composition, temporal=True, quota=1), "limited")
    assert unlimited["run"]["metrics"]["trades"] == 4
    assert limited["run"]["metrics"]["trades"] == 1
    assert len(ledger.load(db, run_id=limited["run"]["run_id"])) == 1


def test_capital_and_compound_change_following_lots_in_complete_simulation(source):
    db, _, data, composition, _, protocol = source
    data.loc[[301, 303, 305, 307], "high"] = 115
    db.save_ohlcv_data("QA", "1 minuto", data)
    snapshot = datasets.capture(db, "QA", "M1")
    definition = experiments.status(db, protocol)["protocol"]["definition"]
    definition["revision"] = 2
    definition["roles"]["train"]["data_manifest_id"] = snapshot.manifest_id
    pid = experiments.create(db, definition)["protocol_id"]
    local = (*source[:4], snapshot, pid)
    fixed = evaluate(local, changed(composition, points=True, compound=False), "fixed")
    compounded = evaluate(local, changed(composition, points=True), "compound")
    fixed_book = ledger.load(db, run_id=fixed["run"]["run_id"])
    compounded_book = ledger.load(db, run_id=compounded["run"]["run_id"])
    assert list(fixed_book.volume) == [2.5, 2.5, 2.5, 2.5]
    assert list(compounded_book.volume) == [2.5, 2.5, 2.51, 2.51]
    assert list(fixed_book.pnl) == pytest.approx([25, 25, 25, 25])
    assert list(compounded_book.pnl) == pytest.approx([25, 25, 25.1, 25.1])
    assert fixed["run"]["metrics"]["compound"] is False
    assert fixed["run"]["metrics"]["net_profit"] < compounded["run"]["metrics"]["net_profit"]


def test_trade_from_warms_prefix_without_opening_previous_signals(source):
    db, cfg, data, composition, snapshot, _ = source
    robot = next(r for r in catalog.python_robots() if r.key == "python:ema_atr")
    result = backtest.run_python(db, cfg, robot, "QA", "M1", composition=changed(composition, temporal=True),
                                 data_manifest_id=snapshot.manifest_id, trade_from=data.time.iloc[304])
    book = ledger.load(db, run_id=result["run_id"])
    assert list(book.entry_time) == list(data.time.iloc[[304, 306]])


def test_cancellation_after_signal_generation_keeps_reserved_run_for_retry(source, monkeypatch):
    db, cfg, _, composition, _, protocol = source
    chosen = changed(composition, points=True)
    item = experiments.request(db, protocol, "cancel", chosen, executor="composition_v1", config=cfg, use_cache=False)
    controlled = budget.Budget(db, cfg, limits=budget.Limits(), batch_id=item["batch_id"])
    original = EmaAtrTrend._signals
    should_cancel = [True]

    def signals(self, frame, out):
        original(self, frame, out)
        if should_cancel[0]:
            controlled.cancel("cancelamento após cálculo dos sinais")

    monkeypatch.setattr(EmaAtrTrend, "_signals", signals)
    failed = experiments.evaluate(db, item["request_id"], config=cfg, budget=controlled)
    assert failed["request"]["state"] == "interrupted", failed
    assert store.load_runs(db) == []
    should_cancel[0] = False
    resumed = experiments.evaluate(db, item["request_id"], config=cfg)
    assert resumed["request"]["state"] == "run_recorded", resumed
    assert resumed["run"]["run_id"] == failed["request"]["run_id"]
    assert len(store.load_runs(db)) == 1


def test_composition_identity_changes_with_policy_and_loaded_adapter(source, monkeypatch):
    _, cfg, _, composition, snapshot, _ = source
    from mt5_extracao.strategies import research
    strategy = BaseAdapter(composition).strategy()
    settings = BaseAdapter(composition).backtest_config(research.backtest_config(cfg))
    first = backtest._python_execution_identity(snapshot, strategy, settings, composition=composition)
    altered = backtest._python_execution_identity(snapshot, strategy, settings,
                                                 composition=changed(composition, temporal=True))
    assert first["cache_eligible"] and first["digest"] != altered["digest"]
    old = BaseAdapter.run

    def different(self, *args, **kwargs):
        return old(self, *args, **kwargs)

    monkeypatch.setattr(BaseAdapter, "run", different)
    loaded = backtest._python_execution_identity(snapshot, strategy, settings, composition=composition)
    assert loaded["digest"] != first["digest"]


def test_missing_clock_or_context_is_refused_instead_of_inferred(source):
    _, _, data, composition, snapshot, _ = source
    selected = changed(composition, temporal=True, quota=1)
    with pytest.raises(ValueError, match="entry_time_basis"):
        BaseAdapter(selected).run(data, snapshot.spec, timeframe="M1")
    with pytest.raises(ValueError, match="timeframe"):
        BaseAdapter(selected).run(data, snapshot.spec, timeframe="H1", time_basis="broker")


@pytest.mark.parametrize("conflict", ["strategy", "params", "robot", "context"])
def test_ambiguous_or_mismatched_composition_is_rejected_before_publishing(source, conflict):
    db, cfg, _, composition, _, _ = source
    robot = next(r for r in catalog.python_robots() if r.key == "python:ema_atr")
    kwargs = {"composition": composition}
    if conflict == "strategy":
        kwargs["strategy"] = BaseAdapter(composition).strategy()
    if conflict == "params":
        kwargs["params"] = {}
    if conflict == "robot":
        robot = next(r for r in catalog.python_robots() if r.key == "python:donchian")
    if conflict == "context":
        value = composition.to_dict()
        value["context"]["symbol"] = "OTHER"
        kwargs["composition"] = Composition(value)
    with pytest.raises(ValueError):
        backtest.run_python(db, cfg, robot, "QA", "M1", **kwargs)
    assert store.load_runs(db) == []


def test_composition_does_not_make_an_ineligible_strategy_cache_eligible(source):
    db, cfg, _, _, snapshot, _ = source
    composition = base_composition("stoch_ema9", symbol="QA", timeframe="M1", risk_fraction=.0025)
    robot = next(r for r in catalog.python_robots() if r.key == "python:stoch_ema9")
    run = backtest.run_python(db, cfg, robot, "QA", "M1", composition=composition,
                              data_manifest_id=snapshot.manifest_id)
    assert run["metrics"]["composition_hash"] == composition.config_hash
    assert run["metrics"]["execution_identity"]["cache_eligible"] is False


def test_cli_executor_is_explicit_and_old_base_contract_remains_restricted(source, tmp_path, capsys):
    from tests.test_cli_agent import run
    db, cfg, _, composition, _, protocol = source
    path = tmp_path / "config.ini"
    cfg["DATABASE"] = {"type": "sqlite", "path": str(db.db_path)}
    cfg["APP"] = {"notifications": "false"}
    with path.open("w") as stream:
        cfg.write(stream)
    selected = changed(composition, temporal=True, compound=False)
    document = tmp_path / "composition.json"
    document.write_text(selected.to_json(), encoding="utf-8")
    base = ["--config", str(path), "--json", "experiments"]
    refused = run(capsys, base + ["request", "--protocol", protocol, "--key", "base",
                                  "--composition", str(document)])
    assert not refused["ok"] and refused["data"]["state"] == "rejected"
    asked = run(capsys, base + ["request", "--protocol", protocol, "--key", "composed",
                               "--composition", str(document), "--executor", "composition_v1"])
    assert asked["ok"] and asked["data"]["payload"]["executor"] == "composition_v1"
    result = run(capsys, base + ["evaluate", asked["data"]["request_id"]])
    assert result["ok"] and result["data"]["run"]["metrics"]["trades"] == 4
    assert result["data"]["run"]["metrics"]["compound"] is False
    again = run(capsys, base + ["evaluate", asked["data"]["request_id"]])
    assert again["data"]["run"]["run_id"] == result["data"]["run"]["run_id"]


def test_unknown_executor_record_is_not_silently_interpreted_as_base(source):
    import json
    from sqlalchemy import text
    db, cfg, _, composition, _, protocol = source
    item = experiments.request(db, protocol, "future", composition, config=cfg)
    payload = dict(item["payload"], executor="composition_v2")
    with db.engine.begin() as conn:
        conn.execute(text("UPDATE _experiment_requests SET payload_json=:p WHERE request_id=:i"),
                     dict(p=json.dumps(payload), i=item["request_id"]))
    result = experiments.evaluate(db, item["request_id"], config=cfg)
    assert "Executor desconhecido" in result["evaluation_error"]
    assert store.load_runs(db) == []
