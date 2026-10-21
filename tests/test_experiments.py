"""T20.12 inicial: APIs reais, journal durável, orçamento e publicação recuperável."""
import copy
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import threading
import os
import subprocess
import sys

import pytest
from sqlalchemy import inspect, text

from mt5_extracao import experiments
from mt5_extracao.analytics import datasets
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.experiments import service, store
from mt5_extracao.robots import backtest, budget, store as runs
from tests.experiment_helpers import setup


@pytest.fixture
def source(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path/"journal.sqlite"))
    definition, composition, bars = setup(db)
    p = experiments.create(db, definition)
    yield db, definition, composition, bars, p["protocol_id"]
    db.close()


def asked(source, key="one", **kwargs):
    db, _, composition, _, protocol = source
    return experiments.request(db, protocol, key, composition, **kwargs)


def count(db, table):
    if not inspect(db.engine).has_table(table):
        return 0
    with db.engine.connect() as conn:
        return conn.execute(text("SELECT COUNT(*) FROM "+table)).scalar_one()


def test_immutable_protocol_freezes_roles_without_recapture_and_preserves_old_tables(source, monkeypatch):
    db, definition, _, _, protocol = source
    before = count(db, "_dataset_manifests")
    monkeypatch.setattr(datasets, "capture", lambda *a, **k: pytest.fail("não recapturar dados atuais"))
    assert experiments.create(db, definition)["protocol_id"] == protocol
    changed = copy.deepcopy(definition);changed["seed"] += 1
    with pytest.raises(ValueError, match="imutável"):
        experiments.create(db, changed)
    assert count(db, "_dataset_manifests") == before
    state = experiments.status(db, protocol)
    assert state["exposure"]["holdout"]["status"] == "no_recorded_access"
    assert not state["qualified_final_holdout"]
    assert state["exposure"]["holdout"]["prior_external_exposure"] == "unknown"


def test_evaluation_intervals_ordered_while_warmup_can_overlap(source):
    db, definition, _, bars, _ = source
    modified = copy.deepcopy(definition);modified.update(name="overlap")
    modified["roles"]["validation"]["evaluation_start"] = bars.time.iloc[350].isoformat()
    with pytest.raises(ValueError, match="não sobrepostos"):
        experiments.create(db, modified)
    wrong = copy.deepcopy(definition);wrong.update(name="wrong_clock")
    wrong["roles"]["train"]["evaluation_start"] = "1900-01-01"
    with pytest.raises(ValueError, match="intervalo"):
        experiments.create(db, wrong)


@pytest.mark.parametrize("fault", ["compound", "filter", "profit"])
def test_unmappable_modes_are_rejected_and_recorded_before_execution(source, fault, monkeypatch):
    db, _, composition, _, protocol = source
    value = composition.to_dict()
    if fault == "compound":value["sizing"]["params"]["compound"] = False
    if fault == "filter":value["filters"] = [dict(mode="adx_min", params=dict(period=14, threshold=20))]
    if fault == "profit":value["profit_exit"] = dict(mode="fixed_r", params=dict(target_r=2))
    monkeypatch.setattr(backtest, "run_python", lambda *a, **k: pytest.fail("não executar composição não mapeável"))
    item = experiments.request(db, protocol, fault, value)
    assert item["state"] == "rejected" and item["error"]
    assert experiments.evaluate(db, item["request_id"])["request"]["state"] == "rejected"
    assert count(db, "_robot_runs") == 0
    assert any(e["kind"] == "request_registered" for e in experiments.status(db, protocol)["events"])


def test_request_idempotency_concurrent_and_new_deliberate_cached_query_is_not_another_observation(source, monkeypatch):
    db, _, _, _, protocol = source
    with ThreadPoolExecutor(max_workers=2) as pool:
        values = list(pool.map(lambda _: asked(source), range(2)))
    assert values[0]["request_id"] == values[1]["request_id"] and count(db, "_experiment_requests") == 1
    original = backtest.run_python;calls = []
    def observed(*args, **kwargs):
        calls.append(1)
        state = experiments.status(db, protocol)
        assert state["exposure"]["train"]["status"] == "access_recorded"
        return original(*args, **kwargs)
    monkeypatch.setattr(backtest, "run_python", observed)
    first = experiments.evaluate(db, values[0]["request_id"])
    assert first["request"]["state"] == "run_recorded", first
    assert first["run"]["metrics"]["risk_per_trade"] == .003
    assert first["run"]["params"] == source[2].to_dict()["strategy"]["params"]
    same = experiments.evaluate(db, values[0]["request_id"])
    assert same["run"]["run_id"] == first["run"]["run_id"]
    second = asked(source, "another_deliberate_query")
    reused = experiments.evaluate(db, second["request_id"])
    assert reused["request"]["state"] == "reused", reused
    assert reused["run"]["run_id"] == first["run"]["run_id"] and calls == [1]
    assert count(db, "_experiment_requests") == 2 and count(db, "_experiment_executions") == 1
    assert count(db, "_robot_runs") == 1
    assert experiments.status(db, protocol)["exposure"]["train"]["recorded_accesses"] == 2
    accesses = [event for event in experiments.status(db, protocol)["events"] if event["kind"] == "data_access"]
    assert {event["request_id"] for event in accesses} == {values[0]["request_id"], second["request_id"]}
    assert all(event["request_id"] == event["payload"]["request_id"] for event in accesses)
    with pytest.raises(ValueError, match="outro conteúdo"):
        asked(source, "one", use_cache=False)


def test_discard_is_audited_without_a_calculation(source):
    db, _, _, _, protocol = source
    item = asked(source, discard_reason="alternativa descartada pelo autor")
    assert experiments.evaluate(db, item["request_id"])["request"]["state"] == "discarded"
    assert count(db, "_robot_runs") == count(db, "_experiment_executions") == 0
    assert experiments.status(db, protocol)["requests"][0]["payload"]["discard_reason"]


def test_ineligible_identity_cannot_reuse_but_same_request_remains_idempotent(source, monkeypatch):
    db, _, _, _, _ = source
    monkeypatch.setattr(backtest, "_python_execution_identity", lambda *a, **k: dict(version=1, cache_eligible=False, status="unqualified"))
    one = experiments.evaluate(db, asked(source)["request_id"])
    assert one["request"]["state"] == "run_recorded", one
    same = experiments.evaluate(db, one["request"]["request_id"])
    two = experiments.evaluate(db, asked(source, "two")["request_id"])
    assert same["run"]["run_id"] == one["run"]["run_id"] != two["run"]["run_id"]
    with db.engine.connect() as conn:
        assert list(conn.execute(text("SELECT observation_id FROM _experiment_executions")).scalars()) == [None, None]
    assert not one["ledger_qualified"] and not one["statistical_approval"]


def test_control_bind_failure_rolls_back_run_reference_and_journal_then_retries_same_runid(source, monkeypatch):
    db, _, _, _, _ = source
    item = asked(source)
    bind = datasets.bind;calls = []
    def failed_bind(*args, **kwargs):
        calls.append(1)
        bind(*args, **kwargs)
        if len(calls) == 1:raise ValueError("falha plantada após bind")
    monkeypatch.setattr(datasets, "bind", failed_bind)
    failed = experiments.evaluate(db, item["request_id"])
    assert failed["request"]["state"] == "failed", failed
    reserved = failed["request"]["run_id"]
    assert reserved and count(db, "_robot_runs") == count(db, "_dataset_references") == 0
    completed = experiments.evaluate(db, item["request_id"])
    assert completed["request"]["state"] == "run_recorded", completed
    assert completed["run"]["run_id"] == reserved and count(db, "_robot_runs") == 1
    assert count(db, "_experiment_executions") == 2


def test_cancel_then_resume_uses_budget_checkpoint_and_reserved_runid(source, monkeypatch):
    db, _, _, _, _ = source
    item = asked(source)
    original = backtest.run_python;calls = []
    def interrupted(*a, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            kwargs["control"].job.parent.cancel("cancelamento dirigido")
            kwargs["control"].check()
        return original(*a, **kwargs)
    monkeypatch.setattr(backtest, "run_python", interrupted)
    first = experiments.evaluate(db, item["request_id"])
    assert first["request"]["state"] == "interrupted"
    again = experiments.evaluate(db, item["request_id"])
    assert again["request"]["state"] == "run_recorded", again
    assert again["run"]["run_id"] == first["request"]["run_id"] and calls == [1, 1]
    checkpoint = budget.checkpoint(db, item["batch_id"])
    assert checkpoint["status"] == "completed" and checkpoint["owner"] == {}
    assert sum(v["status"] == "running" for v in checkpoint["jobs"][0]["history"]) == 2


@pytest.mark.parametrize("fault", ["limits", "batch", "db", "active"])
def test_injected_budget_cannot_relax_protocol_or_change_ownership(source, fault):
    db, definition, _, _, _ = source
    item = asked(source)
    controlled = budget.Budget(db, limits=budget.Limits(**definition["budget"]["limits"]), batch_id=item["batch_id"])
    if fault == "limits":controlled.limits = replace(controlled.limits, min_free_mb=1)
    if fault == "batch":controlled.batch_id = "different"
    if fault == "db":controlled.db = None
    if fault == "active":controlled._claimed = True
    result = experiments.evaluate(db, item["request_id"], budget=controlled)
    assert "Budget deve" in result["evaluation_error"] and count(db, "_robot_runs") == 0


def test_holdout_journal_failure_denies_calculation_and_existing_result_delivery(source, monkeypatch):
    db, _, _, _, _ = source
    item = asked(source, role="holdout")
    def refuse(*a, **k):raise ValueError("journal indisponível")
    monkeypatch.setattr(store, "access", refuse)
    result = experiments.evaluate(db, item["request_id"])
    assert "journal indisponível" in result["evaluation_error"] and count(db, "_robot_runs") == 0


def test_tuning_sticky_shared_across_protocols_revisions_and_new_proposals(source):
    db, definition, composition, _, protocol = source
    other = copy.deepcopy(definition);other["name"] = "another"
    other_id = experiments.create(db, other)["protocol_id"]
    experiments.holdout_access(db, protocol, "initial_read")
    assert experiments.status(db, other_id)["exposure"]["holdout"]["status"] == "access_recorded"
    value = composition.to_dict();value["strategy"]["params"]["fast"] += 1
    experiments.request(db, other_id, "new_proposal", value)
    assert experiments.status(db, protocol)["exposure"]["holdout"]["status"] == "used_for_tuning"
    definition["revision"] = 2
    newer = experiments.create(db, definition)["protocol_id"]
    assert experiments.status(db, newer)["exposure"]["holdout"]["status"] == "used_for_tuning"
    assert not experiments.status(db, newer)["qualified_final_holdout"]


def test_post_publication_error_preserves_run_recorded_without_claiming_a_ledger(source, monkeypatch):
    db, _, _, _, _ = source
    original = backtest.run_python
    def failed_after(*a, **k):
        original(*a, **k)
        raise ValueError("importer posterior não confirmado")
    monkeypatch.setattr(backtest, "run_python", failed_after)
    item = asked(source)
    result = experiments.evaluate(db, item["request_id"])
    assert result["request"]["state"] == "run_recorded" and not result["ledger_qualified"]
    assert experiments.evaluate(db, item["request_id"])["run"]["run_id"] == result["run"]["run_id"]
    assert count(db, "_robot_runs") == 1


def test_immutable_snapshot_survives_current_data_correction(source):
    db, definition, _, bars, _ = source
    item = asked(source)
    corrected = bars.copy();corrected[["open", "high", "low", "close"]] += 1000
    db.save_ohlcv_data("QA", "1 hora", corrected)
    result = experiments.evaluate(db, item["request_id"])
    assert result["request"]["state"] == "run_recorded", result
    assert result["run"]["data_manifest_id"] == definition["roles"]["train"]["data_manifest_id"]
    assert result["run"]["data_end"] == str(bars.time.iloc[359])


def test_finite_but_invalid_composition_is_also_an_audited_refusal(source):
    db, _, composition, _, protocol = source
    value = composition.to_dict();value["strategy"]["params"]["fast"] = 0
    item = experiments.request(db, protocol, "invalid", value)
    assert item["state"] == "rejected" and item["error"]
    assert len(experiments.status(db, protocol)["requests"]) == 1 and count(db, "_robot_runs") == 0


def test_journal_failure_after_run_insert_rolls_back_every_transactional_component(source, monkeypatch):
    db, _, _, _, _ = source
    item = asked(source)
    original = store.finish;calls = []
    def fail_journal(conn, item, state, *a, **k):
        original(conn, item, state, *a, **k)
        if state == "run_recorded":
            calls.append(1)
            if len(calls) == 1:raise ValueError("journal falhou na fronteira de publicação")
    monkeypatch.setattr(store, "finish", fail_journal)
    failed = experiments.evaluate(db, item["request_id"])
    assert failed["request"]["state"] == "failed" and count(db, "_robot_runs") == count(db, "_dataset_references") == 0
    assert not any(e["kind"] == "run_recorded" for e in experiments.status(db, item["protocol_id"])["events"])
    again = experiments.evaluate(db, item["request_id"])
    assert again["request"]["state"] == "run_recorded", again
    assert again["run"]["run_id"] == failed["request"]["run_id"]


def test_request_budget_limits_count_retries_and_refuse_before_another_calculation(source, monkeypatch):
    db, definition, composition, _, _ = source
    definition["name"] = "one_admission";definition["budget"]["limits"]["max_jobs"] = 1
    protocol = experiments.create(db, definition)["protocol_id"]
    item = experiments.request(db, protocol, "one", composition)
    calls = []
    def interrupted(*a, **k):
        calls.append(1)
        k["control"].job.parent.cancel("interrompido antes do cálculo")
        k["control"].check()
    monkeypatch.setattr(backtest, "run_python", interrupted)
    first = experiments.evaluate(db, item["request_id"])
    assert first["request"]["state"] == "interrupted"
    again = experiments.evaluate(db, item["request_id"])
    assert "Orçamento de testes" in again["evaluation_error"] and calls == [1]


def test_concurrent_evaluator_does_not_steal_active_job_or_fail_the_other_request(source, monkeypatch):
    db, _, _, _, _ = source
    item = asked(source)
    original = backtest.run_python;started = threading.Event();release = threading.Event()
    def blocked(*a, **k):
        started.set()
        assert release.wait(30)
        return original(*a, **k)
    monkeypatch.setattr(backtest, "run_python", blocked)
    with ThreadPoolExecutor(max_workers=2) as pool:
        pending = pool.submit(experiments.evaluate, db, item["request_id"])
        try:
            assert started.wait(30)
            other = experiments.evaluate(db, item["request_id"])
            assert "executor ativo" in other["evaluation_error"] and other["request"]["state"] == "running"
        finally:
            release.set()
        result = pending.result(timeout=60)
    assert result["request"]["state"] == "run_recorded", result
    assert count(db, "_robot_runs") == count(db, "_experiment_executions") == 1


def test_corrected_manifest_cannot_wash_overlapping_holdout_exposure(source):
    db, definition, _, bars, protocol = source
    experiments.holdout_access(db, protocol, "tuning", purpose="tuning")
    changed = bars.copy();changed.loc[500, ["open", "high", "low", "close"]] += 1
    db.save_ohlcv_data("QA", "1 hora", changed)
    newer = datasets.capture(db, "QA", "H1", end=bars.time.iloc[-1])
    definition["name"] = "changed_manifest"
    assert newer.manifest_id != definition["roles"]["holdout"]["data_manifest_id"]
    definition["roles"]["holdout"]["data_manifest_id"] = newer.manifest_id
    other = experiments.create(db, definition)["protocol_id"]
    assert experiments.status(db, other)["exposure"]["holdout"]["status"] == "used_for_tuning"


@pytest.mark.parametrize("phase", ["before", "after"])
def test_process_death_recovers_exact_reserved_run_without_duplicate_publication(source, monkeypatch, phase):
    from tests import experiment_helpers
    from mt5_extracao.strategies import indicators
    db, _, _, _, _ = source
    item = asked(source)
    # Internar a MESMA constante não altera código/semântica; o processo filho
    # começa sem esse detalhe de memória do pai. Não pode invalidar a retomada.
    constant = next(value for value in indicators.smma.__code__.co_consts[1:]
                    if type(value) is str and not value.isidentifier())
    held_constant = sys.intern(constant)
    assert held_constant == constant
    original = backtest.run_python
    monkeypatch.setattr(experiment_helpers, "original_run", original)
    monkeypatch.setattr(backtest, "run_python", experiment_helpers.process_crash)
    script = """
import sys
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao import experiments
from mt5_extracao.robots import backtest
from tests import experiment_helpers
experiment_helpers.original_run=backtest.run_python
backtest.run_python=experiment_helpers.process_crash
db=DatabaseManager(db_path=sys.argv[1])
experiments.evaluate(db,sys.argv[2])
"""
    child = subprocess.run([sys.executable, "-c", script, str(db.db_path), item["request_id"]],
                           env={**os.environ, "MT5X_EXPERIMENT_CRASH_PHASE": phase}, capture_output=True, text=True, timeout=40)
    assert child.returncode == (23 if phase == "before" else 24), child.stderr
    old = store.request(db, item["request_id"])
    assert old["run_id"] and count(db, "_robot_runs") == (0 if phase == "before" else 1)
    result = experiments.evaluate(db, item["request_id"])
    assert result["request"]["state"] == "run_recorded", result
    assert result["run"]["run_id"] == old["run_id"] and count(db, "_robot_runs") == 1
    assert count(db, "_experiment_executions") == (2 if phase == "before" else 1)
    assert budget.checkpoint(db, item["batch_id"])["status"] == "completed"


def test_retry_preserves_reserved_run_even_when_another_request_populates_cache(source, monkeypatch):
    db, _, _, _, _ = source
    original = backtest.run_python
    calls = []

    def fail_once(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise ValueError("falha antes da publicação")
        return original(*args, **kwargs)

    monkeypatch.setattr(backtest, "run_python", fail_once)
    first = asked(source)
    failed = experiments.evaluate(db, first["request_id"])
    assert failed["request"]["state"] == "failed", failed
    reserved = failed["request"]["run_id"]
    other = experiments.evaluate(db, asked(source, "other-query")["request_id"])
    assert other["request"]["state"] == "run_recorded", other
    assert other["run"]["run_id"] != reserved
    resumed = experiments.evaluate(db, first["request_id"])
    assert resumed["request"]["state"] == "run_recorded", resumed
    assert resumed["run"]["run_id"] == reserved
    assert budget.checkpoint(db, first["batch_id"])["jobs"][0]["run_id"] == reserved
    assert len(calls) == 3 and count(db, "_robot_runs") == 2


def test_injected_budget_cannot_disable_the_frozen_job_cap(source):
    db, definition, composition, _, _ = source
    definition["name"] = "bounded-request"
    definition["budget"]["limits"]["max_jobs"] = 1
    protocol = experiments.create(db, definition)["protocol_id"]
    item = experiments.request(db, protocol, "one", composition)
    relaxed = budget.Budget(db, limits=budget.Limits(min_free_mb=0), batch_id=item["batch_id"])
    result = experiments.evaluate(db, item["request_id"], budget=relaxed)
    assert "Budget deve" in result["evaluation_error"]
    assert count(db, "_robot_runs") == count(db, "_experiment_executions") == 0
    assert budget.checkpoint(db, item["batch_id"]) is None


def test_unavailable_access_journal_denies_delivery_of_an_already_recorded_run(source, monkeypatch):
    db, _, _, _, _ = source
    item = asked(source, role="holdout")
    saved = experiments.evaluate(db, item["request_id"])
    assert saved["request"]["state"] == "run_recorded", saved

    def unavailable(*args, **kwargs):
        raise ValueError("acesso não registrado")

    monkeypatch.setattr(store, "access", unavailable)
    denied = experiments.evaluate(db, item["request_id"])
    assert "acesso não registrado" in denied["evaluation_error"] and "run" not in denied
    assert denied["request"]["run_id"] == saved["run"]["run_id"]
    assert count(db, "_robot_runs") == 1
