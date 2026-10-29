"""Regressões do serviço compartilhado, separadas da ficha AO/AC."""
import concurrent.futures
import threading

import pandas as pd
import pytest

from mt5_extracao.analytics import feature_store, indicator_context, indicator_materializer as materializer, producer_jobs, resampling
from tests.test_indicator_materialization import db, facts, seed, source


@pytest.mark.parametrize("boundary", ["first_batch", "completion", "cache_skip"])
def test_source_change_aborts_snapshot_and_resume_uses_current_source(db, monkeypatch, boundary):
    frame = source(45)
    seed(db, frame)
    ident = "trend.alligator_jaw_13"
    if boundary == "cache_skip":
        materializer.materialize(db, "TEST", "M5", feature_ids=[ident])
    original = feature_store.save_values
    changed = False
    receipt = None
    def correct_before_commit(db_, records, **kwargs):
        nonlocal changed, receipt
        if not changed and (bool(records) == (boundary == "first_batch")):
            changed = True
            correction = frame.iloc[[100]].copy(); correction["high"] += 20
            seed(db_, correction)
            receipt = resampling.latest_bars(db_, "TEST", "M5").computed_at_utc.max()
        return original(db_, records, **kwargs)
    monkeypatch.setattr(feature_store, "save_values", correct_before_commit)
    with pytest.raises(RuntimeError, match="fonte mudou"):
        materializer.materialize(db, "TEST", "M5", feature_ids=[ident])
    assert changed
    assert producer_jobs.latest_run(db, "TEST", "M5", materializer.FAMILY)["status"] == "FAILED"
    checkpoint = producer_jobs.get_checkpoint(db, "TEST", "M5", materializer.FAMILY)
    assert checkpoint is None or checkpoint["status"] != "COMPLETE" or (
        boundary == "cache_skip" and checkpoint["source_sequence"] != resampling.latest_source_sequence(db, "TEST", "M5"))
    confirmed = facts(db)
    assert len(confirmed) == (0 if boundary == "first_batch" else 45)
    before = feature_store.query_as_of(db, receipt - pd.Timedelta(nanoseconds=1), symbol="TEST", feature_id=ident)
    monkeypatch.setattr(feature_store, "save_values", original)
    result = materializer.materialize(db, "TEST", "M5", feature_ids=[ident])
    assert result["job"]["status"] == "COMPLETE"
    checkpoint = producer_jobs.get_checkpoint(db, "TEST", "M5", materializer.FAMILY)
    assert checkpoint["source_sequence"] == resampling.latest_source_sequence(db, "TEST", "M5")
    assert all(row in facts(db) for row in confirmed)
    after = feature_store.query_as_of(db, receipt, symbol="TEST", feature_id=ident)
    affected = after[after.observation_time >= pd.Timestamp("2026-09-01T10:40Z")]
    assert not affected.empty and affected.available_at_utc.ge(receipt).all()
    if not before.empty:
        pd.testing.assert_frame_equal(before, feature_store.query_as_of(db, receipt - pd.Timedelta(nanoseconds=1),
                                                                       symbol="TEST", feature_id=ident))


def test_overlapping_store_retries_are_idempotent(db):
    ident = "price.return_1"
    implementation = materializer.implementation_signature()
    materializer._register(db, [ident], implementation)
    record = dict(symbol="TEST", timeframe="M5", feature_id=ident,
                  algorithm_version=materializer.algorithm_version(ident, implementation),
                  observation_time="2026-09-01T09:00Z", available_at="2026-09-01T09:05Z",
                  observed_at="2026-09-01T09:05Z",
                  value=0.1, source_revision="owned-concurrent-fixture")
    barrier = threading.Barrier(2)
    def save(_):
        barrier.wait(timeout=10)
        return feature_store.save_values(db, [record])
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(save, range(2)))
    assert sum(r["inserted"] for r in outcomes) == 1
    assert sum(r["unchanged"] for r in outcomes) == 1
    assert all(r["revised"] == 0 for r in outcomes)
    history = feature_store.query_history(db, symbol="TEST", feature_id=ident)
    assert len(history) == 1 and history.iloc[0].revision == 1 and history.iloc[0].numeric_value == 0.1


def test_global_version_transition_keeps_explicit_legacy_context(db, monkeypatch):
    seed(db, source(25))
    ident = "price.return_1"
    monkeypatch.setattr(materializer, "implementation_signature", lambda feature_id=None, **kwargs: "1" * 64)
    materializer.materialize(db, "TEST", "M5", feature_ids=[ident])
    legacy = materializer.algorithm_version(ident)
    original = facts(db)
    decisions = pd.DataFrame({"decision_time": ["2026-09-02T00:00Z"]})
    monkeypatch.setattr(materializer, "implementation_signature", lambda feature_id=None, **kwargs: "2" * 64)
    current = indicator_context.context(db, decisions, symbol="TEST", timeframe="M5", feature_ids=[ident])
    assert current.iloc[0][ident + "::state"] != "AVAILABLE"
    pinned = indicator_context.context(db, decisions, symbol="TEST", timeframe="M5", feature_ids=[ident], algorithm_version=legacy)
    assert pinned.iloc[0][ident + "::state"] == "AVAILABLE" and pinned.iloc[0][ident + "::value"] is not None
    assert facts(db) == original



def test_source_sequence_uses_the_supplied_transaction_for_schema_inspection(db):
    """Uma guarda de snapshot não pode abrir outra conexão sob lock SQLite."""
    from sqlalchemy import event

    # Banco legado sem o schema Analytics: inspeção não pode ser omitida.
    with db.engine.begin() as transaction:
        def same_empty_connection(conn, cursor, statement, parameters, context, executemany):
            assert conn is transaction, "A inspeção da tabela ausente saiu da transação"
        event.listen(db.engine, "before_cursor_execute", same_empty_connection)
        try:
            assert resampling.latest_source_sequence(db, "TEST", "M5", connection=transaction) is None
        finally:
            event.remove(db.engine, "before_cursor_execute", same_empty_connection)
    seed(db, source(15))
    expected = resampling.latest_source_sequence(db, "TEST", "M5")
    with db.engine.begin() as transaction:
        def same_connection(conn, cursor, statement, parameters, context, executemany):
            assert conn is transaction, "A inspeção saiu da transação de consistência"
        event.listen(db.engine, "before_cursor_execute", same_connection)
        try:
            assert resampling.latest_source_sequence(db, "TEST", "M5", connection=transaction) == expected
        finally:
            event.remove(db.engine, "before_cursor_execute", same_connection)
