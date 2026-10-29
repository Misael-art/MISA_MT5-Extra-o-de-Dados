"""BD14: eventos persistidos, deduplicação histórica e retomada exata."""
import json
import sqlite3

import pandas as pd
import pytest
from sqlalchemy import text

from mt5_extracao import cli, sdk
from mt5_extracao.analytics import datasets, feature_store, features, indicator_materializer, producer_jobs
from mt5_extracao.database_manager import DatabaseManager
from tests.test_feature_compositions import A, ABS, ALL, B, DIFF, db, define, freeze, put, source
from tests.test_frozen_observation_context import frozen


def materialize(source, snapshot, **kwargs):
    kwargs.setdefault("as_of", "2026-01-01T00:00Z")
    return features.materialize_snapshot(source, snapshot.manifest_id, targets=[ABS], **kwargs)


def output_facts(source):
    with source.engine.connect() as conn:
        return conn.execute(text("SELECT " + ",".join(feature_store.VALUE_COLUMNS) + ",value_hash "
            "FROM _feature_values WHERE algorithm_version LIKE '%+mat.%' "
            "ORDER BY feature_id,observation_time,revision")).all()


def test_materializes_real_receipts_expiry_and_old_observation_without_decision_grid(source, frozen):
    parents = feature_store.query_history(source)
    result = materialize(source, frozen, batch_size=2)
    assert result["job"]["status"] == "COMPLETE" and result["input_basis"] == "frozen_snapshot"
    assert result["inserted"] == 4 and result["revised"] == 6 and result["transitions"] == 10
    assert not result["qualification"]["latest_live_source_certified"]
    outputs = {item["feature_id"]:item for item in result["outputs"]}
    version = outputs[ABS[0]]["algorithm_version"]
    history = feature_store.query_history(source, feature_id=ABS[0], algorithm_version=version)
    old = history[history.observation_time == pd.Timestamp("2025-01-01T00:00Z")]
    assert old.numeric_value.tolist()[0] == 2.
    assert pd.isna(old.numeric_value.iloc[1]) and old.numeric_value.tolist()[2:] == [6.,3.]
    assert old.available_at_utc.tolist() == [pd.Timestamp(stamp) for stamp in (
        "2025-01-01T00:01Z", "2025-01-01T00:01:30Z", "2025-01-01T00:03Z", "2025-01-02T00:00Z")]
    assert old.valid_until_utc.iloc[0] == pd.Timestamp("2025-01-01T00:01:30Z")
    assert history.loc[history.observation_time == pd.Timestamp("2025-01-01T00:01Z"), "numeric_value"].tolist() == [26.]
    for row in old.itertuples():
        metadata = json.loads(row.metadata_json)
        assert metadata["materialization"]["source_manifest_id"] == frozen.manifest_id
        assert metadata["basis"] == "materialized_frozen_context"
    assert datasets.for_run(source, result["job"]["run_id"]).manifest_id == frozen.manifest_id
    pd.testing.assert_frame_equal(parents, feature_store.query_history(source).loc[
        lambda frame: ~frame.algorithm_version.str.contains("+mat.", regex=False)].reset_index(drop=True))
    before = output_facts(source)
    repeated = materialize(source, frozen, batch_size=1)
    assert repeated["job"]["status"] == "SKIPPED" and repeated["unchanged"] == 10
    assert output_facts(source) == before


def test_new_snapshot_reuses_old_revisions_and_preserves_their_original_manifest(source, frozen):
    first = materialize(source, frozen)
    before = output_facts(source)
    put(source, A, 10., available="2025-01-02T00:01Z")
    revised = freeze(source)
    result = materialize(source, revised)
    assert result["inserted"] == 0 and result["revised"] == 2 and result["unchanged"] == 10
    assert all(row in output_facts(source) for row in before)
    assert [d["algorithm_version"] for d in first["outputs"]] == [d["algorithm_version"] for d in result["outputs"]]
    assert len(output_facts(source)) == 12
    assert materialize(source, revised)["job"]["status"] == "SKIPPED"


def test_ack_failure_resumes_to_exact_full_reference_and_does_not_replay_old_events(source, frozen, tmp_path, monkeypatch):
    destination = tmp_path / "full.sqlite"
    with sqlite3.connect(source.db_path) as original, sqlite3.connect(destination) as target:
        original.backup(target)
    reference = DatabaseManager(db_path=str(destination), broker_utc_offset=0)
    save = feature_store.save_values
    def interrupt(db_, records, **kwargs):
        result = save(db_, records, **kwargs)
        if records:
            raise RuntimeError("ACK QA perdido")
        return result
    monkeypatch.setattr(feature_store, "save_values", interrupt)
    try:
        with pytest.raises(RuntimeError, match="ACK QA"):
            materialize(source, frozen, batch_size=1)
        prefix = output_facts(source)
        assert len(prefix) == 1
        monkeypatch.setattr(feature_store, "save_values", save)
        resumed = materialize(source, frozen, batch_size=1)
        materialize(reference, datasets.load(reference, frozen.manifest_id))
        assert resumed["job"]["status"] == "COMPLETE" and resumed["unchanged"] == 1
        assert output_facts(source) == output_facts(reference)
        assert all(row in output_facts(source) for row in prefix)
    finally:
        reference.close()


def test_foreign_snapshot_remains_readable_but_cannot_write_into_another_namespace(source, frozen, tmp_path):
    package = tmp_path / "snapshot.json"
    datasets.export_snapshot(source, frozen.manifest_id, package)
    other = DatabaseManager(db_path=str(tmp_path / "other.sqlite"), broker_utc_offset=0)
    try:
        imported = datasets.import_snapshot(other, package)
        assert features.compose_snapshot(imported, pd.DataFrame(dict(decision_time=["2025-01-01T00:03Z"])),
                                        targets=[ABS])["context"][0][ABS[0] + "::value"] == 26.
        with pytest.raises(ValueError, match="outra base.*namespace"):
            materialize(other, imported)
    finally:
        other.close()


def test_corrupt_existing_hash_is_not_silently_repaired_or_accepted(source, frozen):
    materialize(source, frozen)
    with source.engine.begin() as conn:
        conn.execute(text("UPDATE _feature_values SET numeric_value=999 WHERE algorithm_version LIKE '%+mat.%' "
                          "AND feature_id=:ident AND revision=1"), dict(ident=ABS[0]))
    corrupted = output_facts(source)
    with pytest.raises(ValueError, match="Hash/proveniência"):
        materialize(source, frozen)
    assert output_facts(source) == corrupted


def test_cli_sdk_delegate_to_same_writer_and_explicit_stored_version(source, frozen, monkeypatch, capsys):
    monkeypatch.setattr(cli.Context, "db", property(lambda _: source))
    assert cli.main(["--json", "analytics", "features", "materialize-composite", "--manifest", frozen.manifest_id,
                     "--target", *ABS, "--batch-size", "2", "--as-of", "2026-01-01T00:00Z"]) == 0
    result = json.loads(capsys.readouterr().out)["data"]["materialization"]
    repeated = sdk.materialize_composite_snapshot(source, frozen.manifest_id, targets=[ABS], as_of="2026-01-01T00:00Z")
    assert repeated["outputs"] == result["outputs"] and repeated["job"]["status"] == "SKIPPED"
    version = next(d["algorithm_version"] for d in result["outputs"] if d["feature_id"] == ABS[0])
    assert cli.main(["--json", "analytics", "features", "context", "--symbol", "QA", "--tf", "M1",
                    "--features", ABS[0], "--algorithm-version", version, "--as-of", "2025-01-02T00:00Z"]) == 0
    assert json.loads(capsys.readouterr().out)["data"]["context"][0][ABS[0] + "::value"] == 26.


def test_future_expiry_is_only_validity_until_explicit_cut_and_does_not_block_new_receipts(source):
    define(source)
    put(source, A, 1., valid_until="2099-01-01T00:00Z")
    put(source, B, 3.)
    first = materialize(source, freeze(source))
    assert first["inserted"] == 2 and first["revised"] == 0
    put(source, A, 9., available="2025-01-01T00:03Z")
    updated = materialize(source, freeze(source))
    assert updated["revised"] == 2 and updated["unchanged"] == 2
    assert max(row[3] for row in output_facts(source)) < "2026"


def test_old_snapshot_cannot_publish_a_later_expiry_after_newer_inputs_were_materialized(source):
    define(source)
    put(source, A, 1., valid_until="2025-01-01T00:05Z")
    put(source, B, 3.)
    old = freeze(source)
    materialize(source, old, as_of="2025-01-01T00:02Z")
    put(source, A, 9., available="2025-01-01T00:03Z")
    materialize(source, freeze(source), as_of="2025-01-01T00:04Z")
    before = output_facts(source)
    with pytest.raises(indicator_materializer.SourceChanged, match="omite entradas.*mais recente"):
        materialize(source, old, as_of="2025-01-01T00:06Z")
    assert output_facts(source) == before


def test_backdated_new_event_is_rejected_before_any_child_write(source):
    define(source); put(source, A, 1.); put(source, B, 3.)
    put(source, A, 9., available="2025-01-01T00:03Z")
    materialize(source, freeze(source))
    before = output_facts(source)
    put(source, B, 4., available="2025-01-01T00:02Z")
    with pytest.raises(indicator_materializer.SourceChanged, match="retrocede o recebimento"):
        materialize(source, freeze(source))
    assert output_facts(source) == before


def test_concurrent_writer_is_detected_in_commit_and_retry_adopts_the_confirmed_fact(source, frozen, monkeypatch):
    save = feature_store.save_values
    fired = []
    def race(db_, records, **kwargs):
        if records and not fired:
            fired.append(True)
            other = DatabaseManager(db_path=source.db_path, broker_utc_offset=0)
            try:
                save(other, [records[0]])
            finally:
                other.close()
        return save(db_, records, **kwargs)
    monkeypatch.setattr(feature_store, "save_values", race)
    with pytest.raises(indicator_materializer.SourceChanged, match="Histórico composto mudou"):
        materialize(source, frozen, batch_size=1)
    assert len(output_facts(source)) == 1
    monkeypatch.setattr(feature_store, "save_values", save)
    resumed = materialize(source, frozen, batch_size=1)
    assert resumed["unchanged"] == 1 and len(output_facts(source)) == 10
    assert materialize(source, frozen)["job"]["status"] == "SKIPPED"


@pytest.mark.parametrize("phase", ["transaction", "after_commit"])
@pytest.mark.parametrize("error_type", [KeyboardInterrupt, OSError])
def test_interruption_and_disk_errors_preserve_only_confirmed_facts(source, frozen, monkeypatch, phase, error_type):
    save, start = feature_store.save_values, producer_jobs.start_run
    runs, fired = [], []
    error = error_type("falha QA controlada")
    def track(*args, **kwargs):
        run = start(*args, **kwargs); runs.append(run); return run
    def fail(db_, records, **kwargs):
        if records and not fired:
            fired.append(True)
            if phase == "transaction":
                prior = kwargs.pop("transaction_hook")
                def hook(conn, counts):
                    prior(conn, counts)
                    raise error
                return save(db_, records, transaction_hook=hook, **kwargs)
            save(db_, records, **kwargs)
            raise error
        return save(db_, records, **kwargs)
    monkeypatch.setattr(producer_jobs, "start_run", track)
    monkeypatch.setattr(feature_store, "save_values", fail)
    with pytest.raises(error_type) as caught:
        materialize(source, frozen, batch_size=1)
    assert caught.value is error and not runs[0]["rss_sampler"].thread.is_alive()
    assert len(output_facts(source)) == (1 if phase == "after_commit" else 0)
    with source.engine.connect() as conn:
        status = conn.execute(text("SELECT status FROM _analytics_producer_runs WHERE run_id=:id"),
                              dict(id=runs[0]["run_id"])).scalar_one()
    assert status == ("CANCELLED" if error_type is KeyboardInterrupt else "FAILED")
    monkeypatch.setattr(feature_store, "save_values", save)
    materialize(source, frozen, batch_size=1)
    assert len(output_facts(source)) == 10


def test_cancellation_after_confirmed_batch_resumes_without_false_complete(source, frozen, monkeypatch):
    save, cancelled = feature_store.save_values, []
    def after_commit(db_, records, **kwargs):
        result = save(db_, records, **kwargs)
        if records:
            cancelled.append(True)
        return result
    monkeypatch.setattr(feature_store, "save_values", after_commit)
    with pytest.raises(indicator_materializer.Cancelled):
        materialize(source, frozen, batch_size=1, cancel_check=lambda: bool(cancelled))
    assert len(output_facts(source)) == 1
    monkeypatch.setattr(feature_store, "save_values", save)
    assert materialize(source, frozen)["unchanged"] == 1


def test_resource_guard_inside_transaction_rolls_back_values_and_checkpoint(source, frozen, monkeypatch):
    save, blocked = feature_store.save_values, []
    def wrap(db_, records, **kwargs):
        if records:
            prior = kwargs.pop("transaction_hook")
            def hook(conn, counts):
                blocked.append(True)
                return prior(conn, counts)
            return save(db_, records, transaction_hook=hook, **kwargs)
        return save(db_, records, **kwargs)
    monkeypatch.setattr(feature_store, "save_values", wrap)
    with pytest.raises(RuntimeError, match="limite QA"):
        materialize(source, frozen, write_guard=lambda: "limite QA" if blocked else None)
    assert output_facts(source) == []
    with source.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM _analytics_producer_checkpoints")).scalar_one() == 0


def test_live_parent_change_does_not_complete_the_pinned_job_as_live_data(source, frozen, monkeypatch):
    save, changed = feature_store.save_values, []
    def change_live(db_, records, **kwargs):
        if records and not changed:
            changed.append(True)
            # Usa a implementação original para não interceptar a correção do pai.
            record = dict(symbol="QA", timeframe="M1", feature_id=A[0], algorithm_version=A[1],
                observation_time="2025-01-01T00:00Z", value=999.,
                available_at="2025-01-02T00:02Z", observed_at="2025-01-02T00:02Z")
            save(db_, [record])
        return save(db_, records, **kwargs)
    monkeypatch.setattr(feature_store, "save_values", change_live)
    result = materialize(source, frozen)
    assert result["job"]["status"] == "COMPLETE"
    assert result["input_basis"] == "frozen_snapshot" and not result["qualification"]["latest_live_source_certified"]
    assert all(row[8] != 999. for row in output_facts(source))


def test_materialized_channel_is_a_typed_frozen_leaf_for_the_next_composition(source, frozen):
    result = materialize(source, frozen)
    version = next(d["algorithm_version"] for d in result["outputs"] if d["feature_id"] == ABS[0])
    third = ("qa.third", "v1")
    features.register_composite(source, third[0], "Terceiro nível", third[1], operation="absolute", inputs=[(ABS[0],version)])
    snapshot = freeze(source, [(ABS[0],version), third])
    downstream = features.materialize_snapshot(source, snapshot.manifest_id, targets=[third], as_of="2026-01-01T00:00Z")
    assert downstream["transitions"] == 5
    assert downstream["outputs"][0]["parameters"]["materialization"]["parents"] == [dict(feature_id=ABS[0],algorithm_version=version)]


@pytest.mark.parametrize("as_of,match", [("2026-01-01", "fuso"), ("2099-01-01T00:00Z", "futuro")])
def test_cut_requires_utc_and_never_anticipates_future_receipts(source, frozen, as_of, match):
    with pytest.raises(ValueError, match=match):
        materialize(source, frozen, as_of=as_of)
    assert output_facts(source) == []


def test_global_event_budget_is_checked_before_any_value_is_published(source, frozen, monkeypatch):
    from mt5_extracao.analytics import query_service
    monkeypatch.setattr(query_service, "MAX_SELECTION_ROWS", 19)
    with pytest.raises(ValueError, match="pares nó/evento"):
        materialize(source, frozen)
    assert output_facts(source) == []


def test_every_run_retains_its_exact_cut_and_source_after_later_checkpoints(source, frozen):
    early = materialize(source, frozen, as_of="2025-01-01T00:02Z")
    late = materialize(source, frozen)
    with source.engine.connect() as conn:
        for result in (early, late):
            config = json.loads(conn.execute(text("SELECT configuration_json FROM _analytics_producer_runs WHERE run_id=:id"),
                                            dict(id=result["job"]["run_id"])).scalar_one())
            assert config["as_of"] == result["as_of"] and config["manifest_id"] == frozen.manifest_id
            assert config["input_basis"] == "frozen_snapshot"


def test_empty_frozen_parent_history_never_invents_observations(source):
    define(source)
    with pytest.raises(ValueError, match="Nenhuma observação"):
        materialize(source, freeze(source))
    assert output_facts(source) == []


def test_equal_availability_uses_same_observed_precedence_as_canonical_sql(source):
    from mt5_extracao.analytics import indicator_context
    define(source)
    for value, observed, expiry in [(1., "2025-01-01T00:01:30Z", "2025-01-01T00:04Z"),
                                    (9., "2025-01-01T00:01Z", "2025-01-01T00:03Z")]:
        feature_store.save_value(source, "QA", "M1", *A, "2025-01-01T00:00Z", value,
            available_at="2025-01-01T00:02Z", observed_at=observed, valid_until=expiry)
    put(source, B, 3., available="2025-01-01T00:02Z")
    snapshot = freeze(source)
    sql = feature_store.query_as_of(source, "2025-01-01T00:02Z", feature_id=A[0], algorithm_version=A[1])
    frame = indicator_context.from_snapshot(snapshot, pd.DataFrame(dict(decision_time=["2025-01-01T00:02Z"])),
        feature_ids=[A[0]], algorithm_version=A[1], observation_time="2025-01-01T00:00Z")
    assert frame.iloc[0][A[0] + "::value"] == sql.iloc[0].numeric_value == 1.
    result = materialize(source, snapshot)
    version = next(d["algorithm_version"] for d in result["outputs"] if d["feature_id"] == ABS[0])
    rows = feature_store.query_history(source, feature_id=ABS[0], algorithm_version=version)
    assert rows.iloc[0].numeric_value == 2. and rows.iloc[0].valid_until_utc == pd.Timestamp("2025-01-01T00:04Z")
    assert rows.available_at_utc.tolist() == [pd.Timestamp("2025-01-01T00:02Z"),pd.Timestamp("2025-01-01T00:04Z")]


def test_namespace_change_before_commit_cannot_publish_a_false_complete(source, frozen, monkeypatch):
    save, changed = feature_store.save_values, []
    def fork_before_write(db_, records, **kwargs):
        if records and not changed:
            changed.append(True)
            datasets.fork(source)
        return save(db_, records, **kwargs)
    monkeypatch.setattr(feature_store, "save_values", fork_before_write)
    with pytest.raises(indicator_materializer.SourceChanged, match="Identidade da base mudou"):
        materialize(source, frozen)
    assert output_facts(source) == []


def test_calculation_change_between_passes_never_publishes_mismatched_values(source, frozen, monkeypatch):
    save, changed = feature_store.save_values, []
    operator = features._COMPOSITE_OPERATORS["absolute"]
    original = operator["evaluate"]
    def mutate(db_, records, **kwargs):
        result = save(db_, records, **kwargs)
        if records and not changed:
            changed.append(True)
            monkeypatch.setitem(operator, "evaluate", lambda values: abs(values[0]) + 1.)
        return result
    monkeypatch.setattr(feature_store, "save_values", mutate)
    with pytest.raises(indicator_materializer.SourceChanged, match="cálculo mudou"):
        materialize(source, frozen, batch_size=1)
    assert 0 < len(output_facts(source)) < 10
    assert all(row[8] != 27. for row in output_facts(source))
    monkeypatch.setattr(feature_store, "save_values", save)
    monkeypatch.setitem(operator, "evaluate", original)
    materialize(source, frozen)
    assert len(output_facts(source)) == 10


def test_legacy_job_schema_gains_configuration_without_replacing_old_receipts(db):
    with db.engine.begin() as conn:
        conn.execute(text("""CREATE TABLE _analytics_producer_runs (
            run_id TEXT PRIMARY KEY, symbol TEXT NOT NULL, tf TEXT NOT NULL, family TEXT NOT NULL,
            algorithm_version TEXT NOT NULL, config_hash TEXT NOT NULL, source_sequence_start BIGINT,
            source_sequence_end BIGINT, status TEXT NOT NULL, started_at_utc TEXT NOT NULL, finished_at_utc TEXT,
            duration_ms BIGINT, peak_rss_bytes BIGINT, database_bytes_before BIGINT, database_bytes_after BIGINT,
            rows_inserted BIGINT NOT NULL DEFAULT 0, rows_revised BIGINT NOT NULL DEFAULT 0,
            rows_unchanged BIGINT NOT NULL DEFAULT 0, error TEXT)"""))
        conn.execute(text("INSERT INTO _analytics_producer_runs "
            "(run_id,symbol,tf,family,algorithm_version,config_hash,status,started_at_utc) "
            "VALUES ('old','QA','M1','old','v1','old','COMPLETE','2025-01-01T00:00:00Z')"))
    configuration = dict(input_basis="frozen_snapshot", as_of="2025-01-01T00:00Z")
    run = producer_jobs.start_run(db, "QA", "M1", "new", "v1", "new", 0, configuration=configuration)
    try:
        with db.engine.connect() as conn:
            old = conn.execute(text("SELECT status,configuration_json FROM _analytics_producer_runs WHERE run_id='old'")).one()
            assert tuple(old) == ("COMPLETE",None)
            stored = conn.execute(text("SELECT configuration_json FROM _analytics_producer_runs WHERE run_id=:id"),
                                  dict(id=run["run_id"])).scalar_one()
            assert json.loads(stored) == configuration
    finally:
        producer_jobs.finish_run(db, run, source_sequence=0, status="COMPLETE")


def simultaneous_writers(source, frozen, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    history = features._materialization_history
    barrier = Barrier(2)
    def same_generation(*args, **kwargs):
        result = history(*args, **kwargs)
        barrier.wait(timeout=20)
        return result
    monkeypatch.setattr(features, "_materialization_history", same_generation)
    def write():
        try:
            return materialize(source, frozen, batch_size=5000)
        except indicator_materializer.SourceChanged as exc:
            return exc
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(write) for _ in range(2)]
        results = [future.result(timeout=30) for future in futures]
    assert sum(isinstance(item, indicator_materializer.SourceChanged) for item in results) == 1
    assert sum(isinstance(item, dict) and item["job"]["status"] == "COMPLETE" for item in results) == 1
    assert len(output_facts(source)) == 10
    monkeypatch.setattr(features, "_materialization_history", history)
    assert materialize(source, frozen)["job"]["status"] == "SKIPPED"


def test_two_actual_writers_cannot_both_confirm_the_same_preflight_generation(source, frozen, monkeypatch):
    simultaneous_writers(source, frozen, monkeypatch)


@pytest.mark.parametrize("left,right,expected", [(0.,0.,0.),(None,0.,None),(1e308,-1e308,None)])
def test_zero_missing_and_nonfinite_results_keep_distinct_typed_states(source, left, right, expected):
    define(source)
    put(source, A, left, **({"quality":"UNAVAILABLE"} if left is None else {}))
    put(source, B, right)
    snapshot = freeze(source)
    result = materialize(source, snapshot)
    version = next(d["algorithm_version"] for d in result["outputs"] if d["feature_id"] == ABS[0])
    row = feature_store.query_history(source, feature_id=ABS[0], algorithm_version=version).iloc[0]
    if expected is None:
        assert pd.isna(row.numeric_value) and row.quality == "UNAVAILABLE"
    else:
        assert row.numeric_value == expected and row.quality == "VALID"
    assert materialize(source, snapshot)["job"]["status"] == "SKIPPED"


@pytest.mark.parametrize("limits,error_type", [({"timeout_seconds":1e-9},TimeoutError),({"max_memory_mb":1e-6},MemoryError)])
def test_existing_runtime_budgets_abort_before_values_and_retain_input_reference(source, frozen, limits, error_type):
    with pytest.raises(error_type):
        materialize(source, frozen, **limits)
    assert output_facts(source) == []
    with source.engine.connect() as conn:
        run = conn.execute(text("SELECT run_id,status,configuration_json FROM _analytics_producer_runs")).one()
        assert run.status == "FAILED" and json.loads(run.configuration_json)["manifest_id"] == frozen.manifest_id
    assert datasets.for_run(source, run.run_id).manifest_id == frozen.manifest_id
