"""Interrupção encerra recursos/recibo e preserva apenas lotes confirmados."""
import pytest

from mt5_extracao.analytics import feature_store, indicator_materializer, producer_jobs, producers
from mt5_extracao.database_manager import DatabaseManager
from tests.test_indicator_materialization import db, facts, seed, source


class AbortSignal(BaseException):
    pass


def execute(db, engine):
    if engine == "registry":
        return indicator_materializer.materialize(db, "TEST", "M5", feature_ids=["price.return_1"], batch_size=1)
    return producers.compute(db, "TEST", "M5", families=["calendar"], batch_size=1, max_retries=0)[0]


@pytest.mark.parametrize("engine", ["registry", "calendar"])
@pytest.mark.parametrize("phase", ["transaction", "after_commit"])
@pytest.mark.parametrize("error_type", [KeyboardInterrupt, SystemExit, AbortSignal])
def test_interruption_cleans_sampler_and_resumes_exact_committed_history(db, tmp_path, monkeypatch, engine, phase, error_type):
    seed(db, source(4))
    original_start, original_save = producer_jobs.start_run, feature_store.save_values
    runs, fired = [], []
    error = error_type("interrupção QA")
    def start(*args, **kwargs):
        run = original_start(*args, **kwargs)
        runs.append(run)
        return run
    def save(manager, records, **kwargs):
        if records and not fired:
            fired.append(True)
            if phase == "transaction":
                original_hook = kwargs.pop("transaction_hook")
                def hook(conn, counts):
                    original_hook(conn, counts)
                    raise error
                return original_save(manager, records, transaction_hook=hook, **kwargs)
            original_save(manager, records, **kwargs)
            raise error
        return original_save(manager, records, **kwargs)
    monkeypatch.setattr(producer_jobs, "start_run", start)
    monkeypatch.setattr(feature_store, "save_values", save)
    try:
        with pytest.raises(error_type) as caught:
            execute(db, engine)
        assert caught.value is error
        assert len(runs) == 1
        assert not runs[0]["rss_sampler"].thread.is_alive()
        family = indicator_materializer.FAMILY if engine == "registry" else "calendar"
        job = producer_jobs.latest_run(db, "TEST", "M5", family)
        assert job["status"] == ("FAILED" if error_type is AbortSignal else "CANCELLED")
        assert job["finished_at_utc"] and job["error"] == "interrupção QA"
        prefix = facts(db)
        checkpoint = producer_jobs.get_checkpoint(db, "TEST", "M5", family)
        if phase == "transaction":
            assert prefix == [] and checkpoint is None
        else:
            assert prefix and checkpoint["status"] == "RUNNING"
        monkeypatch.setattr(feature_store, "save_values", original_save)
        resumed = execute(db, engine)
        assert resumed["job"]["status"] == "COMPLETE"
        assert producer_jobs.get_checkpoint(db, "TEST", "M5", family)["status"] == "COMPLETE"
        assert all(row in facts(db) for row in prefix)
        assert all(row[7] == 1 for row in facts(db))
        reference = DatabaseManager(db_path=str(tmp_path / "reference.sqlite"), broker_utc_offset=0)
        try:
            seed(reference, source(4))
            execute(reference, engine)
            assert facts(db) == facts(reference)
        finally:
            reference.close()
    finally:
        for run in runs:
            run["rss_sampler"].stop()


@pytest.mark.parametrize("engine", ["registry", "calendar"])
@pytest.mark.parametrize("error_type", [KeyboardInterrupt, RuntimeError])
def test_journal_failure_preserves_original_error_and_stops_resources(db, monkeypatch, caplog, engine, error_type):
    seed(db, source(4))
    original_start = producer_jobs.start_run
    runs = []
    error = error_type("erro original QA")
    def start(*args, **kwargs):
        run = original_start(*args, **kwargs)
        runs.append(run)
        return run
    def save(*args, **kwargs):
        raise error
    def finish(*args, **kwargs):
        raise OSError("disco QA indisponível")
    monkeypatch.setattr(producer_jobs, "start_run", start)
    monkeypatch.setattr(producer_jobs, "finish_run", finish)
    monkeypatch.setattr(feature_store, "save_values", save)
    try:
        with pytest.raises(error_type) as caught:
            execute(db, engine)
        assert caught.value is error
        assert runs and not runs[0]["rss_sampler"].thread.is_alive()
        assert "disco QA indisponível" in caplog.text
        assert "não foi persistido" in caplog.text
        family = indicator_materializer.FAMILY if engine == "registry" else "calendar"
        assert producer_jobs.latest_run(db, "TEST", "M5", family)["status"] == "RUNNING"
        assert producer_jobs.get_checkpoint(db, "TEST", "M5", family) is None
    finally:
        for run in runs:
            run["rss_sampler"].stop()


@pytest.mark.parametrize("status", ["SKIPPED", "COMPLETE"])
def test_finish_stops_sampler_before_optional_metrics_can_fail(db, monkeypatch, status):
    run = producer_jobs.start_run(db, "TEST", "M5", "qa", "v1", "qa", 0)
    def unavailable(manager):
        raise OSError("métrica de disco QA indisponível")
    monkeypatch.setattr(producer_jobs, "_db_bytes", unavailable)
    try:
        with pytest.raises(OSError, match="métrica de disco QA"):
            producer_jobs.finish_run(db, run, source_sequence=0, status=status)
        assert not run["rss_sampler"].thread.is_alive()
        assert producer_jobs.latest_run(db, "TEST", "M5", "qa")["status"] == "RUNNING"
    finally:
        run["rss_sampler"].stop()
