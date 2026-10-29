"""Mesmo produtor congelado em schema PostgreSQL próprio; sem servidor, aceite pendente."""
import pytest

from mt5_extracao.analytics import datasets
from tests.postgres_helpers import isolated_postgresql_database
from tests.test_analytics_profiles_pg import PG_URL
from tests.test_feature_compositions import A, freeze, put, source
from tests.test_frozen_observation_context import frozen
from tests.test_composite_materialization import materialize, output_facts, simultaneous_writers

pytestmark = pytest.mark.skipif(not PG_URL, reason="defina MT5X_TEST_PG_URL para testar com PostgreSQL")


@pytest.fixture
def pg():
    pytest.importorskip("psycopg")
    with isolated_postgresql_database(PG_URL, broker_utc_offset=0) as manager:
        datasets.declare_source(manager, {"provider": "qa", "server": "Synthetic"})
        assert manager.time_context()["offset_state"] == "declared"
        yield manager


@pytest.fixture
def db(pg):
    return pg


def test_frozen_chain_replay_and_late_old_observation_revision_are_exact(pg, source, frozen):
    result = materialize(source, frozen, batch_size=2)
    assert result["inserted"] == 4 and result["revised"] == 6
    before = output_facts(source)
    assert materialize(source, frozen)["job"]["status"] == "SKIPPED"
    assert output_facts(source) == before
    put(source, A, 10., available="2025-01-02T00:01Z")
    updated = materialize(source, freeze(source))
    assert updated["revised"] == 2 and updated["unchanged"] == 10
    assert all(row in output_facts(source) for row in before)


def test_postgresql_writers_serialize_and_reject_stale_generation(pg, source, frozen, monkeypatch):
    simultaneous_writers(source, frozen, monkeypatch)


def test_namespace_write_waits_for_value_commit_then_prevents_false_complete(pg, source, frozen, monkeypatch):
    import time
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    from sqlalchemy import text
    from mt5_extracao.analytics import datasets, feature_store, indicator_materializer
    from mt5_extracao.database_manager import DatabaseManager
    ready, inside, attempting, changed = (Event() for _ in range(4))
    pids = []
    original_namespace = datasets.identity(source)["namespace"]
    def fork():
        other = DatabaseManager(db_type="postgresql", db_path=pg.db_path, broker_utc_offset=0)
        try:
            with other.engine.connect() as conn:
                pids.append(conn.execute(text("SELECT pg_backend_pid()")).scalar_one())
            ready.set()
            assert inside.wait(10)
            attempting.set()
            datasets.fork(other)
            changed.set()
        finally:
            other.close()
    save = feature_store.save_values
    def guarded_commit(db_, records, **kwargs):
        if records:
            prior = kwargs.pop("transaction_hook")
            def hook(conn, counts):
                prior(conn, counts)
                inside.set()
                assert attempting.wait(10)
                deadline = time.monotonic() + 5
                while not conn.execute(text("SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid=:pid "
                    "AND relation=to_regclass('_dataset_origin') AND NOT granted)"), dict(pid=pids[0])).scalar_one():
                    assert not changed.is_set(), "namespace mudou antes do commit de valores"
                    assert time.monotonic() < deadline, "não observou a trava de identidade"
                    time.sleep(.02)
                assert datasets.identity(db_, connection=conn)["namespace"] == original_namespace
            result = save(db_, records, transaction_hook=hook, **kwargs)
            assert changed.wait(10)
            return result
        return save(db_, records, **kwargs)
    monkeypatch.setattr(feature_store, "save_values", guarded_commit)
    with ThreadPoolExecutor(max_workers=1) as pool:
        worker = pool.submit(fork)
        assert ready.wait(10)
        with pytest.raises(indicator_materializer.SourceChanged, match="Identidade da base mudou"):
            materialize(source, frozen)
        worker.result(timeout=10)
    assert len(output_facts(source)) == 10
    assert datasets.identity(source)["namespace"] != original_namespace
