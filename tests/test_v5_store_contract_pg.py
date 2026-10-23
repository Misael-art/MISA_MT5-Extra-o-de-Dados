"""V5-01/V5-02 em PostgreSQL: carimbo em voo e escritores concorrentes com conexões independentes."""
import os
import threading

import pandas as pd
import pytest
from sqlalchemy import event

from mt5_extracao.analytics import feature_store
from mt5_extracao.database_manager import DatabaseManager
from tests.postgres_helpers import isolated_postgresql_database
from tests.test_v4_store_contract import FID, VER, _record, _values
from tests.test_v5_store_contract import _in_thread, _pause_on, _single

PG_URL = os.environ.get("MT5X_TEST_PG_URL")
pytestmark = pytest.mark.skipif(not PG_URL, reason="defina MT5X_TEST_PG_URL para testar com PostgreSQL")


def test_stamp_and_concurrent_writers_in_postgresql():
    pytest.importorskip("psycopg")
    with isolated_postgresql_database(PG_URL) as db:
        feature_store.register_definition(db, FID, "probe", VER, {}, [], value_type="NUMBER")
        feature_store.save_values(db, [_record(m, v) for m, v in ((0, 1.0), (5, 2.0), (10, 3.0))])
        reader = DatabaseManager(db_type="postgresql", db_path=db.db_path)
        reached, release, remove = _pause_on(db.engine, "UPDATE _feature_store_generations")
        thread, errors = _in_thread(lambda: feature_store.save_values(db, [_record(10, 300.0)]))
        assert reached.wait(30)
        snap = feature_store.store_snapshot(reader)
        cut = pd.Timestamp.now(tz="UTC")
        before = _values(reader, basis="knowledge", as_of=cut, knowledge_frontier=snap["knowledge_frontier"])
        release.set()
        thread.join(30)
        remove()
        assert not errors and before == [1.0, 2.0, 3.0]
        assert _values(reader, basis="knowledge", as_of=cut, knowledge_frontier=snap["knowledge_frontier"]) == before
        assert _values(reader, basis="knowledge", as_of=snap["knowledge_frontier"]) == before
        other = DatabaseManager(db_type="postgresql", db_path=db.db_path)
        reached, release, remove = _pause_on(db.engine, "UPDATE _feature_store_clock SET generation",
                                             when="before_cursor_execute")
        first, errors_a = _in_thread(lambda: _single(db, 10, 30.0))
        assert reached.wait(30)
        read = threading.Event()
        event.listen(other.engine, "before_cursor_execute",
                     lambda c, cu, s, *a: read.set() if "FROM _feature_values" in s and "revision" in s else None)
        later, errors_b = _in_thread(lambda: _single(other, 10, 3000.0))
        read.wait(3)
        release.set()
        first.join(30)
        later.join(30)
        remove()
        assert errors_a == [] and errors_b == []
        history = feature_store.query_history(db, feature_id=FID)
        assert history[history.observation_time == history.observation_time.max()]["revision"].tolist() == [1, 2, 3, 4]
        for engine in (reader.engine, other.engine):
            engine.dispose()
