"""V4-01/02 em PostgreSQL: escritor individual e commit em voo com conexões independentes."""
import os

import pandas as pd
import pytest

from mt5_extracao.analytics import feature_store
from mt5_extracao.database_manager import DatabaseManager
from tests.postgres_helpers import isolated_postgresql_database
from tests.test_v4_store_contract import FID, VER, _inflight, _record, _values

PG_URL = os.environ.get("MT5X_TEST_PG_URL")
pytestmark = pytest.mark.skipif(not PG_URL, reason="defina MT5X_TEST_PG_URL para testar com PostgreSQL")


@pytest.mark.parametrize("fail", [False, True])
def test_inflight_and_single_writer_in_postgresql(fail):
    pytest.importorskip("psycopg")
    with isolated_postgresql_database(PG_URL) as db:
        feature_store.register_definition(db, FID, "probe", VER, {}, [], value_type="NUMBER")
        feature_store.save_values(db, [_record(m, v) for m, v in ((0, 1.0), (5, 2.0), (10, 3.0))])
        reader = DatabaseManager(db_type="postgresql", db_path=db.db_path)
        release, thread, errors = _inflight(db, [_record(10, 300.0)], fail=fail)
        snap = feature_store.store_snapshot(reader)
        cut = pd.Timestamp.now(tz="UTC")
        assert _values(reader, generation_through=snap["generation"]) == [1.0, 2.0, 3.0]
        release.set()
        thread.join(30)
        assert bool(errors) == fail
        assert _values(reader, generation_through=snap["generation"]) == [1.0, 2.0, 3.0]
        assert _values(reader, basis="knowledge", as_of=cut) == [1.0, 2.0, 3.0]
        r = _record(15, 4.0)
        feature_store.save_value(db, r.pop("symbol"), r.pop("timeframe"), r.pop("feature_id"),
                                 r.pop("algorithm_version"), r.pop("observation_time"), r.pop("value"), **r)
        assert _values(reader, basis="knowledge")[-1] == 4.0
        reader.engine.dispose()
