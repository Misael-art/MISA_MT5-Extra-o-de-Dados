"""BD03 em PostgreSQL: a base é o schema/banco, identificada pelo conteúdo, não por nome de arquivo."""
import os

import pandas as pd
import pytest

from mt5_extracao.analytics import datasets, feature_store
from mt5_extracao.database_manager import DatabaseManager
from tests.market_data import trend
from tests.postgres_helpers import isolated_postgresql_database

PG_URL = os.environ.get("MT5X_TEST_PG_URL")
pytestmark = pytest.mark.skipif(not PG_URL, reason="defina MT5X_TEST_PG_URL para testar com PostgreSQL")
SYMBOL = "EURUSD"


@pytest.fixture
def two_schemas():
    pytest.importorskip("psycopg")
    with isolated_postgresql_database(PG_URL) as a, isolated_postgresql_database(PG_URL) as b:
        yield a, b


def test_schemas_are_isolated_bases_with_stable_identity(two_schemas):
    a, b = two_schemas
    for db, value in ((a, 1.0), (b, 2.0)):
        db.save_ohlcv_data(SYMBOL, "1 hora", trend(60, freq="h", start="2026-01-05"))
        feature_store.register_definition(db, "bd03.probe", "probe", "1", {}, [], value_type="NUMBER")
        stamp = pd.Timestamp("2026-01-05T11:00Z")
        feature_store.save_value(db, SYMBOL, "H1", "bd03.probe", "1", pd.Timestamp("2026-01-05T10:00Z"), value,
                                 observed_at=stamp, available_at=stamp)
    ids = datasets.identity(a), datasets.identity(b)
    assert ids[0]["namespace"] != ids[1]["namespace"] and ids[0]["backend"] == "postgresql"
    reopened = DatabaseManager(db_type="postgresql", db_path=a.db_path)
    assert datasets.identity(reopened)["namespace"] == ids[0]["namespace"]
    joined = pd.concat([feature_store.with_base_identity(db, feature_store.query_history(db, symbol=SYMBOL))
                        for db in (a, b)])
    assert joined.set_index("base_namespace")["numeric_value"].to_dict() == {
        ids[0]["namespace"]: 1.0, ids[1]["namespace"]: 2.0}


def test_incompatible_feed_is_refused_and_fork_records_parent(two_schemas):
    a, _ = two_schemas
    a.save_ohlcv_data(SYMBOL, "1 hora", trend(60, freq="h", start="2026-01-05"))
    datasets.capture(a, SYMBOL, "H1", source={"provider": "feed-a"})
    with pytest.raises(ValueError, match="recusada"):
        datasets.capture(a, SYMBOL, "H1", source={"provider": "feed-b"})
    parent = datasets.identity(a)["namespace"]
    assert datasets.fork(a)["parent_namespace"] == parent
