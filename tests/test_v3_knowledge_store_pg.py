"""V3-01 em PostgreSQL: conhecimento efetivo exige gravação no corte."""
import os
from types import SimpleNamespace

import pandas as pd
import pytest

from mt5_extracao.analytics import feature_store, indicator_materializer, resampling
from tests.postgres_helpers import isolated_postgresql_database
from tests.test_analytics_calendar import m1_bars
from tests.test_v3_knowledge_store import MARKET_DAY, RECEIVED, SYMBOL

PG_URL = os.environ.get("MT5X_TEST_PG_URL")
pytestmark = pytest.mark.skipif(not PG_URL, reason="defina MT5X_TEST_PG_URL para testar com PostgreSQL")


def test_knowledge_basis_in_postgresql():
    pytest.importorskip("psycopg")
    with isolated_postgresql_database(PG_URL, broker_utc_offset=0) as db:
        db.save_ohlcv_data(SYMBOL, "1 minuto", m1_bars(pd.date_range("2026-09-14 09:00", periods=90, freq="min")),
                           received_at=RECEIVED)
        resampling.resample_symbol(db, SYMBOL, ["M5"], symbol_info=SimpleNamespace(name=SYMBOL))
        indicator_materializer.materialize(db, SYMBOL, "M5", feature_ids=["price.return_1"])
        args = dict(symbol=SYMBOL, timeframe="M5", feature_id="price.return_1")
        assert len(feature_store.query_as_of(db, MARKET_DAY, basis="research", **args)) == 18
        assert feature_store.query_as_of(db, MARKET_DAY, basis="knowledge", **args).empty
        assert len(feature_store.query_as_of(db, pd.Timestamp.now(tz="UTC"), basis="knowledge", **args)) == 18
