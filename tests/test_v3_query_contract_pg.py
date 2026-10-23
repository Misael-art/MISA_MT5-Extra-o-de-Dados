"""V3-01..04 em PostgreSQL: leitura em transação somente leitura, sem DDL/DML, versões e cursor."""
import os
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import event

from mt5_extracao.analytics import datasets, feature_store, indicator_materializer, query_service, resampling
from mt5_extracao.database_manager import DatabaseManager
from tests.postgres_helpers import isolated_postgresql_database
from tests.test_analytics_calendar import m1_bars

PG_URL = os.environ.get("MT5X_TEST_PG_URL")
pytestmark = pytest.mark.skipif(not PG_URL, reason="defina MT5X_TEST_PG_URL para testar com PostgreSQL")
SELECT = dict(symbol="EURUSD", timeframe="M5", feature_ids=["price.return_1"], as_of="2100-01-01T00:00Z")


def test_read_only_query_contract_in_postgresql():
    pytest.importorskip("psycopg")
    with isolated_postgresql_database(PG_URL, broker_utc_offset=0) as db:
        db.save_ohlcv_data("EURUSD", "1 minuto", m1_bars(pd.date_range("2026-09-14 09:00", periods=90, freq="min")),
                           received_at=pd.Timestamp("2026-10-23T12:00Z"))
        datasets.identity(db)
        resampling.resample_symbol(db, "EURUSD", ["M5"], symbol_info=SimpleNamespace(name="EURUSD"))
        indicator_materializer.materialize(db, "EURUSD", "M5", feature_ids=["price.return_1"])
        feature_store.register_definition(db, "price.return_1", "old", "old-percent-v0", {}, [], unit="percent",
                                          value_type="NUMBER")
        stamp = pd.Timestamp("2026-09-14T10:00Z")
        feature_store.save_value(db, "EURUSD", "M5", "price.return_1", "old-percent-v0", stamp, 1.5,
                                 observed_at=stamp, available_at=stamp)
        reader = DatabaseManager(db_type="postgresql", db_path=db.db_path, broker_utc_offset=0, read_only=True)
        kinds = []
        event.listen(reader.engine, "before_cursor_execute", lambda c, cu, s, *a: kinds.append(s.split()[0].upper()))
        env = query_service.series(reader, **SELECT)
        assert env["base"]["state"] == "identified" and env["values"]  # leu o schema certo (search_path preservado)
        assert env["status"] == "ok" and set(kinds) <= {"SELECT", "WITH", "SHOW", "BEGIN", "COMMIT", "ROLLBACK"}  # sem DDL/DML
        assert env["definitions"]["price.return_1"]["versions"]["old-percent-v0"]["unit"] == "percent"
        knowledge = query_service.series(reader, **{**SELECT, "as_of": "2026-09-15T00:00Z", "basis": "knowledge"})
        assert knowledge["values"] == []
        page = query_service.series(reader, **SELECT, page_size=3)
        assert query_service.series(reader, **{**SELECT, "basis": "knowledge"}, page_size=3,
                                    cursor=page["page"]["next_cursor"])["errors"][0]["code"] == "CURSOR_MISMATCH"
        reader.engine.dispose()
