"""Laboratório (Fase 16) no PostgreSQL. Roda no job de PostgreSQL do CI (MT5X_TEST_PG_URL); sem servidor, é pulado."""
import os

import pandas as pd
import pytest
from sqlalchemy import inspect, text

from mt5_extracao.analytics import feature_store, features
from mt5_extracao.trade_analytics import ledger
from tests.postgres_helpers import isolated_postgresql_database

PG_URL = os.environ.get("MT5X_TEST_PG_URL")
pytestmark = pytest.mark.skipif(not PG_URL, reason="defina MT5X_TEST_PG_URL para testar com PostgreSQL")


@pytest.fixture
def pg():
    pytest.importorskip("psycopg")
    with isolated_postgresql_database(PG_URL) as db:
        yield db


def test_ledger_round_trip(pg):
    trades = [{"direction": 1, "entry_time": pd.Timestamp("2026-01-05 10:00"), "exit_time": "2026-01-05 10:30",
               "entry_price": 1.0, "exit_price": 2.0, "volume": 1.0, "pnl": 5.0},
              {"exit_time": "2026-01-06 10:30", "pnl": -1.0}]
    assert ledger.save_trades(pg, "r", trades, robot_key="a", symbol="WIN$N") == 2
    ledger.save_trades(pg, "r", trades, robot_key="a", symbol="WIN$N")       # de novo: não duplica
    df = ledger.load(pg, symbol="WIN$N")
    assert df["completeness"].tolist() == [ledger.COMPLETE, ledger.PARTIAL]
    assert ledger.runs(pg)[0]["completeness"] == ledger.PARTIAL


def test_feature_store_point_in_time_round_trip(pg):
    features.register(pg, "close_m1", "M1 close", "1.0", dependencies=["bars:M1"])
    feature_store.save_value(
        pg, "EURUSD", "M1", "close_m1", "1.0", "2026-10-14T09:59:00Z", 1.1,
        observed_at="2026-10-14T10:00:00Z", available_at="2026-10-14T10:00:00Z", source_revision="1",
    )
    feature_store.save_value(
        pg, "EURUSD", "M1", "close_m1", "1.0", "2026-10-14T09:59:00Z", 1.2,
        observed_at="2026-10-14T11:00:00Z", available_at="2026-10-14T11:00:00Z", source_revision="2",
    )

    before_correction = feature_store.query_as_of(pg, "2026-10-14T10:59:59Z", symbol="EURUSD",
                                                   timeframe="M1", feature_id="close_m1")
    after_correction = feature_store.query_as_of(pg, "2026-10-14T11:00:00Z", symbol="EURUSD",
                                                  timeframe="M1", feature_id="close_m1")
    assert before_correction["numeric_value"].tolist() == [1.1]
    assert after_correction["numeric_value"].tolist() == [1.2]
    assert {"_feature_definitions", "_feature_values"} <= set(inspect(pg.engine).get_table_names())
