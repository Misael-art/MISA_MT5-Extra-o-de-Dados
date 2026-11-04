"""BD05/R06 em PostgreSQL: fold preservado, zona persistida e as-of pela zona."""
import os

import pandas as pd
import pytest

import bench
from tests.postgres_helpers import isolated_postgresql_database
from tests.test_bd05_zone import SYMBOL, ZONE, _hourly

PG_URL = os.environ.get("MT5X_TEST_PG_URL")
pytestmark = pytest.mark.skipif(not PG_URL, reason="defina MT5X_TEST_PG_URL para testar com PostgreSQL")


@pytest.fixture
def pg():
    pytest.importorskip("psycopg")
    with isolated_postgresql_database(PG_URL, time_basis="utc", broker_timezone=ZONE) as db:
        yield db


def test_fold_and_as_of_in_postgresql(pg):
    dst = bench.dst_transition()
    assert pg.save_ohlcv_data(SYMBOL, "1 hora", _hourly(dst["bar_start_local"]),
                              received_at=pd.Timestamp("2026-10-30", tz="UTC"))
    assert len(pg.load_ohlcv(SYMBOL, "1 hora")) == len(dst)
    assert len(pg.load_ohlcv_as_of(SYMBOL, "1 hora", pd.Timestamp("2026-10-30", tz="UTC"))) == len(dst)
    pg.broker_timezone, pg._time_basis_checked = "America/New_York", False
    assert not pg.save_ohlcv_data(SYMBOL, "1 hora", _hourly(pd.date_range("2026-11-02", periods=2, freq="h")))
