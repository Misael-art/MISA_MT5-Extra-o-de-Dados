"""BD04 em PostgreSQL: histórico do bruto, precedência por recebimento e leitura as-of."""
import os

import pandas as pd
import pytest
from sqlalchemy import text

from tests.postgres_helpers import isolated_postgresql_database
from tests.test_analytics_calendar import m1_bars

PG_URL = os.environ.get("MT5X_TEST_PG_URL")
pytestmark = pytest.mark.skipif(not PG_URL, reason="defina MT5X_TEST_PG_URL para testar com PostgreSQL")
SYMBOL = "EURUSD"
T1, T2 = pd.Timestamp("2026-09-14 13:00", tz="UTC"), pd.Timestamp("2026-09-15 13:00", tz="UTC")


@pytest.fixture
def pg():
    pytest.importorskip("psycopg")
    with isolated_postgresql_database(PG_URL) as db:
        yield db


def _bar(db, close, received_at):
    bars = m1_bars(pd.date_range("2026-09-14 09:00", periods=3, freq="min"))
    bars.loc[1, ["close", "high"]] = [close, max(close, bars.loc[1, "high"])]
    assert db.save_ohlcv_data(SYMBOL, "1 minuto", bars, received_at=received_at)


def test_revisions_stale_and_as_of_in_postgresql(pg):
    _bar(pg, 101.0, T1)
    _bar(pg, 250.0, T2)
    _bar(pg, 101.0, T1)
    assert pg.load_ohlcv(SYMBOL, "1 minuto")["close"].iloc[1] == 250.0
    assert pg.load_ohlcv_as_of(SYMBOL, "1 minuto", T2 - pd.Timedelta(hours=1))["close"].iloc[1] == 101.0
    with pg.engine.connect() as conn:
        statuses = [row[0] for row in conn.execute(text("SELECT status FROM _ohlcv_revisions ORDER BY revision"))]
    assert statuses == ["superseded", "accepted", "stale"]
