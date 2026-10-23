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
T1, T2, T3, T4 = (pd.Timestamp(v, tz="UTC") for v in
                  ("2026-09-14 13:00", "2026-09-15 13:00", "2026-09-16 13:00", "2026-09-17 13:00"))


@pytest.fixture
def pg():
    pytest.importorskip("psycopg")
    with isolated_postgresql_database(PG_URL, broker_utc_offset=-3) as db:
        yield db


def _bar(db, close, received_at):
    bars = m1_bars(pd.date_range("2026-09-14 09:00", periods=3, freq="min"))
    bars.loc[1, ["close", "high"]] = [close, max(close, bars.loc[1, "high"])]
    assert db.save_ohlcv_data(SYMBOL, "1 minuto", bars, received_at=received_at)


def _current(db):
    return db.load_ohlcv(SYMBOL, "1 minuto")["close"].iloc[1]


def test_r01_first_receipt_wins_precedence_in_postgresql(pg):
    _bar(pg, 250.0, T2)
    _bar(pg, 101.0, T1)
    assert _current(pg) == 250.0
    assert pg.load_ohlcv_as_of(SYMBOL, "1 minuto", T3)["known_since_utc"].iloc[1] == T2


def test_r02_causal_as_of_in_postgresql(pg):
    _bar(pg, 101.0, T2)
    early = pg.load_ohlcv_as_of(SYMBOL, "1 minuto", T1)  # fechada, ainda não recebida
    assert early.empty
    assert pg.load_ohlcv_as_of(SYMBOL, "1 minuto", T2)["close"].iloc[1] == 101.0


def test_r03_reconfirmation_and_tie_in_postgresql(pg):
    _bar(pg, 101.0, T1)
    _bar(pg, 250.0, T2)
    _bar(pg, 250.0, T4)
    _bar(pg, 300.0, T3)
    assert _current(pg) == 250.0
    with pg.engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM _ohlcv_receipts")).scalar_one()
    _bar(pg, 250.0, T4)  # retry do mesmo evento
    with pg.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM _ohlcv_receipts")).scalar_one() == count
