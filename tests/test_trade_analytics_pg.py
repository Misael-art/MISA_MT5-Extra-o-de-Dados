"""Laboratório (Fase 16) no PostgreSQL. Roda no job de PostgreSQL do CI (MT5X_TEST_PG_URL); sem servidor, é pulado."""
import os

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.trade_analytics import ledger

PG_URL = os.environ.get("MT5X_TEST_PG_URL")
pytestmark = pytest.mark.skipif(not PG_URL, reason="defina MT5X_TEST_PG_URL para testar com PostgreSQL")


@pytest.fixture
def pg():
    pytest.importorskip("psycopg")
    engine = create_engine(PG_URL)
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
    engine.dispose()
    db = DatabaseManager(db_type="postgresql", db_path=PG_URL)
    yield db
    db.engine.dispose()


def test_ledger_round_trip(pg):
    trades = [{"direction": 1, "entry_time": pd.Timestamp("2026-01-05 10:00"), "exit_time": "2026-01-05 10:30",
               "entry_price": 1.0, "exit_price": 2.0, "volume": 1.0, "pnl": 5.0},
              {"exit_time": "2026-01-06 10:30", "pnl": -1.0}]
    assert ledger.save_trades(pg, "r", trades, robot_key="a", symbol="WIN$N") == 2
    ledger.save_trades(pg, "r", trades, robot_key="a", symbol="WIN$N")       # de novo: não duplica
    df = ledger.load(pg, symbol="WIN$N")
    assert df["completeness"].tolist() == [ledger.COMPLETE, ledger.PARTIAL]
    assert ledger.runs(pg)[0]["completeness"] == ledger.PARTIAL
