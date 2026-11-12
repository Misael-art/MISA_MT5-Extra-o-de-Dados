"""Excursões e cache aditivo em PostgreSQL, em schema de QA isolado."""
import os

import pandas as pd
import pytest

from mt5_extracao.trade_analytics import aggregate, ledger
from tests.postgres_helpers import isolated_postgresql_database
from tests.test_trade_excursions import source, trade

PG_URL = os.environ.get("MT5X_TEST_PG_URL")
pytestmark = pytest.mark.skipif(not PG_URL, reason="defina MT5X_TEST_PG_URL para testar com PostgreSQL")


def test_short_excursions_round_trip_without_rewriting_ledger():
    with isolated_postgresql_database(PG_URL) as db:
        bars = source(db)
        ledger.save_trades(db, "r", [trade(bars, -1)], symbol="QA", timeframe="M5")
        first = aggregate.frame(db, None)
        second = aggregate.frame(db, None)
        pd.testing.assert_frame_equal(first[["mae", "mfe", "excursion_basis"]],
                                      second[["mae", "mfe", "excursion_basis"]])
        assert first.mae.iloc[0] == 4. and first.mfe.iloc[0] == 2.
        assert first.excursion_basis.iloc[0] == "ask_bar_envelope"
        assert pd.isna(ledger.load(db).mae.iloc[0])
