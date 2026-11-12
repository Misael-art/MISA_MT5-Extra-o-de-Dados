"""Amostra de execuções repetidas em schema PostgreSQL próprio."""
import configparser
import os

import pytest

from mt5_extracao.trade_analytics import aggregate, context, ledger
from tests.postgres_helpers import isolated_postgresql_database
from tests.test_trade_repeated_runs import repeated_runs

PG_URL = os.environ.get("MT5X_TEST_PG_URL")
pytestmark = pytest.mark.skipif(not PG_URL, reason="defina MT5X_TEST_PG_URL para testar com PostgreSQL")


def test_repeated_runs_preserve_the_book_and_selected_run():
    config = configparser.ConfigParser(interpolation=None)
    config.read_dict({"APP": {"broker_utc_offset": "0"}})
    with isolated_postgresql_database(PG_URL, broker_utc_offset=0) as db:
        original, trades, _ = repeated_runs(db, config)
        lab = aggregate.frame(db, config)
        assert len(lab) == len(trades) and set(lab.run_id) == {original["run_id"]}
        assert context.population(lab)["repeated_runs_excluded"] == 2
        assert len(ledger.load(db)) == 3 * len(trades)
        assert len(aggregate.frame(db, config, include_repeated_runs=True)) == 3 * len(trades)
        assert set(aggregate.frame(db, config, run_id="repeat-a").run_id) == {"repeat-a"}
