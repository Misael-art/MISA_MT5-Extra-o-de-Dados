"""Ligação observacional via ledger/trajectory PostgreSQL real."""
import os
import pytest
from mt5_extracao.analytics import datasets
from mt5_extracao.laboratory import execution
from tests.postgres_helpers import isolated_postgresql_database
from tests.test_laboratory_financial import prepare
from tests.test_laboratory_execution import analyze

PG_URL=os.environ.get('MT5X_TEST_PG_URL')
pytestmark=pytest.mark.skipif(not PG_URL,reason='defina MT5X_TEST_PG_URL para PostgreSQL real')


def test_execution_receipt_preserves_postgresql_observations(monkeypatch):
    with isolated_postgresql_database(PG_URL,broker_utc_offset=0) as db:
        datasets.declare_source(db,dict(provider='qa_prices',server='Synthetic'))
        _,runs=prepare(db,monkeypatch);report=analyze(db,runs[0])
        receipt=execution.link(db,report,runs[0]['run_id']);context=execution.candle(report,receipt,300)
        assert context['events'] and context['events'][0]['initial_stop_price']==90
        assert execution.candle(report,receipt,320)['capital']['floating_equity']==10007
