"""Mesma comparação no store PostgreSQL real, com dados próprios."""
import os
import pytest
from mt5_extracao.analytics import datasets
from mt5_extracao.laboratory import preset_assets
from tests.postgres_helpers import isolated_postgresql_database
from tests.test_laboratory_financial import prepare

PG_URL=os.environ.get('MT5X_TEST_PG_URL')
pytestmark=pytest.mark.skipif(not PG_URL,reason='defina MT5X_TEST_PG_URL para PostgreSQL real')


def test_financial_comparison_postgresql(monkeypatch):
    with isolated_postgresql_database(PG_URL,broker_utc_offset=0) as db:
        datasets.declare_source(db,dict(provider='qa_prices',server='Synthetic'))
        _,runs=prepare(db,monkeypatch)
        result=preset_assets.compare_runs(db,[r['run_id'] for r in runs[:2]],mode='same_preset')
        assert result['compatible'],result['reasons']
        assert all(r['summary']['trades']==1 for r in result['rows'])
