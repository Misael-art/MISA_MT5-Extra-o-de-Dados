"""Semântica de revisão/recibo do mesmo produtor em PostgreSQL isolado."""
import os
import pytest
from mt5_extracao.analytics import datasets
from tests.postgres_helpers import isolated_postgresql_database
from tests.test_super_atr_materializer import adapter
from tests.test_super_atr_materializer import test_super_atr_store_zero_warmup_receipt_and_idempotence as check_store
from tests.test_super_atr_materializer import test_append_preserves_first_provenance_and_correction_preserves_old_cut as check_revision

PG_URL=os.environ.get('MT5X_TEST_PG_URL')
pytestmark=pytest.mark.skipif(not PG_URL,reason='defina MT5X_TEST_PG_URL para PostgreSQL real')

@pytest.mark.parametrize('check',[check_store,check_revision])
def test_postgresql_super_atr_contract(adapter,check):
    with isolated_postgresql_database(PG_URL,broker_utc_offset=0) as db:
        datasets.declare_source(db,dict(provider='qa_prices',server='Synthetic'))
        check(db,adapter)
