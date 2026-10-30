"""Mesmo contrato de revisão/relógio/categoria no PostgreSQL real e isolado."""
import os
import pytest
from tests.postgres_helpers import isolated_postgresql_database
from tests.test_macro_sources import response
from tests.test_macro_sources import test_macro_zero_is_valid_and_receipt_is_not_backdated as check_zero
from tests.test_macro_sources import test_macro_revision_preserves_old_cut_and_unrelated_observation as check_revision
from tests.test_macro_sources import test_cycle_is_a_cited_category_and_snapshot_preserves_it as check_cycle

PG_URL=os.environ.get('MT5X_TEST_PG_URL')
pytestmark=pytest.mark.skipif(not PG_URL,reason='defina MT5X_TEST_PG_URL para PostgreSQL real')

@pytest.mark.parametrize('check',[check_zero,check_revision,check_cycle])
def test_postgresql_macro_contract(response,check):
    from mt5_extracao.analytics import datasets
    with isolated_postgresql_database(PG_URL,broker_utc_offset=0) as db:
        datasets.declare_source(db,{'provider':'qa.synthetic','server':'QA'})
        check(db,response)
