"""Pares referenciais no schema PostgreSQL próprio, sem conta/terminal."""
import pytest
from tests.test_analytics_profiles_pg import PG_URL
from tests.test_composite_materialization_pg import pg
from tests.test_frozen_association_query import seed_reference, query

pytestmark=pytest.mark.skipif(not PG_URL,reason='defina MT5X_TEST_PG_URL para testar com PostgreSQL')


def test_reference_pair_uses_frozen_postgresql_source_and_calendar(pg):
    reference=seed_reference(pg)
    result=query(reference)
    assert result['relation']['effect']==pytest.approx(1.) and result['relation']['n_pairs']==160
    assert result['population']['n_distinct_source_pairs']==160
