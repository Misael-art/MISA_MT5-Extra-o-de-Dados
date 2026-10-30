"""Mesmos painéis sociais em PostgreSQL isolado, com transporte declarado de QA."""
import pytest
from tests.postgres_helpers import isolated_postgresql_database
from tests.test_analytics_profiles_pg import PG_URL
from tests.test_mention_social import transport,test_social_panel_uses_existing_store_and_is_not_a_census as check_panel,test_provider_budgets_are_isolated_in_existing_journal as check_budgets

pytestmark=pytest.mark.skipif(not PG_URL,reason='defina MT5X_TEST_PG_URL para PostgreSQL')


@pytest.mark.parametrize('provider',['reddit','twitter','youtube'])
def test_postgresql_social_panel(transport,provider):
    from mt5_extracao.analytics import datasets
    with isolated_postgresql_database(PG_URL,broker_utc_offset=0) as db:
        datasets.declare_source(db,{'provider':'qa_prices','server':'Synthetic'})
        check_panel(db,transport,provider)


def test_postgresql_provider_http_budgets_do_not_share_cooldown():
    with isolated_postgresql_database(PG_URL,broker_utc_offset=0) as db:
        check_budgets(db)
