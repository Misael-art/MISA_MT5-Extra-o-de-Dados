"""Classificação PT no PostgreSQL canônico, somente documentos próprios."""
import pytest
from tests.postgres_helpers import isolated_postgresql_database
from tests.test_analytics_profiles_pg import PG_URL
from tests.test_mention_pt_pipeline import test_portuguese_classifier_reuses_late_count_revision_without_changing_q as check_pipeline

pytestmark=pytest.mark.skipif(not PG_URL,reason='defina MT5X_TEST_PG_URL para PostgreSQL')


def test_postgresql_portuguese_pipeline():
    from mt5_extracao.analytics import datasets
    with isolated_postgresql_database(PG_URL,broker_utc_offset=0) as db:
        datasets.declare_source(db,{'provider':'qa_prices','server':'Synthetic'})
        check_pipeline(db)
