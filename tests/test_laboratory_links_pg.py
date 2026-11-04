"""Mesmo vínculo/relatório em PostgreSQL isolado, sem dados de conta ou modelo."""
import pytest

from tests.postgres_helpers import isolated_postgresql_database
from tests.test_analytics_profiles_pg import PG_URL
from tests import test_laboratory_links as cases

pytestmark=pytest.mark.skipif(not PG_URL,reason='defina MT5X_TEST_PG_URL para testar com PostgreSQL')


@pytest.fixture
def linked_pg(tmp_path,monkeypatch):
    with isolated_postgresql_database(PG_URL,broker_utc_offset=0) as db:
        monkeypatch.setattr(cases,'DatabaseManager',lambda **kwargs:db)
        generator=cases.linked.__wrapped__(tmp_path)
        try:yield next(generator)
        finally:generator.close()


def test_postgresql_linked_receipts_keep_q_and_classification_clocks_separate(linked_pg):
    cases.test_receipt_is_a_pulse_and_classification_is_not_backdated(linked_pg)


def test_postgresql_publication_crosses_by_frozen_explicit_link(linked_pg):
    cases.test_publication_is_explicit_retrospective_and_numeric_crossings_use_existing_matrix(linked_pg)


def test_postgresql_late_batch_is_not_resampled_or_summed_into_one_candle(linked_pg):
    cases.test_batch_of_observations_is_not_summed_into_a_candle(linked_pg)
