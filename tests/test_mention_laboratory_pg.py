"""MEN-06: mesma leitura congelada e SMA no PostgreSQL."""
import pytest
from tests.test_analytics_profiles_pg import PG_URL
from tests.test_composite_materialization_pg import pg
from tests import test_mention_laboratory as cases

pytestmark=pytest.mark.skipif(not PG_URL,reason='defina MT5X_TEST_PG_URL para testar com PostgreSQL')


def test_postgresql_stored_counts_mean_keeps_gaps_and_availability(pg):
    cases.test_mean_uses_consecutive_windows_no_carry_no_zero_and_keeps_source_availability(pg)


def test_postgresql_topic_discovery_and_read_do_not_run_classifier(pg,monkeypatch):
    cases.test_topic_is_discovered_and_known_abstentions_remain_visible_without_model_or_network(pg,monkeypatch)
