"""MEN-05: transporte e rollback do snapshot v5 no PostgreSQL."""
import pytest

from tests.test_analytics_profiles_pg import PG_URL
from tests.test_composite_materialization_pg import pg
from tests import test_mention_snapshots as cases

pytestmark=pytest.mark.skipif(not PG_URL,reason='defina MT5X_TEST_PG_URL para testar com PostgreSQL')


def test_postgresql_topic_snapshot_keeps_utc_window_without_market(pg,monkeypatch):
    cases.test_topic_snapshot_needs_no_market_timezone_or_fake_prices(pg,monkeypatch)


def test_postgresql_topic_snapshot_preserves_revisions_and_transfers_without_current_facts(pg,tmp_path,monkeypatch):
    cases.test_frozen_before_and_after_late_classification_and_population_revision_are_immutable(pg,tmp_path,monkeypatch)


def test_postgresql_topic_snapshot_cancel_after_manifest_insert_rolls_back(pg,monkeypatch):
    cases.test_cancellation_at_manifest_insert_rolls_back_all_new_blocks(pg,monkeypatch)
