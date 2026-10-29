"""Mesmo journal PostgreSQL, schema exclusivo e sem terminal."""
import pytest
from tests.test_analytics_profiles_pg import PG_URL, pg
from tests.test_producer_orphan_recovery import recovery_preserves_unproven_and_committed_data, simultaneous_reconcilers

pytestmark=pytest.mark.skipif(not PG_URL,reason="defina MT5X_TEST_PG_URL para testar com PostgreSQL")


def test_dead_owner_recovery_preserves_data_and_unproven_owners(pg):
    recovery_preserves_unproven_and_committed_data(pg)


def test_two_postgresql_reconcilers_confirm_once(pg,monkeypatch):
    simultaneous_reconcilers(pg,monkeypatch)
