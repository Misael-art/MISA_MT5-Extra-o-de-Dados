"""Lote entre observações e fronteira de256 eventos no PostgreSQL isolado."""
import pytest
from tests.test_analytics_profiles_pg import PG_URL
from tests.test_composite_materialization_pg import pg
from tests.test_feature_compositions import source
from tests.test_frozen_event_batching import (
    test_many_observations_use_bounded_shared_execution_and_independent_values as check_many,
    test_crossing_calculation_batch_boundary_keeps_held_branch_deduplicated as check_held)

pytestmark=pytest.mark.skipif(not PG_URL,reason="defina MT5X_TEST_PG_URL para testar com PostgreSQL")


@pytest.fixture
def db(pg):
    return pg


def test_postgresql_many_observations_use_bounded_batches(source,monkeypatch):
    check_many(source,monkeypatch)


def test_postgresql_batch_boundary_deduplicates_held_branch(source,monkeypatch):
    check_held(source,monkeypatch)
