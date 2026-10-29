"""Mesmo produtor congelado em schema PostgreSQL próprio; sem servidor, aceite pendente."""
import pytest

from tests.test_analytics_profiles_pg import PG_URL, pg
from tests.test_feature_compositions import A, freeze, put, source
from tests.test_frozen_observation_context import frozen
from tests.test_composite_materialization import materialize, output_facts, simultaneous_writers

pytestmark = pytest.mark.skipif(not PG_URL, reason="defina MT5X_TEST_PG_URL para testar com PostgreSQL")


@pytest.fixture
def db(pg):
    return pg


def test_frozen_chain_replay_and_late_old_observation_revision_are_exact(pg, source, frozen):
    result = materialize(source, frozen, batch_size=2)
    assert result["inserted"] == 4 and result["revised"] == 6
    before = output_facts(source)
    assert materialize(source, frozen)["job"]["status"] == "SKIPPED"
    assert output_facts(source) == before
    put(source, A, 10., available="2025-01-02T00:01Z")
    updated = materialize(source, freeze(source))
    assert updated["revised"] == 2 and updated["unchanged"] == 10
    assert all(row in output_facts(source) for row in before)


def test_postgresql_writers_serialize_and_reject_stale_generation(pg, source, frozen, monkeypatch):
    simultaneous_writers(source, frozen, monkeypatch)
