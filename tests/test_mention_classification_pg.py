"""MEN-04: mesmos contratos com lock/rollback no PostgreSQL."""
import pytest

from tests.test_analytics_profiles_pg import PG_URL
from tests.test_composite_materialization_pg import pg
from tests import test_mention_classification as cases

pytestmark=pytest.mark.skipif(not PG_URL,reason='defina MT5X_TEST_PG_URL para testar com PostgreSQL')


def test_postgresql_late_object_classification_replay_preserves_quantity(pg):
    cases.test_qualification_artifact_and_counts_q10_p3_n2_with_late_clock_and_replay(pg)


def test_postgresql_cancel_classification_preserves_pending_population(pg,monkeypatch):
    cases.test_cancelled_classification_keeps_q_and_pending_pn_and_closes_run(pg,monkeypatch)


def test_postgresql_population_change_during_classifier_rejects_stale_labels(pg,monkeypatch):
    cases.test_generation_change_during_cpu_cannot_publish_stale_pn(pg,monkeypatch)
