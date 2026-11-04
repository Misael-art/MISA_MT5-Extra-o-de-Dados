"""Segundo publicador e guarda de quota persistida no PostgreSQL."""
import pytest
from tests.test_analytics_profiles_pg import PG_URL
from tests.test_composite_materialization_pg import pg
from tests.test_mention_stackexchange import clock
from tests import test_mention_stackexchange as cases

pytestmark=pytest.mark.skipif(not PG_URL,reason='defina MT5X_TEST_PG_URL para testar com PostgreSQL')


def test_postgresql_second_source_and_quota_replay_keep_counts_and_origin(pg,monkeypatch,clock):
    cases.test_independent_source_uses_same_count_contract_replays_and_preserves_price_origin(pg,monkeypatch,clock)


def test_postgresql_second_source_missing_id_resumes_same_panel(pg,monkeypatch,clock):
    cases.test_missing_id_leaves_same_roster_for_resume_not_a_smaller_zero_population(pg,monkeypatch,clock)


def test_postgresql_quota_claim_cancel_rolls_back_before_http(pg,monkeypatch,clock):
    cases.test_cancel_inside_quota_checkpoint_rolls_back_its_reservation(pg,monkeypatch,clock)
