"""T19.23 persistência aditiva no job PostgreSQL existente."""
import os

import pytest

from tests.postgres_helpers import isolated_postgresql_database
from tests.test_native_inputs import robot
from tests.test_native_inputs import test_session_receipt_is_persisted_reopened_idempotently_without_rewriting_run as receipt_round_trip

PG_URL = os.environ.get("MT5X_TEST_PG_URL")
pytestmark = pytest.mark.skipif(not PG_URL, reason="defina MT5X_TEST_PG_URL para testar com PostgreSQL")


@pytest.fixture
def pg():
    pytest.importorskip("psycopg")
    with isolated_postgresql_database(PG_URL) as db:
        yield db


def test_receipt_round_trip_and_immutability_pg(robot,tmp_path,pg):
    receipt_round_trip(robot,tmp_path,pg)
