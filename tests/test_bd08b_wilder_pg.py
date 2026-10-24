"""BD08b/T19.3 em PostgreSQL: mesma referência, revisão, cursor e ausência."""
import os

import pytest

from tests.postgres_helpers import isolated_postgresql_database
from tests import test_bd08b_wilder as cases

PG_URL = os.environ.get("MT5X_TEST_PG_URL")
pytestmark = pytest.mark.skipif(not PG_URL, reason="defina MT5X_TEST_PG_URL para testar com PostgreSQL")


def test_wilder_store_and_covered_cursor_in_postgresql():
    pytest.importorskip("psycopg")
    with isolated_postgresql_database(PG_URL, broker_utc_offset=0) as db:
        cases.exercise_store(db)


def test_wilder_ineligible_source_in_postgresql(monkeypatch):
    pytest.importorskip("psycopg")
    with isolated_postgresql_database(PG_URL, broker_utc_offset=0) as db:
        cases.exercise_ineligible_source(db, monkeypatch)
