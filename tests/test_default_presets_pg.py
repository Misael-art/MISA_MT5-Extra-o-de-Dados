"""Default no job PostgreSQL existente; não exige terminal."""
import os
import pytest
from tests.postgres_helpers import isolated_postgresql_database
from tests.test_s09_native_profile import robot
from tests.test_default_presets import test_default_roundtrip_idempotence_selection_and_existing_data_preserved as check_roundtrip

PG_URL=os.environ.get("MT5X_TEST_PG_URL")
pytestmark=pytest.mark.skipif(not PG_URL,reason="defina MT5X_TEST_PG_URL para testar com PostgreSQL")


def test_default_presets_roundtrip_pg(robot):
    pytest.importorskip("psycopg")
    with isolated_postgresql_database(PG_URL) as db:
        check_roundtrip(robot,db)
