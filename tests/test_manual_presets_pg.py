"""Manual no job PostgreSQL existente, sem terminal real."""
import os
import pytest
from tests.postgres_helpers import isolated_postgresql_database
from tests.test_s09_native_profile import robot
from tests.test_manual_presets import test_clone_named_manual_roundtrip_preserves_default_lineage_and_selected_snapshot as check_clone

PG_URL=os.environ.get("MT5X_TEST_PG_URL")
pytestmark=pytest.mark.skipif(not PG_URL,reason="defina MT5X_TEST_PG_URL para testar com PostgreSQL")


def test_named_manual_roundtrip_pg(robot):
    pytest.importorskip("psycopg")
    with isolated_postgresql_database(PG_URL) as db:check_clone(robot,db)
