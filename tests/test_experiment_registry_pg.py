"""Índice canônico de protocolos no PostgreSQL, sem efeitos da consulta."""
import copy
import os

import pytest
from sqlalchemy import inspect

from mt5_extracao.experiments import service, store
from tests.experiment_helpers import setup
from tests.postgres_helpers import isolated_postgresql_database

PG_URL = os.environ.get("MT5X_TEST_PG_URL")
pytestmark = pytest.mark.skipif(not PG_URL, reason="defina MT5X_TEST_PG_URL; CI inclui test_*_pg.py")


def test_protocol_index_on_old_database_and_registered_revisions_is_readonly():
    with isolated_postgresql_database(PG_URL) as db:
        before = inspect(db.engine).get_table_names()
        assert store.list_protocols(db) == []
        assert inspect(db.engine).get_table_names() == before
        definition, composition, _ = setup(db)
        one = service.create(db, definition)["protocol_id"]
        changed = copy.deepcopy(definition)
        changed["revision"] = 2
        two = service.create(db, changed)["protocol_id"]
        service.request(db, two, "discard", composition, discard_reason="QA")
        journal = store.status(db, two)
        rows = store.list_protocols(db)
        assert {r["protocol_id"]: r["revision"] for r in rows} == {one: 1, two: 2}
        assert all(set(row) == {"protocol_id", "name", "revision", "created_at"} for row in rows)
        assert store.status(db, two) == journal
