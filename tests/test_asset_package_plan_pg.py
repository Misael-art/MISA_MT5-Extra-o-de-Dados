"""Inventário PostgreSQL pelo caminho de leitura em um schema isolado."""

import os

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from mt5_extracao.analytics import datasets
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.resources import package_plan
from tests.postgres_helpers import isolated_postgresql_database

PG_URL = os.environ.get("MT5X_TEST_PG_URL")
pytestmark = pytest.mark.skipif(not PG_URL, reason="defina MT5X_TEST_PG_URL para testar com PostgreSQL")


def test_package_plan_reflects_scoped_database_with_read_only_transactions():
    with isolated_postgresql_database(PG_URL) as source:
        identity = datasets.identity(source)
        with source.engine.begin() as connection:
            connection.execute(text("CREATE TABLE bars (time TEXT PRIMARY KEY, price NUMERIC DEFAULT 0)"))
            connection.execute(text("INSERT INTO _symbol_tables VALUES ('TEST', '1_hora', 'bars')"))
            connection.execute(text("CREATE VIEW prices AS SELECT price FROM bars"))
            connection.execute(text("CREATE INDEX prices_index ON bars(price)"))
        reader = DatabaseManager(db_type="postgresql", db_path=source.db_path, read_only=True)
        try:
            with pytest.raises(DBAPIError) as denied, reader.engine.begin() as connection:
                connection.execute(text("CREATE TABLE unexpected (id INTEGER)"))
            assert denied.value.orig.sqlstate == "25006"
            plan = package_plan.plan(reader, ["TEST"], statement_timeout_ms=15000)
            assert plan["base"] == identity
            assert plan["inventory"]["views"][0]["view"] == "prices"
            bars = next(table for table in plan["inventory"]["tables"] if table["table"] == "bars")
            assert bars["primary_key"] == ["time"] and bars["selection"] == "selected"
            assert package_plan.validate_plan(plan) == plan
        finally:
            reader.close()
