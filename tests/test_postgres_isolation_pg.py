"""Aceites ORG-04; executar somente com PostgreSQL descartável exclusivo."""
import os
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError

from tests import postgres_helpers as helpers

PG_URL = os.environ.get("MT5X_TEST_PG_URL")
pytestmark = pytest.mark.skipif(not PG_URL, reason="defina MT5X_TEST_PG_URL para testar com PostgreSQL")


@pytest.fixture
def admin():
    pytest.importorskip("psycopg")
    engine = create_engine(PG_URL)
    try:
        yield engine
    finally:
        engine.dispose()


def schemas(admin):
    with admin.connect() as conn:
        return set(conn.execute(text("SELECT nspname FROM pg_namespace")).scalars())


def current_schema(db):
    with db.engine.connect() as conn:
        return conn.execute(text("SELECT current_schema()")).scalar_one()


def test_public_sentinel_and_shadow_table_are_preserved(admin):
    table = "org04_sentinel_" + uuid4().hex
    with admin.begin() as conn:
        conn.execute(text(f'CREATE TABLE public."{table}" (value TEXT)'))
        conn.execute(text(f'INSERT INTO public."{table}" VALUES (\'preservado\')'))
    before = schemas(admin)
    try:
        with helpers.isolated_postgresql_database(PG_URL) as db:
            schema = current_schema(db)
            assert schema.startswith("mt5x_test_")
            with db.engine.begin() as conn:
                assert conn.execute(text("SHOW search_path")).scalar_one() == schema
                conn.execute(text(f'CREATE TABLE "{table}" (value TEXT)'))
                conn.execute(text(f'INSERT INTO "{table}" VALUES (\'isolado\')'))
                assert conn.execute(text(f'SELECT value FROM "{table}"')).scalar_one() == "isolado"
                assert conn.execute(text(f'SELECT value FROM public."{table}"')).scalar_one() == "preservado"
        assert schemas(admin) == before
        with admin.connect() as conn:
            assert conn.execute(text(f'SELECT value FROM public."{table}"')).scalar_one() == "preservado"
    finally:
        with admin.begin() as conn:
            conn.execute(text(f'DROP TABLE public."{table}"'))


def test_exception_in_test_cleans_only_its_schema(admin):
    before = schemas(admin)
    with pytest.raises(RuntimeError, match="falha simulada"):
        with helpers.isolated_postgresql_database(PG_URL) as db:
            assert current_schema(db) not in before
            raise RuntimeError("falha simulada")
    assert schemas(admin) == before


def test_constructor_failure_cleans_schema(admin, monkeypatch):
    before = schemas(admin)
    def fail(*args, **kwargs):
        raise RuntimeError("construtor indisponível")
    monkeypatch.setattr(helpers, "DatabaseManager", fail)
    with pytest.raises(RuntimeError, match="construtor indisponível"):
        with helpers.isolated_postgresql_database(PG_URL):
            pytest.fail("não deve produzir banco")
    assert schemas(admin) == before


def test_close_failure_still_cleans_schema(admin, monkeypatch):
    before = schemas(admin)
    real_close = helpers.DatabaseManager.close
    def fail(db):
        real_close(db)
        raise RuntimeError("falha ao fechar")
    monkeypatch.setattr(helpers.DatabaseManager, "close", fail)
    with pytest.raises(RuntimeError, match="falha ao fechar"):
        with helpers.isolated_postgresql_database(PG_URL):
            pass
    assert schemas(admin) == before


def test_disconnected_database_is_closed_and_schema_cleaned(admin, monkeypatch):
    before = schemas(admin)
    closed = []
    monkeypatch.setattr(helpers, "DatabaseManager", lambda **kwargs: SimpleNamespace(
        is_connected=lambda: False, close=lambda: closed.append(True)))
    with pytest.raises(RuntimeError, match="conectar ao schema"):
        with helpers.isolated_postgresql_database(PG_URL):
            pytest.fail("não deve produzir banco desconectado")
    assert closed == [True]
    assert schemas(admin) == before


def test_create_failure_does_not_drop_schema_it_does_not_own(admin, monkeypatch):
    token = uuid4().hex
    schema = "mt5x_test_" + token
    with admin.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    monkeypatch.setattr(helpers, "uuid4", lambda: SimpleNamespace(hex=token))
    try:
        with pytest.raises(ProgrammingError):
            with helpers.isolated_postgresql_database(PG_URL):
                pytest.fail("schema preexistente não pertence ao helper")
        assert schema in schemas(admin)
    finally:
        with admin.begin() as conn:
            conn.execute(text(f'DROP SCHEMA "{schema}"'))


def test_concurrent_contexts_have_unique_schemas_and_independent_tables(admin):
    before = schemas(admin)
    barrier = Barrier(2)
    def worker(value):
        with helpers.isolated_postgresql_database(PG_URL) as db:
            schema = current_schema(db)
            with db.engine.begin() as conn:
                conn.execute(text("CREATE TABLE scope_marker (value INTEGER)"))
                conn.execute(text("INSERT INTO scope_marker VALUES (:value)"), {"value": value})
            barrier.wait(timeout=20)
            with db.engine.connect() as conn:
                assert conn.execute(text("SELECT value FROM scope_marker")).scalars().all() == [value]
            barrier.wait(timeout=20)
            return schema
    with ThreadPoolExecutor(max_workers=2) as pool:
        names = list(pool.map(worker, [1, 2]))
    assert len(set(names)) == 2
    assert schemas(admin) == before
