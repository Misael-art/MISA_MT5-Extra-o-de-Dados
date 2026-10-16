"""Helpers para isolar schemas dos testes PostgreSQL."""

from contextlib import contextmanager
from uuid import uuid4

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from mt5_extracao.database_manager import DatabaseManager


def _schema_scoped_url(pg_url: str, schema: str) -> str:
    url = make_url(pg_url)
    options = url.query.get("options", "")
    if isinstance(options, tuple):
        options = " ".join(options)
    scoped_options = " ".join(part for part in (options, f"-csearch_path={schema}") if part)
    return url.update_query_dict({"options": scoped_options}).render_as_string(hide_password=False)


@contextmanager
def isolated_postgresql_database(pg_url: str, *, time_basis="broker", broker_utc_offset=-3):
    # Nunca conectar implicitamente ao banco padrão quando a URL faltar.
    if not pg_url:
        raise ValueError("Defina MT5X_TEST_PG_URL com um banco PostgreSQL descartável para testes.")
    # Remove somente o schema criado por esta execução.
    schema = f"mt5x_test_{uuid4().hex}"
    admin_engine = create_engine(pg_url, pool_pre_ping=True)
    db = None
    created = False
    try:
        with admin_engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        created = True
        db = DatabaseManager(db_type="postgresql", db_path=_schema_scoped_url(pg_url, schema),
                             time_basis=time_basis, broker_utc_offset=broker_utc_offset)
        if not db.is_connected():
            raise RuntimeError("Não foi possível conectar ao schema PostgreSQL isolado para testes.")
        yield db
    finally:
        try:
            if db is not None:
                db.close()
        finally:
            try:
                if created:
                    with admin_engine.begin() as conn:
                        conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
            finally:
                admin_engine.dispose()
