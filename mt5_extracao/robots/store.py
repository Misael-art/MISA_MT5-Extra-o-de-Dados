"""
Registro dos robôs importados pelo usuário (tabela interna `_robots`, criada sob demanda).

Só guarda o que não dá para redescobrir lendo as pastas: de onde o robô foi importado e em que
estágio ele está. Mudança de schema aditiva: bases antigas continuam abrindo.
"""
import json
from datetime import datetime
from typing import Dict

_CREATE = """
CREATE TABLE IF NOT EXISTS _robots (
    key TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    name TEXT,
    path TEXT,
    stage TEXT,
    meta_json TEXT,
    updated_at TEXT
)
"""


def _has_table(db) -> bool:
    from sqlalchemy import inspect
    return inspect(db.engine).has_table("_robots")


def load_records(db) -> Dict[str, dict]:
    """key -> registro (vazio se a tabela ainda não existir ou não houver banco)."""
    from sqlalchemy import text
    if db is None or getattr(db, "engine", None) is None or not _has_table(db):
        return {}
    with db.engine.connect() as conn:
        rows = conn.execute(text("SELECT key, kind, name, path, stage, meta_json, updated_at FROM _robots")).mappings()
        return {r["key"]: {**dict(r), "meta": json.loads(r["meta_json"] or "{}")} for r in rows}


def save_record(db, key: str, kind: str, name: str, path: str = "", stage: str = "", meta: dict = None) -> None:
    from sqlalchemy import text
    with db.engine.begin() as conn:
        conn.execute(text(_CREATE))
        conn.execute(text(
            "INSERT INTO _robots (key, kind, name, path, stage, meta_json, updated_at) "
            "VALUES (:k, :kind, :n, :p, :s, :m, :u) "
            "ON CONFLICT(key) DO UPDATE SET kind = excluded.kind, name = excluded.name, path = excluded.path, "
            "stage = excluded.stage, meta_json = excluded.meta_json, updated_at = excluded.updated_at"),
            {"k": key, "kind": kind, "n": name, "p": path, "s": stage,
             "m": json.dumps(meta or {}, ensure_ascii=False),
             "u": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})


def delete_record(db, key: str) -> None:
    from sqlalchemy import text
    if _has_table(db):
        with db.engine.begin() as conn:
            conn.execute(text("DELETE FROM _robots WHERE key = :k"), {"k": key})
