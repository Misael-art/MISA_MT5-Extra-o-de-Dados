"""Texto em coluna própria e fotografia imutável no banco do extrator."""
import hashlib
import json
import re
from datetime import datetime, timezone

from sqlalchemy import text, inspect

from mt5_extracao.analytics import datasets

from . import snapshot

NOTES = "_chart_study_notes"
REQUESTS = "_chart_study_requests"
CAPTURES = "_chart_study_captures"
IDENTIFIER = re.compile(r"[A-Za-z0-9_-]{1,160}\Z")


def ensure_schema(db):
    with db.engine.begin() as conn:
        conn.execute(text(f"""CREATE TABLE IF NOT EXISTS {NOTES} (
            note_id TEXT PRIMARY KEY, note_text TEXT NOT NULL, symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL, bar_time_utc TEXT NOT NULL, bar_time_native TEXT, anchor_price DOUBLE PRECISION NOT NULL,
            captured_at_utc TEXT NOT NULL, snapshot_json TEXT NOT NULL, snapshot_sha256 TEXT NOT NULL,
            capture_sha256 TEXT NOT NULL, created_at_utc TEXT NOT NULL, updated_at_utc TEXT NOT NULL,
            revision INTEGER NOT NULL, magic TEXT NOT NULL, chart_id TEXT NOT NULL
        )"""))
        if "bar_time_native" not in {column["name"] for column in inspect(conn).get_columns(NOTES)}:
            conn.execute(text(f"ALTER TABLE {NOTES} ADD COLUMN bar_time_native TEXT"))
        conn.execute(text(f"""CREATE TABLE IF NOT EXISTS {REQUESTS} (
            request_id TEXT PRIMARY KEY, payload_sha256 TEXT NOT NULL, note_id TEXT NOT NULL,
            response_json TEXT NOT NULL, completed_at_utc TEXT NOT NULL
        )"""))
        conn.execute(text(f"""CREATE TABLE IF NOT EXISTS {CAPTURES} (
            note_id TEXT PRIMARY KEY, source_sha256 TEXT NOT NULL, snapshot_json TEXT NOT NULL
        )"""))


def freeze(db, note_id, capture, captured_at, *, build_snapshot=snapshot.build):
    """A prévia abaixo do editor e a nota salva compartilham os mesmos bytes."""
    if not isinstance(note_id, str) or not IDENTIFIER.fullmatch(note_id):
        raise ValueError("Identificador de captura inválido.")
    digest = hashlib.sha256(snapshot.json_text(capture).encode()).hexdigest()
    ensure_schema(db)
    with db.engine.connect() as conn:
        row = conn.execute(text(f"SELECT * FROM {CAPTURES} WHERE note_id=:id"), {"id": note_id}).mappings().first()
    if row:
        if row["source_sha256"] != digest:
            raise ValueError("A captura original foi alterada; crie outra anotação.")
        return json.loads(row["snapshot_json"])
    frozen = build_snapshot(capture, captured_at)
    # Notas novas congelam a base de origem; as já salvas mantêm seus bytes (sem recálculo).
    frozen.setdefault("base", {key: value for key, value in datasets.identity(db).items() if key != "backend"})
    body = snapshot.json_text(frozen)
    with db.engine.begin() as conn:
        conn.execute(text(f"INSERT INTO {CAPTURES} (note_id,source_sha256,snapshot_json) VALUES (:id,:hash,:body) "
                          "ON CONFLICT (note_id) DO NOTHING"), {"id": note_id, "hash": digest, "body": body})
        row = conn.execute(text(f"SELECT * FROM {CAPTURES} WHERE note_id=:id"), {"id": note_id}).mappings().one()
        if row["source_sha256"] != digest:
            raise ValueError("Outra captura já usa este identificador.")
        return json.loads(row["snapshot_json"])


def get(db, note_id):
    ensure_schema(db)
    with db.engine.connect() as conn:
        row = conn.execute(text(f"SELECT * FROM {NOTES} WHERE note_id=:id"), {"id": note_id}).mappings().first()
    if row is None:
        return None
    return {**dict(row), "snapshot": json.loads(row["snapshot_json"])}


def list_notes(db, *, symbol=None, timeframe=None, magic=None):
    ensure_schema(db)
    where, params = [], {}
    for name, value in (("symbol", symbol), ("timeframe", timeframe), ("magic", magic)):
        if value is not None:
            where.append(f"{name}=:{name}")
            params[name] = str(value)
    clause = " WHERE " + " AND ".join(where) if where else ""
    with db.engine.connect() as conn:
        rows = conn.execute(text(f"SELECT * FROM {NOTES}" + clause + " ORDER BY created_at_utc,note_id"), params).mappings()
        return [dict(row) for row in rows]


def apply(db, request, capture=None, captured_at=None, *, build_snapshot=snapshot.build):
    """Operação idempotente; editar texto conserva todos os campos da fotografia."""
    if not isinstance(request, dict) or request.get("contract") != 1:
        raise ValueError("Pedido de anotação inválido.")
    note_id, request_id = request.get("note_id", ""), request.get("request_id", "")
    if not isinstance(note_id, str) or not IDENTIFIER.fullmatch(note_id) or not isinstance(request_id, str) or not IDENTIFIER.fullmatch(request_id):
        raise ValueError("Identificador de anotação inválido.")
    content = request.get("text")
    if not isinstance(content, str) or not content.strip() or len(content) > 20000:
        raise ValueError("Escreva uma anotação de1 a20000 caracteres.")
    kind = request.get("kind")
    if kind not in ("note_create", "note_update"):
        raise ValueError("Operação de anotação desconhecida.")
    payload_hash = hashlib.sha256(snapshot.json_text(request).encode()).hexdigest()
    ensure_schema(db)
    with db.engine.connect() as conn:
        old = conn.execute(text(f"SELECT * FROM {REQUESTS} WHERE request_id=:id"), {"id": request_id}).mappings().first()
    if old:
        if old["payload_sha256"] != payload_hash:
            raise ValueError("Este pedido já foi usado com outro conteúdo.")
        return json.loads(old["response_json"])
    frozen = None
    if kind == "note_create":
        if captured_at is None:
            raise ValueError("Horário de captura ausente.")
        frozen = freeze(db, note_id, capture, captured_at, build_snapshot=build_snapshot)
    now = datetime.now(timezone.utc).isoformat()
    with db.engine.begin() as conn:
        prior = conn.execute(text(f"SELECT * FROM {REQUESTS} WHERE request_id=:id"), {"id": request_id}).mappings().first()
        if prior:
            if prior["payload_sha256"] != payload_hash:
                raise ValueError("Este pedido já foi usado com outro conteúdo.")
            return json.loads(prior["response_json"])
        current = conn.execute(text(f"SELECT * FROM {NOTES} WHERE note_id=:id"), {"id": note_id}).mappings().first()
        if kind == "note_create":
            if current is not None:
                raise ValueError("A nota já existe; use a edição para conservar a captura original.")
            body = snapshot.json_text(frozen)
            row = {"note_id": note_id, "note_text": content, "symbol": capture["symbol"], "timeframe": capture["timeframe"],
                   "bar_time_utc": frozen.get("bar_time_utc") or "", "bar_time_native": frozen.get("bar_time_native") or snapshot.native_clock(capture["bar_epoch"]), "anchor_price": capture["anchor_price"],
                   "captured_at_utc": frozen["captured_at_utc"], "snapshot_json": body,
                   "snapshot_sha256": hashlib.sha256(body.encode()).hexdigest(), "capture_sha256": frozen["source_sha256"],
                   "created_at_utc": now, "updated_at_utc": now, "revision": 1,
                   "magic": str(capture.get("magic", "")), "chart_id": str(capture.get("chart_id", ""))}
            names = ",".join(row)
            conn.execute(text(f"INSERT INTO {NOTES} ({names}) VALUES (" + ",".join(":" + k for k in row) + ")"), row)
            revision, digest = 1, row["snapshot_sha256"]
        else:
            expected = request.get("expected_revision")
            if current is None:
                raise ValueError("Nota não encontrada; aguarde sua primeira sincronização.")
            if isinstance(expected, bool) or not isinstance(expected, int) or expected != current["revision"]:
                raise ValueError("A nota foi alterada em outra janela. Reabra antes de editar.")
            updated = conn.execute(text(f"UPDATE {NOTES} SET note_text=:content,updated_at_utc=:now,revision=revision+1 "
                                        "WHERE note_id=:id AND revision=:expected"),
                                   {"content": content, "now": now, "id": note_id, "expected": expected})
            if updated.rowcount != 1:
                raise ValueError("A nota mudou durante a gravação; reabra antes de editar.")
            revision, digest = current["revision"] + 1, current["snapshot_sha256"]
        response = {"contract": 1, "request_id": request_id, "note_id": note_id, "revision": revision,
                    "snapshot_sha256": digest, "state": "SAVED", "completed_at_utc": now}
        conn.execute(text(f"INSERT INTO {REQUESTS} (request_id,payload_sha256,note_id,response_json,completed_at_utc) "
                          "VALUES (:id,:hash,:note,:response,:now)"),
                     {"id": request_id, "hash": payload_hash, "note": note_id, "response": snapshot.json_text(response), "now": now})
    return response
