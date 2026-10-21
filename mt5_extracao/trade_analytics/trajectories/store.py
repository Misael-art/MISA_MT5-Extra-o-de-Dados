"""Sidecar aditivo de observações, ligado ao ledger e ao snapshot canônicos."""
from datetime import datetime, timezone
import hashlib
import json
import math
import struct
import threading

import numpy as np
from sqlalchemy import inspect, text

from mt5_extracao.analytics import datasets
from mt5_extracao.trade_analytics import cache, ledger
from . import cycles
from .clocks import Clock

TABLE = "_trade_trajectories"
_LOCK = threading.Lock()


def _json(value):
    def scalar(obj):
        if isinstance(obj, np.generic):
            return obj.item()
        raise TypeError(f"Tipo não suportado na trajetória: {type(obj).__name__}")
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False, default=scalar)


def _hash(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _ensure(db):
    with _LOCK, db.engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE IF NOT EXISTS {TABLE} (trajectory_id TEXT PRIMARY KEY, "
                          "run_id TEXT NOT NULL, ledger_sha256 TEXT NOT NULL, manifest_id TEXT, "
                          "payload_json TEXT NOT NULL, created_at TEXT NOT NULL)"))
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx{TABLE}_run ON {TABLE} (run_id,created_at)"))


def _clock(snapshot, clock):
    if snapshot is not None:
        expected = Clock("broker", snapshot.manifest["time"]["broker_utc_offset_hours"])
        if clock is not None and clock != expected:
            raise ValueError("Relógio da trajetória difere da revisão dos dados; confira a origem.")
        return expected, "data_manifest"
    if clock is None:
        raise ValueError("Run antigo sem relógio fixado; declare a base temporal antes de gravar a trajetória.")
    return clock, "caller_declared_legacy"


def _reconcile(part, payload, source, clock, conn):
    if not inspect(conn).has_table(ledger.TABLE):
        raise ValueError("Run não está no ledger; grave as operações antes das observações.")
    if not part.empty and set(part.source) != {source}:
        raise ValueError("Origem da trajetória não corresponde às operações do run.")
    units = cycles.summarize(payload, run_id="validation", source=source, clock=clock)
    if {u["identity"]["seq"] for u in units} != set(part.seq):
        raise ValueError("Ciclos não correspondem às identidades do ledger; recupere o produtor original.")
    if not part.empty and set(part.timeframe) != {payload.get("timeframe")}:
        raise ValueError("Resolução das observações difere do ledger; confira o produtor.")
    types = {c["name"]: c["type"] for c in inspect(conn).get_columns(ledger.TABLE)}
    def matches(value, stored, column):
        if value is None or stored is None:
            return value is stored
        value, stored = float(value), float(stored)
        # PostgreSQL REAL legado é float32; comparar sua projeção exata não é
        # ampliar tolerância monetária nem reescrever os valores do usuário.
        if conn.dialect.name == "postgresql" and str(types[column]).upper() == "REAL":
            # O driver pode decodificar o texto float4 como float64 decimal.
            # Comparar as duas projeções binárias conserva a precisão do schema.
            return struct.pack("!f", value) == struct.pack("!f", stored) or value == stored == 0
        return math.isclose(value, stored, rel_tol=1e-12, abs_tol=1e-9)
    for unit in units:
        row = part[part.seq == unit["identity"]["seq"]].iloc[0]
        events = unit["points"]
        first, last = events[0], events[-1]
        if (unit["right_censored"] or not matches(unit["final_net_pnl"], row.pnl, "pnl") or
                not matches(first.get("fill_price"), row.entry_price, "entry_price") or
                not matches(last.get("fill_price"), row.exit_price, "exit_price") or
                not matches(max(e["volume_open"] for e in events), row.volume, "volume") or
                first.get("direction") != row.direction or
                any(e.get("direction") != row.direction for e in events) or
                clock.timestamp(first["start_time"]) != clock.timestamp(row.entry_time) or
                clock.timestamp(last.get("bar_time")) != clock.timestamp(row.exit_time)):
            raise ValueError("Observações não conciliam com entrada, saída e resultado do ledger.")


def save(db, run_id, payload, *, source, clock=None, producer_identity=None):
    """Acrescenta uma versão sem alterar o livro. Repetir o mesmo conteúdo é idempotente."""
    snapshot = datasets.for_run(db, run_id)
    declared, clock_origin = _clock(snapshot, clock)
    _ensure(db)
    with db.engine.begin() as conn:
        part = ledger.load(db, run_id=run_id, connection=conn)
        _reconcile(part, payload, source, declared, conn)
        fingerprint = cache._frame_digest(part)
        envelope = {"schema_version": 1, "run_id": run_id, "source": source,
                    "ledger_sha256": fingerprint, "manifest_id": snapshot.manifest_id if snapshot else None,
                    "clock": {"basis": declared.basis, "broker_utc_offset_hours": declared.broker_utc_offset_hours},
                    "clock_origin": clock_origin, "producer_identity": producer_identity or {},
                    "observations": payload,
                    "ledger_precision": "postgresql_storage_projection" if conn.dialect.name == "postgresql" else "float64"}
        encoded = _json(envelope)
        ident = _hash(encoded)
        conn.execute(text(f"INSERT INTO {TABLE} (trajectory_id,run_id,ledger_sha256,manifest_id,payload_json,created_at) "
                          "VALUES (:id,:run,:ledger,:manifest,:payload,:created) ON CONFLICT(trajectory_id) DO NOTHING"),
                     {"id": ident, "run": run_id, "ledger": fingerprint, "manifest": envelope["manifest_id"],
                      "payload": encoded, "created": datetime.now(timezone.utc).isoformat()})
    return ident


def load(db, run_id):
    """Consulta compatível; legado sem sidecar retorna None sem modificar o schema."""
    if not inspect(db.engine).has_table(TABLE):
        return None
    snapshot = datasets.for_run(db, run_id)
    manifest = snapshot.manifest_id if snapshot is not None else None
    with db.consistent_read() as conn:
        part = ledger.load(db, run_id=run_id, connection=conn)
        rows = conn.execute(text(f"SELECT * FROM {TABLE} WHERE run_id=:run ORDER BY created_at DESC,trajectory_id DESC"),
                            {"run": run_id}).mappings().all()
        if not rows:
            return None
        fingerprint = cache._frame_digest(part)
        for record in rows:
            if record["ledger_sha256"] != fingerprint or record["manifest_id"] != manifest:
                continue
            raw = record["payload_json"]
            if _hash(raw) != record["trajectory_id"]:
                raise ValueError("Trajetória sem integridade; recupere as observações do produtor original.")
            envelope = json.loads(raw)
            if (envelope.get("schema_version") != 1 or envelope.get("run_id") != run_id or
                    envelope.get("ledger_sha256") != fingerprint or envelope.get("manifest_id") != manifest):
                raise ValueError("Identidade da trajetória inválida; recupere a origem.")
            clock = Clock(**envelope["clock"])
            if snapshot is not None:
                _clock(snapshot, clock)
            _reconcile(part, envelope["observations"], envelope["source"], clock, conn)
            return {"trajectory_id": record["trajectory_id"], **envelope}
    raise ValueError("Ledger ou revisão de dados mudou; a trajetória antiga foi preservada. Refaça a execução.")
