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


# --- execuções (backtests) ------------------------------------------------------------

_CREATE_RUNS = """
CREATE TABLE IF NOT EXISTS _robot_runs (
    run_id TEXT PRIMARY KEY,
    robot_key TEXT NOT NULL,
    source TEXT NOT NULL,
    symbol TEXT,
    timeframe TEXT,
    data_start TEXT,
    data_end TEXT,
    bars INTEGER,
    params_json TEXT,
    metrics_json TEXT,
    equity_json TEXT,
    created_at TEXT NOT NULL
)
"""
_RUN_COLUMNS = ("run_id", "robot_key", "source", "symbol", "timeframe", "data_start", "data_end", "bars",
                "params_json", "metrics_json", "equity_json", "created_at")


def save_run(db, run: dict) -> None:
    """Grava um backtest: métricas e curva de capital (já reduzida a poucos pontos)."""
    from sqlalchemy import text
    row = {"run_id": run["run_id"], "robot_key": run["robot_key"], "source": run["source"],
           "symbol": run["symbol"], "timeframe": run["timeframe"], "data_start": run["data_start"],
           "data_end": run["data_end"], "bars": int(run["bars"]),
           "params_json": json.dumps(run.get("params", {}), ensure_ascii=False),
           "metrics_json": json.dumps(run["metrics"], ensure_ascii=False),
           "equity_json": json.dumps(run["equity"]), "created_at": run["created_at"]}
    with db.engine.begin() as conn:
        conn.execute(text(_CREATE_RUNS))
        conn.execute(text(f"INSERT INTO _robot_runs ({', '.join(_RUN_COLUMNS)}) "
                          f"VALUES ({', '.join(':' + c for c in _RUN_COLUMNS)})"), row)


def load_runs(db, robot_key: str = None) -> list:
    """Execuções gravadas, a mais recente primeiro (lista vazia se a tabela ainda não existir)."""
    from sqlalchemy import inspect, text
    if db is None or getattr(db, "engine", None) is None or not inspect(db.engine).has_table("_robot_runs"):
        return []
    sql = f"SELECT {', '.join(_RUN_COLUMNS)} FROM _robot_runs"
    params = {}
    if robot_key:
        sql += " WHERE robot_key = :k"
        params["k"] = robot_key
    with db.engine.connect() as conn:
        rows = [dict(r) for r in conn.execute(text(sql + " ORDER BY created_at DESC, run_id DESC"), params).mappings()]
    for row in rows:
        row["params"] = json.loads(row.pop("params_json") or "{}")
        row["metrics"] = json.loads(row.pop("metrics_json") or "{}")
        row["equity"] = json.loads(row.pop("equity_json") or "[]")
    return rows
