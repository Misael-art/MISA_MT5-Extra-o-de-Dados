"""
Registro dos robôs importados pelo usuário (tabela interna `_robots`, criada sob demanda).

Só guarda o que não dá para redescobrir lendo as pastas: de onde o robô foi importado e em que
estágio ele está. Mudança de schema aditiva: bases antigas continuam abrindo.
"""
import json
import threading
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
_VALUATION_COLUMNS = ("account_balance_json", "floating_equity_json", "valuation_json",
                      "performance_equity_json", "account_cashflows_json")


# A bateria de backtests da interface grava de várias threads. Criar a tabela e acrescentar a coluna nova são
# "conferir e depois alterar": sem a trava, duas gravações simultâneas em uma base nova tentavam acrescentar a mesma
# coluna e uma delas falhava ("duplicate column name").
_SCHEMA_LOCK = threading.Lock()


def save_run(db, run: dict) -> None:
    """Grava um backtest: métricas e curva de capital (já reduzida a poucos pontos)."""
    with _SCHEMA_LOCK:
        _save_run(db, run)


def _save_run(db, run: dict) -> None:
    from sqlalchemy import text
    row = {"run_id": run["run_id"], "robot_key": run["robot_key"], "source": run["source"],
           "symbol": run["symbol"], "timeframe": run["timeframe"], "data_start": run["data_start"],
           "data_end": run["data_end"], "bars": int(run["bars"]),
           "params_json": json.dumps(run.get("params", {}), ensure_ascii=False),
           "metrics_json": json.dumps(run["metrics"], ensure_ascii=False),
           "equity_json": json.dumps(run["equity"]), "created_at": run["created_at"],
           "trades_json": json.dumps(run.get("trades", [])),
           "account_balance_json": json.dumps(run.get("account_balance"), allow_nan=False),
           "floating_equity_json": json.dumps(run.get("floating_equity"), allow_nan=False),
           "performance_equity_json": json.dumps(run.get("performance_equity"), allow_nan=False),
           "account_cashflows_json": json.dumps(run.get("account_cashflows"), allow_nan=False),
           "valuation_json": json.dumps(run.get("valuation"), ensure_ascii=False, allow_nan=False)}
    with db.engine.begin() as conn:
        conn.execute(text(_CREATE_RUNS))
        if not _has_trades_column(conn):          # base criada antes da T8.6: coluna nova, nada é apagado
            conn.execute(text("ALTER TABLE _robot_runs ADD COLUMN trades_json TEXT"))
        from sqlalchemy import inspect
        existing = {c["name"] for c in inspect(conn).get_columns("_robot_runs")}
        for column in _VALUATION_COLUMNS:
            if column not in existing:
                conn.execute(text(f"ALTER TABLE _robot_runs ADD COLUMN {column} TEXT"))
        columns = _RUN_COLUMNS + ("trades_json",) + _VALUATION_COLUMNS
        conn.execute(text(f"INSERT INTO _robot_runs ({', '.join(columns)}) "
                          f"VALUES ({', '.join(':' + c for c in columns)})"), row)
        manifest_id = run["metrics"].get("data_manifest_id")
        if manifest_id is not None:
            from mt5_extracao.analytics import datasets
            datasets.bind(db, run["run_id"], manifest_id, connection=conn)


def _has_trades_column(conn) -> bool:
    from sqlalchemy import inspect
    return any(c["name"] == "trades_json" for c in inspect(conn).get_columns("_robot_runs"))


def load_runs(db, robot_key: str = None, *, run_id: str = None) -> list:
    """Execuções gravadas, a mais recente primeiro (lista vazia se a tabela ainda não existir)."""
    from sqlalchemy import inspect, text
    if db is None or getattr(db, "engine", None) is None or not inspect(db.engine).has_table("_robot_runs"):
        return []
    params = {}
    with db.engine.connect() as conn:
        existing = {c["name"] for c in inspect(conn).get_columns("_robot_runs")}
        columns = (_RUN_COLUMNS + (("trades_json",) if _has_trades_column(conn) else ()) +
                   tuple(c for c in _VALUATION_COLUMNS if c in existing))
        sql = f"SELECT {', '.join(columns)} FROM _robot_runs"
        where = []
        if robot_key:
            where.append("robot_key = :k")
            params["k"] = robot_key
        if run_id is not None:
            where.append("run_id = :r")
            params["r"] = run_id
        if where:
            sql += " WHERE " + " AND ".join(where)
        rows = [dict(r) for r in conn.execute(text(sql + " ORDER BY created_at DESC, run_id DESC"), params).mappings()]
    for row in rows:
        row["trades"] = json.loads(row.pop("trades_json", None) or "[]")      # [[horário, resultado, volume], ...]
        row["params"] = json.loads(row.pop("params_json") or "{}")
        row["metrics"] = json.loads(row.pop("metrics_json") or "{}")
        row["data_manifest_id"] = row["metrics"].get("data_manifest_id")
        row["data_provenance_status"] = "frozen_reference" if row["data_manifest_id"] else "legacy_current_data"
        from mt5_extracao import profit_factor
        try:
            pnl = [t[1] for t in row["trades"]]
        except (TypeError, IndexError):
            pnl = None
        row["metrics"].update(profit_factor.read_fields(row["metrics"], pnl, row["metrics"].get("trades")))
        row["equity"] = json.loads(row.pop("equity_json") or "[]")
        row["account_balance"] = json.loads(row.pop("account_balance_json", None) or "null")
        row["floating_equity"] = json.loads(row.pop("floating_equity_json", None) or "null")
        row["performance_equity"] = json.loads(row.pop("performance_equity_json", None) or "null")
        row["account_cashflows"] = json.loads(row.pop("account_cashflows_json", None) or "null")
        row["valuation"] = json.loads(row.pop("valuation_json", None) or "null") or {
            "version": 1, "status": "unavailable", "closed_curve_basis": "legacy_closed_results",
            "reason": "Registro sem trajetória patrimonial; refaça o backtest para medi-la.",
        }
    return rows


# --- bateria de segurança -------------------------------------------------------------

_CREATE_CHECKS = """
CREATE TABLE IF NOT EXISTS _robot_checks (
    check_id TEXT PRIMARY KEY,
    robot_key TEXT NOT NULL,
    preset TEXT,
    approved INTEGER NOT NULL,
    allows_real INTEGER NOT NULL,
    static_status TEXT,
    statistical_status TEXT,
    run_id TEXT,
    items_json TEXT,
    created_at TEXT NOT NULL
)
"""
# colunas acrescentadas depois (aditivas): código avaliado, e ativo e timeframe do backtest que deu a aprovação
_CHECK_EXTRA = ("fingerprint", "symbol", "timeframe")
_CHECK_COLUMNS = ("check_id", "robot_key", "preset", "approved", "allows_real", "static_status",
                  "statistical_status", "run_id", "items_json", "created_at")


def save_check(db, check: dict) -> None:
    """Grava o resultado de uma bateria (cada execução é uma linha; a mais recente é a que vale)."""
    from sqlalchemy import text
    row = {**{c: check.get(c) for c in _CHECK_COLUMNS}, "approved": int(check["approved"]),
           "allows_real": int(check["allows_real"]), "items_json": json.dumps(check["items"], ensure_ascii=False)}
    for extra in _CHECK_EXTRA:
        row[extra] = check.get(extra, "")
    with db.engine.begin() as conn:
        conn.execute(text(_CREATE_CHECKS))
        for extra in _CHECK_EXTRA:                 # base anterior: colunas novas, nada é apagado
            if not _has_column(conn, "_robot_checks", extra):
                conn.execute(text(f"ALTER TABLE _robot_checks ADD COLUMN {extra} TEXT"))
        columns = _CHECK_COLUMNS + _CHECK_EXTRA
        conn.execute(text(f"INSERT INTO _robot_checks ({', '.join(columns)}) "
                          f"VALUES ({', '.join(':' + c for c in columns)})"), row)


def _has_column(conn, table: str, column: str) -> bool:
    from sqlalchemy import inspect
    return any(c["name"] == column for c in inspect(conn).get_columns(table))


def load_checks(db, robot_key: str = None, *, runs=None) -> list:
    """Baterias gravadas, a mais recente primeiro (lista vazia se a tabela ainda não existir)."""
    from sqlalchemy import inspect, text
    if db is None or getattr(db, "engine", None) is None or not inspect(db.engine).has_table("_robot_checks"):
        return []
    params = {}
    with db.engine.connect() as conn:
        columns = _CHECK_COLUMNS + tuple(c for c in _CHECK_EXTRA if _has_column(conn, "_robot_checks", c))
        sql = f"SELECT {', '.join(columns)} FROM _robot_checks"
        if robot_key:
            sql += " WHERE robot_key = :k"
            params["k"] = robot_key
        rows = [dict(r) for r in conn.execute(text(sql + " ORDER BY created_at DESC, check_id DESC"), params).mappings()]
    run_metrics = {run["run_id"]: run["metrics"] for run in
                   (load_runs(db, robot_key) if runs is None else runs)}
    for row in rows:
        row["approved"], row["allows_real"] = bool(row["approved"]), bool(row["allows_real"])
        for extra in _CHECK_EXTRA:
            row[extra] = row.get(extra) or ""
        row["items"] = json.loads(row.pop("items_json") or "[]")
        metrics = run_metrics.get(row["run_id"])
        if metrics is not None and metrics.get("profit_factor") is None:
            from mt5_extracao import profit_factor
            row["approved_recorded"], row["allows_real_recorded"] = row["approved"], row["allows_real"]
            row["approved"] = row["allows_real"] = False
            row["statistical_status"] = "reprovada"
            row["items"].append({"group": "estatística", "id": "profit_factor_contract",
                                 "name": "PF definido na execução vinculada", "status": "falha", "icon": "✗",
                                 "detail": profit_factor.reason(metrics),
                                 "fix": "Rode novamente a validação/bateria com uma amostra adequada."})
    return rows
