"""
Livro de operações (T16.1): uma linha por operação de qualquer backtest, importação de conta ou re-simulação, na
tabela interna `_trade_ledger` (aditiva, criada sob demanda). É o mesmo livro que a T12.6A (estatísticas do
HADOUKEN) usa.

    save_trades(db, run_id, trades, robot_key="python:donchian", symbol="WIN$N", timeframe="M5", source="python")
    df = load(db, robot_key="python:donchian")          # lente "por robô"
    df = load(db, symbol="WIN$N")                        # lente "por ativo"

`trades` aceita objetos `strategies.backtester.Trade` ou dicionários com as colunas de COLUMNS. Operação sem horário de
entrada ou sem direção entra com completeness = "parcial" (ex.: robôs MQL5 cujo CSV ainda não traz a entrada).
"""
import logging
import threading
from dataclasses import asdict, is_dataclass
from typing import Iterable, List, Optional

import pandas as pd
from sqlalchemy import inspect, text

log = logging.getLogger(__name__)

TABLE = "_trade_ledger"
COMPLETE, PARTIAL = "completo", "parcial"

# coluna -> tipo SQL (os nomes são o contrato com a T12.6A; só acrescente colunas, nunca renomeie)
COLUMNS = {
    "run_id": "TEXT NOT NULL",
    "seq": "INTEGER NOT NULL",
    "robot_key": "TEXT",
    "symbol": "TEXT",
    "timeframe": "TEXT",
    "source": "TEXT NOT NULL",
    "direction": "INTEGER",
    "entry_time": "TIMESTAMP",
    "exit_time": "TIMESTAMP",
    "entry_price": "REAL",
    "exit_price": "REAL",
    "stop_price": "REAL",
    "target_price": "REAL",
    "volume": "REAL",
    "pnl": "REAL",
    "r_multiple": "REAL",
    "bars": "INTEGER",
    "exit_reason": "TEXT",
    "mae": "REAL",
    "mfe": "REAL",
    "params_hash": "TEXT",
    "completeness": "TEXT NOT NULL",
}
_TIMES = ("entry_time", "exit_time")
_SCHEMA_LOCK = threading.Lock()


def _usable(db) -> bool:
    return db is not None and getattr(db, "engine", None) is not None


def ensure_table(db) -> None:
    """Cria a tabela e os índices das duas lentes; numa base antiga acrescenta colunas que faltem."""
    with _SCHEMA_LOCK, db.engine.begin() as conn:
        cols = ", ".join(f"{name} {kind}" for name, kind in COLUMNS.items())
        conn.execute(text(f"CREATE TABLE IF NOT EXISTS {TABLE} ({cols}, PRIMARY KEY (run_id, seq))"))
        existing = {c["name"] for c in inspect(conn).get_columns(TABLE)}
        for name, kind in COLUMNS.items():
            if name not in existing:
                conn.execute(text(f"ALTER TABLE {TABLE} ADD COLUMN {name} {kind.replace(' NOT NULL', '')}"))
        for name, cols in (("robot", "robot_key, entry_time"), ("symbol", "symbol, entry_time"),
                           ("run", "run_id")):
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx{TABLE}_{name} ON {TABLE} ({cols})"))


def _row(trade, seq: int, base: dict) -> dict:
    data = asdict(trade) if is_dataclass(trade) else dict(trade)
    row = {name: data.get(name) for name in COLUMNS}
    row.update({k: v for k, v in base.items() if row.get(k) is None})
    row["seq"] = seq
    for name in _TIMES:
        value = row.get(name)
        if value is not None and not isinstance(value, str):
            value = pd.Timestamp(value)
        row[name] = None if value is None or (not isinstance(value, str) and pd.isna(value)) else \
            pd.Timestamp(value).to_pydatetime()
    for name in ("direction", "bars", "seq"):
        if row.get(name) is not None:
            row[name] = int(row[name])
    for name in ("entry_price", "exit_price", "stop_price", "target_price", "volume", "pnl", "r_multiple",
                 "mae", "mfe"):
        if row.get(name) is not None:
            value = float(row[name])
            row[name] = None if pd.isna(value) else value
    row["completeness"] = COMPLETE if row["entry_time"] is not None and row["direction"] in (1, -1) else PARTIAL
    return row


def save_trades(db, run_id: str, trades: Iterable, robot_key: Optional[str] = None, symbol: Optional[str] = None,
                timeframe: Optional[str] = None, source: str = "python", params_hash: Optional[str] = None) -> int:
    """
    Grava as operações do run (substitui as que o run já tinha: gravar de novo não duplica). Devolve quantas.
    """
    if not _usable(db):
        return 0
    base = {"run_id": run_id, "robot_key": robot_key, "symbol": symbol, "timeframe": timeframe, "source": source,
            "params_hash": params_hash}
    rows = [_row(trade, seq, base) for seq, trade in enumerate(trades)]
    for row in rows:
        row["run_id"] = run_id          # o run manda: uma operação não pode cair em outro run
    ensure_table(db)
    names = list(COLUMNS)
    insert = text(f"INSERT INTO {TABLE} ({', '.join(names)}) VALUES ({', '.join(':' + n for n in names)})")
    with db.engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {TABLE} WHERE run_id = :r"), {"r": run_id})
        if rows:
            conn.execute(insert, rows)
    return len(rows)


def load(db, robot_key: Optional[str] = None, symbol: Optional[str] = None, timeframe: Optional[str] = None,
         source: Optional[str] = None, run_id: Optional[str] = None, start=None, end=None,
         completeness: Optional[str] = None, include_resim: bool = False) -> pd.DataFrame:
    """
    Operações filtradas (entrada em [start, end)), ordenadas por entrada; DataFrame vazio com as colunas se não houver.
    Re-simulações (source "resim:...") ficam de fora, salvo `include_resim` ou um `source`/`run_id` pedido.
    """
    empty = pd.DataFrame(columns=list(COLUMNS))
    if not _usable(db) or not inspect(db.engine).has_table(TABLE):
        return empty
    where, params = [], {}
    for name, value in (("robot_key", robot_key), ("symbol", symbol), ("timeframe", timeframe),
                        ("source", source), ("run_id", run_id), ("completeness", completeness)):
        if value is not None:
            where.append(f"{name} = :{name}")
            params[name] = value
    if not include_resim and source is None and run_id is None:
        where.append("source NOT LIKE 'resim:%'")
    if start is not None:
        where.append("COALESCE(entry_time, exit_time) >= :start")
        params["start"] = pd.Timestamp(start).to_pydatetime()
    if end is not None:
        where.append("COALESCE(entry_time, exit_time) < :end")
        params["end"] = pd.Timestamp(end).to_pydatetime()
    sql = f"SELECT * FROM {TABLE}" + (" WHERE " + " AND ".join(where) if where else "") + \
        " ORDER BY COALESCE(entry_time, exit_time), run_id, seq"
    with db.engine.connect() as conn:
        frame = pd.read_sql(text(sql), conn, params=params)
    for name in _TIMES:
        frame[name] = pd.to_datetime(frame[name], errors="coerce")
    return frame


def runs(db, include_resim: bool = False) -> List[dict]:
    """Resumo por run: [{run_id, robot_key, symbol, timeframe, source, trades, completeness}] (para os seletores)."""
    if not _usable(db) or not inspect(db.engine).has_table(TABLE):
        return []
    where = "" if include_resim else "WHERE source NOT LIKE 'resim:%' "
    with db.engine.connect() as conn:
        return [dict(r) for r in conn.execute(text(
            f"SELECT run_id, robot_key, symbol, timeframe, source, COUNT(*) AS trades, MAX(completeness) AS "
            f"completeness FROM {TABLE} {where}GROUP BY run_id, robot_key, symbol, timeframe, source "
            f"ORDER BY robot_key, symbol, timeframe")).mappings()]


def delete_run(db, run_id: str) -> int:
    if not _usable(db) or not inspect(db.engine).has_table(TABLE):
        return 0
    with db.engine.begin() as conn:
        return conn.execute(text(f"DELETE FROM {TABLE} WHERE run_id = :r"), {"r": run_id}).rowcount or 0
