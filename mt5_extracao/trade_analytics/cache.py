"""
Cache do enriquecimento (T16.5): o contexto de cada operação é calculado uma vez e guardado em `_trade_features`
(aditiva). Trocar filtro na aba só re-agrega em memória.

A assinatura de cada run (`_trade_features_runs`) junta o que muda o resultado do enriquecimento:
`enrich.FEATURE_VERSION`, as chaves de `[TRADE_ANALYTICS]` que entram na conta, as operações do run (horários e
resultado) e o calendário (quantos eventos e a última importação). Mudou qualquer um, o run é recalculado.

    feats = features(db, config, trades)      # trades = ledger.load(...); devolve as colunas de enrich.COLUMNS
    invalidate(db)                            # força recalcular tudo (ex.: depois de importar o calendário)
"""
import hashlib
import logging
from typing import Optional

import pandas as pd
from sqlalchemy import inspect, text

from mt5_extracao.trade_analytics import enrich

log = logging.getLogger(__name__)

TABLE, RUNS = "_trade_features", "_trade_features_runs"
_SETTINGS = ("time_bucket_min", "volatility_period", "event_window_before_min", "event_window_after_min",
             "event_symbols")


def _settings_part(config) -> str:
    if config is None or not config.has_section(enrich.SECTION):
        return ""
    return "|".join(f"{k}={config.get(enrich.SECTION, k, fallback='')}" for k in _SETTINGS)


def _calendar_part(db) -> str:
    from mt5_extracao.trade_analytics import calendar as cal
    if not inspect(db.engine).has_table(cal.TABLE):
        return "sem-calendario"
    with db.engine.connect() as conn:
        n, last = conn.execute(text(f"SELECT COUNT(*), MAX(imported_at) FROM {cal.TABLE}")).fetchone()
    return f"{n}:{last}"


def _trades_part(part: pd.DataFrame) -> str:
    cols = [c for c in ("seq", "entry_time", "exit_time", "pnl", "entry_price", "symbol", "timeframe") if c in part]
    return pd.util.hash_pandas_object(part[cols].astype(str), index=False).sum().__str__()


def signature(db, config, part: pd.DataFrame, common: Optional[str] = None) -> str:
    common = common if common is not None else f"{enrich.FEATURE_VERSION}|{_settings_part(config)}|{_calendar_part(db)}"
    return hashlib.sha1(f"{common}|{_trades_part(part)}".encode("utf-8")).hexdigest()[:20]


def _ensure(db) -> None:
    with db.engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE IF NOT EXISTS {RUNS} (run_id TEXT PRIMARY KEY, signature TEXT NOT NULL, "
                          f"feature_version INTEGER, computed_at TEXT)"))


def _load(db, run_ids) -> pd.DataFrame:
    if not run_ids or not inspect(db.engine).has_table(TABLE):
        return pd.DataFrame(columns=list(enrich.COLUMNS))
    marks = ", ".join(f":r{i}" for i in range(len(run_ids)))
    with db.engine.connect() as conn:
        return pd.read_sql(text(f"SELECT * FROM {TABLE} WHERE run_id IN ({marks})"), conn,
                           params={f"r{i}": r for i, r in enumerate(run_ids)})


def _store(db, run_id: str, sig: str, feats: pd.DataFrame) -> None:
    with db.engine.begin() as conn:
        if inspect(conn).has_table(TABLE):
            conn.execute(text(f"DELETE FROM {TABLE} WHERE run_id = :r"), {"r": run_id})
        conn.execute(text(f"DELETE FROM {RUNS} WHERE run_id = :r"), {"r": run_id})
        conn.execute(text(f"INSERT INTO {RUNS} VALUES (:r, :s, :v, :t)"),
                     {"r": run_id, "s": sig, "v": enrich.FEATURE_VERSION,
                      "t": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")})
    feats.astype(object).where(pd.notna(feats), None).to_sql(TABLE, db.engine, if_exists="append", index=False)


def features(db, config, trades: pd.DataFrame) -> pd.DataFrame:
    """Contexto das operações: do cache quando a assinatura bate; recalcula e guarda os runs que mudaram."""
    if trades is None or trades.empty:
        return pd.DataFrame(columns=list(enrich.COLUMNS))
    _ensure(db)
    common = f"{enrich.FEATURE_VERSION}|{_settings_part(config)}|{_calendar_part(db)}"
    with db.engine.connect() as conn:
        known = dict(conn.execute(text(f"SELECT run_id, signature FROM {RUNS}")).fetchall())
    fresh, stale = [], []
    for run_id, part in trades.groupby("run_id", sort=False):
        sig = signature(db, config, part, common)
        (fresh if known.get(run_id) == sig else stale).append((run_id, part, sig))
    parts = []
    if fresh:
        cached = _load(db, [r for r, _, _ in fresh])
        parts.append(cached)
    for run_id, part, sig in stale:
        feats = enrich.enrich(db, config, part)
        try:
            _store(db, run_id, sig, feats)
        except Exception as e:            # cache é otimização: falhar ao gravar não impede a análise
            log.warning(f"Laboratório: contexto do run {run_id} não guardado no cache ({e}).")
        parts.append(feats)
    out = pd.concat([p for p in parts if not p.empty], ignore_index=True) if any(not p.empty for p in parts) \
        else pd.DataFrame(columns=list(enrich.COLUMNS))
    for name in ("run_id",):
        out[name] = out[name].astype(str)
    out["seq"] = pd.to_numeric(out["seq"]).astype(int)
    log.info(f"Laboratório: {len(fresh)} run(s) do cache, {len(stale)} recalculado(s).")
    return out


def invalidate(db, run_id: Optional[str] = None) -> None:
    if not inspect(db.engine).has_table(RUNS):
        return
    with db.engine.begin() as conn:
        if run_id is None:
            conn.execute(text(f"DELETE FROM {RUNS}"))
        else:
            conn.execute(text(f"DELETE FROM {RUNS} WHERE run_id = :r"), {"r": run_id})
