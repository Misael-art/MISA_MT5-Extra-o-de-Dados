"""
Cache do enriquecimento (T16.5): o contexto de cada operação é calculado uma vez e guardado em `_trade_features`
(aditiva). Trocar filtro na aba só re-agrega em memória.

A assinatura de cada run (`_trade_features_runs`) junta o que muda o resultado do enriquecimento:
código/versões/configuração, todos os campos das operações, metadados econômicos/protocolo já declarados
e conteúdo da fonte. A leitura consistente congela as entradas do enriquecedor. SHA256 e contagem conferem
também as linhas do cache antes de reutilizá-las; uma assinatura antiga só provoca recálculo.

    feats = features(db, config, trades)      # trades = ledger.load(...); devolve as colunas de enrich.COLUMNS
    invalidate(db)                            # força recalcular tudo (ex.: depois de importar o calendário)
"""
import hashlib
import json
import logging
import platform
import threading
from pathlib import Path
import numpy as np
from typing import Optional

import pandas as pd
from sqlalchemy import inspect, text

from mt5_extracao import timeframes
from mt5_extracao.trade_analytics import enrich

log = logging.getLogger(__name__)

TABLE, RUNS = "_trade_features", "_trade_features_runs"
CACHE_VERSION = 2
_SCHEMA_LOCK = threading.RLock()
_CODE_FILES = (Path(__file__), Path(enrich.__file__), Path(timeframes.__file__),
               Path(enrich.indicators.__file__))
_INTEGERS = {"seq", "direction", "bars", "feature_version", "day_of_week", "day_of_month",
             "week_of_month", "month", "is_month_end", "hour", "event_impact", "win_flag"}
_FLOATS = {"minutes_from_open", "minutes_to_event", "event_surprise", "entry_volume", "volume_rank",
           "atr", "atr_pct", "bb_width", "prior_return_pct", "volatility_rank", "duration_min", "mae", "mfe"}


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str)


def _frame_digest(frame):
    """SHA256 de todas as colunas, sem soma de hashes ou arredondamento de floats."""
    digest = hashlib.sha256()
    keys = [c for c in ("run_id", "seq") if c in frame]
    data = frame.sort_values(keys, kind="stable") if keys else frame
    for column in sorted(data.columns):
        values = data[column]
        if column in _INTEGERS:
            payload = pd.to_numeric(values).astype("Int64").astype("string").to_json(orient="values").encode()
            kind = "integer"
        elif column in _FLOATS or pd.api.types.is_numeric_dtype(values) or pd.api.types.is_bool_dtype(values):
            array = pd.to_numeric(values).to_numpy(dtype="float64", na_value=np.nan).copy()
            array[np.isnan(array)] = np.nan
            array[array == 0] = 0.0  # SQL pode normalizar o sinal do zero.
            payload = array.astype("<f8").tobytes()
            kind = "float64"
        else:
            payload = values.astype("string").to_json(orient="values").encode()
            kind = "text"
        digest.update(_json([column, kind, len(payload)]).encode())
        digest.update(payload)
    digest.update(str(len(data)).encode())
    return digest.hexdigest()


def _settings_part(config):
    return {"bucket": enrich.time_bucket_minutes(config),
            "volatility_period": max(1, enrich._setting(config, "volatility_period", 14)),
            "before": enrich._setting(config, "event_window_before_min", 30),
            "after": enrich._setting(config, "event_window_after_min", 30),
            "event_symbols": config.get(enrich.SECTION, "event_symbols", fallback="") if config is not None else "",
            "economic_settings": dict(config["STRATEGY"]) if config is not None and config.has_section("STRATEGY") else {}}


def _code_part():
    from mt5_extracao.trade_analytics import calendar as cal
    import sqlalchemy
    return {"sources": [hashlib.sha256(path.read_bytes()).hexdigest() for path in (*_CODE_FILES, Path(cal.__file__))],
            "python": platform.python_version(), "pandas": pd.__version__, "numpy": np.__version__,
            "sqlalchemy": sqlalchemy.__version__, "feature_version": enrich.FEATURE_VERSION,
            "vol_lookback": enrich.VOL_LOOKBACK, "volume_lookback": enrich.VOLUME_LOOKBACK,
            "vol_labels": enrich.VOL_LABELS, "columns": enrich.COLUMNS}


class _Inputs:
    """Imagem efêmera de leitura para o enriquecedor existente; não é outro store."""
    def __init__(self, bars, specs, events):
        self.bars, self.specs, self.snapshot_events = bars, specs, events

    def load_ohlcv(self, symbol, timeframe):
        tf = timeframes.parse(timeframe)
        return self.bars.get((symbol, tf.name if tf else timeframe), pd.DataFrame()).copy(deep=True)

    def get_symbol_spec(self, symbol):
        value = self.specs.get(symbol)
        return dict(value) if value else None


def _freeze(db, part, snapshot):
    if snapshot is not None:
        view = snapshot.view()
        for symbol, tf in set(zip(part["symbol"], part["timeframe"])):
            view.load_ohlcv(symbol, tf)
        return view, {"manifest": snapshot.manifest_id}
    from mt5_extracao.trade_analytics import calendar as cal
    bars, specs = {}, {}
    with db.consistent_read() as conn:
        entries = part[part.entry_time.notna()]
        for symbol, name in sorted(set(zip(entries.symbol, entries.timeframe)), key=str):
            tf = timeframes.parse(name) if name else None
            bars[(symbol, tf.name if tf else name)] = db.load_ohlcv(symbol, tf.label, connection=conn) if symbol and tf else pd.DataFrame()
        for symbol in part.symbol.dropna().unique():
            specs[symbol] = db.get_symbol_spec(symbol, connection=conn)
        events = cal.merged(db, connection=conn)
    source = {"bars": [[symbol, tf, _frame_digest(frame)] for (symbol, tf), frame in sorted(bars.items(), key=str)],
              "specs": specs, "events": _frame_digest(events)}
    return _Inputs(bars, specs, events), source


def _run_part(db, part):
    """Inclui custos/parâmetros/protocolo já declarados, sem criar outro contrato."""
    if not inspect(db.engine).has_table("_robot_runs"):
        return []
    existing = {col["name"] for col in inspect(db.engine).get_columns("_robot_runs")}
    cols = [col for col in ("run_id", "source", "params_json", "metrics_json", "valuation_json") if col in existing]
    result = []
    with db.engine.connect() as conn:
        for ident in sorted(part.run_id.unique()):
            row = conn.execute(text(f"SELECT {', '.join(cols)} FROM _robot_runs WHERE run_id=:r"), {"r": ident}).mappings().first()
            if row is not None:
                result.append(dict(row))
    return result


def _snapshot(db, part):
    from mt5_extracao.analytics import datasets
    ids = part["run_id"].drop_duplicates().tolist()
    snapshots = [datasets.for_run(db, ident) for ident in ids]
    if any(s is not None for s in snapshots) and len(ids) != 1:
        raise ValueError("Agrupe por run antes de consultar uma revisão fixada de dados.")
    return snapshots[0] if len(snapshots) == 1 else None


def signature(db, config, part: pd.DataFrame, common: Optional[str] = None, *, bars_cache=None, snapshot=..., inputs=None) -> str:
    snapshot = _snapshot(db, part) if snapshot is ... else snapshot
    _, source = _freeze(db, part, snapshot) if inputs is None else inputs
    envelope = dict(version=CACHE_VERSION, code=_code_part(), settings=_settings_part(config),
                    trades=_frame_digest(part), source=source, run=_run_part(db, part), common=common)
    return hashlib.sha256(_json(envelope).encode("utf-8")).hexdigest()


def _ensure(db):
    with _SCHEMA_LOCK, db.engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE IF NOT EXISTS {RUNS} (run_id TEXT PRIMARY KEY, signature TEXT NOT NULL, "
                          "feature_version INTEGER, computed_at TEXT, payload_sha256 TEXT, row_count INTEGER)"))
        existing = {col["name"] for col in inspect(conn).get_columns(RUNS)}
        for column, kind in (("payload_sha256", "TEXT"), ("row_count", "INTEGER")):
            if column not in existing:
                conn.execute(text(f"ALTER TABLE {RUNS} ADD COLUMN {column} {kind}"))
        if inspect(conn).has_table(TABLE):
            existing = {col["name"] for col in inspect(conn).get_columns(TABLE)}
            for column in enrich.COLUMNS:
                if column not in existing:
                    kind = "BOOLEAN" if column == "is_month_end" else "INTEGER" if column in _INTEGERS else "DOUBLE PRECISION" if column in _FLOATS else "TEXT"
                    conn.execute(text(f"ALTER TABLE {TABLE} ADD COLUMN {column} {kind}"))


def _load(db, run_ids) -> pd.DataFrame:
    if not run_ids or not inspect(db.engine).has_table(TABLE):
        return pd.DataFrame(columns=list(enrich.COLUMNS))
    marks = ", ".join(f":r{i}" for i in range(len(run_ids)))
    with db.engine.connect() as conn:
        return pd.read_sql(text(f"SELECT {', '.join(enrich.COLUMNS)} FROM {TABLE} WHERE run_id IN ({marks})"), conn,
                           params={f"r{i}": r for i, r in enumerate(run_ids)})


def _store(db, run_id: str, sig: str, feats: pd.DataFrame) -> None:
    with db.engine.begin() as conn:
        if inspect(conn).has_table(TABLE):
            conn.execute(text(f"DELETE FROM {TABLE} WHERE run_id = :r"), {"r": run_id})
        conn.execute(text(f"DELETE FROM {RUNS} WHERE run_id = :r"), {"r": run_id})
        from sqlalchemy import Boolean, Float, Integer, Text
        # Tipos explícitos também quando uma coluna inteira só tem NULL nesta
        # execução. PostgreSQL não aceita NULL::VARCHAR numa coluna INTEGER.
        types = {column: (Boolean() if column == "is_month_end" else Integer() if column in _INTEGERS
                          else Float(53) if column in _FLOATS else Text()) for column in feats.columns}
        if inspect(conn).has_table(TABLE):
            types.update({column["name"]: column["type"] for column in inspect(conn).get_columns(TABLE)
                          if column["name"] in types})
        payload = feats.copy()
        for column, kind in types.items():
            if isinstance(kind, Integer):
                payload[column] = pd.to_numeric(payload[column]).astype("Int64")
        payload.astype(object).where(pd.notna(payload), None).to_sql(TABLE, conn, if_exists="append", index=False,
                                                                    dtype=types)
        conn.execute(text(f"INSERT INTO {RUNS} (run_id,signature,feature_version,computed_at,payload_sha256,row_count) "
                          "VALUES (:r, :s, :v, :t, :h, :n)"),
                     {"r": run_id, "s": sig, "v": enrich.FEATURE_VERSION, "h": _frame_digest(feats), "n": len(feats),
                      "t": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")})


def features(db, config, trades: pd.DataFrame) -> pd.DataFrame:
    """Contexto das operações: do cache quando a assinatura bate; recalcula e guarda os runs que mudaram."""
    if trades is None or trades.empty:
        return pd.DataFrame(columns=list(enrich.COLUMNS))
    _ensure(db)
    with db.engine.connect() as conn:
        known = {row["run_id"]: dict(row) for row in conn.execute(text(f"SELECT * FROM {RUNS}")).mappings()}
    parts, fresh, stale = [], 0, 0
    for run_id, part in trades.groupby("run_id", sort=False):
        snapshot = _snapshot(db, part)
        inputs = _freeze(db, part, snapshot)
        sig = signature(db, config, part, snapshot=snapshot, inputs=inputs)
        record = known.get(run_id, {})
        cached = _load(db, [run_id]) if record.get("signature") == sig else None
        try:
            valid = cached is not None and record.get("row_count") == len(cached) and record.get("payload_sha256") == _frame_digest(cached)
            if valid:
                valid = set(zip(cached.run_id, cached.seq)) == set(zip(part.run_id, part.seq)) and len(cached) == len(part)
        except (ValueError, TypeError, OverflowError):
            valid = False  # Cache corrompido é recalculado; o ledger não é alterado.
        if valid:
            parts.append(cached)
            fresh += 1
            continue
        stale += 1
        feats = enrich.enrich(inputs[0], config, part)
        try:
            _store(db, run_id, sig, feats)
        except Exception as exc:
            log.warning(f"Laboratório: contexto do run {run_id} não guardado no cache ({exc}).")
        parts.append(feats)
    out = pd.concat([p for p in parts if not p.empty], ignore_index=True) if any(not p.empty for p in parts) \
        else pd.DataFrame(columns=list(enrich.COLUMNS))
    for name in ("run_id",):
        out[name] = out[name].astype(str)
    out["seq"] = pd.to_numeric(out["seq"]).astype(int)
    log.info(f"Laboratório: {fresh} run(s) do cache, {stale} recalculado(s).")
    return out


def invalidate(db, run_id: Optional[str] = None) -> None:
    if not inspect(db.engine).has_table(RUNS):
        return
    with db.engine.begin() as conn:
        if run_id is None:
            conn.execute(text(f"DELETE FROM {RUNS}"))
        else:
            conn.execute(text(f"DELETE FROM {RUNS} WHERE run_id = :r"), {"r": run_id})
