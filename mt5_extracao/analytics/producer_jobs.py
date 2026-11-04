"""Checkpoints e métricas duráveis dos jobs de produtores Analytics."""
import json
import logging
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import inspect, text

log = logging.getLogger(__name__)


class _RSSSampler:
    """Amostra RSS durante o job para que a métrica não herde o pico de outro job."""

    def __init__(self):
        self.peak = current_rss_bytes()
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._sample, name="analytics-rss-sampler", daemon=True)
        self.thread.start()

    def _sample(self):
        while not self.stop_event.wait(0.05):
            self.peak = max(self.peak, current_rss_bytes())

    def stop(self):
        self.stop_event.set()
        self.thread.join(timeout=1.0)
        self.peak = max(self.peak, current_rss_bytes())
        return self.peak

_CREATE_CHECKPOINTS = """
CREATE TABLE IF NOT EXISTS _analytics_producer_checkpoints (
    symbol TEXT NOT NULL,
    tf TEXT NOT NULL,
    family TEXT NOT NULL,
    algorithm_version TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    source_sequence BIGINT NOT NULL,
    last_bar_start_utc TEXT,
    state_json TEXT NOT NULL,
    status TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    PRIMARY KEY (symbol, tf, family)
)
"""
_CREATE_RUNS = """
CREATE TABLE IF NOT EXISTS _analytics_producer_runs (
    run_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    tf TEXT NOT NULL,
    family TEXT NOT NULL,
    algorithm_version TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    source_sequence_start BIGINT,
    source_sequence_end BIGINT,
    status TEXT NOT NULL,
    started_at_utc TEXT NOT NULL,
    finished_at_utc TEXT,
    duration_ms BIGINT,
    peak_rss_bytes BIGINT,
    database_bytes_before BIGINT,
    database_bytes_after BIGINT,
    rows_inserted BIGINT NOT NULL DEFAULT 0,
    rows_revised BIGINT NOT NULL DEFAULT 0,
    rows_unchanged BIGINT NOT NULL DEFAULT 0,
    error TEXT
)
"""


def ensure_schema(db):
    with db.engine.begin() as conn:
        conn.execute(text(_CREATE_CHECKPOINTS))
        conn.execute(text(_CREATE_RUNS))


def _utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")


def _db_bytes(db):
    if getattr(db, "db_type", None) != "sqlite":
        return None
    path = getattr(db, "db_path", None)
    if not path:
        return None
    result = 0
    for candidate in (Path(path), Path(f"{path}-wal")):
        try:
            result += candidate.stat().st_size
        except OSError:
            pass
    return result


def current_rss_bytes():
    try:
        import psutil
        return int(psutil.Process().memory_info().rss)
    except (ImportError, OSError):
        return 0


def get_checkpoint(db, symbol, timeframe, family):
    ensure_schema(db)
    with db.engine.connect() as conn:
        row = conn.execute(text(
            "SELECT symbol, tf, family, algorithm_version, config_hash, source_sequence, last_bar_start_utc, "
            "state_json, status, updated_at_utc FROM _analytics_producer_checkpoints "
            "WHERE symbol = :symbol AND tf = :tf AND family = :family"),
            {"symbol": str(symbol), "tf": str(timeframe), "family": str(family)}).mappings().first()
    if row is None:
        return None
    result = dict(row)
    result["state"] = json.loads(result.pop("state_json"))
    return result


def start_run(db, symbol, timeframe, family, algorithm_version, config_hash, source_sequence):
    ensure_schema(db)
    run_id = str(uuid.uuid4())
    started = _utc_now()
    db_before = _db_bytes(db)
    with db.engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO _analytics_producer_runs "
            "(run_id, symbol, tf, family, algorithm_version, config_hash, source_sequence_start, status, "
            "started_at_utc, database_bytes_before) VALUES "
            "(:run_id, :symbol, :tf, :family, :version, :config_hash, :sequence, 'RUNNING', :started, :db_bytes)"),
            {"run_id": run_id, "symbol": str(symbol), "tf": str(timeframe), "family": str(family),
             "version": str(algorithm_version), "config_hash": str(config_hash), "sequence": source_sequence,
             "started": started, "db_bytes": db_before})
    return {"run_id": run_id, "started_at_utc": started, "started_monotonic": time.monotonic(),
            "rss_sampler": _RSSSampler(), "database_bytes_before": db_before}


def advance_checkpoint(conn, *, symbol, timeframe, family, algorithm_version, config_hash, source_sequence,
                       last_bar_start, state=None, status="COMPLETE"):
    """Atualiza o checkpoint dentro da transação de valores correspondente."""
    conn.execute(text(
        "INSERT INTO _analytics_producer_checkpoints "
        "(symbol, tf, family, algorithm_version, config_hash, source_sequence, last_bar_start_utc, state_json, "
        "status, updated_at_utc) VALUES "
        "(:symbol, :tf, :family, :version, :config_hash, :sequence, :last_bar, :state, :status, :updated) "
        "ON CONFLICT (symbol, tf, family) DO UPDATE SET algorithm_version = excluded.algorithm_version, "
        "config_hash = excluded.config_hash, source_sequence = excluded.source_sequence, "
        "last_bar_start_utc = excluded.last_bar_start_utc, state_json = excluded.state_json, "
        "status = excluded.status, updated_at_utc = excluded.updated_at_utc"),
        {"symbol": str(symbol), "tf": str(timeframe), "family": str(family), "version": str(algorithm_version),
         "config_hash": str(config_hash), "sequence": int(source_sequence), "last_bar": last_bar_start,
         "state": json.dumps(state or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
         "status": str(status), "updated": _utc_now()})


def finish_run(db, run, *, source_sequence, status, counts=None, error=None):
    peak = run["rss_sampler"].stop()
    counts = counts or {}
    finished = _utc_now()
    after = _db_bytes(db)
    try:
        growth = max(0, after - run["database_bytes_before"]) if after is not None else None
    except TypeError:
        growth = None
    error_text = None if error is None else str(error)[:2000]
    if error_text:
        log.warning("Job Analytics %s terminou com %s: %s", run["run_id"], status, error_text)
    with db.engine.begin() as conn:
        conn.execute(text(
            "UPDATE _analytics_producer_runs SET source_sequence_end = :sequence, status = :status, "
            "finished_at_utc = :finished, duration_ms = :duration, peak_rss_bytes = :rss, "
            "database_bytes_after = :db_after, rows_inserted = :inserted, rows_revised = :revised, "
            "rows_unchanged = :unchanged, error = :error WHERE run_id = :run_id"),
            {"sequence": source_sequence, "status": status, "finished": finished,
             "duration": max(0, int((time.monotonic() - run["started_monotonic"]) * 1000)),
             "rss": peak, "db_after": after,
             "inserted": int(counts.get("inserted", 0)), "revised": int(counts.get("revised", 0)),
             "unchanged": int(counts.get("unchanged", 0)), "error": error_text, "run_id": run["run_id"]})
    return {"run_id": run["run_id"], "status": status, "duration_ms": max(
        0, int((time.monotonic() - run["started_monotonic"]) * 1000)),
        "peak_rss_bytes": peak, "database_growth_bytes": growth}


def abort_run(db, run, *, source_sequence, error, counts=None, cancelled=False):
    """Encerra recursos ao abortar, preservando o erro original se o recibo falhar."""
    status = "CANCELLED" if cancelled or isinstance(error, (KeyboardInterrupt, SystemExit)) else "FAILED"
    try:
        finish_run(db, run, source_sequence=source_sequence, status=status, counts=counts, error=error)
    except BaseException as journal_error:
        note = f"Estado {status} do job Analytics {run['run_id']} não foi persistido: {str(journal_error)[:2000]}"
        log.error(note)
        if hasattr(error, "add_note"):
            error.add_note(note)
    finally:
        run["rss_sampler"].stop()


def latest_run(db, symbol, timeframe, family):
    if not inspect(db.engine).has_table("_analytics_producer_runs"):
        return None
    with db.engine.connect() as conn:
        row = conn.execute(text(
            "SELECT run_id, status, started_at_utc, finished_at_utc, duration_ms, peak_rss_bytes, "
            "database_bytes_before, database_bytes_after, rows_inserted, rows_revised, rows_unchanged, error "
            "FROM _analytics_producer_runs WHERE symbol = :symbol AND tf = :tf AND family = :family "
            "ORDER BY started_at_utc DESC LIMIT 1"),
            {"symbol": str(symbol), "tf": str(timeframe), "family": str(family)}).mappings().first()
    return dict(row) if row else None
