"""Checkpoints e métricas duráveis dos jobs de produtores Analytics."""
import json
import logging
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import inspect, text
from mt5_extracao import process_owner

log = logging.getLogger(__name__)
_SCHEMA_LOCK = threading.RLock()


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
    error TEXT,
    configuration_json TEXT,
    owner_json TEXT,
    recovered_at_utc TEXT
)
"""


def ensure_schema(db):
    with _SCHEMA_LOCK, db.engine.begin() as conn:
        if conn.dialect.name == "sqlite" and not conn.connection.driver_connection.in_transaction:
            conn.exec_driver_sql("BEGIN IMMEDIATE")
        conn.execute(text(_CREATE_CHECKPOINTS))
        conn.execute(text(_CREATE_RUNS))
        columns = {column["name"] for column in inspect(conn).get_columns("_analytics_producer_runs")}
        for name in ("configuration_json", "owner_json", "recovered_at_utc"):
            if name not in columns:
                clause = "IF NOT EXISTS " if conn.dialect.name == "postgresql" else ""
                conn.execute(text("ALTER TABLE _analytics_producer_runs ADD COLUMN " + clause + name + " TEXT"))


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


def reconcile_orphan_runs(db, *, symbol=None, timeframe=None, family=None):
    """Marca INTERRUPTED somente com morte local comprovada; não modifica fatos/checkpoints."""
    scope = {key: str(value) for key, value in
             (("symbol", symbol), ("tf", timeframe), ("family", family)) if value is not None}
    if any(not value.strip() for value in scope.values()):
        raise ValueError("Filtros de recuperação não podem ser vazios.")
    ensure_schema(db)
    conditions = " AND ".join(["status='RUNNING'", "run_id > :cursor"] + [key + " = :" + key for key in scope])
    recovered, inspected, cursor = [], 0, ""
    recovered_at = _utc_now()
    while True:
        with db.engine.connect() as conn:
            rows = conn.execute(text("SELECT run_id, owner_json FROM _analytics_producer_runs WHERE " +
                conditions + " ORDER BY run_id LIMIT 500"), {**scope, "cursor": cursor}).mappings().all()
        if not rows:
            break
        inspected += len(rows)
        for row in rows:
            try:
                owner = json.loads(row["owner_json"]) if row["owner_json"] else None
            except (ValueError, TypeError):
                owner = None
            # Não promover metadados antigos baseados apenas em hostname/PID a
            # prova de propriedade local (nomes de host podem se repetir).
            if not isinstance(owner, dict) or not owner.get("host_scope") or process_owner.is_alive(owner):
                continue
            with db.engine.begin() as conn:
                changed = conn.execute(text("UPDATE _analytics_producer_runs SET status='INTERRUPTED', "
                    "recovered_at_utc=:recovered, "
                    "error=:error WHERE run_id=:run_id AND status='RUNNING' AND owner_json=:owner"),
                    dict(run_id=row["run_id"], owner=row["owner_json"], recovered=recovered_at,
                         error="Processo proprietário encerrado; execução interrompida. Retome pelos lotes confirmados; "
                               "instante de término e métricas finais desconhecidos.")).rowcount
            if changed:
                recovered.append(row["run_id"])
        cursor = rows[-1]["run_id"]
    return dict(scope=scope, inspected=inspected, recovered=recovered,
                preserved=inspected-len(recovered), recovered_at_utc=recovered_at)


def start_run(db, symbol, timeframe, family, algorithm_version, config_hash, source_sequence, *, configuration=None):
    if configuration is not None and not isinstance(configuration, dict):
        raise ValueError("Configuração do job precisa ser um objeto JSON.")
    try:
        configuration_json = None if configuration is None else json.dumps(
            configuration, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (ValueError, TypeError) as exc:
        raise ValueError("Configuração do job precisa conter somente valores JSON válidos.") from exc
    reconcile_orphan_runs(db, symbol=symbol, timeframe=timeframe, family=family)
    run_id = str(uuid.uuid4())
    started = _utc_now()
    db_before = _db_bytes(db)
    with db.engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO _analytics_producer_runs "
            "(run_id, symbol, tf, family, algorithm_version, config_hash, source_sequence_start, status, "
            "started_at_utc, database_bytes_before, configuration_json, owner_json) VALUES "
            "(:run_id, :symbol, :tf, :family, :version, :config_hash, :sequence, 'RUNNING', :started, :db_bytes, :configuration, :owner)"),
            {"run_id": run_id, "symbol": str(symbol), "tf": str(timeframe), "family": str(family),
             "version": str(algorithm_version), "config_hash": str(config_hash), "sequence": source_sequence,
             "started": started, "db_bytes": db_before, "configuration": configuration_json,
             "owner": json.dumps(process_owner.current(), sort_keys=True)})
    return {"run_id": run_id, "started_at_utc": started, "started_monotonic": time.monotonic(),
            "rss_sampler": _RSSSampler(), "database_bytes_before": db_before}


def attach_run_inputs(db, run, inputs):
    """Anexa uma única seleção de entrada ao run ativo; depois fica imutável.

    Permite fixar IDs de um painel após descobrir seu roster, sem guardar texto
    ou prender transação durante rede. Não altera cursor/fatos nem cria journal.
    """
    if not isinstance(inputs,dict):raise ValueError('Manifesto de entrada precisa ser objeto JSON.')
    raw=json.dumps(inputs,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)
    if len(raw.encode())>64*1024:raise ValueError('Manifesto de entrada excede64KiB; não guarde corpus no journal.')
    with db.engine.begin() as conn:
        lock=' FOR UPDATE' if conn.dialect.name=='postgresql' else ''
        row=conn.execute(text('SELECT status,configuration_json FROM _analytics_producer_runs WHERE run_id=:id'+lock),
                         {'id':run['run_id']}).first()
        if row is None or row[0]!='RUNNING':raise ValueError('Somente run ativo pode receber seu manifesto de entrada.')
        configuration=json.loads(row[1]) if row[1] is not None else {}
        if 'input_manifest' in configuration:
            if configuration['input_manifest']!=inputs:raise ValueError('Manifesto do run já foi fixado; não pode mudar de população.')
            return False
        configuration['input_manifest']=json.loads(raw)
        updated=json.dumps(configuration,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)
        prior_clause='configuration_json IS NULL' if row[1] is None else 'configuration_json=:old'
        changed=conn.execute(text('UPDATE _analytics_producer_runs SET configuration_json=:new '
            'WHERE run_id=:id AND status=\'RUNNING\' AND '+prior_clause),
            dict(new=updated,id=run['run_id'],old=row[1]))
        if changed.rowcount!=1:raise RuntimeError('Run mudou antes de fixar a entrada; reexecute sem sobrescrever o recibo.')
    return True


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
    if row is None:
        return None
    result = dict(row)
    if result["status"] == "INTERRUPTED":
        # As colunas legadas NOT NULL conservam seus placeholders. Sem finalização,
        # não são contagens finais e a superfície de leitura não as apresenta como zero.
        for name in ("duration_ms", "peak_rss_bytes", "database_bytes_after",
                     "rows_inserted", "rows_revised", "rows_unchanged"):
            result[name] = None
        result["metrics_state"] = "UNAVAILABLE"
    return result
