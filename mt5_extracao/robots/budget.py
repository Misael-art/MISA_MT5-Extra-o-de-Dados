"""Orçamento cooperativo e checkpoint operacional das filas existentes (T20.24).

Não é um registro estatístico de tentativas nem um novo agendador. Interrupção
nativa é conferida na volta do Tester: nenhum terminal alheio é encerrado.
"""
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import socket
import threading
import time
import uuid

from sqlalchemy import inspect, text


class Interrupted(RuntimeError):
    """Uma execução parcial não pode ser publicada como backtest completo."""


@dataclass(frozen=True)
class Limits:
    max_jobs: int = 0
    max_seconds: float = 0
    hard_cpu_pct: float = 0
    hard_ram_pct: float = 0
    min_free_mb: float = 256

    def __post_init__(self):
        for key, value in asdict(self).items():
            if not math.isfinite(value) or value < 0 or (key.endswith("_pct") and value > 100) or (key == "max_jobs" and value != int(value)):
                raise ValueError("Orçamento inválido; use limites não negativos e percentuais até 100.")

    @classmethod
    def from_config(cls, config):
        values = {}
        for key, default in asdict(cls()).items():
            raw = config.get("ROBOTS", "batch_" + key, fallback=str(default)) if config is not None else default
            try:
                value = float(raw)
            except (TypeError, ValueError):
                raise ValueError(f"Orçamento batch_{key} inválido; use um número não negativo.") from None
            if not math.isfinite(value) or value < 0 or (key.endswith("_pct") and value > 100) or (key == "max_jobs" and value != int(value)):
                raise ValueError(f"Orçamento batch_{key} inválido; confira [ROBOTS].")
            values[key] = int(value) if key == "max_jobs" else value
        return cls(**values)


def _stamp():
    return datetime.now(timezone.utc).isoformat()


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _sample(path, *, cpu_interval=None):
    import psutil
    import shutil
    proc = psutil.Process()
    io = proc.io_counters() if hasattr(proc, "io_counters") else None
    return dict(cpu=psutil.cpu_percent(interval=cpu_interval), ram=psutil.virtual_memory().percent,
                rss=proc.memory_info().rss, cpu_seconds=sum(proc.cpu_times()[:2]),
                read_bytes=getattr(io, "read_bytes", None), write_bytes=getattr(io, "write_bytes", None),
                free_bytes=shutil.disk_usage(path).free)


def _owner():
    import psutil
    return dict(host=socket.gethostname(), pid=os.getpid(), started=psutil.Process().create_time())


def _alive(owner):
    import psutil
    if owner.get("host") != socket.gethostname():
        return True  # Não roubar execução de outro host por um timeout presumido.
    try:
        return psutil.Process(owner["pid"]).create_time() == owner["started"]
    except psutil.NoSuchProcess:
        return False
    except (psutil.AccessDenied, KeyError):
        return True


_SCHEMA_LOCK = threading.RLock()

def ensure_schema(db):
    with _SCHEMA_LOCK, db.engine.begin() as conn:
        conn.execute(text("CREATE TABLE IF NOT EXISTS _robot_batch_queue (batch_id TEXT PRIMARY KEY, "
                          "plan_json TEXT NOT NULL, owner_json TEXT NOT NULL, status TEXT NOT NULL, "
                          "reason TEXT NOT NULL, elapsed DOUBLE PRECISION NOT NULL, updated_at TEXT NOT NULL, metrics_json TEXT)"))
        if "metrics_json" not in {c["name"] for c in inspect(conn).get_columns("_robot_batch_queue")}:
            conn.execute(text("ALTER TABLE _robot_batch_queue ADD COLUMN metrics_json TEXT"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS _robot_batch_jobs (batch_id TEXT NOT NULL, "
                          "job_key TEXT NOT NULL, run_id TEXT NOT NULL, status TEXT NOT NULL, "
                          "history_json TEXT NOT NULL, PRIMARY KEY (batch_id, job_key))"))


def checkpoint(db, batch_id):
    if not inspect(db.engine).has_table("_robot_batch_queue"):
        return None
    with db.engine.connect() as conn:
        row = conn.execute(text("SELECT * FROM _robot_batch_queue WHERE batch_id=:b"), {"b": batch_id}).mappings().first()
        if row is None:
            return None
        data = dict(row)
        data["plan"] = json.loads(data.pop("plan_json"))
        data["owner"] = json.loads(data.pop("owner_json"))
        data["measurements"] = json.loads(data.pop("metrics_json", None) or "{}")
        data["jobs"] = [dict(x) for x in conn.execute(text("SELECT job_key, run_id, status, history_json FROM "
                         "_robot_batch_jobs WHERE batch_id=:b ORDER BY job_key"), {"b": batch_id}).mappings()]
    for job in data["jobs"]:
        job["history"] = json.loads(job.pop("history_json"))
    return data


class Budget:
    def __init__(self, db=None, config=None, *, limits=None, sample=None, clock=time.monotonic, batch_id=None):
        self.db, self.config = db, config
        self.limits = limits or Limits.from_config(config)
        self.explicit_id = batch_id is not None
        self.batch_id = batch_id or uuid.uuid4().hex
        self._clock, self._start = clock, clock()
        self._sample = sample or (lambda path: _sample(path, cpu_interval=.05 if self.limits.hard_cpu_pct else None))
        from mt5_extracao.resources import guard
        self._write_guard = guard.make(db, config) if db is not None else None
        # O disco do banco SQLite contém os blocos/snapshots; temporários podem
        # estar em outro volume e são conferidos adicionalmente pelo consumidor.
        self.path = Path(db.db_path).resolve().parent if db is not None and getattr(db, "dialect", "") == "sqlite" else Path.cwd()
        self._lock = threading.RLock()
        self._reason_lock = threading.Lock()
        self._stop = threading.Event()
        self.reason, self.measurements = "", {}
        self._sample_at = None
        self._claimed, self._owner_json = False, _json(_owner())
        self._previous_elapsed = 0.0
        self._admitted = 0
        self._jobs = {}
        self._plan = {"jobs": []}

    def cancel(self, reason="Cancelado pelo usuário; retome o lote para continuar o que falta."):
        # A UI não espera por I/O de publicação nem pela leitura de recursos.
        with self._reason_lock:
            self.reason = self.reason or reason
            self._stop.set()

    @property
    def elapsed(self):
        return self._previous_elapsed + max(0.0, self._clock() - self._start)

    def check(self, *, path=None, reserve_bytes=0):
        with self._lock:
            if self._stop.is_set():
                raise Interrupted(self.reason)
            if self._write_guard is not None:
                reason = self._write_guard()
                if reason:
                    self.cancel("Gravação bloqueada pela política de Recursos: " + reason)
                    raise Interrupted(self.reason)
            if self.limits.max_seconds and self.elapsed >= self.limits.max_seconds:
                self.cancel("Tempo máximo do lote atingido; aumente batch_max_seconds ou retome com outro orçamento.")
                raise Interrupted(self.reason)
            now = self._clock()
            if path is not None or self._sample_at is None or now - self._sample_at >= .25:
                try:
                    measured = self._sample(Path(path) if path is not None else self.path)
                except Exception:
                    self.cancel("Não foi possível medir os recursos; confira o disco e retome o lote.")
                    raise Interrupted(self.reason) from None
                self.measurements = dict(measured, elapsed_seconds=self.elapsed, scope="process_and_host_not_per_job",
                                         sampled_rss_peak=max(measured["rss"], self.measurements.get("sampled_rss_peak", 0)))
                self._sample_at = now
            m = self.measurements
            for key, threshold, label in (("cpu", self.limits.hard_cpu_pct, "CPU"), ("ram", self.limits.hard_ram_pct, "RAM")):
                if threshold and m[key] >= threshold:
                    self.cancel(f"Limite de {label} do lote atingido; libere recursos ou ajuste batch_hard_{key}_pct.")
            if m["free_bytes"] < self.limits.min_free_mb * 1024**2 + reserve_bytes:
                self.cancel("Disco sem espaço para a reserva do lote; libere espaço ou ajuste batch_min_free_mb.")
            if self._stop.is_set():
                raise Interrupted(self.reason)

    def prepare(self, jobs, *, start=None, end=None, preset=None, resume_latest=False):
        """Fixa o plano uma vez; retomada relê os mesmos manifests, não a base atual."""
        from dataclasses import asdict
        from datetime import timedelta
        from mt5_extracao import timeframes
        from mt5_extracao.strategies import research
        from mt5_extracao.analytics import datasets
        from . import catalog, mt5_tools, backtest as backtest_mod
        entries, signatures = [], []
        # Hashes do código, sem caminhos pessoais no checkpoint.
        root = Path(__file__).resolve().parents[1]
        code = {p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in sorted(root.rglob("*.py"))}
        for robot, symbol, timeframe in jobs:
            tf = timeframes.parse(timeframe)
            if tf is None:
                raise ValueError(f"Timeframe inválido: {timeframe}")
            key = hashlib.sha256(_json([robot.key, symbol, tf.name]).encode()).hexdigest()
            if any(entry["key"] == key for entry in entries):
                raise ValueError("Combinação repetida no lote; remova a duplicata antes de executar.")
            entries.append(dict(key=key, robot=robot.key, symbol=symbol, timeframe=tf.name, kind=robot.kind))
            signatures.append(dict(key=key, robot=robot.key, kind=robot.kind,
                                   fingerprint=catalog.fingerprint(robot) if robot.kind == catalog.MQL5 and hasattr(robot, "files") else "",
                                   preset={k: v for k, v in getattr(robot, "presets", {}).get(preset, {}).items() if k != "__file__"}))
        signature = hashlib.sha256(_json(dict(jobs=sorted(entries, key=lambda x: x["key"]),
                                 sources=sorted(signatures, key=lambda x: x["key"]), code=code, preset=preset,
                                 start=str(start), end=str(end), settings=asdict(research.backtest_config(self.config)),
                                 tester_model=mt5_tools.tester_model(self.config),
                                 runtime_code=backtest_mod._execution_code_digest())).encode()).hexdigest()
        previous = checkpoint(self.db, self.batch_id) if self.db is not None else None
        if previous is not None:
            if previous["plan"].get("version") != 1:
                raise ValueError("Versão de checkpoint desconhecida; use uma versão compatível do aplicativo.")
            if previous["plan"].get("signature") != signature:
                raise ValueError("Checkpoint de outro plano/código/configuração; use um novo lote.")
            self.begin(previous["plan"])
            by_key = {entry["key"]: entry for entry in previous["plan"]["jobs"]}
            return [by_key[entry["key"]] for entry in entries]
        native_end = end or datetime.now()
        native_start = start or native_end - timedelta(days=365)
        for entry in entries:
            self.check()
            if entry["kind"] == catalog.PYTHON and self.db is not None:
                try:
                    snapshot = datasets.capture(self.db, entry["symbol"], entry["timeframe"], start, end)
                    entry["data_manifest_id"] = snapshot.manifest_id
                except (ValueError, OSError):
                    entry["preparation_error"] = ("Sem revisão utilizável de {} {}; ".format(entry["symbol"], entry["timeframe"]) +
                                                  "confira o histórico e inicie outro lote após corrigir os dados.")
            if entry["kind"] == catalog.MQL5:
                entry.update(start=native_start.isoformat(), end=native_end.isoformat())
        if resume_latest and not self.explicit_id and self.db is not None and inspect(self.db.engine).has_table("_robot_batch_queue"):
            with self.db.engine.connect() as conn:
                candidates = conn.execute(text("SELECT batch_id,plan_json FROM _robot_batch_queue "
                                               "ORDER BY updated_at DESC,batch_id DESC")).mappings().all()
            current_data = {entry["key"]: entry.get("data_manifest_id") for entry in entries if entry["kind"] == catalog.PYTHON}
            for candidate in candidates:
                try:
                    old = json.loads(candidate["plan_json"])
                except (TypeError, ValueError):
                    continue
                if old.get("version") != 1 or old.get("signature") != signature:
                    continue
                old_data = {entry["key"]: entry.get("data_manifest_id") for entry in old["jobs"] if entry["kind"] == catalog.PYTHON}
                if old_data != current_data:
                    continue
                self.batch_id = candidate["batch_id"]
                self.begin(old)
                by_key = {entry["key"]: entry for entry in old["jobs"]}
                return [by_key[entry["key"]] for entry in entries]
        plan = dict(version=1, signature=signature, jobs=entries)
        self.begin(plan)
        return entries

    def begin(self, plan):
        """Plano fechado; a identidade inclui as revisões escolhidas pelo chamador."""
        self.check()
        if self.db is None:
            return
        ensure_schema(self.db)
        payload = _json(plan)
        with self.db.engine.begin() as conn:
            conn.execute(text("INSERT INTO _robot_batch_queue (batch_id, plan_json, owner_json, status, reason, elapsed, updated_at) "
                              "VALUES (:b,:p,:empty,:state,:reason,0,:t) ON CONFLICT (batch_id) DO NOTHING"),
                         {"b": self.batch_id, "p": payload, "t": _stamp(), "empty": "{}", "state": "pending", "reason": ""})
            row = conn.execute(text("SELECT plan_json, owner_json, elapsed FROM _robot_batch_queue WHERE batch_id=:b"), {"b": self.batch_id}).mappings().one()
            if row["plan_json"] != payload:
                raise ValueError("Checkpoint de outro plano/código/dados; use um novo identificador de lote.")
            previous = json.loads(row["owner_json"])
            if previous and _alive(previous):
                raise ValueError("O lote já tem um executor ativo; aguarde o término antes de retomar.")
            changed = conn.execute(text("UPDATE _robot_batch_queue SET owner_json=:o, status=:s, reason=:r, updated_at=:t "
                                        "WHERE batch_id=:b AND owner_json=:old"),
                                   {"b": self.batch_id, "o": self._owner_json, "old": row["owner_json"], "t": _stamp(), "s": "running", "r": ""}).rowcount
            if changed != 1:
                raise ValueError("Outro executor assumiu o lote; não houve execução duplicada.")
            self._previous_elapsed = float(row["elapsed"])
            self._jobs = {x["job_key"]: dict(x) for x in conn.execute(text("SELECT * FROM _robot_batch_jobs WHERE batch_id=:b"), {"b": self.batch_id}).mappings()}
        self._admitted = sum(sum(event["status"] == "running" for event in json.loads(job["history_json"]))
                             for job in self._jobs.values())
        self._plan = plan
        self._claimed = True

    def admit(self, key):
        with self._lock:
            self.check()
            if self.limits.max_jobs and self._admitted >= self.limits.max_jobs:
                # O teto de admissões não cancela testes já admitidos.
                with self._reason_lock:
                    self.reason = self.reason or "Orçamento de testes do lote atingido; aumente batch_max_jobs ao retomar os itens restantes."
                raise Interrupted(self.reason)
            self._admitted += 1
            prior = self._jobs.get(key)
            run_id = prior["run_id"] if prior else uuid.uuid4().hex
            if self.db is not None:
                self._record(key, run_id, "running", "")
            return JobBudget(self, key, run_id)

    def recovered(self, key):
        from . import store
        prior = self._jobs.get(key)
        if prior is None:
            return None
        records = store.load_runs(self.db, run_id=prior["run_id"])
        if records:
            from . import catalog
            record = records[0]
            expected = next((entry for entry in self._plan["jobs"] if entry["key"] == key), None)
            valid = expected is not None and all(record[field] == expected[name] for field, name in
                                                  (("robot_key", "robot"), ("symbol", "symbol"), ("timeframe", "timeframe")))
            if valid and expected["kind"] == catalog.PYTHON:
                valid = record["source"] == "backtest" and record.get("data_manifest_id") == expected.get("data_manifest_id")
            elif valid:
                valid = record["source"] == "strategy tester" and all(
                    record["data_" + name] == datetime.fromisoformat(expected[name]).strftime("%Y-%m-%d %H:%M:%S")
                    for name in ("start", "end"))
            if not valid:
                raise ValueError("Resultado do checkpoint não corresponde ao plano/revisão; restaure a referência antes de retomar.")
            return record
        if prior["status"] == "completed":
            raise ValueError("Resultado do checkpoint está ausente; restaure o artefato antes de retomar.")
        return None

    def _record(self, key, run_id, state, reason, conn=None):
        previous = self._jobs.get(key, {})
        history = json.loads(previous.get("history_json", "[]"))
        history.append(dict(status=state, reason=reason, at=_stamp()))
        params = dict(b=self.batch_id, k=key, r=run_id, s=state, h=_json(history))
        sql = text("INSERT INTO _robot_batch_jobs (batch_id,job_key,run_id,status,history_json) VALUES (:b,:k,:r,:s,:h) "
                   "ON CONFLICT (batch_id,job_key) DO UPDATE SET status=:s, history_json=:h")
        if conn is not None:
            conn.execute(sql, params)
        else:
            with self.db.engine.begin() as connection:
                connection.execute(sql, params)
        if conn is None:
            self._jobs[key] = dict(job_key=key, run_id=run_id, status=state, history_json=params["h"])

    def remember(self, key, record):
        """Mantém a referência exata reutilizada, sem regravar o resultado."""
        if self.db is not None:
            with self._lock:
                prior = self._jobs.get(key)
                if prior is not None and prior["run_id"] != record["run_id"]:
                    raise ValueError("Checkpoint já reservou outro resultado; retome sem o cache legado.")
                if prior is None:
                    self._record(key, record["run_id"], "completed", "reutilizado pela identidade de execução; origem preservada")

    def finish(self, *, failed=False):
        if self.db is not None and self._claimed:
            state = "interrupted" if self._stop.is_set() or self.reason else "failed" if failed else "completed"
            with self.db.engine.begin() as conn:
                unfinished = conn.execute(text("SELECT job_key,run_id FROM _robot_batch_jobs "
                                               "WHERE batch_id=:b AND status=:s"),
                                          {"b": self.batch_id, "s": "running"}).mappings().all()
                if unfinished and state == "completed":
                    state = "failed"
                    self.reason = "Executor retornou sem publicar um resultado completo; confira o checkpoint."
                for job in unfinished:
                    self._record(job["job_key"], job["run_id"], "interrupted", self.reason or "execução sem publicação", conn)
                conn.execute(text("UPDATE _robot_batch_queue SET owner_json=:empty,status=:s,reason=:r,elapsed=:e,updated_at=:t,metrics_json=:m "
                                  "WHERE batch_id=:b AND owner_json=:o"),
                             dict(s=state, r=self.reason, e=self.elapsed, t=_stamp(), b=self.batch_id,
                                  o=self._owner_json, empty="{}", m=_json(self.measurements)))
            self._claimed = False


class JobBudget:
    def __init__(self, parent, key, run_id):
        self.parent, self.key, self.run_id = parent, key, run_id

    def check(self, **kwargs):
        self.parent.check(**kwargs)

    def result(self, state, reason="", conn=None):
        if self.parent.db is not None:
            with self.parent._lock:
                self.parent._record(self.key, self.run_id, state, reason, conn)

    @contextmanager
    def publication(self):
        with self.parent._lock:
            self.check()
            yield
