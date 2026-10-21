"""
Bateria de backtests em segundo plano (botão "Iniciar Backtest" da aba "Minha base").

    jobs = [Job(robot, "PETR4", "1 dia"), ...]
    runner = BatchRunner(db, config, jobs, on_event=print)
    runner.start()            # não bloqueia; runner.cancel() interrompe o que ainda não começou

Os testes rodam em um `concurrent.futures.ThreadPoolExecutor`. O `ResourceGovernor` lê o uso de CPU e de memória
(psutil) e ajusta quantos testes rodam ao mesmo tempo, para o computador não passar do limite configurado em
`[ROBOTS] batch_max_cpu_pct` e `batch_max_ram_pct`. Robôs MQL5 rodam um por vez: o MT5 não abre duas vezes a mesma
instalação. Uma falha não interrompe os outros testes.

`on_event` é chamado pelas threads de trabalho: quem usa na interface precisa repassar para a thread do Tkinter.
"""
import logging
import os
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import Callable, List, Optional, Tuple

from . import backtest, catalog
from . import budget as budget_mod

log = logging.getLogger(__name__)

DEFAULT_MAX_CPU, DEFAULT_MAX_RAM = 75.0, 85.0
HEADROOM = 15.0             # só aumenta os testes simultâneos com esta folga de CPU abaixo do limite
SAMPLE_INTERVAL = 0.25      # leituras mais próximas que isso repetem a anterior (a medida de CPU fica instável)
STARTED, DONE, FINISHED = "start", "done", "finished"


@dataclass
class Job:
    robot: object            # catalog.Robot
    symbol: str
    timeframe: str           # rótulo da base (ex.: "1 dia")


def _psutil_sample() -> Tuple[float, float]:
    import psutil
    return float(psutil.cpu_percent(interval=None)), float(psutil.virtual_memory().percent)


def limits_from_config(config) -> dict:
    """Limites de `[ROBOTS]`, com os padrões quando a chave falta ou está inválida."""
    def read(key, default):
        try:
            return float(config.get("ROBOTS", key, fallback=str(default))) if config is not None else default
        except (TypeError, ValueError):
            return default
    return {"max_cpu": read("batch_max_cpu_pct", DEFAULT_MAX_CPU), "max_ram": read("batch_max_ram_pct", DEFAULT_MAX_RAM),
            "max_workers": int(read("batch_max_workers", 0)) or None}


class ResourceGovernor:
    """Decide quantos testes podem rodar ao mesmo tempo a partir do uso de CPU e de memória do computador."""

    def __init__(self, max_cpu: float = DEFAULT_MAX_CPU, max_ram: float = DEFAULT_MAX_RAM,
                 max_workers: Optional[int] = None, sample: Optional[Callable] = None,
                 clock: Callable = time.monotonic):
        self.max_cpu, self.max_ram = float(max_cpu), float(max_ram)
        self.max_workers = max(1, int(max_workers or max(1, (os.cpu_count() or 2) - 1)))
        self._sample, self._clock = sample or _psutil_sample, clock
        self._last, self._at = (0.0, 0.0), None
        self.limit = 1
        if sample is None:
            _psutil_sample()            # a primeira leitura de CPU do psutil não mede nada: serve de ponto de partida

    def read(self) -> Tuple[float, float]:
        """(CPU %, RAM %). Devolve a leitura anterior se a última foi há menos de SAMPLE_INTERVAL."""
        now = self._clock()
        if self._at is None or now - self._at >= SAMPLE_INTERVAL:
            try:
                self._last = self._sample()
            except Exception as e:      # sem leitura, mantém a anterior: o lote não para por causa do monitor
                log.debug(f"Não foi possível ler o uso de CPU e memória: {e}")
            self._at = now
            self._adjust()
        return self._last

    def _adjust(self) -> None:
        cpu, ram = self._last
        if cpu >= self.max_cpu or ram >= self.max_ram:
            self.limit = max(1, self.limit - 1)
        elif cpu < self.max_cpu - HEADROOM:
            self.limit = min(self.max_workers, self.limit + 1)


def run_job(db, config, job: Job, *, control=None, entry=None) -> dict:
    if job.robot.kind == catalog.MQL5:
        from datetime import datetime
        dates = {key: datetime.fromisoformat(entry[key]) for key in ("start", "end")} if entry else {}
        return backtest.run_mql5(db, config, job.robot, job.symbol, job.timeframe, control=control, **dates)
    return backtest.run_python(db, config, job.robot, job.symbol, job.timeframe, control=control,
                               data_manifest_id=entry.get("data_manifest_id") if entry else None)


class BatchRunner:
    """
    Roda `jobs` em segundo plano. Eventos (dicionários) entregues a `on_event`:
      {"kind": "start", "index" (1 = primeiro a começar), "total", "job", "cpu", "ram", "workers"}
      {"kind": "done", "completed", "total", "job", "run" (ou None), "error" (ou ""), "cpu", "ram"}
      {"kind": "finished", "total", "ok", "failed", "canceled"}
    `mql5_setup`, se informado, é chamado uma vez antes do primeiro robô MQL5 (ex.: fechar o MT5) e devolve a função
    que desfaz o preparo no fim (ou None).
    """

    def __init__(self, db, config, jobs: List[Job], on_event: Optional[Callable] = None,
                 governor: Optional[ResourceGovernor] = None, run: Optional[Callable] = None,
                 mql5_setup: Optional[Callable] = None, budget=None, batch_id=None):
        self.db, self.config, self.jobs = db, config, list(jobs)
        self.on_event = on_event or (lambda event: None)
        self.governor = governor or ResourceGovernor(**limits_from_config(config))
        self._run = run
        self.budget = budget or budget_mod.Budget(db, config, batch_id=batch_id)
        self.batch_id = self.budget.batch_id
        self._entries = {}
        self.error = ""
        self._mql5_setup = mql5_setup
        self._cancel = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.results: List[dict] = []

    def start(self) -> "BatchRunner":
        if self.running:
            raise ValueError("Este executor já está em andamento; aguarde antes de iniciar outro lote.")
        self._thread = threading.Thread(target=self._coordinate, name="mt5x-backtests", daemon=True)
        self._thread.start()
        return self

    def cancel(self) -> None:
        """Não começa mais nenhum teste; os que já estão rodando terminam."""
        self._cancel.set()
        if self._run is None:
            self.budget.cancel()

    def join(self, timeout: Optional[float] = None) -> bool:
        if self._thread is not None:
            self._thread.join(timeout)
        return self._thread is None or not self._thread.is_alive()

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def _emit(self, **event) -> None:
        try:
            self.on_event(event)
        except Exception:
            log.exception("Erro ao avisar o andamento dos backtests")

    def _execute(self, job: Job, control) -> dict:
        try:
            entry = self._entries[id(job)]
            if entry.get("preparation_error"):
                raise ValueError(entry["preparation_error"])
            record = self._run(self.db, self.config, job) if self._run is not None else run_job(
                self.db, self.config, job, control=control, entry=entry)
            return {"job": job, "run": record, "error": ""}
        except budget_mod.Interrupted as exc:
            control.result("interrupted", str(exc))
            return {"job": job, "run": None, "error": str(exc), "interrupted": True}
        except Exception as exc:
            control.result("failed", str(exc))
            log.warning(f"Backtest de {job.robot.name} em {job.symbol} ({job.timeframe}) falhou: {exc}")
            return {"job": job, "run": None, "error": str(exc) or type(exc).__name__}

    def _next(self, pending: List[Job], mql5_busy: bool) -> Optional[Job]:
        for job in pending:
            if job.robot.kind != catalog.MQL5 or not mql5_busy:
                pending.remove(job)
                return job
        return None

    def _coordinate(self) -> None:
        total, pending, running = len(self.jobs), list(self.jobs), {}
        started = failed = interrupted = 0
        cleanup = None
        governor = self.governor
        try:
            plan = self.budget.prepare([(job.robot, job.symbol, job.timeframe) for job in self.jobs])
            self._entries = {id(job): entry for job, entry in zip(self.jobs, plan)}
            recovered = []
            for job in pending:
                record = self.budget.recovered(self._entries[id(job)]["key"])
                if record is not None:
                    result = dict(job=job, run=record, error="")
                    self.results.append(result)
                    recovered.append(job)
                    cpu, ram = governor.read()
                    self._emit(kind=DONE, completed=len(self.results), total=total, cpu=cpu, ram=ram, **result)
            pending = [job for job in pending if job not in recovered]
            with ThreadPoolExecutor(max_workers=governor.max_workers, thread_name_prefix="mt5x-backtest") as pool:
                while (pending and not self._cancel.is_set()) or running:
                    cpu, ram = governor.read()
                    while pending and not self._cancel.is_set() and len(running) < max(1, governor.limit):
                        job = self._next(pending, any(j.robot.kind == catalog.MQL5 for j in running.values()))
                        if job is None:
                            break
                        try:
                            control = self.budget.admit(self._entries[id(job)]["key"])
                        except budget_mod.Interrupted as exc:
                            self.error = str(exc)
                            self._cancel.set()
                            break
                        if job.robot.kind == catalog.MQL5 and self._mql5_setup is not None:
                            setup, self._mql5_setup = self._mql5_setup, None
                            try:
                                cleanup = setup()
                            except Exception as e:      # sem o preparo, os robôs MQL5 falham cada um com o seu motivo
                                log.warning(f"Preparo para os robôs MQL5 falhou: {e}")
                        started += 1
                        self._emit(kind=STARTED, index=started, total=total, job=job, cpu=cpu, ram=ram,
                                   workers=len(running) + 1)
                        running[pool.submit(self._execute, job, control)] = job
                    if not running:
                        continue
                    finished, _ = wait(list(running), timeout=SAMPLE_INTERVAL, return_when=FIRST_COMPLETED)
                    for future in finished:
                        running.pop(future)
                        result = future.result()
                        self.results.append(result)
                        interrupted += bool(result.get("interrupted"))
                        failed += result["run"] is None and not result.get("interrupted")
                        if result.get("interrupted"):
                            self._cancel.set()
                            self.error = result["error"]
                        cpu, ram = governor.read()
                        self._emit(kind=DONE, completed=len(self.results), total=total, cpu=cpu, ram=ram, **result)
        except budget_mod.Interrupted as exc:
            self.error = str(exc)
            self._cancel.set()
        except Exception as exc:
            self.error = str(exc)
            self.budget.cancel("O lote falhou antes de terminar; consulte o checkpoint e retome.")
            log.exception("Não foi possível concluir o lote de backtests")
        finally:
            if self._cancel.is_set() and not self.budget.reason:
                self.budget.cancel()
            try:
                self.budget.finish(failed=bool(failed))
            except Exception:
                self.error = "Não foi possível gravar o checkpoint; confira o disco e reinicie o aplicativo antes de retomar."
                log.exception("Falha ao gravar o estado final do lote")
            if cleanup is not None:
                try:
                    cleanup()
                except Exception as e:
                    log.warning(f"Não foi possível desfazer o preparo dos robôs MQL5: {e}")
            done = len(self.results)
            self._emit(kind=FINISHED, total=total, ok=done - failed - interrupted, failed=failed, canceled=total - done + interrupted)
