"""T7.2: bateria de backtests em segundo plano, com o número de testes simultâneos regulado por CPU e memória."""
import configparser
import threading
import time
import types

from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.robots import catalog, runner
from tests.market_data import random_walk


def _robot(name, kind=catalog.PYTHON):
    return types.SimpleNamespace(key=f"{kind}:{name}", name=name, kind=kind)


def _governor(readings, max_workers=4, **kw):
    """Monitor com leituras fixas (a última se repete) e relógio que anda a cada consulta."""
    readings, ticks = list(readings), iter(range(10 ** 6))
    return runner.ResourceGovernor(max_workers=max_workers, clock=lambda: float(next(ticks)),
                                   sample=lambda: readings.pop(0) if len(readings) > 1 else readings[0], **kw)


def test_governor_raises_and_lowers_the_number_of_workers():
    governor = _governor([(10, 40), (20, 40), (65, 40), (80, 40), (90, 40), (90, 40), (10, 90), (10, 40)], max_workers=3)
    limits = []
    for _ in range(8):
        governor.read()
        limits.append(governor.limit)
    # folga de CPU: sobe até o máximo; 65% fica dentro da folga: mantém; acima de 75% de CPU ou 85% de RAM: desce até 1
    assert limits == [2, 3, 3, 2, 1, 1, 1, 2]


def test_governor_reuses_a_reading_taken_too_recently():
    calls = []
    governor = runner.ResourceGovernor(max_workers=4, clock=lambda: 100.0, sample=lambda: calls.append(1) or (5.0, 5.0))
    assert governor.read() == governor.read() == (5.0, 5.0)
    assert len(calls) == 1 and governor.limit == 2


def test_limits_come_from_config_with_fallback():
    config = configparser.ConfigParser()
    assert runner.limits_from_config(config) == {"max_cpu": 75.0, "max_ram": 85.0, "max_workers": None}
    config.read_dict({"ROBOTS": {"batch_max_cpu_pct": "60", "batch_max_ram_pct": "x", "batch_max_workers": "2"}})
    assert runner.limits_from_config(config) == {"max_cpu": 60.0, "max_ram": 85.0, "max_workers": 2}
    assert runner.limits_from_config(None)["max_cpu"] == 75.0


def test_batch_runs_every_job_and_a_failure_does_not_stop_the_others():
    jobs = [runner.Job(_robot(name), symbol, "1 dia") for symbol in ("PETR4", "VALE3") for name in ("a", "b", "c")]
    events = []

    def run(db, config, job):
        if (job.symbol, job.robot.name) == ("VALE3", "b"):
            raise ValueError("sem barras")
        return {"symbol": job.symbol, "robot_key": job.robot.key}
    batch = runner.BatchRunner(None, None, jobs, on_event=events.append, governor=_governor([(10, 40)]), run=run).start()
    assert batch.join(10) and not batch.running
    starts = [e for e in events if e["kind"] == runner.STARTED]
    done = [e for e in events if e["kind"] == runner.DONE]
    assert [e["index"] for e in starts] == [1, 2, 3, 4, 5, 6] and all(e["total"] == 6 for e in starts)
    assert sorted(e["completed"] for e in done) == [1, 2, 3, 4, 5, 6] and starts[0]["cpu"] == 10
    failed = [e for e in done if e["run"] is None]
    assert len(failed) == 1 and failed[0]["error"] == "sem barras" and failed[0]["job"].symbol == "VALE3"
    assert events[-1] == {"kind": runner.FINISHED, "total": 6, "ok": 5, "failed": 1, "canceled": 0}


def test_busy_machine_keeps_one_test_at_a_time_and_idle_machine_runs_in_parallel():
    def peak(readings):
        active, seen, lock = [0], [0], threading.Lock()

        def run(db, config, job):
            with lock:
                active[0] += 1
                seen[0] = max(seen[0], active[0])
            time.sleep(0.05)
            with lock:
                active[0] -= 1
            return {}
        jobs = [runner.Job(_robot(f"r{i}"), "PETR4", "1 dia") for i in range(8)]
        assert runner.BatchRunner(None, None, jobs, governor=_governor(readings), run=run).start().join(20)
        return seen[0]
    assert peak([(95, 40)]) == 1
    assert peak([(5, 95)]) == 1
    assert peak([(5, 40)]) > 1


def test_mql5_robots_never_run_two_at_a_time_and_setup_runs_once():
    active, overlap, calls = [0], [], []

    def run(db, config, job):
        if job.robot.kind == catalog.MQL5:
            active[0] += 1
            overlap.append(active[0])
            time.sleep(0.03)
            active[0] -= 1
        return {}

    def setup():
        calls.append("setup")
        return lambda: calls.append("cleanup")
    jobs = [runner.Job(_robot(f"m{i}", catalog.MQL5), "EURUSD", "1 hora") for i in range(4)] + \
           [runner.Job(_robot("p"), "EURUSD", "1 hora")]
    batch = runner.BatchRunner(None, None, jobs, governor=_governor([(5, 40)]), run=run, mql5_setup=setup).start()
    assert batch.join(20)
    assert overlap == [1, 1, 1, 1] and calls == ["setup", "cleanup"] and len(batch.results) == 5


def test_cancel_stops_before_the_jobs_that_did_not_start():
    events, release = [], threading.Event()

    def run(db, config, job):
        release.wait(5)
        return {}
    jobs = [runner.Job(_robot(f"r{i}"), "PETR4", "1 dia") for i in range(5)]
    batch = runner.BatchRunner(None, None, jobs, on_event=events.append, governor=_governor([(95, 40)]), run=run).start()
    deadline = time.time() + 5
    while not events and time.time() < deadline:
        time.sleep(0.01)
    batch.cancel()
    release.set()
    assert batch.join(10)
    assert events[-1] == {"kind": runner.FINISHED, "total": 5, "ok": 1, "failed": 0, "canceled": 4}


def test_real_python_backtests_are_recorded(tmp_path):
    """Sem dublê: estratégias do projeto, banco SQLite de verdade e o monitor do psutil."""
    from mt5_extracao.robots import store
    db = DatabaseManager(db_path=str(tmp_path / "runner.db"))
    for symbol, seed in (("PETR4", 1), ("VALE3", 2)):
        assert db.save_ohlcv_data(symbol, "1 dia", random_walk(700, seed=seed))
    robots = [r for r in catalog.python_robots() if r.key in ("python:donchian", "python:ema_atr")]
    assert len(robots) == 2
    jobs = [runner.Job(robot, symbol, "1 dia") for symbol in ("PETR4", "VALE3") for robot in robots]
    events = []
    batch = runner.BatchRunner(db, None, jobs, on_event=events.append).start()
    assert batch.join(120)
    assert events[-1]["ok"] == len(jobs), [e.get("error") for e in events if e.get("error")]
    recorded = {(r["robot_key"], r["symbol"], r["timeframe"]) for r in store.load_runs(db)}
    assert recorded == {(robot.key, symbol, "D1") for symbol in ("PETR4", "VALE3") for robot in robots}
    assert all(0 <= e["cpu"] <= 100 and 0 < e["ram"] <= 100 for e in events if e["kind"] == runner.DONE)
