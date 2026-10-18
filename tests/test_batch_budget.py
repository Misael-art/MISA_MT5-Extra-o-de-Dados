"""T20.24: execução real local, interrupção/publicação e checkpoint sem MT5."""
import configparser
from dataclasses import asdict
import json
import threading

import pytest
from sqlalchemy import text

from mt5_extracao.bootstrap.config_builder import DEFAULTS
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.robots import backtest, budget, catalog, runner, store
from mt5_extracao.strategies import get_strategy
from mt5_extracao.timeframes import Timeframe
from tests.market_data import trend
from tests.test_robots_batch import env  # fixture de terminal falso existente


def sample(path):
    return dict(cpu=10, ram=20, rss=1000, cpu_seconds=1, read_bytes=2, write_bytes=3, free_bytes=10**12)


@pytest.fixture
def local(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "queue.sqlite"))
    db.save_ohlcv_data("QA", "1 hora", trend(350, freq="h", spread=2, volume=500))
    robot = next(r for r in catalog.python_robots() if r.key == "python:ema_atr")
    return db, robot


def run(local, control, symbols=("QA",)):
    db, robot = local
    return backtest.run_batch(db, None, [robot], symbols, [Timeframe.H1], budget=control, batch_id=control.batch_id)


def test_defaults_and_config_validation():
    config = configparser.ConfigParser()
    assert budget.Limits.from_config(config) == budget.Limits()
    for key, value in asdict(budget.Limits()).items():
        assert float(DEFAULTS["ROBOTS"]["batch_" + key]) == value
    for value in ("nan", "inf", "-1", "texto", "1.5"):
        config.read_dict({"ROBOTS": {"batch_max_jobs": value}})
        with pytest.raises(ValueError, match="Orçamento"):
            budget.Limits.from_config(config)
    with pytest.raises(ValueError):
        budget.Limits(hard_ram_pct=101)


@pytest.mark.parametrize("limits,field,value,reason", [
    (budget.Limits(hard_cpu_pct=60), "cpu", 60, "CPU"),
    (budget.Limits(hard_ram_pct=80), "ram", 80, "RAM"),
    (budget.Limits(min_free_mb=1), "free_bytes", 1024**2-1, "Disco"),
])
def test_resource_limits_stop_before_admission(limits, field, value, reason):
    measured = sample(None); measured[field] = value
    control = budget.Budget(limits=limits, sample=lambda path: measured)
    with pytest.raises(budget.Interrupted, match=reason):
        control.admit("x")
    assert control._admitted == 0


def test_deadline_and_explicit_disk_reservation():
    clock = [0.0]
    control = budget.Budget(limits=budget.Limits(max_seconds=5), sample=sample, clock=lambda: clock[0])
    control.check(); clock[0] = 5
    with pytest.raises(budget.Interrupted, match="Tempo"):
        control.check()
    control = budget.Budget(limits=budget.Limits(min_free_mb=0), sample=lambda p: dict(sample(p), free_bytes=10))
    with pytest.raises(budget.Interrupted, match="Disco"):
        control.check(path=".", reserve_bytes=11)


def test_budget_stops_batch_and_resume_does_not_repeat_complete_run(local):
    db, robot = local
    db.save_ohlcv_data("QB", "1 hora", trend(350, freq="h"))
    first = budget.Budget(db, limits=budget.Limits(max_jobs=1), sample=sample)
    results = run(local, first, ("QA", "QB"))
    assert results[0]["run"] and results[1]["status"] == "interrupted"
    saved = budget.checkpoint(db, first.batch_id)
    assert saved["status"] == "interrupted" and len(store.load_runs(db)) == 1
    assert saved["measurements"]["scope"] == "process_and_host_not_per_job"
    again = budget.Budget(db, batch_id=first.batch_id, limits=budget.Limits(max_jobs=2), sample=sample)
    results = run(local, again, ("QA", "QB"))
    assert results[0]["skipped"] and results[1]["run"]
    assert len(store.load_runs(db)) == 2
    assert budget.checkpoint(db, first.batch_id)["status"] == "completed"


def test_resume_uses_frozen_revision_even_after_database_correction(local):
    db, robot = local
    first = budget.Budget(db, sample=sample)
    result = run(local, first)[0]["run"]
    changed = db.load_ohlcv("QA", "1 hora"); changed.loc[0, "close"] += .001
    db.save_ohlcv_data("QA", "1 hora", changed)
    again = budget.Budget(db, batch_id=first.batch_id, sample=sample)
    recovered = run(local, again)[0]
    assert recovered["skipped"] and recovered["run"]["run_id"] == result["run_id"]
    assert len(store.load_runs(db)) == 1


def test_active_owner_and_changed_plan_are_not_silently_replaced(local):
    db, robot = local
    owner = budget.Budget(db, sample=sample)
    owner.prepare([(robot, "QA", "H1")])
    try:
        other = budget.Budget(db, batch_id=owner.batch_id, sample=sample)
        with pytest.raises(ValueError, match="ativo"):
            other.prepare([(robot, "QA", "H1")])
        with pytest.raises(ValueError, match="outro plano"):
            other.prepare([(robot, "QA", "D1")])
    finally:
        owner.cancel(); owner.finish()


def test_failure_history_is_not_erased_on_retry(local, monkeypatch):
    db, robot = local
    real = backtest.run_python
    failing = [True]
    def transient(*args, **kwargs):
        if failing[0]:
            raise ValueError("falha plantada")
        return real(*args, **kwargs)
    monkeypatch.setattr(backtest, "run_python", transient)
    first = budget.Budget(db, sample=sample)
    assert run(local, first)[0]["error"] == "falha plantada"
    failing[0] = False
    again = budget.Budget(db, batch_id=first.batch_id, sample=sample)
    assert run(local, again)[0]["run"]
    history = budget.checkpoint(db, first.batch_id)["jobs"][0]["history"]
    assert [item["status"] for item in history] == ["running", "failed", "running", "completed"]


def test_cancel_during_signal_calculation_never_publishes_partial_run(local):
    db, robot = local
    control = budget.Budget(db, sample=sample)
    entry = control.prepare([(robot, "QA", "H1")])[0]
    job = control.admit(entry["key"])
    strategy = get_strategy("ema_atr")
    real = strategy.gerar_sinais
    def signals(frame):
        result = real(frame)
        control.cancel()
        return result
    strategy.gerar_sinais = signals
    try:
        with pytest.raises(budget.Interrupted, match="Cancelado"):
            backtest.run_python(db, None, robot, "QA", "H1", strategy=strategy,
                                data_manifest_id=entry["data_manifest_id"], control=job)
        assert not store.load_runs(db)
    finally:
        control.finish()


def test_publication_and_checkpoint_are_in_same_transaction(local, monkeypatch):
    db, robot = local
    control = budget.Budget(db, sample=sample)
    entry = control.prepare([(robot, "QA", "H1")])[0]
    job = control.admit(entry["key"])
    def broken(*a, **kw):
        raise ValueError("checkpoint não gravou")
    monkeypatch.setattr(job, "result", broken)
    try:
        with pytest.raises(ValueError, match="checkpoint"):
            backtest.run_python(db, None, robot, "QA", "H1", data_manifest_id=entry["data_manifest_id"], control=job)
        assert store.load_runs(db) == []
    finally:
        control.cancel(); control.finish()


def test_crash_after_publication_recovers_run_without_reexecution(local, monkeypatch):
    db, robot = local
    first = budget.Budget(db, sample=sample)
    results = run(local, first)
    # Simula processo encerrado entre commit do resultado e atualização final do lote.
    with db.engine.begin() as conn:
        conn.execute(text("UPDATE _robot_batch_queue SET status='running', owner_json=:o WHERE batch_id=:b"),
                     dict(o=json.dumps(dict(host=__import__('socket').gethostname(), pid=999999999, started=0)), b=first.batch_id))
    import sys
    code = backtest.run_python.__code__
    def observe(frame, event, arg):
        if event == "call" and frame.f_code is code:
            pytest.fail("resultado já publicado foi repetido")
    resumed = budget.Budget(db, batch_id=first.batch_id, sample=sample)
    previous = sys.getprofile()
    sys.setprofile(observe)
    try:
        again = run(local, resumed)
    finally:
        sys.setprofile(previous)
    assert again[0]["skipped"] and again[0]["run"]["run_id"] == results[0]["run"]["run_id"]


def test_native_cancel_waits_for_call_and_rejects_publication(env):
    robot = env.robots("alfa")[0]
    control = budget.Budget(env.db, env.config, sample=sample)
    def terminal(*args):
        code = env.terminal(*args)
        if "/config:" in args[0]:
            control.cancel()
        return code
    result = backtest.run_batch(env.db, env.config, [robot], ["EURUSD"], [Timeframe.H1],
                                run=terminal, is_running=lambda: False, budget=control)
    assert result[0]["status"] == "interrupted" and len(env.tested) == 1
    assert not store.load_runs(env.db)
    assert budget.checkpoint(env.db, control.batch_id)["status"] == "interrupted"


def test_async_recovery_and_control_keep_ui_thread_available(local):
    db, robot = local
    jobs = [runner.Job(robot, "QA", "H1")]
    first = runner.BatchRunner(db, None, jobs, budget=budget.Budget(db, sample=sample)).start()
    assert first.join(15) and len(first.results) == 1 and first.results[0]["run"]
    second = runner.BatchRunner(db, None, jobs, budget=budget.Budget(db, batch_id=first.batch_id, sample=sample)).start()
    assert second.join(15) and len(store.load_runs(db)) == 1


def test_resource_write_guard_is_reused(local, monkeypatch):
    db, _ = local
    from mt5_extracao.resources import guard
    monkeypatch.setattr(guard, "make", lambda *a: lambda: "base acima do limite")
    control = budget.Budget(db, sample=sample)
    with pytest.raises(budget.Interrupted, match="Recursos"):
        control.check()
    assert not store.load_runs(db)


def test_unknown_host_owner_is_not_stolen(local):
    db, robot = local
    first = budget.Budget(db, sample=sample)
    first.prepare([(robot, "QA", "H1")]); first.finish()
    with db.engine.begin() as conn:
        conn.execute(text("UPDATE _robot_batch_queue SET owner_json=:o WHERE batch_id=:b"),
                     dict(o=json.dumps(dict(host="outro-host-qa", pid=1, started=0)), b=first.batch_id))
    other = budget.Budget(db, batch_id=first.batch_id, sample=sample)
    with pytest.raises(ValueError, match="ativo"):
        other.prepare([(robot, "QA", "H1")])


def test_resume_accepts_different_queue_order_without_extra_execution(local):
    db, robot = local
    db.save_ohlcv_data("QB", "1 hora", trend(350, freq="h"))
    first = budget.Budget(db, sample=sample)
    original = run(local, first, ("QA", "QB"))
    again = budget.Budget(db, batch_id=first.batch_id, sample=sample)
    reversed_results = run(local, again, ("QB", "QA"))
    assert all(item["skipped"] for item in reversed_results)
    assert reversed_results[0]["run"]["run_id"] == original[1]["run"]["run_id"]
    assert len(store.load_runs(db)) == 2


def test_missing_data_blocks_only_its_job(local):
    db, _ = local
    control = budget.Budget(db, sample=sample)
    results = run(local, control, ("AUSENTE", "QA"))
    assert results[0]["run"] is None and "AUSENTE" in results[0]["error"]
    assert results[1]["run"]
    assert budget.checkpoint(db, control.batch_id)["status"] == "failed"


def test_missing_published_artifact_is_not_implicitly_recomputed(local):
    db, _ = local
    first = budget.Budget(db, sample=sample)
    run(local, first)
    # Corrupção simulada só no banco de teste, nunca em dados de usuário.
    with db.engine.begin() as conn:
        conn.execute(text("DELETE FROM _robot_runs"))
    other = budget.Budget(db, batch_id=first.batch_id, sample=sample)
    result = run(local, other)
    assert result[0]["run"] is None and "restaure" in result[0]["error"]
    assert not store.load_runs(db)


@pytest.mark.parametrize("windows", [False, True])
def test_budget_paths_do_not_depend_on_host_platform(local, monkeypatch, windows):
    from mt5_extracao.bootstrap import paths
    monkeypatch.setattr(paths, "is_windows", lambda: windows)
    db, robot = local
    seen = []
    def measure(path):
        seen.append(path)
        return sample(path)
    control = budget.Budget(db, sample=measure)
    assert run(local, control)[0]["run"]
    assert seen and seen[0] == __import__('pathlib').Path(db.db_path).resolve().parent


def test_cli_reports_partial_batch_and_resume_id(env, monkeypatch, capsys):
    from mt5_extracao import cli
    from mt5_extracao.robots import mt5_tools
    monkeypatch.setattr(mt5_tools, "terminal_running", lambda *a: False)
    monkeypatch.setattr(mt5_tools, "run_windows_command", env.terminal)
    env.config.set("ROBOTS", "batch_max_jobs", "1")
    with open(env.args[1], "w", encoding="utf-8") as output:
        env.config.write(output)
    argv = ["robots", "backtest", "alfa,beta", "--symbols", "EURUSD", "--tfs", "H1"]
    assert cli.main(env.args + ["--json"] + argv) == cli.EXIT_FAIL
    data = json.loads(capsys.readouterr().out)["data"]
    assert data["batch_status"] == "interrupted" and data["summary"]["tested"] == 1
    assert data["batch_id"] and len(env.tested) == 1
    env.config.set("ROBOTS", "batch_max_jobs", "2")
    with open(env.args[1], "w", encoding="utf-8") as output:
        env.config.write(output)
    assert cli.main(env.args + ["--json"] + argv + ["--batch-id", data["batch_id"]]) == cli.EXIT_OK
    resumed = json.loads(capsys.readouterr().out)["data"]
    assert resumed["batch_status"] == "completed" and resumed["summary"]["skipped"] == 1
    assert len(env.tested) == 2


def test_example_ini_budget_is_readable_by_default_parser():
    from pathlib import Path
    config = configparser.ConfigParser()
    config.read(Path(__file__).resolve().parents[1] / "config/config.ini.example", encoding="utf-8")
    assert budget.Limits.from_config(config) == budget.Limits()


def test_cancellation_does_not_wait_for_publication_io():
    control = budget.Budget(sample=sample)
    entered, release = threading.Event(), threading.Event()
    def holding():
        with control._lock:
            entered.set()
            release.wait(3)
    thread = threading.Thread(target=holding)
    thread.start()
    try:
        assert entered.wait(1)
        finished = threading.Event()
        canceler = threading.Thread(target=lambda: (control.cancel(), finished.set()))
        canceler.start()
        assert finished.wait(.3), "a UI esperou pelo bloqueio de publicação"
        canceler.join(1)
    finally:
        release.set(); thread.join(2)
    with pytest.raises(budget.Interrupted):
        control.check()


def test_prepare_shares_one_frozen_manifest_per_source_and_timeframe(local, monkeypatch):
    from mt5_extracao.analytics import datasets
    db, robot = local
    other = next(r for r in catalog.python_robots() if r.key == "python:donchian")
    db.save_ohlcv_data("QB", "1 hora", trend(12, freq="h", spread=2))
    db.save_ohlcv_data("QA", "15 minutos", trend(12, freq="15min", spread=2))
    original = datasets.capture
    calls = []

    def capture(*args, **kwargs):
        snapshot = original(*args, **kwargs)
        calls.append((args[1], args[2]))
        if len(calls) == 1:
            corrected = db.load_ohlcv("QA", "1 hora")
            corrected.loc[0, "close"] += .001
            assert db.save_ohlcv_data("QA", "1 hora", corrected)
        return snapshot

    monkeypatch.setattr(datasets, "capture", capture)
    control = budget.Budget(db, sample=sample)
    jobs = [(robot, "QA", "1 hora"), (other, "QA", "H1"),
            (robot, "QB", "H1"), (other, "QB", "1 hora"),
            (robot, "QA", "M15")]
    plan = control.prepare(jobs)
    assert calls == [("QA", "H1"), ("QB", "H1"), ("QA", "M15")]
    assert plan[0]["data_manifest_id"] == plan[1]["data_manifest_id"]
    assert plan[2]["data_manifest_id"] == plan[3]["data_manifest_id"]
    assert len({item["data_manifest_id"] for item in plan}) == 3
    first = datasets.load(db, plan[0]["data_manifest_id"])
    assert first.bars.iloc[0]["close"] != db.load_ohlcv("QA", "1 hora").iloc[0]["close"]
    fresh = budget.Budget(db, sample=sample).prepare([(robot, "QA", "H1")])
    assert len(calls) == 4 and calls[-1] == ("QA", "H1")
    assert fresh[0]["data_manifest_id"] != plan[0]["data_manifest_id"]
