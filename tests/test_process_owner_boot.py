"""Prova de boot anterior na mesma máquina, com identidades controladas."""
import hashlib
import json
import math
import os
import socket
from types import SimpleNamespace

import psutil
import pytest

from mt5_extracao import process_owner
from tests.test_batch_budget import local, sample  # fixture e amostra existentes
from tests.test_indicator_materialization import db


CURRENT_BOOT = "11111111-1111-4111-8111-111111111111"
PREVIOUS_BOOT = "22222222-2222-4222-8222-222222222222"
NAMESPACE = "4026531836"


def scope(machine, boot, namespace):
    return hashlib.sha256("\0".join([machine, boot, namespace]).encode()).hexdigest()


@pytest.fixture
def local_boot(monkeypatch):
    parts = ["qa-machine", CURRENT_BOOT, NAMESPACE]
    monkeypatch.setattr(process_owner, "os", SimpleNamespace(name="posix", getpid=os.getpid))
    monkeypatch.setattr(process_owner, "_scope_parts", lambda: parts, raising=False)
    monkeypatch.setattr(process_owner, "_scope", lambda: scope(*parts))
    monkeypatch.setattr(psutil, "boot_time", lambda: 500.0)
    return parts


def previous_owner():
    return {"host": socket.gethostname(), "pid": 999999999, "started": 100.0,
            "host_scope": scope("qa-machine", PREVIOUS_BOOT, NAMESPACE),
            "boot_id": PREVIOUS_BOOT, "pid_namespace": NAMESPACE}


def test_verified_previous_boot_proves_owner_ended(local_boot, monkeypatch):
    def forbidden(*args):
        raise AssertionError("PID de outro boot não deve ser consultado no namespace atual")

    monkeypatch.setattr(psutil, "Process", forbidden)
    assert not process_owner.is_alive(previous_owner())


def test_current_identity_contains_consistent_boot_evidence(local_boot):
    owner = process_owner.current()
    assert owner["boot_id"] == CURRENT_BOOT and owner["pid_namespace"] == NAMESPACE
    assert owner["host_scope"] == scope(*local_boot)


@pytest.mark.parametrize("change", [
    {"host": "other-host"}, {"pid": True}, {"started": math.nan}, {"started": 600.0},
    {"boot_id": None}, {"boot_id": "unknown"}, {"pid_namespace": None}, {"pid_namespace": True},
    {"pid_namespace": "-1"}, {"host_scope": None},
    {"pid_namespace": "9" * 5000}, {"pid_namespace": str(2**64)},
    {"host_scope": scope("other-machine", PREVIOUS_BOOT, NAMESPACE)},
    {"boot_id": CURRENT_BOOT, "pid_namespace": "999", "host_scope": scope("qa-machine", CURRENT_BOOT, "999")},
    {"boot_id": "33333333-3333-4333-8333-333333333333"},
])
def test_unproven_identity_never_means_dead(local_boot, change):
    assert process_owner.is_alive({**previous_owner(), **change})


def test_legacy_scope_without_boot_evidence_remains_unknown(local_boot):
    owner = previous_owner()
    owner.pop("boot_id")
    owner.pop("pid_namespace")
    assert process_owner.is_alive(owner)


def test_hostname_and_pid_without_machine_scope_do_not_prove_death(local_boot):
    owner = previous_owner()
    owner.pop("host_scope")
    assert process_owner.is_alive(owner)


def test_resource_lease_preserves_identity_without_machine_scope(local_boot, tmp_path):
    from mt5_extracao.resources import actions
    folder = tmp_path / "archive"
    folder.mkdir()
    owner = previous_owner()
    owner.pop("host_scope")
    lock = folder / ".lock"
    lock.write_text(json.dumps({"resource_lock_version": 2, "identity": owner,
                                "state": "active", "action": "unproven legacy action"}))
    before = lock.read_bytes()
    with pytest.raises(actions.Busy):
        with actions.exclusive(folder, "controlled request"):
            pytest.fail("identidade incompleta não autoriza a ação")
    assert lock.read_bytes() == before


@pytest.mark.parametrize("boot_time", [math.nan, math.inf, -1.0])
def test_invalid_current_boot_time_preserves_owner(local_boot, monkeypatch, boot_time):
    monkeypatch.setattr(psutil, "boot_time", lambda: boot_time)
    assert process_owner.is_alive(previous_owner())


def test_permission_denied_reading_boot_time_preserves_owner(local_boot, monkeypatch):
    def denied():
        raise psutil.AccessDenied()

    monkeypatch.setattr(psutil, "boot_time", denied)
    assert process_owner.is_alive(previous_owner())


@pytest.mark.parametrize("separate_reader_denied", [False, True])
def test_existing_resource_lease_reconciles_verified_previous_boot(
        local_boot, tmp_path, monkeypatch, separate_reader_denied):
    from mt5_extracao.resources import actions
    folder = tmp_path / "archive"
    folder.mkdir()
    lock = folder / ".lock"
    lock.write_text(json.dumps({"resource_lock_version": 2, "identity": previous_owner(),
                                "state": "active", "action": "controlled interrupted action"}))
    held = []
    original_lock = actions._lock_resource_file
    original_read = type(lock).read_text

    def observed_lock(handle, *, release=False):
        original_lock(handle, release=release)
        if release:
            held.remove(handle)
        else:
            held.append(handle)

    def read_text(path, *args, **kwargs):
        if separate_reader_denied and path == lock and held:
            raise PermissionError("Leitor separado impedido pelo bloqueio obrigatório")
        return original_read(path, *args, **kwargs)

    monkeypatch.setattr(actions, "_lock_resource_file", observed_lock)
    monkeypatch.setattr(type(lock), "read_text", read_text)
    with actions.exclusive(folder, "controlled recovery"):
        if separate_reader_denied:
            with pytest.raises(PermissionError):
                lock.read_text()
        # Windows impede um segundo handle de ler o byte bloqueado pelo lease.
        stream = held[-1]
        stream.seek(0)
        assert json.loads(stream.read())["state"] == "active"
    assert json.loads(lock.read_text())["state"] == "released"


def test_batch_queue_recovers_verified_previous_boot(local_boot, local):
    from sqlalchemy import text
    from mt5_extracao.robots import budget
    db, robot = local
    first = budget.Budget(db, sample=sample)
    first.prepare([(robot, "QA", "H1")])
    first.finish()
    with db.engine.begin() as conn:
        conn.execute(text("UPDATE _robot_batch_queue SET owner_json=:owner WHERE batch_id=:batch"),
                     {"owner": json.dumps(previous_owner()), "batch": first.batch_id})
    resumed = budget.Budget(db, batch_id=first.batch_id, sample=sample)
    try:
        resumed.prepare([(robot, "QA", "H1")])
        assert resumed._claimed
    finally:
        resumed.finish()


def test_batch_queue_preserves_owner_without_machine_proof(local_boot, local):
    from sqlalchemy import text
    from mt5_extracao.robots import budget
    db, robot = local
    first = budget.Budget(db, sample=sample)
    first.prepare([(robot, "QA", "H1")])
    first.finish()
    legacy = previous_owner()
    legacy.pop("host_scope")
    with db.engine.begin() as conn:
        conn.execute(text("UPDATE _robot_batch_queue SET owner_json=:owner WHERE batch_id=:batch"),
                     {"owner": json.dumps(legacy), "batch": first.batch_id})
    with db.engine.connect() as conn:
        before = dict(conn.execute(text("SELECT * FROM _robot_batch_queue WHERE batch_id=:batch"),
                                   {"batch": first.batch_id}).mappings().one())
    other = budget.Budget(db, batch_id=first.batch_id, sample=sample)
    with pytest.raises(ValueError, match="encerramento"):
        other.prepare([(robot, "QA", "H1")])
    with db.engine.connect() as conn:
        after = dict(conn.execute(text("SELECT * FROM _robot_batch_queue WHERE batch_id=:batch"),
                                  {"batch": first.batch_id}).mappings().one())
    assert after == before


@pytest.mark.parametrize("raw_owner", ["{}", "null", "false", "[]", '"unknown"'])
def test_running_batch_without_valid_owner_never_authorizes_claim(local, raw_owner):
    from sqlalchemy import text
    from mt5_extracao.robots import budget
    db, robot = local
    first = budget.Budget(db, sample=sample)
    first.begin({"version": 1, "signature": "controlled-empty-plan", "jobs": []})
    first.finish()
    with db.engine.begin() as conn:
        conn.execute(text("UPDATE _robot_batch_queue SET owner_json=:owner,status='running' WHERE batch_id=:batch"),
                     {"owner": raw_owner, "batch": first.batch_id})
    other = budget.Budget(db, batch_id=first.batch_id, sample=sample)
    with pytest.raises(ValueError):
        other.begin(first._plan)
    with db.engine.connect() as conn:
        row = conn.execute(text("SELECT owner_json,status FROM _robot_batch_queue WHERE batch_id=:batch"),
                           {"batch": first.batch_id}).one()
    assert tuple(row) == (raw_owner, "running")


def test_unknown_queue_state_never_authorizes_claim(local):
    from sqlalchemy import text
    from mt5_extracao.robots import budget
    db, robot = local
    first = budget.Budget(db, sample=sample)
    first.begin({"version": 1, "signature": "controlled-empty-plan", "jobs": []})
    first.finish()
    with db.engine.begin() as conn:
        conn.execute(text("UPDATE _robot_batch_queue SET status='unknown-schema' WHERE batch_id=:batch"),
                     {"batch": first.batch_id})
    other = budget.Budget(db, batch_id=first.batch_id, sample=sample)
    with pytest.raises(ValueError):
        other.begin(first._plan)


def test_producer_recovery_uses_verified_previous_boot(local_boot, db):
    from mt5_extracao import sdk
    from tests.test_producer_orphan_recovery import begin, replace_owner, row
    run = begin(db)
    replace_owner(db, run, previous_owner())
    result = sdk.recover_producer_jobs(db)
    assert result["recovered"] == [run["run_id"]]
    assert row(db, run)["status"] == "INTERRUPTED"
    assert sdk.recover_producer_jobs(db)["recovered"] == []


def test_unavailable_current_owner_never_claims_a_batch(local, monkeypatch):
    from sqlalchemy import text
    from mt5_extracao.robots import budget
    database, _ = local
    budget.ensure_schema(database)
    monkeypatch.setattr(budget, "_owner", lambda: None)
    control = budget.Budget(database, sample=sample)
    with pytest.raises(ValueError, match="identidade"):
        control.begin({"version": 1, "signature": "controlled-empty-plan", "jobs": []})
    with database.engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM _robot_batch_queue")).scalar_one() == 0


@pytest.mark.parametrize("raw_history", ["{", "null", "{}", "[]", '["unknown"]'])
def test_invalid_job_history_never_changes_queue_owner(local, raw_history):
    from sqlalchemy import text
    from mt5_extracao.robots import budget
    database, _ = local
    first = budget.Budget(database, sample=sample)
    first.begin({"version": 1, "signature": "controlled-empty-plan", "jobs": []})
    first.admit("controlled-job")
    first.finish()
    with database.engine.begin() as conn:
        conn.execute(text("UPDATE _robot_batch_jobs SET history_json=:history WHERE batch_id=:batch"),
                     {"history": raw_history, "batch": first.batch_id})
    with database.engine.connect() as conn:
        before = dict(conn.execute(text("SELECT * FROM _robot_batch_queue WHERE batch_id=:batch"),
                                   {"batch": first.batch_id}).mappings().one())
    other = budget.Budget(database, batch_id=first.batch_id, sample=sample)
    with pytest.raises(ValueError):
        other.begin(first._plan)
    with database.engine.connect() as conn:
        after = dict(conn.execute(text("SELECT * FROM _robot_batch_queue WHERE batch_id=:batch"),
                                  {"batch": first.batch_id}).mappings().one())
    assert before == after
