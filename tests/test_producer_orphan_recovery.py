"""Morte comprovada, journal legado e concorrência; nunca presumir morte por idade."""
import json
import socket
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import psutil
import pytest
from sqlalchemy import text

from mt5_extracao import cli, process_owner, sdk
from mt5_extracao.analytics import feature_store, producer_jobs
from tests.test_indicator_materialization import db


def begin(db, family="qa"):
    run = producer_jobs.start_run(db, "QA", "M1", family, "v1", "hash", 0)
    run["rss_sampler"].stop()
    return run


def row(db, run):
    with db.engine.connect() as conn:
        return dict(conn.execute(text("SELECT * FROM _analytics_producer_runs WHERE run_id=:id"),
                                 dict(id=run["run_id"])).mappings().one())


def replace_owner(db, run, owner):
    with db.engine.begin() as conn:
        conn.execute(text("UPDATE _analytics_producer_runs SET owner_json=:owner WHERE run_id=:id"),
                     dict(id=run["run_id"], owner=None if owner is None else json.dumps(owner)))


def dead_owner():
    assert not psutil.pid_exists(999999999)
    return {**process_owner.current(), "pid": 999999999}


def recovery_preserves_unproven_and_committed_data(db):
    dead, live, foreign, unknown, terminal = [begin(db) for _ in range(5)]
    producer_jobs.finish_run(db, terminal, source_sequence=0, status="COMPLETE")
    replace_owner(db, dead, dead_owner())
    replace_owner(db, foreign, {**dead_owner(), "host": socket.gethostname()+"-foreign-qa"})
    replace_owner(db, unknown, None)
    feature_store.register_definition(db, "qa.orphan", "Valor confirmado", "v1", {}, [])
    def checkpoint(conn, counts):
        producer_jobs.advance_checkpoint(conn, symbol="QA", timeframe="M1", family="qa", algorithm_version="v1",
            config_hash="hash", source_sequence=0, last_bar_start="2025-01-01T00:00Z", state={"committed": 1}, status="RUNNING")
    feature_store.save_values(db, [dict(symbol="QA", timeframe="M1", feature_id="qa.orphan", algorithm_version="v1",
        observation_time="2025-01-01T00:00Z", available_at="2025-01-01T00:01Z",
        observed_at="2025-01-01T00:01Z", value=4.)], transaction_hook=checkpoint)
    before = feature_store.query_history(db).to_dict("records")
    checkpoint_before = producer_jobs.get_checkpoint(db,"QA","M1","qa")
    protected = {r["run_id"]: row(db, r) for r in (live, foreign, unknown, terminal)}
    result = sdk.recover_producer_jobs(db)
    assert result["recovered"] == [dead["run_id"]] and result["inspected"] == 4 and result["preserved"] == 3
    recovered = row(db, dead)
    assert recovered["status"] == "INTERRUPTED" and recovered["recovered_at_utc"]
    assert recovered["finished_at_utc"] is None and recovered["duration_ms"] is None
    assert all(row(db, r)==protected[r["run_id"]] for r in (live, foreign, unknown, terminal))
    assert feature_store.query_history(db).to_dict("records") == before
    assert producer_jobs.get_checkpoint(db,"QA","M1","qa") == checkpoint_before
    assert sdk.recover_producer_jobs(db)["recovered"] == []


def test_recovery_preserves_active_foreign_legacy_terminal_and_confirmed_data(db):
    recovery_preserves_unproven_and_committed_data(db)


@pytest.mark.parametrize("owner", [None, [], {}, {"host": ""}, {"host": "foreign", "pid": 1, "started": 0},
    {"host": socket.gethostname(), "pid": True, "started": 0},
    {"host": socket.gethostname(), "pid": 0, "started": 0},
    {"host": socket.gethostname(), "pid": 1, "started": float("nan")},
    {"host": socket.gethostname(), "pid": "1", "started": 0}])
def test_unknown_or_foreign_identity_never_means_dead(owner):
    assert process_owner.is_alive(owner)


def test_live_owner_and_reused_pid_generation_are_distinguished():
    owner = process_owner.current()
    assert process_owner.is_alive(owner)
    assert not process_owner.is_alive({**owner, "started": owner["started"]-1})
    assert not process_owner.is_alive(dead_owner())


def test_permission_denied_never_means_dead(monkeypatch):
    owner = process_owner.current()
    def denied(*args):
        raise psutil.AccessDenied(owner["pid"])
    monkeypatch.setattr(psutil, "Process", denied)
    assert process_owner.is_alive(owner) and process_owner.current() is None


def test_other_boot_machine_or_pid_namespace_is_not_treated_as_local():
    assert process_owner.is_alive({**dead_owner(), "host_scope": "other-machine-boot-or-namespace"})
    assert process_owner.is_alive({**dead_owner(), "host_scope": None})


def test_missing_local_scope_does_not_prove_a_foreign_or_dead_owner(monkeypatch):
    owner=dead_owner()
    monkeypatch.setattr(process_owner,"_scope",lambda:None)
    assert process_owner.is_alive(owner)


def test_windows_scope_uses_stable_machine_guid_without_exposing_it(monkeypatch):
    from contextlib import nullcontext
    from types import SimpleNamespace
    fake=SimpleNamespace(HKEY_LOCAL_MACHINE=1,KEY_READ=2,KEY_WOW64_64KEY=4,
        OpenKey=lambda *a:nullcontext(1),QueryValueEx=lambda *a:("synthetic-machine-guid",1))
    monkeypatch.setitem(sys.modules,"winreg",fake)
    monkeypatch.setattr(process_owner,"os",SimpleNamespace(name="nt"))
    first=process_owner._scope()
    assert first==process_owner._scope() and len(first)==64 and "synthetic-machine-guid" not in first


def test_legacy_hostname_pid_alone_is_not_a_recoverable_journal_owner(db):
    run=begin(db)
    owner=dead_owner();owner.pop("host_scope")
    replace_owner(db,run,owner)
    assert producer_jobs.reconcile_orphan_runs(db)["recovered"]==[]
    assert row(db,run)["status"]=="RUNNING"


def test_malformed_json_and_missing_identity_are_preserved(db):
    run=begin(db)
    with db.engine.begin() as conn:
        conn.execute(text("UPDATE _analytics_producer_runs SET owner_json='{bad' WHERE run_id=:id"), dict(id=run["run_id"]))
    before=row(db,run)
    assert producer_jobs.reconcile_orphan_runs(db)["recovered"]==[]
    assert row(db,run)==before


def test_start_run_automatically_reconciles_only_its_own_scope(db):
    own, other = begin(db), begin(db,"other")
    for run in (own,other):replace_owner(db,run,dead_owner())
    new=begin(db)
    assert row(db,own)["status"]=="INTERRUPTED" and row(db,other)["status"]=="RUNNING"
    assert row(db,new)["status"]=="RUNNING"
    assert producer_jobs.reconcile_orphan_runs(db,family="other")["recovered"]==[other["run_id"]]


def simultaneous_reconcilers(db, monkeypatch):
    run=begin(db);replace_owner(db,run,dead_owner())
    barrier=Barrier(2)
    alive=process_owner.is_alive
    def together(owner):
        result=alive(owner);barrier.wait(timeout=10);return result
    monkeypatch.setattr(process_owner,"is_alive",together)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures=[pool.submit(producer_jobs.reconcile_orphan_runs,db) for _ in range(2)]
        results=[f.result(timeout=20) for f in futures]
    assert sum(len(r["recovered"]) for r in results)==1
    assert row(db,run)["status"]=="INTERRUPTED"


def test_two_reconcilers_cannot_publish_recovery_twice(db,monkeypatch):
    simultaneous_reconcilers(db,monkeypatch)


def test_recovery_passes_first_page_even_when_live_entries_are_preserved(db):
    seed=begin(db)
    owner=json.dumps(dead_owner())
    live_owner=row(db,seed)["owner_json"]
    with db.engine.begin() as conn:
        conn.execute(text("INSERT INTO _analytics_producer_runs "
            "(run_id,symbol,tf,family,algorithm_version,config_hash,status,started_at_utc,owner_json) "
            "VALUES (:id,'QA','M1','qa','v1','hash','RUNNING','2025-01-01T00:00Z',:owner)"),
            [dict(id=f"page-{n:04d}",owner=live_owner if n<500 else owner) for n in range(501)])
    result=producer_jobs.reconcile_orphan_runs(db)
    assert result["inspected"]==502 and result["preserved"]==501
    assert result["recovered"]==["page-0500"]


def test_terminal_update_between_inspection_and_cas_is_preserved(db,monkeypatch):
    run=begin(db);replace_owner(db,run,dead_owner())
    def completed(owner):
        producer_jobs.finish_run(db,run,source_sequence=0,status="COMPLETE",counts={"inserted":7})
        return False
    monkeypatch.setattr(process_owner,"is_alive",completed)
    assert producer_jobs.reconcile_orphan_runs(db)["recovered"]==[]
    assert row(db,run)["status"]=="COMPLETE" and row(db,run)["rows_inserted"]==7


def test_abrupt_child_exit_is_reconciled_and_unknown_final_metrics_are_not_zero(db):
    code = """
import os,sys
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.analytics import producer_jobs
db=DatabaseManager(db_path=sys.argv[1],broker_utc_offset=0)
run=producer_jobs.start_run(db,'QA','M1','qa','v1','hash',0)
print(run['run_id'],flush=True)
os._exit(9)
"""
    result=subprocess.run([sys.executable,"-c",code,str(db.db_path)],capture_output=True,text=True,timeout=20)
    assert result.returncode==9,result.stderr
    run_id=result.stdout.strip().splitlines()[-1]
    assert producer_jobs.reconcile_orphan_runs(db)["recovered"]==[run_id]
    latest=producer_jobs.latest_run(db,"QA","M1","qa")
    assert latest["status"]=="INTERRUPTED" and latest["metrics_state"]=="UNAVAILABLE"
    assert latest["rows_inserted"] is None and latest["rows_revised"] is None and latest["duration_ms"] is None


def test_legacy_not_null_counts_survive_additive_owner_migration(db):
    with db.engine.begin() as conn:
        conn.execute(text("""CREATE TABLE _analytics_producer_runs (
            run_id TEXT PRIMARY KEY, symbol TEXT NOT NULL, tf TEXT NOT NULL, family TEXT NOT NULL,
            algorithm_version TEXT NOT NULL, config_hash TEXT NOT NULL, source_sequence_start BIGINT,
            source_sequence_end BIGINT, status TEXT NOT NULL, started_at_utc TEXT NOT NULL, finished_at_utc TEXT,
            duration_ms BIGINT, peak_rss_bytes BIGINT, database_bytes_before BIGINT, database_bytes_after BIGINT,
            rows_inserted BIGINT NOT NULL DEFAULT 0, rows_revised BIGINT NOT NULL DEFAULT 0,
            rows_unchanged BIGINT NOT NULL DEFAULT 0, error TEXT, configuration_json TEXT)"""))
        conn.execute(text("INSERT INTO _analytics_producer_runs "
            "(run_id,symbol,tf,family,algorithm_version,config_hash,status,started_at_utc) "
            "VALUES ('legacy','QA','M1','qa','v1','hash','RUNNING','2025-01-01T00:00Z')"))
    assert producer_jobs.reconcile_orphan_runs(db)["recovered"]==[]
    old=row(db,{"run_id":"legacy"})
    assert old["status"]=="RUNNING" and old["owner_json"] is None and old["recovered_at_utc"] is None
    assert old["rows_inserted"]==0 and old["configuration_json"] is None


def test_cli_recovery_uses_canonical_timeframe_and_sdk(db,monkeypatch):
    run=begin(db);replace_owner(db,run,dead_owner())
    parser=cli.build_parser()
    args=parser.parse_args(['analytics','features','recover-jobs','--symbol','QA','--tf','1 minuto','--family','qa'])
    from types import SimpleNamespace
    captures=[]
    monkeypatch.setattr(cli,"_data",lambda **kw:captures.append(kw))
    assert cli.cmd_analytics(args,SimpleNamespace(db=db))==0
    assert captures[0]['recovery']['recovered']==[run['run_id']]
