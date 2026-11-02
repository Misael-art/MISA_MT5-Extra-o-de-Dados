"""T19.29: contratos determinísticos sem MT5 real; não são prova nativa operacional."""
import calendar
import configparser
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
import uuid

import pytest
from sqlalchemy import text

from mt5_extracao.presets import defaults,manual,execution
from mt5_extracao.presets.compiler import TestContext as NativeTestContext
from mt5_extracao.robots import budget,catalog,mt5_tools,store as robot_store
from mt5_extracao.trade_analytics import ledger
from tests.test_s09_native_profile import robot
from tests.test_default_presets import db


class FakeSession:
    fault=""
    tickets=2
    def __init__(self,db,config,actor,symbol,timeframe,**kwargs):
        self.db,self.config,self.robot,self.symbol,self.timeframe=db,config,actor,symbol,timeframe
        self.id=uuid.uuid4().hex;self.directory=kwargs["root"]/self.id
        self.native_options=kwargs["native_options"];self.mode=kwargs["mode"]
        self.expected=kwargs["expected_native_inputs"];self.preset=kwargs["preset"]
        self.state="created";self.error="";self.record=None;self.native_input_receipt=None
        self.cancelled=False;self.ran=False;self.commands=[]
    def _run_command(self,*args,**kwargs):self.commands.append((args,kwargs));return SimpleNamespace(returncode=0)
    def _check_cancel(self):
        if self.cancelled:raise execution.sessions.Cancelled("interrompido")
    def cancel(self):self.cancelled=True
    def run(self):
        self.ran=True
        if self.cancelled:self.state="cancelled";self.error="interrompido";return self
        self.directory.mkdir(parents=True)
        ctx=self.native_options
        name="mt5x_test"
        filename=mt5_tools.write_tester_set(self.directory,name,ctx["params"])
        ini=mt5_tools.tester_ini("S09.ex5",self.symbol,self.timeframe,datetime(2026,10,14),datetime(2026,10,15),
            self.config.getfloat("STRATEGY","initial_equity"),name,ctx["model"],filename,visual=self.mode=="replay",port=9000)
        path=self.directory/(name+".ini");path.write_bytes(ini.encode("utf-16"))
        self.native_options["run"]('terminal64.exe /config:"C:\\private\\'+path.name+'"',self.directory,None,30)
        epoch=calendar.timegm(datetime(2026,10,14,9,20).timetuple())
        log=self.directory/"Tester"/"Agent-127.0.0.1-9000"/"logs"/"run.log";log.parent.mkdir(parents=True)
        mode=f"MT5X_NATIVE_MODE session={self.id} tester=1 visual={int(self.mode=='replay')} demo=1\n"
        if self.fault=="mode":mode=""
        if self.fault=="visual":mode=mode.replace("visual=1","visual=0")
        signal=f"S09_SIGNAL event_at={epoch-300} confirmed_at={epoch} available_at={epoch} direction=1 opened=1 execution_state=4 volume=1 order={self.tickets} deal={self.tickets} reason=\n"
        if self.fault=="zero":signal=""
        if self.fault=="uncertain":signal=signal.replace("opened=1 execution_state=4","opened=0 execution_state=6")
        if self.fault=="future":signal=signal.replace(f"confirmed_at={epoch}",f"confirmed_at={epoch+1}")
        log.write_bytes((mode+signal).encode("utf-16"))
        count=0 if self.fault in ("zero","uncertain","unclosed") else 1
        metrics=dict(trades=count,net_profit=2.0 if count else 0.0,native_cash_result=2.0 if count else 0.0,
             entries=count,entries_without_stop=0,cash_reconciled=True,source_fingerprint=catalog.fingerprint(self.robot),
             native_profile_isolated=True,native_detail_capture="observer",artifact_directory=str(self.directory),
             tester_model="cada tick",currency="USD",data_quality={"real_tick_coverage_status":"not_reported"})
        if self.fault=="cash":metrics["cash_reconciled"]=False
        if self.fault=="stop":metrics["entries_without_stop"]=1
        self.record=dict(run_id=ctx["control"].run_id if ctx.get("control") is not None else uuid.uuid4().hex,robot_key=self.robot.key,source="strategy tester",symbol=self.symbol,
             timeframe=self.timeframe,data_start="2026-10-14 00:00:00",data_end="2026-10-15 00:00:00",bars=0,
             params=ctx["params"],metrics=metrics,equity=[],trades=[["2026-10-14 09:25:00",2.0,1.0]] if count else [],created_at="2026-10-14")
        if self.fault=="unclosed":metrics["entries"]=1
        if self.fault=="changed_price":
            metrics["net_profit"]=metrics["native_cash_result"]=3.0
            self.record["trades"][0][1]=3.0
        robot_store.save_run(self.db,self.record)
        rows=[dict(direction=1,entry_time="2026-10-14 09:20:00",exit_time="2026-10-14 09:25:00",
                   entry_price=100,exit_price=102,stop_price=99,target_price=102,volume=1,pnl=2,exit_reason="tp")] if count else []
        if self.fault=="ledger":rows=[]
        if self.fault=="changed_price":rows[0]["exit_price"]=103;rows[0]["pnl"]=3
        ledger.save_trades(self.db,self.record["run_id"],rows,robot_key=self.robot.key,symbol=self.symbol,timeframe=self.timeframe,source="mql5_tester")
        self.native_input_receipt=dict(schema_version=1,session=self.id,symbol=self.symbol,timeframe=self.timeframe,
            tester=True,demo=True,known_at=epoch,values=self.expected,complete=True,compared=True,preset_verified=True,
            operational_acceptance=False,sha256="f"*64)
        robot_store.save_native_input_receipt(self.db,self.record["run_id"],self.native_input_receipt,
                                             artifact_directory=self.directory,robot_key=self.robot.key)
        self.state="completed";return self


@pytest.fixture
def prepared(robot,db,tmp_path,monkeypatch):
    default=defaults.ensure_default(db,robot,symbol="EURUSD",timeframe="M5")
    own=manual.save_as_manual(db,robot,default.identity.id,"Piloto S09")
    exported=manual.export_manual(db,robot,own.identity.id,tmp_path/"exports",
        test_context=NativeTestContext("2026-10-14","2026-10-15",deposit=100000))
    monkeypatch.setattr(execution.sessions,"Session",FakeSession)
    monkeypatch.setattr(FakeSession,"fault","");monkeypatch.setattr(FakeSession,"tickets",2)
    config=configparser.ConfigParser(interpolation=None);config.read_string("[STRATEGY]\ninitial_equity=9000\n")
    return SimpleNamespace(db=db,robot=robot,config=config,exported=exported,root=tmp_path/"sessions")


def job(p,mode="backtest"):
    return execution.PresetExecution(p.db,p.config,p.robot,p.exported["manifest_path"],mode=mode,root=p.root)


def test_direct_preset_uses_existing_budget_and_releases_its_own_claim(prepared):
    p=prepared;run=job(p).run()
    assert run.status=="reconciled_backtest",run.error
    control=run.session.native_options.get("control")
    assert isinstance(control,budget.JobBudget)
    saved=budget.checkpoint(p.db,control.parent.batch_id)
    assert saved["status"]=="completed" and saved["owner"]=={}
    entry,=saved["plan"]["jobs"]
    assert entry["execution"]["mode"]=="preset_package" and entry["execution"]["session_mode"]=="backtest"
    assert entry["execution"]["package_hash"]==run.package.package_hash
    assert saved["jobs"][0]["run_id"]==run.evidence["run_id"]==control.run_id
    assert saved["jobs"][0]["status"]=="completed"
    assert Path(entry["execution"]["manifest_path"])==p.exported["manifest_path"].resolve()


def test_direct_preset_checks_resource_budget_before_any_native_launch(prepared,monkeypatch):
    p=prepared;p.config["ROBOTS"]={"batch_hard_ram_pct":"85"}
    monkeypatch.setattr(budget,"_sample",lambda *args,**kwargs:dict(cpu=0,ram=90,rss=1,free_bytes=10**10))
    run=job(p).run()
    assert run.status=="cancelled" and "RAM" in run.error
    assert not run.session.ran and not run.session.directory.exists()


def test_native_verification_failure_finishes_own_budget_without_approving_result(prepared,monkeypatch):
    monkeypatch.setattr(FakeSession,"fault","cash")
    run=job(prepared).run()
    assert run.status=="verification_failed" and run.evidence is None
    control=run.session.native_options.get("control")
    assert isinstance(control,budget.JobBudget)
    saved=budget.checkpoint(prepared.db,control.parent.batch_id)
    assert saved["status"]=="failed" and saved["owner"]=={}
    assert saved["jobs"][0]["status"]=="failed"


def test_replay_runner_reuses_the_admitted_budget_and_preserves_session_mode(prepared,monkeypatch):
    from mt5_extracao.robots import runner
    p=prepared;parent=budget.Budget(p.db,p.config)
    entry=dict(mode="preset_package",session_mode="replay",manifest_path=str(p.exported["manifest_path"]),
               package_hash=p.exported["package"].package_hash)
    jobs=parent.prepare([(p.robot,"EURUSD","M5")],start=datetime(2026,10,14),end=datetime(2026,10,15),execution=entry)
    control=parent.admit(jobs[0]["key"]);created=[];factory=execution.PresetExecution
    def native(*args,**kwargs):
        assert kwargs["control"] is control and kwargs["mode"]=="replay"
        value=factory(*args,root=p.root,**kwargs);created.append(value);return value
    monkeypatch.setattr(execution,"PresetExecution",native)
    try:
        record=runner.run_job(p.db,p.config,SimpleNamespace(robot=p.robot),control=control,entry=dict(execution=entry))
        assert len(created)==1 and created[0]._owned_budget is None
        assert created[0].evidence["mode"]=="replay" and record["run_id"]==control.run_id
        assert parent._claimed and budget.checkpoint(p.db,parent.batch_id)["jobs"][0]["status"]=="running"
        control.result("completed")
    finally:parent.finish()
    assert budget.checkpoint(p.db,parent.batch_id)["owner"]=={}


def test_native_run_id_must_match_the_budget_attempt(prepared,monkeypatch):
    original=FakeSession.run
    def wrong_run(self):
        original(self);self.record["run_id"]="foreign-qa-run";return self
    monkeypatch.setattr(FakeSession,"run",wrong_run)
    run=job(prepared).run()
    assert run.status=="verification_failed" and "tentativa admitida" in run.error and run.evidence is None
    control=run.session.native_options["control"]
    assert budget.checkpoint(prepared.db,control.parent.batch_id)["status"]=="failed"


def test_budget_result_write_failure_is_reported_and_does_not_hide_native_artifacts(prepared,monkeypatch):
    def failed(*args,**kwargs):raise OSError("synthetic checkpoint write failure")
    monkeypatch.setattr(budget.JobBudget,"result",failed)
    run=job(prepared).run()
    assert run.status=="verification_failed" and run.evidence is None and "Orçamento não finalizado" in run.error
    control=run.session.native_options["control"];saved=budget.checkpoint(prepared.db,control.parent.batch_id)
    assert saved["status"]=="failed" and saved["owner"]=={}
    assert (run.session.directory/"preset_evidence.json").is_file()
    assert execution.load_evidence(prepared.db)[0]["run_id"]==control.run_id


def test_exported_set_and_ini_are_exactly_loaded_then_native_contracts_reopen_without_financial_rewrite(prepared):
    p=prepared;run=job(p).run()
    assert run.session.native_options["run_uses_session_launcher"] is True
    assert run.status=="reconciled_backtest",run.error
    proof=run.evidence
    assert proof["tester_capture"]["parameters_sha256"]==p.exported["package"].manifest["parameters_sha256"]
    assert proof["tester_capture"]["matches_exported_set"] and proof["actual_mode"]==dict(tester=True,visual=False,demo=True)
    assert proof["functional_native_activity"] and not proof["functional_cycle"]
    assert not proof["allows_live"] and not proof["statistical_approval"] and not proof["cache_eligible"]
    assert execution.load_evidence(p.db)==[proof]
    assert (run.session.directory/"preset_tester.ini").is_file()
    assert p.config.getfloat("STRATEGY","initial_equity")==9000
    assert run.session.config.getfloat("STRATEGY","initial_equity")==100000
    assert all(not key.startswith("__") for key in run.session.robot.presets[run.session.preset])
    assert (run.session.directory/"preset_evidence.json").is_file()
    with pytest.raises(ValueError,match="já foi iniciada"):run.run()


def test_replay_compares_observed_ledger_and_signals_excluding_run_specific_tickets(prepared,monkeypatch):
    p=prepared;backtest=job(p).run();assert backtest.status=="reconciled_backtest"
    monkeypatch.setattr(FakeSession,"tickets",20)
    replay=job(p,"replay").run()
    assert replay.status=="reconciled_replay",replay.error
    assert replay.evidence["reference_match"] and replay.evidence["functional_cycle"]
    assert replay.evidence["actual_mode"]["visual"]
    assert replay.evidence["reference_backtest_run_id"]==backtest.evidence["run_id"]


@pytest.mark.parametrize("fault",["cash","stop","ledger","mode","future","uncertain","unclosed"])
def test_incomplete_or_uncertain_native_evidence_never_accepts(prepared,monkeypatch,fault):
    monkeypatch.setattr(FakeSession,"fault",fault)
    run=job(prepared).run()
    assert run.status=="verification_failed" and run.error
    assert run.evidence is None and execution.load_evidence(prepared.db)==[]


def test_requested_visual_is_not_proof_when_native_flag_disagrees(prepared,monkeypatch):
    monkeypatch.setattr(FakeSession,"fault","visual")
    run=job(prepared,"replay").run()
    assert run.status=="verification_failed" and "Visual nativo" in run.error


def test_zero_trades_is_contract_only_and_cancellation_does_not_accept_or_stop_others(prepared,monkeypatch):
    monkeypatch.setattr(FakeSession,"fault","zero")
    run=job(prepared).run()
    assert run.status=="contract_only" and not run.evidence["functional_native_activity"] and not run.evidence["functional_cycle"]
    canceled=job(prepared);canceled.cancel();canceled.run()
    assert canceled.status=="cancelled" and canceled.evidence is None
    assert run.evidence==execution.load_evidence(prepared.db)[0]


def test_changed_file_before_start_is_refused_without_launching(prepared):
    run=job(prepared);prepared.exported["set_path"].write_bytes(b"changed")
    run.run()
    assert run.status=="verification_failed" and not run.session.ran and run.session.state=="failed"


def test_live_mode_is_rejected_and_evidence_cannot_be_replaced(prepared):
    with pytest.raises(ValueError,match="ao vivo indisponível"):job(prepared,"play")
    run=job(prepared).run()
    execution._save_evidence(prepared.db,run.evidence)
    with pytest.raises(ValueError,match="outra evidência"):
        execution._save_evidence(prepared.db,dict(run.evidence,status="modified"))


def test_replay_difference_records_first_divergence_without_widening_tolerance(prepared,monkeypatch):
    first=job(prepared).run();assert first.status=="reconciled_backtest"
    monkeypatch.setattr(FakeSession,"fault","changed_price")
    different=job(prepared,"replay").run()
    assert different.status=="native_divergence",different.error
    assert different.evidence["cash_reconciled"] and not different.evidence["functional_cycle"]
    assert different.evidence["first_difference"]==dict(section="ledger",index=0,field="exit_price",expected=102.0,observed=103.0)
