"""Pacote/serviço VWAP com Session falsa: não constitui execução no terminal."""
import configparser
from datetime import datetime,timezone
import uuid
import pytest
from mt5_extracao.presets import defaults,manual,execution
from mt5_extracao.presets.compiler import TestContext as Context
from mt5_extracao.robots import catalog,mt5_tools,store
from mt5_extracao.trade_analytics import ledger
from tests.test_vwap_native_profile import robot,db
from tests.test_vwap_events import trace,PARAMS
from tests.test_preset_execution import FakeSession

class VWAPSession(FakeSession):
    fault=""
    def run(self):
        self.ran=True;self.directory.mkdir(parents=True)
        ctx=self.native_options
        filename=mt5_tools.write_tester_set(self.directory,"vwap",ctx["params"])
        ini=mt5_tools.tester_ini("VWAP.ex5",self.symbol,self.timeframe,datetime(2026,10,20),datetime(2026,10,22),
            self.config.getfloat("STRATEGY","initial_equity"),"vwap",ctx["model"],filename,visual=self.mode=="replay",port=9000)
        ini_path=self.directory/"vwap.ini";ini_path.write_bytes(ini.encode("utf-16"))
        self.native_options["run"]('terminal64.exe /config:"C:\\private\\'+ini_path.name+'"',self.directory,None,30)
        params=PARAMS
        path,contexts,_=trace(self.directory,params=params)
        row=contexts[0];epoch=row["available_at"]
        mode=f"MT5X_NATIVE_MODE session={self.id} tester=1 visual={int(self.mode=='replay')} demo=1\n"
        signal=f"HDK_STRATEGY_SIGNAL event_at={row['event_at']} confirmed_at={row['confirmed_at']} available_at={epoch} direction=1 opened=1 execution_state=4 volume=1 order=2 deal=2 reason=\n"
        if self.fault.startswith("truncated_state"):
            state=int(self.fault[-1])
            signal+=f"HDK_STRATEGY_SIGNAL event_at={epoch} confirmed_at={epoch+300} available_at={epoch+300} direction=-1 opened=0 execution_state={state} volume=0 order=0 deal=0\n"
        text=path.read_text()
        if self.fault=="changed_context":text=text.replace("open=100.0","open=100.01",1)
        if self.fault=="missing_context":text=""
        if self.fault=="uncertain":signal=signal.replace("opened=1 execution_state=4","opened=0 execution_state=6")
        path.write_text(mode+text+"\n"+signal)
        stamp=lambda at:datetime.fromtimestamp(at,timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        metrics=dict(trades=1,net_profit=-2.25,native_cash_result=-2.25,entries=1,entries_without_stop=0,cash_reconciled=True,
            source_fingerprint=catalog.fingerprint(self.robot),native_profile_isolated=True,native_detail_capture="observer",
            artifact_directory=str(self.directory),tester_model="cada tick",currency="USD",data_quality={})
        self.record=dict(run_id=uuid.uuid4().hex,robot_key=self.robot.key,source="strategy tester",symbol=self.symbol,timeframe=self.timeframe,
            data_start="2026-10-20",data_end="2026-10-22",bars=0,params=ctx["params"],metrics=metrics,equity=[],
            trades=[[stamp(epoch+300),-2.25,1.]],created_at="2026-10-22")
        store.save_run(self.db,self.record)
        rows=[dict(direction=1,entry_time=stamp(epoch),exit_time=stamp(epoch+300),entry_price=99.25,exit_price=97.,
            stop_price=97.,target_price=0. if self.fault=="missing_tp" else 100.,volume=1.,pnl=-2.25,exit_reason="sl")]
        ledger.save_trades(self.db,self.record["run_id"],rows,robot_key=self.robot.key,symbol=self.symbol,timeframe=self.timeframe,source="mql5_tester")
        self.native_input_receipt=dict(schema_version=1,session=self.id,symbol=self.symbol,timeframe=self.timeframe,tester=True,demo=True,
            known_at=epoch,values=self.expected,complete=True,compared=True,preset_verified=True,operational_acceptance=False,sha256="f"*64)
        store.save_native_input_receipt(self.db,self.record["run_id"],self.native_input_receipt,artifact_directory=self.directory,robot_key=self.robot.key)
        self.state="completed";return self

@pytest.fixture
def prepared(robot,db,tmp_path,monkeypatch):
    default=defaults.ensure_default(db,robot,symbol="EURUSD",timeframe="M5")
    own=manual.save_as_manual(db,robot,default.identity.id,"VWAP Teste")
    data=own.composition.to_dict();data["strategy"]["params"].update(PARAMS)
    own=manual.save_revision(db,robot,own.identity.id,data,expected_revision=own.identity.revision)
    exported=manual.export_manual(db,robot,own.identity.id,tmp_path/"exports",test_context=Context("2026-10-20","2026-10-22",deposit=100000))
    monkeypatch.setattr(execution.sessions,"Session",VWAPSession);monkeypatch.setattr(VWAPSession,"fault","")
    config=configparser.ConfigParser();config.read_string("[STRATEGY]\ninitial_equity=100000\n")
    return lambda mode="backtest":execution.PresetExecution(db,config,robot,exported["manifest_path"],mode=mode,root=tmp_path/"sessions")


def test_vwap_service_reconciles_central_vwap_tp_replay_and_retains_negative_result(prepared):
    first=prepared().run();assert first.status=="reconciled_backtest",first.error
    replay=prepared("replay").run();assert replay.status=="reconciled_replay",replay.error
    assert replay.evidence["functional_cycle"] and replay.evidence["net_profit"]==-2.25
    assert replay.evidence["strategy_reference"]["contexts"]==2
    assert not replay.evidence["allows_live"] and not replay.evidence["cache_eligible"]
    assert replay.evidence["ledger"][0]["target_price"]==100


@pytest.mark.parametrize("fault",["missing_context","uncertain","missing_tp","truncated_state3","truncated_state5","truncated_state6"])
def test_vwap_service_keeps_incomplete_or_divergent_evidence_unaccepted(prepared,monkeypatch,fault):
    monkeypatch.setattr(VWAPSession,"fault",fault)
    run=prepared().run()
    assert run.status=="verification_failed" and run.evidence is None,run.error


def test_replay_detects_changed_native_context_even_if_ledger_and_signals_match(prepared,monkeypatch):
    first=prepared().run();assert first.status=="reconciled_backtest",first.error
    monkeypatch.setattr(VWAPSession,"fault","changed_context")
    replay=prepared("replay").run();assert replay.status=="native_divergence",replay.error
    assert replay.evidence["first_difference"]["section"]=="strategy_reference" and not replay.evidence["functional_cycle"]
