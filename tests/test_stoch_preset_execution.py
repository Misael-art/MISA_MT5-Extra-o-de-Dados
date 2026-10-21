"""Pacote/serviço S10 com Session falsa: não constitui execução no terminal."""
import configparser
from datetime import datetime,timezone
import uuid
import pytest
from mt5_extracao.presets import defaults,manual,execution
from mt5_extracao.presets.compiler import TestContext as Context
from mt5_extracao.robots import catalog,mt5_tools,store
from mt5_extracao.trade_analytics import ledger
from tests.test_stoch_native_profile import robot,db
from tests.test_stoch_events import trace,PARAMS
from tests.test_preset_execution import FakeSession

class S10Session(FakeSession):
    fault=""
    def run(self):
        self.ran=True;self.directory.mkdir(parents=True)
        ctx=self.native_options
        filename=mt5_tools.write_tester_set(self.directory,"s10",ctx["params"])
        ini=mt5_tools.tester_ini("S10.ex5",self.symbol,self.timeframe,datetime(2026,10,20),datetime(2026,10,22),
            self.config.getfloat("STRATEGY","initial_equity"),"s10",ctx["model"],filename,visual=self.mode=="replay",port=9000)
        ini_path=self.directory/"s10.ini";ini_path.write_bytes(ini.encode("utf-16"))
        self.native_options["run"]('terminal64.exe /config:"C:\\private\\'+ini_path.name+'"',self.directory,None,30)
        params={**PARAMS,"target_points":0}
        path,contexts,_=trace(self.directory,params=params)
        row=contexts[0];epoch=row["available_at"]
        mode=f"MT5X_NATIVE_MODE session={self.id} tester=1 visual={int(self.mode=='replay')} demo=1\n"
        signal=f"HDK_STRATEGY_SIGNAL event_at={row['event_at']} confirmed_at={row['confirmed_at']} available_at={epoch} direction=1 opened=1 execution_state=4 volume=1 order=2 deal=2 reason=\n"
        text=path.read_text()
        if self.fault=="changed_context":text=text.replace("open=100.0","open=100.01",1)
        if self.fault=="missing_context":text=""
        if self.fault=="uncertain":signal=signal.replace("opened=1 execution_state=4","opened=0 execution_state=6")
        path.write_text(mode+text+"\n"+signal)
        stamp=lambda at:datetime.fromtimestamp(at,timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        metrics=dict(trades=1,net_profit=-6.75,native_cash_result=-6.75,entries=1,entries_without_stop=0,cash_reconciled=True,
            source_fingerprint=catalog.fingerprint(self.robot),native_profile_isolated=True,native_detail_capture="observer",
            artifact_directory=str(self.directory),tester_model="cada tick",currency="USD",data_quality={})
        self.record=dict(run_id=uuid.uuid4().hex,robot_key=self.robot.key,source="strategy tester",symbol=self.symbol,timeframe=self.timeframe,
            data_start="2026-10-20",data_end="2026-10-22",bars=0,params=ctx["params"],metrics=metrics,equity=[],
            trades=[[stamp(epoch+300),-6.75,1.]],created_at="2026-10-22")
        store.save_run(self.db,self.record)
        rows=[dict(direction=1,entry_time=stamp(epoch),exit_time=stamp(epoch+300),entry_price=106.25,exit_price=99.5,
            stop_price=99.5,target_price=110. if self.fault=="unexpected_tp" else 0.,volume=1.,pnl=-6.75,exit_reason="sl")]
        ledger.save_trades(self.db,self.record["run_id"],rows,robot_key=self.robot.key,symbol=self.symbol,timeframe=self.timeframe,source="mql5_tester")
        self.native_input_receipt=dict(schema_version=1,session=self.id,symbol=self.symbol,timeframe=self.timeframe,tester=True,demo=True,
            known_at=epoch,values=self.expected,complete=True,compared=True,preset_verified=True,operational_acceptance=False,sha256="f"*64)
        store.save_native_input_receipt(self.db,self.record["run_id"],self.native_input_receipt,artifact_directory=self.directory,robot_key=self.robot.key)
        self.state="completed";return self

@pytest.fixture
def prepared(robot,db,tmp_path,monkeypatch):
    default=defaults.ensure_default(db,robot,symbol="EURUSD",timeframe="M5")
    own=manual.save_as_manual(db,robot,default.identity.id,"S10 Teste")
    data=own.composition.to_dict();data["strategy"]["params"].update(PARAMS,target_points=0)
    own=manual.save_revision(db,robot,own.identity.id,data,expected_revision=own.identity.revision)
    exported=manual.export_manual(db,robot,own.identity.id,tmp_path/"exports",test_context=Context("2026-10-20","2026-10-22",deposit=100000))
    monkeypatch.setattr(execution.sessions,"Session",S10Session);monkeypatch.setattr(S10Session,"fault","")
    config=configparser.ConfigParser();config.read_string("[STRATEGY]\ninitial_equity=100000\n")
    return lambda mode="backtest":execution.PresetExecution(db,config,robot,exported["manifest_path"],mode=mode,root=tmp_path/"sessions")


def test_s10_service_reconciles_no_tp_replay_and_retains_negative_result(prepared):
    first=prepared().run();assert first.status=="reconciled_backtest",first.error
    replay=prepared("replay").run();assert replay.status=="reconciled_replay",replay.error
    assert replay.evidence["functional_cycle"] and replay.evidence["net_profit"]==-6.75
    assert replay.evidence["strategy_reference"]["contexts"]==3
    assert not replay.evidence["allows_live"] and not replay.evidence["cache_eligible"]
    assert replay.evidence["ledger"][0]["target_price"]==0


@pytest.mark.parametrize("fault",["missing_context","uncertain","unexpected_tp"])
def test_s10_service_keeps_incomplete_or_divergent_evidence_unaccepted(prepared,monkeypatch,fault):
    monkeypatch.setattr(S10Session,"fault",fault)
    run=prepared().run()
    assert run.status=="verification_failed" and run.evidence is None,run.error


def test_replay_detects_changed_native_context_even_if_ledger_and_signals_match(prepared,monkeypatch):
    first=prepared().run();assert first.status=="reconciled_backtest",first.error
    monkeypatch.setattr(S10Session,"fault","changed_context")
    replay=prepared("replay").run();assert replay.status=="native_divergence",replay.error
    assert replay.evidence["first_difference"]["section"]=="strategy_reference" and not replay.evidence["functional_cycle"]
