"""Recibo/ledger/eventos de lotes pelo serviço existente; provedor falso."""
import configparser
import pytest
from mt5_extracao.presets import defaults,manual,execution
from mt5_extracao.presets.compiler import TestContext as Context
from tests.test_preset_execution import FakeSession
from tests.test_volume_sizing import robot,fixed,db
from tests.test_s09_native_profile import robot as legacy_robot

class SizedSession(FakeSession):
    sizing_fault=""
    def run(self):
        super().run()
        if self.state!="completed":return self
        signal=execution.native_signals(self.directory)[0]
        path=next((self.directory/"Tester").glob("Agent-*/logs/*.log"))
        values=dict(event_at=signal["event_at"],lot_mode=self.expected["InpLotMode"],requested_volume=self.expected["InpFixedLot"],
                    planned_volume=1,risk_money=1,risk_budget=500,margin_required=100,margin_free=1000,
                    volume_min=.01,volume_max=100,volume_step=.01)
        if self.sizing_fault=="margin":values["margin_required"]="unavailable"
        if self.sizing_fault=="risk":values["risk_money"]=501
        if self.sizing_fault=="volume":values["planned_volume"]=2
        line="S09_SIZING "+" ".join(f"{k}={v}" for k,v in values.items())+"\n"
        if self.sizing_fault=="missing":line=""
        if self.sizing_fault=="duplicate":line+=line
        path.write_bytes((path.read_bytes().decode("utf-16")+line).encode("utf-16"))
        return self

@pytest.fixture
def prepared(robot,db,tmp_path,monkeypatch):
    default=defaults.ensure_default(db,robot,symbol="EURUSD",timeframe="M5")
    own=manual.save_as_manual(db,robot,default.identity.id,"Lotes",composition=fixed(robot,1))
    export=manual.export_manual(db,robot,own.identity.id,tmp_path/"exports",test_context=Context("2026-10-14","2026-10-15",deposit=100000))
    config=configparser.ConfigParser();config.read_dict({"STRATEGY":{"initial_equity":"100000"}})
    monkeypatch.setattr(execution.sessions,"Session",SizedSession)
    monkeypatch.setattr(SizedSession,"sizing_fault","");monkeypatch.setattr(SizedSession,"fault","")
    return db,config,robot,export["manifest_path"],tmp_path/"sessions"


def run(prepared,mode="backtest"):
    db,config,robot,path,root=prepared
    return execution.PresetExecution(db,config,robot,path,root=root,mode=mode).run()


def test_lots_are_materialized_reopened_and_compared_in_replay(prepared,monkeypatch):
    backtest=run(prepared);assert backtest.status=="reconciled_backtest",backtest.error
    assert len(backtest.evidence["inputs"]["values"])==12 and len(backtest.evidence["sizing"])==1
    assert backtest.evidence["sizing"][0]["filled_volume"]==backtest.evidence["ledger"][0]["volume"]==1
    assert execution.load_evidence(prepared[0])[0]==backtest.evidence
    monkeypatch.setattr(SizedSession,"tickets",20)
    replay=run(prepared,"replay");assert replay.status=="reconciled_replay",replay.error
    assert replay.evidence["functional_cycle"] and replay.evidence["sizing"]==backtest.evidence["sizing"]
    assert not replay.evidence["cache_eligible"] and not replay.evidence["allows_live"]

@pytest.mark.parametrize("fault",["margin","risk","volume","missing","duplicate"])
def test_unqualified_sizing_cannot_publish_acceptance(prepared,monkeypatch,fault):
    monkeypatch.setattr(SizedSession,"sizing_fault",fault)
    result=run(prepared)
    assert result.status=="verification_failed" and result.evidence is None
    assert execution.load_evidence(prepared[0])==[]
