from dataclasses import replace
from pathlib import Path
import pytest
from mt5_extracao.composition import Composition,native
from mt5_extracao.composition import pullback_native as pb
from mt5_extracao.robots import catalog,mql
from tests.test_ui_preset_workflow import view,named
from tests.test_volume_sizing import db,info

@pytest.fixture
def robot(tmp_path,monkeypatch):
    source=tmp_path/"pullback.mq5";source.write_text("// approved test source",encoding="utf-8")
    r=catalog.Robot(key=pb.ROBOT_KEY,kind=catalog.MQL5,name="Pullback",version="1.00",path=str(source),files=[str(source)],has_source=True,
        inputs=[mql.MqlInput(name,kind,"0") for name,kind in pb.INPUT_TYPES.items()])
    monkeypatch.setattr(pb,"APPROVED_FINGERPRINT",catalog.fingerprint(r))
    return r

def test_distinct_profile_default_and_shared_partial_mapping(robot):
    c=native.default_composition(robot,symbol="EURUSD",timeframe="H1")
    d=native.native_adapter(c,robot).describe("pullback_manual")
    assert d["profile_id"]=="hadouken.pullback" and len(d["parameters"])==23
    values=d["expected_native_inputs"]
    assert values["InpTrendPeriod"]==200 and values["InpTakeProfitR"]==2
    assert values["InpPartialStages"]==1 and values["InpPartialTrigger1"]==1
    assert values["InpPartialQuantity1"]==.5 and values["InpPartialBreakeven"] is True
    assert values["InpMagic"]!=909202610 and d["modality"]=="native_tester_demo_only"
    assert d["operational_acceptance"] is False
    assert d["capabilities"]["modules"]["protection"]==["strategy_default"]

@pytest.mark.parametrize("change",["source","legacy","partial","risk","stop","period"])
def test_unproven_capability_and_stale_source_are_refused(robot,change):
    data=native.default_composition(robot,symbol="EURUSD",timeframe="M5").to_dict()
    if change=="source":Path(robot.path).write_text("// changed",encoding="utf-8")
    if change=="legacy":robot=replace(robot,key="mql5:hdk_pullback")
    if change=="partial":data["partial_exit_plan"]["params"]["breakeven_after_confirmation"]=False
    if change=="risk":data["sizing"]["params"]["risk_fraction"]=.006
    if change=="stop":data["protection"]={"mode":"fixed_points","params":{"stop_points":100}}
    if change=="period":data["strategy"]["params"]["trend"]=1501
    with pytest.raises(ValueError):native.native_adapter(Composition(data),robot)

def test_pullback_ui_named_manual_preserves_fixed_agenda_and_live_gate(view):
    view.ui.app.mt5_connector=type("Connector",(),{"get_symbol_info":lambda self,symbol:info()})()
    view.reopen.invoke();named(view)
    assert view.profile_label=="Pullback" and view.partial_count.get()=="1"
    assert view.partial_be.get() and view.partial_mode.get()=="Fração (%)"
    assert all(w.instate(["disabled"]) for w in view.partial_widgets)
    view.fields["adx_min"].set("26.5");view.lot_mode.set("Volume fixo");view.fixed_volume.set("1")
    assert not view.preview_error,view.lot_preview.get()
    view.save.invoke();assert not view.dirty,view.status.get()
    view.reopen.invoke();assert view.fields["adx_min"].get()=="26.5"
    assert view.current.composition.to_dict()["partial_exit_plan"]==pb.PARTIAL
    assert "Tester" in view.lot_preview.get()
    view.fixed_volume.set("0.01")
    assert view.preview_error and view.backtest.instate(["disabled"])
    assert "inviável" in view.partial_preview.get()
