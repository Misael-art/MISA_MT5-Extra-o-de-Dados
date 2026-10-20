from dataclasses import replace
from pathlib import Path
import pytest
from mt5_extracao.composition import Composition,native
from mt5_extracao.robots import catalog,mql
from mt5_extracao.strategies.session_entry import SessionEntryPolicy
from tests.test_temporal_atr_native import robot as atr_robot,dc_robot,er_robot,temporal_robot,partial_robot,lots_robot,legacy_robot,db,view,named


@pytest.fixture
def robot(atr_robot,tmp_path,monkeypatch):
    source=tmp_path/"session.mq5";source.write_bytes(Path(atr_robot.path).read_bytes()+b"\n// reviewed session test profile\n")
    inputs=[mql.MqlInput("InpSessionEnabled","bool","false")]+[mql.MqlInput(name,kind,"0") for name,kind,unit in native.SESSION_INPUTS.values()]
    out=replace(atr_robot,version="1.70",path=str(source),files=[str(source)],inputs=atr_robot.inputs+inputs)
    monkeypatch.setattr(native,"SESSION_APPROVED_FINGERPRINT",catalog.fingerprint(out))
    return out


def test_43_inputs_typed_filter_and_previous_profile_refusal(robot,atr_robot):
    data=native.default_composition(robot,symbol="EURUSD",timeframe="M5").to_dict()
    assert data["filters"]==[]
    data["filters"]=[dict(mode="session_v1",params=SessionEntryPolicy(start_minute=540,end_minute=1020,weekdays=31,max_cycles=3).to_dict())]
    result=native.native_adapter(Composition(data),robot).describe("sessao_manual")
    assert len(result["parameters"])==43 and result["profile_version"]==8
    assert result["expected_native_inputs"]["InpSessionEnabled"] is True
    assert result["expected_native_inputs"]["InpSessionMaxCycles"]==3
    assert result["capabilities"]["bindings"]["filters.0.params.anchor_minute"]["unit"]=="broker_minute"
    assert result["capabilities"]["modules"]["filters"]==["session_v1"]
    with pytest.raises(ValueError,match="filtro"):native.native_adapter(Composition(data),atr_robot)


def test_ui_default_manual_reopen_validation_and_disable_preserve_previous_profile(view):
    assert view.session_available and view.session_toggle.instate(["disabled"])
    default=view.current;named(view);view.session_on.set(True)
    view.session_fields["start_minute"].set("09:00");view.session_fields["end_minute"].set("17:00")
    view.session_fields["anchor_minute"].set("17:00");view.session_fields["max_cycles"].set("3")
    for i,var in enumerate(view.session_days):var.set(i<5)
    assert not view.preview_error,view.lot_preview.get()
    view.save.invoke();assert not view.dirty,view.status.get();view.reopen.invoke()
    expected=SessionEntryPolicy(start_minute=540,end_minute=1020,anchor_minute=1020,weekdays=31,max_cycles=3)
    assert view.current.composition.to_dict()["filters"]==[dict(mode="session_v1",params=expected.to_dict())]
    assert default.composition.to_dict()["filters"]==[] and view.session_on.get()
    view.session_fields["start_minute"].set("24:00")
    assert view.preview_error and view.backtest.instate(["disabled"]) and view.replay.instate(["disabled"])
    view.reopen.invoke()
    for var in view.session_days:var.set(False)
    assert view.preview_error
    view.reopen.invoke();view.session_on.set(False)
    assert view.composition_from_fields().to_dict()["filters"]==[]
