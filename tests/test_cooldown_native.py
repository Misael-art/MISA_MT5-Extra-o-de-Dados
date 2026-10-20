from dataclasses import replace
from pathlib import Path
import pytest
from mt5_extracao.composition import Composition,native
from mt5_extracao.robots import catalog,mql
from tests.test_session_entry_native import robot as session_robot,atr_robot,dc_robot,er_robot,temporal_robot,partial_robot,lots_robot,legacy_robot,db,view,named

@pytest.fixture
def robot(session_robot,tmp_path,monkeypatch):
    source=tmp_path/"cooldown.mq5";source.write_bytes(Path(session_robot.path).read_bytes()+b"\n// reviewed cooldown profile\n")
    robot=replace(session_robot,version="1.80",path=str(source),files=[str(source)],inputs=session_robot.inputs+[mql.MqlInput("InpCooldownSeconds","int","0")])
    monkeypatch.setattr(native,"COOLDOWN_APPROVED_FINGERPRINT",catalog.fingerprint(robot))
    return robot

def test_profile_44_inputs_and_older_versions_preserved(robot,session_robot):
    data=native.default_composition(robot,symbol="EURUSD",timeframe="M5").to_dict()
    data["filters"]=[{"mode":"cooldown_v1","params":{"seconds":3600}}]
    profile=native.native_adapter(Composition(data),robot).describe("manual_cooldown")
    assert profile["profile_version"]==9 and len(profile["parameters"])==44
    assert profile["expected_native_inputs"]["InpCooldownSeconds"]==3600
    assert profile["expected_native_inputs"]["InpSessionEnabled"] is False
    assert profile["capabilities"]["bindings"]["filters.0.params.seconds"]["unit"]=="second_after_confirmed_final_close"
    with pytest.raises(ValueError,match="Cooldown"):native.native_adapter(Composition(data),session_robot)

def test_ui_named_manual_save_reopen_export_and_invalid_wait(view):
    assert view.cooldown_available and view.cooldown_entry.instate(["disabled"])
    default=view.current;named(view);view.cooldown_seconds.set("3600")
    assert not view.preview_error,view.lot_preview.get()
    view.save.invoke();assert not view.dirty,view.status.get()
    view.reopen.invoke();assert view.cooldown_seconds.get()=="3600"
    assert view.current.composition.to_dict()["filters"]==[{"mode":"cooldown_v1","params":{"seconds":3600}}]
    assert view.current.manifest["native"]["expected_native_inputs"]["InpCooldownSeconds"]==3600
    assert default.composition.to_dict()["filters"]==[]
    view.cooldown_seconds.set("-1");assert view.preview_error and view.backtest.instate(["disabled"])
    view.reopen.invoke();view.cooldown_seconds.set("0")
    assert view.composition_from_fields().to_dict()["filters"]==[]
