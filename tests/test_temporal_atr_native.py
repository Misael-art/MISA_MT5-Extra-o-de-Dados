from dataclasses import replace
from pathlib import Path
import pytest
from mt5_extracao.composition import Composition,native
from mt5_extracao.robots import catalog,mql
from mt5_extracao.strategies.temporal_atr import AtrTemporalPolicy
from tests.test_temporal_donchian_native import robot as dc_robot,er_robot,temporal_robot,partial_robot,lots_robot,legacy_robot,db,view,named

@pytest.fixture
def robot(dc_robot,tmp_path,monkeypatch):
 source=tmp_path/"atr.mq5";source.write_bytes(Path(dc_robot.path).read_bytes()+b"\n// ATR reviewed test profile\n")
 inputs=[mql.MqlInput(name,kind,"0") for name,kind,unit in native.ATR_INPUTS.values()]
 out=replace(dc_robot,version="1.60",path=str(source),files=[str(source)],inputs=dc_robot.inputs+inputs)
 monkeypatch.setattr(native,"ATR_TEMPORAL_APPROVED_FINGERPRINT",catalog.fingerprint(out));return out

def selection():return dict(mode="temporal_atr_v1",params=AtrTemporalPolicy(max_seconds=600,atr_base_seconds=300,atr_min_seconds=60).to_dict())

def test_35_inputs_typed_mapping_and_previous_profile_refuses_atr(robot,dc_robot):
 data=native.default_composition(robot,symbol="EURUSD",timeframe="M5").to_dict();data["conditional_policy"]=selection()
 out=native.native_adapter(Composition(data),robot).describe("atr_manual")
 assert len(out["parameters"])==35 and out["profile_version"]==7
 assert out["expected_native_inputs"]["InpTimeATRBaseSeconds"]==300 and out["expected_native_inputs"]["InpTimeATRMinSeconds"]==60
 assert out["capabilities"]["bindings"]["conditional_policy.params.atr_base_seconds"]["unit"]=="second"
 with pytest.raises(ValueError,match="conditional_policy"):native.native_adapter(Composition(data),dc_robot)

def test_ui_default_preserved_manual_reopen_and_bounds_error_blocks_execution(view):
 assert view.atr_enabled and view.atr_toggle.instate(["disabled"])
 original=view.current;named(view);view.time_on.set(True);view.time_fields["max_seconds"].set("600");view.atr_on.set(True)
 assert not view.preview_error,view.lot_preview.get()
 view.save.invoke();assert not view.dirty,view.status.get();view.reopen.invoke()
 assert view.atr_on.get() and view.current.composition.to_dict()["conditional_policy"]==selection()
 assert original.composition.to_dict()["conditional_policy"]["mode"]=="disabled"
 view.atr_fields["atr_min_seconds"].set("601")
 assert view.preview_error and view.backtest.instate(["disabled"]) and view.replay.instate(["disabled"])
 view.reopen.invoke();view.atr_on.set(False)
 assert view.composition_from_fields().to_dict()["conditional_policy"]["mode"]=="temporal_v1"
