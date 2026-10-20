"""T19.56: prévia/contrato e UI; somente fakes, sem MT5 real."""
from dataclasses import replace
from types import SimpleNamespace
import math
from pathlib import Path
import pytest
from mt5_extracao.composition import Composition
from mt5_extracao.composition import native
from mt5_extracao.composition.adapter import BaseAdapter
from mt5_extracao.composition.sizing import preview_sizing
from mt5_extracao.robots import catalog,mql
from mt5_extracao.presets import defaults,manual
from mt5_extracao.presets.compiler import TestContext as Context,read_package
from tests.test_s09_native_profile import robot as legacy_robot
from tests.test_default_presets import db
from tests.test_ui_preset_workflow import view,named

@pytest.fixture
def robot(legacy_robot,tmp_path,monkeypatch):
    source=tmp_path/"lots"/"HDK_S09_Referencia.mq5";source.parent.mkdir()
    source.write_bytes(Path(legacy_robot.path).read_bytes()+b"\n// distinct approved lots profile fixture\n")
    out=replace(legacy_robot,version=native.LOTS_ROBOT_VERSION,path=str(source),files=[str(source)],
                inputs=list(legacy_robot.inputs)+[mql.MqlInput("InpLotMode","int","1"),mql.MqlInput("InpFixedLot","double","0.01")])
    monkeypatch.setattr(native,"LOTS_APPROVED_FINGERPRINT",catalog.fingerprint(out))
    return out


def fixed(robot,volume=2.35):
    data=native.default_composition(robot,symbol="EURUSD",timeframe="M5").to_dict()
    data["sizing"]={"mode":"fixed_volume","params":{"volume":volume,"risk_fraction":.005,"compound":True}}
    return Composition(data)


def info():
    return SimpleNamespace(point=.00001,trade_tick_size=.00001,trade_tick_value=1.,volume_min=.01,volume_max=100.,volume_step=.01)


def test_old_profile_keeps_original_package_and_new_profile_maps_lots_explicitly(legacy_robot,robot,db,tmp_path):
    previous=defaults.ensure_default(db,legacy_robot,symbol="EURUSD",timeframe="M5")
    exported=manual.export_preset(db,legacy_robot,previous.identity.id,tmp_path/"old",test_context=Context("2026-10-14","2026-10-15",deposit=100000))
    old=exported["manifest_path"].read_bytes()
    out=native.S09NativeAdapter(fixed(robot),robot).describe("lots_test")
    assert out["profile_version"]==2 and len(out["receipt_fields"])==12
    assert out["parameters"]["InpLotMode"]=="0" and out["parameters"]["InpFixedLot"]=="2.35"
    assert out["capabilities"]["bindings"]["sizing.params.volume"]["unit"]=="lot"
    assert out["expected_native_inputs"]["InpRiskPct"]==.5
    assert out["modality"]=="native_tester_demo_only" and not out["operational_acceptance"]
    assert defaults.ensure_default(db,robot,symbol="EURUSD",timeframe="M5").identity.id!=previous.identity.id
    assert read_package(exported["manifest_path"],legacy_robot).manifest["native"]["profile_version"]==1
    assert exported["manifest_path"].read_bytes()==old
    with pytest.raises(ValueError,match="sizing"):
        native.S09NativeAdapter(fixed(robot),legacy_robot)
    with pytest.raises(ValueError,match="fixed_volume"):
        BaseAdapter(fixed(robot))

@pytest.mark.parametrize("volume",[0,-1,float("nan"),float("inf"),True,"2"])
def test_invalid_volume_never_compiles(robot,volume):
    with pytest.raises(ValueError):fixed(robot,volume)


def test_reference_preview_quantizes_down_and_keeps_margin_unknown(robot):
    value=preview_sizing(fixed(robot).to_dict()["sizing"],info=info(),stop_points=100,deposit=100000)
    assert value["requested"]==2.35 and value["effective"]==2.35
    assert value["risk_money"]==pytest.approx(235,abs=1e-10) and value["risk_budget"]==500
    assert not value["margin_known"] and not value["historical_qualification"]
    fractional=preview_sizing(fixed(robot,.29999999999).to_dict()["sizing"],info=SimpleNamespace(**{**vars(info()),"volume_min":.1,"volume_step":.1}),stop_points=100,deposit=100000)
    assert fractional["effective"]==.2
    integer=preview_sizing(fixed(robot,3.9).to_dict()["sizing"],info=SimpleNamespace(**{**vars(info()),"volume_min":1.,"volume_step":1.}),stop_points=100,deposit=100000)
    assert integer["effective"]==3
    with pytest.raises(ValueError,match="orçamento"):
        preview_sizing(fixed(robot,6).to_dict()["sizing"],info=info(),stop_points=100,deposit=100000)
    with pytest.raises(ValueError,match="mínimo"):
        preview_sizing(fixed(robot,.009).to_dict()["sizing"],info=info(),stop_points=100,deposit=100000)

@pytest.mark.parametrize("missing",[None,"point","trade_tick_size","trade_tick_value","volume_min","volume_max","volume_step"])
def test_missing_spec_never_becomes_generic_or_unlimited(robot,missing):
    spec=info() if missing else None
    if missing:setattr(spec,missing,0)
    with pytest.raises(ValueError,match="Especificação"):
        preview_sizing(fixed(robot).to_dict()["sizing"],info=spec,stop_points=100,deposit=100000)


def test_lots_ui_saves_reopens_exports_and_blocks_volume_above_budget(view):
    view.ui.app.mt5_connector=SimpleNamespace(get_symbol_info=lambda symbol:info())
    view.reopen.invoke();assert not view.preview_error
    assert all(e.instate(["disabled"]) for e in view.entries)
    own=named(view)
    view.lot_mode.set("Volume fixo");view.fixed_volume.set("2.35");view.risk_pct.set("0.25")
    assert view.dirty and not view.preview_error and "2.35 lotes" in view.lot_preview.get()
    view.save.invoke();assert view.current.identity.revision==2,view.status.get()
    view.reopen.invoke();assert view.lot_mode.get()=="Volume fixo" and view.fixed_volume.get()=="2.35"
    view.export.invoke();assert "Pacote exportado" in view.status.get()
    manifest=next(Path(view.export_dir.get()).rglob("*.manifest.json"))
    package=read_package(manifest,view.robot)
    assert package.manifest["native"]["parameters"]["InpFixedLot"]=="2.35"
    assert package.manifest["native"]["parameters"]["InpRiskPct"]=="0.2500"
    view.fixed_volume.set("7")
    assert view.preview_error and view.backtest.instate(["disabled"]) and "orçamento" in view.lot_preview.get()
    assert manual.get_manual(view.db,own.identity.id).identity.revision==2


def test_lots_ui_absent_preview_is_not_an_ipc_dependency_or_preapproval(view):
    assert not view.preview_error and "Prévia indisponível" in view.lot_preview.get()
    assert "Tester deve confirmar" in view.lot_preview.get() and "não há aprovação" in view.lot_preview.get()
    assert not view.backtest.instate(["disabled"])
    assert view.execution is None


def test_lots_fields_reflow_in_small_viewport(view):
    view.window.geometry("650x550")
    for _ in range(6):view.window.update()
    fields=view.lot_combo.master.master
    for field in fields.winfo_children():
        assert field.winfo_x()+field.winfo_width()<=fields.winfo_width()
        assert field.winfo_width()>=max(child.winfo_reqwidth() for child in field.winfo_children())
    assert view.body.winfo_width()<=view.window.winfo_width()
