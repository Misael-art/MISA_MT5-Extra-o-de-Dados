"""HA/HiLo: perfil instalado fechado e UI real sobre fonte/execução simuladas."""
from dataclasses import replace
from pathlib import Path
import pytest
from mt5_extracao.composition import native
from mt5_extracao.composition import ha_hilo_native as ha_hilo
from mt5_extracao.composition.schema import Composition
from mt5_extracao.robots import catalog
from tests.test_ui_preset_workflow import view, named
from tests.test_default_presets import db


@pytest.fixture
def robot(tmp_path, monkeypatch):
    source=tmp_path/"HDK_HaHiLo.mq5"
    defaults={"InpMagicNumber":"20261040","InpRiskPct":"0.5","InpDailyLossPct":"2", "InpWeeklyLossPct":"4",
              "InpMaxDrawdownPct":"10","InpHiLoPeriod":"10","InpHiLoOffset":"0","InpStopBufferPoints":"0",
              "InpDeviationPoints":"20","InpExportTrades":"false"}
    source.write_text('#property version "1.20"\n'+"\n".join(f"input {kind} {name}={defaults[name]};" for name,kind in ha_hilo.INPUT_TYPES.items())+"\nvoid OnTick(){}\n")
    current=replace(catalog.robot_from_file(source),missing_includes=["Canvas/Canvas.mqh"])
    monkeypatch.setattr(ha_hilo,"APPROVED_FINGERPRINT",catalog.fingerprint(current))
    return current


def test_profile_maps_all_inputs_without_inventing_capabilities(robot):
    composition=native.default_composition(robot,symbol="EURUSD",timeframe="H1")
    desc=native.native_adapter(composition,robot).describe("manual_ha_hilo")
    assert len(desc["parameters"])==10
    assert desc["expected_native_inputs"]["InpHiLoPeriod"]==10
    assert desc["expected_native_inputs"]["InpHiLoOffset"]==0
    assert desc["fixed_constraints"]["target_anchor"]=="disabled_required_zero_tp"
    assert desc["fixed_constraints"]["always_in_position"] is False
    assert desc["fixed_constraints"]["trailing"] is False
    assert desc["modality"]=="native_tester_demo_only" and desc["operational_acceptance"] is False
    assert all(modes==[mode] for slot,mode in ha_hilo.BASE_MODES.items() for modes in [desc["capabilities"]["modules"][slot]])
    assert desc["capabilities"]["modules"]["filters"]==[]
    data=composition.to_dict();data["profit_exit"]={"mode":"fixed_points","params":{"target_points":100.0}}
    with pytest.raises(ValueError,match="sem TP"):native.native_adapter(Composition(data),robot)
    data=composition.to_dict();data["sizing"]["params"]["risk_fraction"]=.006
    with pytest.raises(ValueError,match="0,5"):native.native_adapter(Composition(data),robot)
    data=composition.to_dict();data["strategy"]["params"].update(hilo_offset=1)
    with pytest.raises(ValueError):native.native_adapter(Composition(data),robot)


def test_profile_rechecks_pin_complete_input_contract_and_exact_external_include(robot):
    for changed in (replace(robot,version="1.40"), replace(robot,inputs=robot.inputs[:-1]),
                    replace(robot,missing_includes=["Canvas/Other.mqh"]),
                    replace(robot,missing_includes=["../Canvas/Canvas.mqh"]),
                    replace(robot,missing_includes=["Framework/HADOUKEN/Strategies/HDK_StrategyRobot.mqh"])):
        with pytest.raises(ValueError):native.default_composition(changed,symbol="EURUSD",timeframe="H1")
    composition=native.default_composition(robot,symbol="EURUSD",timeframe="H1")
    adapter=native.native_adapter(composition,robot)
    Path(robot.path).write_text("// changed after creating adapter")
    with pytest.raises(ValueError):adapter.describe("s10")


def test_ui_default_manual_edit_reopen_export_and_live_restriction(view):
    assert view.ha_hilo and not view.madrid and not view.vwap and not view.stoch and not view.pullback
    assert not any(getattr(view,key) for key in ("lots_enabled","partials_enabled","temporal_enabled","session_available","cooldown_available"))
    assert len(view.fields)==3
    default=view.current;own=named(view)
    view.fields["hilo_period"].set("12");view.fields["hilo_offset"].set("0")
    view.fields["stop_buffer_points"].set("15");view.risk_pct.set("0.2")
    view.save.invoke();assert not view.dirty,view.status.get()
    view.reopen.invoke()
    assert view.current.identity.id==own.identity.id and view.current.identity.revision==2
    assert view.fields["hilo_period"].get()=="12" and view.risk_pct.get()=="0.2"
    expected=view.current.manifest["native"]["expected_native_inputs"]
    assert (expected["InpHiLoPeriod"],expected["InpHiLoOffset"],expected["InpStopBufferPoints"],expected["InpRiskPct"])==(12,0,15,.2)
    assert int(view.save.grid_info()["row"]) > int(view.entries[2].grid_info()["row"])
    view.export.invoke();assert "Pacote exportado" in view.status.get(),view.status.get()
    from mt5_extracao.presets import store
    assert store.get_preset(view.db,default.identity.id)==default
    view.risk_pct.set("0.6");view.save.invoke()
    assert view.dirty and view.current.identity.revision==2 and "0,5" in view.status.get()


def test_ha_hilo_form_fits_small_viewport(view):
    view.window.geometry("650x550")
    for _ in range(5):view.window.update()
    assert view.body.winfo_width()<=view.window.winfo_width()
    for entry in view.entries:
        assert entry.winfo_x()+entry.winfo_width()<=entry.master.winfo_width()


def test_source_is_rejected_before_creating_the_editor(robot,db,monkeypatch):
    from types import SimpleNamespace
    from mt5_extracao.ui import preset_workflow
    controller=SimpleNamespace(chosen={robot.key:("EURUSD","H1",None)},
        ui=SimpleNamespace(app=SimpleNamespace(db_manager=db,config=None)))
    monkeypatch.setattr(preset_workflow.tk,"Toplevel",lambda *_:pytest.fail("fonte inválida abriu editor"))
    with pytest.raises(ValueError,match="HA/HiLo exige"):
        preset_workflow.PresetWorkflow(controller,replace(robot,version="1.10"))
