"""Madrid: perfil instalado fechado e UI real sobre fonte/execução simuladas."""
from dataclasses import replace
from pathlib import Path
import pytest
from mt5_extracao.composition import native
from mt5_extracao.composition import madrid_native as madrid
from mt5_extracao.composition.schema import Composition
from mt5_extracao.robots import catalog
from tests.test_ui_preset_workflow import view, named
from tests.test_default_presets import db


@pytest.fixture
def robot(tmp_path, monkeypatch):
    source=tmp_path/"HDK_Madrid.mq5"
    defaults={"InpMagicNumber":"20261039","InpRiskPct":"0.5","InpDailyLossPct":"2", "InpWeeklyLossPct":"4",
              "InpMaxDrawdownPct":"10","InpFirstPeriod":"5","InpPeriodStep":"5","InpDistanceAtr":"1.0",
              "InpDistancePoints":"0","InpTrailLine":"10","InpOffsetAtr":"0.25",
              "InpDeviationPoints":"20","InpExportTrades":"false"}
    source.write_text('#property version "1.20"\n'+"\n".join(f"input {kind} {name}={defaults[name]};" for name,kind in madrid.INPUT_TYPES.items())+"\nvoid OnTick(){}\n")
    current=replace(catalog.robot_from_file(source),missing_includes=["Canvas/Canvas.mqh"])
    monkeypatch.setattr(madrid,"APPROVED_FINGERPRINT",catalog.fingerprint(current))
    return current


def test_profile_maps_all_inputs_without_inventing_capabilities(robot):
    composition=native.default_composition(robot,symbol="EURUSD",timeframe="H1")
    desc=native.native_adapter(composition,robot).describe("manual_madrid")
    assert len(desc["parameters"])==13
    assert desc["expected_native_inputs"]["InpFirstPeriod"]==5
    assert desc["expected_native_inputs"]["InpDistancePoints"]==0
    assert desc["fixed_constraints"]["target_anchor"]=="signal_m1_plus_minus_distance"
    assert desc["fixed_constraints"]["moving_target"] is False
    assert desc["fixed_constraints"]["trailing"]=="shared_ema_move_guard"
    data=composition.to_dict();data["strategy"]["params"]["distance_points"]=100
    points=native.native_adapter(Composition(data),robot).describe("points")
    assert points["expected_native_inputs"]["InpDistancePoints"]==100
    assert points["fixed_constraints"]["distance_mode"]=="points"
    assert desc["modality"]=="native_tester_demo_only" and desc["operational_acceptance"] is False
    assert all(modes==[mode] for slot,mode in madrid.BASE_MODES.items() for modes in [desc["capabilities"]["modules"][slot]])
    assert desc["capabilities"]["modules"]["filters"]==[]
    data=composition.to_dict();data["profit_exit"]={"mode":"fixed_points","params":{"target_points":100.0}}
    with pytest.raises(ValueError,match="TP fica"):native.native_adapter(Composition(data),robot)
    data=composition.to_dict();data["sizing"]["params"]["risk_fraction"]=.006
    with pytest.raises(ValueError,match="0,5"):native.native_adapter(Composition(data),robot)
    data=composition.to_dict();data["strategy"]["params"].update(first_period=1000,period_step=1)
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
    assert view.madrid and not view.vwap and not view.stoch and not view.pullback
    assert not any(getattr(view,key) for key in ("lots_enabled","partials_enabled","temporal_enabled","session_available","cooldown_available"))
    assert len(view.fields)==6
    default=view.current;own=named(view)
    view.fields["first_period"].set("7");view.fields["period_step"].set("6")
    view.fields["distance_atr"].set("1,5");view.fields["distance_points"].set("100")
    view.fields["trail_line"].set("8");view.fields["offset_atr"].set("0,3");view.risk_pct.set("0.2")
    view.save.invoke();assert not view.dirty,view.status.get()
    view.reopen.invoke()
    assert view.current.identity.id==own.identity.id and view.current.identity.revision==2
    assert view.fields["distance_atr"].get()=="1.5" and view.risk_pct.get()=="0.2"
    expected=view.current.manifest["native"]["expected_native_inputs"]
    assert (expected["InpFirstPeriod"],expected["InpPeriodStep"],expected["InpDistanceAtr"],expected["InpDistancePoints"],
            expected["InpTrailLine"],expected["InpOffsetAtr"],expected["InpRiskPct"])==(7,6,1.5,100,8,.3,.2)
    assert int(view.save.grid_info()["row"]) > int(view.entries[5].grid_info()["row"])
    view.export.invoke();assert "Pacote exportado" in view.status.get(),view.status.get()
    from mt5_extracao.presets import store
    assert store.get_preset(view.db,default.identity.id)==default
    view.risk_pct.set("0.6");view.save.invoke()
    assert view.dirty and view.current.identity.revision==2 and "0,5" in view.status.get()


def test_madrid_form_fits_small_viewport(view):
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
    with pytest.raises(ValueError,match="Madrid exige"):
        preset_workflow.PresetWorkflow(controller,replace(robot,version="1.10"))
