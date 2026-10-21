"""S10: perfil instalado fechado e UI real sobre fonte/execução simuladas."""
from dataclasses import replace
from pathlib import Path
import pytest
from mt5_extracao.composition import native
from mt5_extracao.composition import stoch_native as stoch
from mt5_extracao.composition.schema import Composition
from mt5_extracao.robots import catalog
from tests.test_ui_preset_workflow import view, named
from tests.test_default_presets import db


@pytest.fixture
def robot(tmp_path, monkeypatch):
    source=tmp_path/"HDK_StochEma9.mq5"
    defaults={"InpMagicNumber":"20261038","InpRiskPct":"0.5","InpDailyLossPct":"2", "InpWeeklyLossPct":"4",
              "InpMaxDrawdownPct":"10","InpKPeriod":"14","InpSlowing":"3","InpDPeriod":"3","InpEmaPeriod":"9",
              "InpUpperLevel":"75","InpLowerLevel":"25","InpStopOffsetPoints":"50","InpTargetPoints":"0",
              "InpDeviationPoints":"20","InpExportTrades":"false"}
    source.write_text('#property version "1.30"\n'+"\n".join(f"input {kind} {name}={defaults[name]};" for name,kind in stoch.INPUT_TYPES.items())+"\nvoid OnTick(){}\n")
    current=replace(catalog.robot_from_file(source),missing_includes=["Canvas/Canvas.mqh"])
    monkeypatch.setattr(stoch,"APPROVED_FINGERPRINT",catalog.fingerprint(current))
    return current


def test_profile_maps_all_inputs_without_inventing_capabilities(robot):
    composition=native.default_composition(robot,symbol="EURUSD",timeframe="H1")
    desc=native.native_adapter(composition,robot).describe("manual_s10")
    assert len(desc["parameters"])==15
    assert desc["expected_native_inputs"]["InpTargetPoints"]==0
    assert desc["expected_native_inputs"]["InpStopOffsetPoints"]==50
    assert desc["fixed_constraints"]["target_anchor"]=="signal_close"
    assert desc["fixed_constraints"]["trailing"]=="ema_move_guard_closed_bar"
    assert desc["modality"]=="native_tester_demo_only" and desc["operational_acceptance"] is False
    assert all(modes==[mode] for slot,mode in stoch.BASE_MODES.items() for modes in [desc["capabilities"]["modules"][slot]])
    assert desc["capabilities"]["modules"]["filters"]==[]
    data=composition.to_dict();data["profit_exit"]={"mode":"fixed_points","params":{"target_points":100.0}}
    with pytest.raises(ValueError,match="âncora"):native.native_adapter(Composition(data),robot)
    data=composition.to_dict();data["sizing"]["params"]["risk_fraction"]=.006
    with pytest.raises(ValueError,match="0,5"):native.native_adapter(Composition(data),robot)
    data=composition.to_dict();data["strategy"]["params"].update(upper_level=25,lower_level=75)
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
    assert view.stoch and not view.pullback
    assert not any(getattr(view,key) for key in ("lots_enabled","partials_enabled","temporal_enabled","session_available","cooldown_available"))
    assert len(view.fields)==8
    default=view.current;own=named(view)
    view.fields["k_period"].set("20");view.fields["upper_level"].set("80,5")
    view.fields["target_points"].set("120");view.risk_pct.set("0.2")
    view.save.invoke();assert not view.dirty,view.status.get()
    view.reopen.invoke()
    assert view.current.identity.id==own.identity.id and view.current.identity.revision==2
    assert view.fields["upper_level"].get()=="80.5" and view.risk_pct.get()=="0.2"
    expected=view.current.manifest["native"]["expected_native_inputs"]
    assert (expected["InpKPeriod"],expected["InpUpperLevel"],expected["InpTargetPoints"],expected["InpRiskPct"])==(20,80.5,120,.2)
    view.export.invoke();assert "Pacote exportado" in view.status.get(),view.status.get()
    from mt5_extracao.presets import store
    assert store.get_preset(view.db,default.identity.id)==default
    view.risk_pct.set("0.6");view.save.invoke()
    assert view.dirty and view.current.identity.revision==2 and "0,5" in view.status.get()


def test_s10_form_fits_small_viewport(view):
    view.window.geometry("650x550")
    for _ in range(5):view.window.update()
    assert view.body.winfo_width()<=view.window.winfo_width()
    for entry in view.entries:
        assert entry.winfo_x()+entry.winfo_width()<=entry.master.winfo_width()
