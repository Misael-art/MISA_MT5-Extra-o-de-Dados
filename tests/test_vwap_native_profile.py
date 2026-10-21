"""VWAP: perfil instalado fechado e UI real sobre fonte/execução simuladas."""
from dataclasses import replace
from pathlib import Path
import pytest
from mt5_extracao.composition import native
from mt5_extracao.composition import vwap_native as vwap
from mt5_extracao.composition.schema import Composition
from mt5_extracao.robots import catalog
from tests.test_ui_preset_workflow import view, named
from tests.test_default_presets import db


@pytest.fixture
def robot(tmp_path, monkeypatch):
    source=tmp_path/"HDK_VwapBands.mq5"
    defaults={"InpMagicNumber":"20261036","InpRiskPct":"0.5","InpDailyLossPct":"2", "InpWeeklyLossPct":"4",
              "InpMaxDrawdownPct":"10","InpEntryBandPct":"1.0","InpTargetBandPct":"0.5","InpStopPoints":"200",
              "InpVolumeSource":"0","InpDeviationPoints":"20","InpExportTrades":"false"}
    source.write_text('#property version "1.20"\n'+"\n".join(f"input {kind} {name}={defaults[name]};" for name,kind in vwap.INPUT_TYPES.items())+"\nvoid OnTick(){}\n")
    current=replace(catalog.robot_from_file(source),missing_includes=["Canvas/Canvas.mqh"])
    monkeypatch.setattr(vwap,"APPROVED_FINGERPRINT",catalog.fingerprint(current))
    return current


def test_profile_maps_all_inputs_without_inventing_capabilities(robot):
    composition=native.default_composition(robot,symbol="EURUSD",timeframe="H1")
    desc=native.native_adapter(composition,robot).describe("manual_s10")
    assert len(desc["parameters"])==11
    assert desc["expected_native_inputs"]["InpTargetBandPct"]==.5
    assert desc["expected_native_inputs"]["InpStopPoints"]==200
    assert desc["fixed_constraints"]["target_anchor"]=="signal_vwap_band"
    assert desc["fixed_constraints"]["trailing"] is False
    data=composition.to_dict();data["strategy"]["params"]["target_band_pct"]=0
    central=native.native_adapter(Composition(data),robot).describe("central")
    assert central["expected_native_inputs"]["InpTargetBandPct"]==0
    assert central["fixed_constraints"]["zero_target_band"]=="central_vwap_required_tp"
    assert desc["modality"]=="native_tester_demo_only" and desc["operational_acceptance"] is False
    assert all(modes==[mode] for slot,mode in vwap.BASE_MODES.items() for modes in [desc["capabilities"]["modules"][slot]])
    assert desc["capabilities"]["modules"]["filters"]==[]
    data=composition.to_dict();data["profit_exit"]={"mode":"fixed_points","params":{"target_points":100.0}}
    with pytest.raises(ValueError,match="fixada"):native.native_adapter(Composition(data),robot)
    data=composition.to_dict();data["sizing"]["params"]["risk_fraction"]=.006
    with pytest.raises(ValueError,match="0,5"):native.native_adapter(Composition(data),robot)
    data=composition.to_dict();data["strategy"]["params"].update(entry_band_pct=.5,target_band_pct=.5)
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
    assert view.vwap and not view.stoch and not view.pullback
    assert not any(getattr(view,key) for key in ("lots_enabled","partials_enabled","temporal_enabled","session_available","cooldown_available"))
    assert len(view.fields)==4
    default=view.current;own=named(view)
    view.fields["entry_band_pct"].set("1,5");view.fields["target_band_pct"].set("0")
    view.fields["stop_offset_points"].set("150");view.fields["volume_source"].set("1");view.risk_pct.set("0.2")
    view.save.invoke();assert not view.dirty,view.status.get()
    view.reopen.invoke()
    assert view.current.identity.id==own.identity.id and view.current.identity.revision==2
    assert view.fields["entry_band_pct"].get()=="1.5" and view.risk_pct.get()=="0.2"
    expected=view.current.manifest["native"]["expected_native_inputs"]
    assert (expected["InpEntryBandPct"],expected["InpTargetBandPct"],expected["InpStopPoints"],expected["InpVolumeSource"],expected["InpRiskPct"])==(1.5,0,150,1,.2)
    view.export.invoke();assert "Pacote exportado" in view.status.get(),view.status.get()
    from mt5_extracao.presets import store
    assert store.get_preset(view.db,default.identity.id)==default
    view.risk_pct.set("0.6");view.save.invoke()
    assert view.dirty and view.current.identity.revision==2 and "0,5" in view.status.get()


def test_vwap_form_fits_small_viewport(view):
    view.window.geometry("650x550")
    for _ in range(5):view.window.update()
    assert view.body.winfo_width()<=view.window.winfo_width()
    for entry in view.entries:
        assert entry.winfo_x()+entry.winfo_width()<=entry.master.winfo_width()
