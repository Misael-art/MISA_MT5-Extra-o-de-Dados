"""T19.23: mapeamento fechado do perfil aprovado, sem SDK/terminal real."""
import copy

import pytest

from mt5_extracao.composition import Capabilities, Composition, base_composition
from mt5_extracao.composition import native
from mt5_extracao.robots import catalog, mql


@pytest.fixture
def robot(tmp_path,monkeypatch):
    # Autoridade instalada é substituída só no teste; o sidecar fornecido pelo usuário não pode fazê-lo.
    source=tmp_path/"HDK_S09_Referencia.mq5";source.write_text("// fonte pública fake para teste de mapper\n")
    r=catalog.Robot(native.ROBOT_KEY,catalog.MQL5,"S09",version=native.ROBOT_VERSION,path=str(source),
                    files=[str(source)],inputs=[mql.MqlInput(name,kind,"0") for name,kind in native.INPUT_TYPES.items()])
    monkeypatch.setattr(native,"APPROVED_FINGERPRINT",catalog.fingerprint(r))
    return r


def composition():
    return base_composition("s09_reference",symbol="EURUSD",timeframe="M5",risk_fraction=.005)


def test_defaults_map_every_input_and_percent_conversion_without_changing_composition(robot):
    comp=native.default_composition(robot,symbol="EURUSD",timeframe="M5")
    before=comp.to_json();out=native.S09NativeAdapter(comp,robot).describe("manual_01")
    expected={"InpFastPeriod":"20","InpSlowPeriod":"50","InpWarmupBars":"250","InpStopPoints":"100",
              "InpTargetPoints":"200","InpRiskPct":"0.500","InpMagic":"909202610","InpDeviationPoints":"20",
              "InpPresetId":"manual_01","InpConfigHash":comp.config_hash}
    assert out["parameters"]==expected and set(out["receipt_fields"])==set(native.INPUT_TYPES)
    assert out["expected_native_inputs"]["InpRiskPct"]==.5
    assert out["capabilities"]["bindings"]["sizing.params.risk_fraction"]["unit"]=="fraction"
    assert out["conversions"]["sizing.params.risk_fraction"]=="fraction × 100 → InpRiskPct percent"
    assert out["fixed_constraints"]["block_averaging"] and out["fixed_constraints"]["positions"]==1
    assert comp.to_json()==before and not out["operational_acceptance"]
    assert Capabilities(out["capabilities"],comp).to_dict()==out["capabilities"]


def test_runtime_profile_requires_published_revision_and_binds_complete_source_graph(robot,monkeypatch):
    robot.version=native.RUNTIME_ROBOT_VERSION
    robot.inputs=[mql.MqlInput(name,kind,"0") for name,kind in native.RUNTIME_INPUT_TYPES.items()]
    monkeypatch.setattr(native,"RUNTIME_APPROVED_FINGERPRINT",catalog.fingerprint(robot))
    monkeypatch.setattr(native,"RUNTIME_HADOUKEN_REVISION",None)
    with pytest.raises(ValueError,match="não publicada"):
        native.S09NativeAdapter(composition(),robot)
    monkeypatch.setattr(native,"RUNTIME_HADOUKEN_REVISION","f"*40)
    output=native.S09NativeAdapter(composition(),robot).describe("bound_preset")
    assert output["profile_version"]==11 and output["runtime_binding_required"]
    assert output["configuration_transition"]=="next_session_only"
    assert set(output["expected_native_inputs"])==set(native.RUNTIME_INPUT_TYPES)
    from mt5_extracao.robots import native_inputs
    assert output["project_source_files"]==native_inputs.project_sources(robot)
    assert output["project_source_sha256"]==native_inputs.project_sources_hash(output["project_source_files"])
    assert not output["operational_acceptance"]


def test_explicit_fixed_policy_precedence_is_visible_and_no_input_is_bound_twice(robot):
    value=composition().to_dict();value["profit_exit"]={"mode":"fixed_points","params":{"target_points":300.0}}
    value["protection"]={"mode":"fixed_points","params":{"stop_points":150}}
    value["sizing"]["params"]["risk_fraction"]=.00025
    out=native.S09NativeAdapter(Composition(value),robot).describe("id")
    assert out["parameters"]["InpTargetPoints"]=="300" and out["parameters"]["InpStopPoints"]=="150"
    assert out["expected_native_inputs"]["InpRiskPct"]==.025
    assert set(out["inactive_paths"])=={"strategy.params.target_points","strategy.params.stop_points"}
    bindings=out["capabilities"]["bindings"]
    assert "profit_exit.params.target_points" in bindings and "strategy.params.target_points" not in bindings
    assert len({v["input"] for v in bindings.values()})==len(bindings)


@pytest.mark.parametrize("change",["key","version","source","inputs","opaque"])
def test_external_declaration_cannot_grant_support_or_bypass_source_change(robot,change):
    adapter=native.S09NativeAdapter(composition(),robot)
    if change=="key":robot.key="mql5:external"
    elif change=="version":robot.version="2.00"
    elif change=="source":__import__("pathlib").Path(robot.path).write_text("changed")
    elif change=="inputs":robot.inputs.pop()
    elif change=="opaque":robot.has_source=False
    with pytest.raises(ValueError,match="fonte/versão/fingerprint"):
        adapter.describe("manual")


@pytest.mark.parametrize("slot,module",[
    ("orientation",{"mode":"inverted","params":{}}),
    ("entry",{"mode":"limit","params":{"distance_points":5}}),
    ("profit_exit",{"mode":"fixed_r","params":{"target_r":2}}),
    ("partial_exit_plan",{"mode":"fixed_r","params":{"trigger_r":1,"fraction":.5,"breakeven_after_confirmation":True}})])
def test_unimplemented_modes_fail_instead_of_ignored_input(robot,slot,module):
    value=composition().to_dict();value[slot]=module
    with pytest.raises(ValueError,match="sem suporte"):
        native.S09NativeAdapter(Composition(value),robot)


@pytest.mark.parametrize("params",[dict(risk_fraction=.006,compound=True),dict(risk_fraction=.005,compound=False)])
def test_financial_limits_cannot_be_raised_or_sizing_mode_silently_changed(robot,params):
    value=composition().to_dict();value["sizing"]["params"]=params
    with pytest.raises(ValueError,match="até 0.5%"):
        native.S09NativeAdapter(Composition(value),robot)


@pytest.mark.parametrize("points",[1.5,2147483648])
def test_native_int_input_is_not_silently_truncated(robot,points):
    value=composition().to_dict();value["profit_exit"]={"mode":"fixed_points","params":{"target_points":points}}
    with pytest.raises(ValueError,match="não truncar"):
        native.S09NativeAdapter(Composition(value),robot)


def test_pullback_and_filters_are_blocked(robot):
    with pytest.raises(ValueError,match="Pullback permanece bloqueado"):
        native.S09NativeAdapter(base_composition("pullback",symbol="EURUSD",timeframe="M5"),robot)
    value=composition().to_dict();value["filters"]=[{"mode":"adx_min","params":{"period":14,"threshold":20}}]
    with pytest.raises(ValueError,match="filtro adicional"):
        native.S09NativeAdapter(Composition(value),robot)


@pytest.mark.parametrize("preset_id",["","../secret","nome com espaços","a"*81])
def test_unsafe_or_empty_public_identity_is_rejected(robot,preset_id):
    with pytest.raises(ValueError):native.S09NativeAdapter(composition(),robot).describe(preset_id)


@pytest.fixture
def protection_robot(robot,monkeypatch):
    robot.version=native.PROTECTION_ROBOT_VERSION
    robot.inputs=[mql.MqlInput(name,kind,"0") for name,kind in native.PROTECTION_INPUT_TYPES.items()]
    monkeypatch.setattr(native,"PROTECTION_APPROVED_FINGERPRINT",catalog.fingerprint(robot))
    monkeypatch.setattr(native,"PROTECTION_HADOUKEN_REVISION","e"*40)
    return robot


def managed_composition(**changes):
    data=composition().to_dict()
    params=dict(stop_mode=1,stop_points=0,stop_atr=2,trailing_mode=2,trailing_points=0,
                trailing_atr=1.5,trailing_trigger_r=.5,breakeven_trigger_r=1,exit_on_opposite=False,exit_on_session_end=False)
    params.update(changes)
    data["protection"]={"mode":"managed_v1","params":params}
    data["profit_exit"]={"mode":"planned_r_v1","params":{"target_r":3}}
    return data


def test_postfill_r_has_distinct_source_profile_and_preserves_planned_mode(protection_robot, monkeypatch):
    data=managed_composition()
    data["profit_exit"]={"mode":"postfill_r_v1","params":{"target_r":3}}
    comp=Composition(data)
    with pytest.raises(ValueError,match="sem suporte"):
        native.S09NativeAdapter(comp,protection_robot)
    protection_robot.version=native.POSTFILL_ROBOT_VERSION
    monkeypatch.setattr(native,"POSTFILL_APPROVED_FINGERPRINT",catalog.fingerprint(protection_robot))
    monkeypatch.setattr(native,"POSTFILL_HADOUKEN_REVISION","f"*40)
    out=native.S09NativeAdapter(comp,protection_robot).describe("postfill")
    assert out["profile_version"]==14 and out["hadouken_revision"]=="f"*40
    assert out["parameters"]["InpTargetMode"]=="3"
    assert out["parameters"]["InpTargetR"]=="3" and out["parameters"]["InpTargetPoints"]=="0"
    assert out["capabilities"]["bindings"]["profit_exit.params.target_r"]["unit"]=="initial_actual_R"
    assert "postfill_r_v1" in out["capabilities"]["modules"]["profit_exit"]
    assert out["runtime_binding_required"] and not out["operational_acceptance"]
    assert "planned-R broker TP" in out["protection_rules"]["postfill_target"]
    assert "actual initial R" in out["protection_rules"]["target_r"]
    data["profit_exit"]={"mode":"planned_r_v1","params":{"target_r":3}}
    planned=native.S09NativeAdapter(Composition(data),protection_robot).describe("planned")
    assert planned["parameters"]["InpTargetMode"]=="1"
    data["profit_exit"]={"mode":"postfill_r_v1","params":{"target_r":3}}
    data["partial_exit_plan"]={"mode":"fixed_r","params":dict(trigger_r=3,fraction=.5,breakeven_after_confirmation=True)}
    with pytest.raises(ValueError,match="anteceder"):
        native.S09NativeAdapter(Composition(data),protection_robot)


def test_common_protection_maps_all_inputs_and_declares_units_cadence_and_inactive_paths(protection_robot):
    comp=Composition(managed_composition())
    out=native.S09NativeAdapter(comp,protection_robot).describe("managed")
    assert out["profile_version"]==13 and out["runtime_binding_required"]
    assert out["history_receipt"]["schema_version"]==2
    assert out["history_receipt"]["older_rounded_history_is_insufficient"]
    assert {"price","volume","entry_sl","entry_tp","order_sl","order_tp"} == set(out["history_receipt"]["lossless_binary64"])
    assert set(out["parameters"])==set(native.PROTECTION_INPUT_TYPES)
    assert len(out["parameters"])==59
    assert out["parameters"]["InpStopMode"]=="1" and out["parameters"]["InpStopPoints"]=="0"
    assert out["parameters"]["InpTargetMode"]=="1" and out["parameters"]["InpTargetR"]=="3"
    assert out["parameters"]["InpTargetPoints"]=="0" and out["parameters"]["InpTrailingATR"]=="1.5"
    assert out["parameters"]["InpBreakevenTriggerR"]=="1"
    assert "strategy.params.stop_points" in out["inactive_paths"]
    assert "strategy.params.target_points" in out["inactive_paths"]
    assert "protection.params.stop_points" in out["inactive_paths"]
    assert "protection.params.trailing_points" in out["inactive_paths"]
    bindings=out["capabilities"]["bindings"]
    assert bindings["protection.params.stop_atr"]["unit"]=="ATR"
    assert bindings["protection.params.breakeven_trigger_r"]["unit"]=="R"
    assert len({item["input"] for item in bindings.values()})==len(bindings)
    assert out["fixed_constraints"]["trailing"] and not out["operational_acceptance"]
    assert "slippage" in out["protection_rules"]["target_r"]
    assert "never current SL" in out["protection_rules"]["management_r"]


@pytest.mark.parametrize("changes",[
    {"stop_mode":0,"stop_points":0,"stop_atr":0}, {"stop_points":10},
    {"stop_atr":0}, {"trailing_mode":0}, {"trailing_atr":0},
    {"trailing_mode":1,"trailing_points":10}, {"trailing_mode":3},
    {"breakeven_trigger_r":-1}, {"trailing_trigger_r":float("nan")},
])
def test_invalid_or_nonzero_inactive_protection_fields_are_rejected(changes):
    with pytest.raises(ValueError):Composition(managed_composition(**changes))


def test_common_defaults_do_not_enable_financial_management(protection_robot):
    out=native.S09NativeAdapter(composition(),protection_robot).describe("defaults")
    for key in ("InpStopMode","InpStopATR","InpTargetMode","InpTargetR","InpTrailingMode","InpTrailingPoints",
                "InpTrailingATR","InpTrailingTriggerR","InpBreakevenTriggerR","InpDynamicTargetATR","InpDynamicTargetMinR","InpDynamicTargetMaxR","InpOrientation"):
        assert out["expected_native_inputs"][key]==0
    assert out["expected_native_inputs"]["InpStopPoints"]==100
    assert out["expected_native_inputs"]["InpTargetPoints"]==200


def test_common_profile_is_gated_and_cannot_activate_frozen_profile(protection_robot,monkeypatch):
    monkeypatch.setattr(native,"PROTECTION_HADOUKEN_REVISION",None)
    with pytest.raises(ValueError,match="não publicada"):
        native.runtime_binding_required(protection_robot)
    protection_robot.version=native.RUNTIME_ROBOT_VERSION
    with pytest.raises(ValueError,match="fonte/versão/fingerprint"):
        native.S09NativeAdapter(Composition(managed_composition()),protection_robot)


def test_planned_r_checks_whole_partial_plan_before_runtime(protection_robot):
    data=managed_composition()
    data["partial_exit_plan"]={"mode":"fixed_r","params":dict(trigger_r=3,fraction=.5,breakeven_after_confirmation=True)}
    with pytest.raises(ValueError,match="anteceder"):
        native.S09NativeAdapter(Composition(data),protection_robot)
    data["partial_exit_plan"]["params"]["trigger_r"]=1
    out=native.S09NativeAdapter(Composition(data),protection_robot).describe("partial")
    assert out["parameters"]["InpPartialBreakeven"]=="true"
    assert out["parameters"]["InpTrailingMode"]=="2"


def test_session_exit_requires_explicit_session_and_does_not_enable_it_implicitly(protection_robot):
    data=managed_composition(exit_on_session_end=True,exit_on_opposite=True)
    with pytest.raises(ValueError,match="exige um filtro"):
        Composition(data)
    from mt5_extracao.strategies.session_entry import SessionEntryPolicy
    data["filters"]=[{"mode":"session_v1","params":SessionEntryPolicy(start_minute=600,end_minute=720,max_cycles=1).to_dict()}]
    out=native.S09NativeAdapter(Composition(data),protection_robot).describe("session_exit")
    assert out["parameters"]["InpExitOnSessionEnd"]=="true"
    assert out["parameters"]["InpSessionEnabled"]=="true"
    assert out["parameters"]["InpExitOnOpposite"]=="true"
    assert "session exit, temporal exit, dynamic virtual target, closed opposite signal" in out["protection_rules"]["precedence"]


def test_dynamic_virtual_target_maps_all_bounds_and_requires_partials_before_minimum(protection_robot):
    data=managed_composition()
    data["profit_exit"]={"mode":"dynamic_atr_v1","params":dict(atr_multiplier=2,minimum_r=2,maximum_r=4)}
    out=native.S09NativeAdapter(Composition(data),protection_robot).describe("dynamic")
    assert out["parameters"]["InpTargetMode"]=="2" and out["parameters"]["InpTargetPoints"]=="0"
    assert out["parameters"]["InpDynamicTargetATR"]=="2"
    assert out["parameters"]["InpDynamicTargetMinR"]=="2" and out["parameters"]["InpDynamicTargetMaxR"]=="4"
    assert "retain physical SL" in out["protection_rules"]["dynamic_target"]
    data["partial_exit_plan"]={"mode":"fixed_r","params":dict(trigger_r=2,fraction=.5,breakeven_after_confirmation=True)}
    with pytest.raises(ValueError,match="anteceder"):
        native.S09NativeAdapter(Composition(data),protection_robot)
    data["profit_exit"]["params"]["maximum_r"]=1
    with pytest.raises(ValueError,match="minimum_r"):
        Composition(data)


def test_inverted_signal_is_explicit_and_does_not_change_protection_or_sizing(protection_robot):
    data=composition().to_dict();data["orientation"]={"mode":"inverted","params":{}}
    out=native.S09NativeAdapter(Composition(data),protection_robot).describe("inverted")
    assert out["parameters"]["InpOrientation"]=="1"
    assert out["parameters"]["InpStopMode"]=="0" and out["parameters"]["InpTrailingMode"]=="0"
    assert out["parameters"]["InpStopPoints"]=="100" and out["parameters"]["InpTargetPoints"]=="200"
    assert out["capabilities"]["modules"]["orientation"]==["normal","inverted"]
