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
