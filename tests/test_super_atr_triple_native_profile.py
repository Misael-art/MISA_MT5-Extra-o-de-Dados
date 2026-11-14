"""Contrato de pacote/UI Triplo; fonte nativa simulada, sem terminal/conta."""
from dataclasses import replace
from pathlib import Path

import pytest

from mt5_extracao.composition import Composition, native
from mt5_extracao.composition import super_atr_triple_native as triple
from mt5_extracao.robots import catalog, mql
from tests.test_default_presets import db
from tests.test_ui_preset_workflow import named, view


@pytest.fixture
def robot(tmp_path, monkeypatch):
    path = tmp_path/"HDK_SuperATRTriplo.mq5"; path.write_text("// synthetic mapper source\n")
    value = catalog.Robot(triple.ROBOT_KEY, catalog.MQL5, "Triplo", version=triple.ROBOT_VERSION, path=str(path),
        files=[str(path)], inputs=[mql.MqlInput(name, kind, "0") for name, kind in triple.INPUT_TYPES.items()],
        missing_includes=["Canvas/Canvas.mqh"])
    monkeypatch.setattr(triple, "APPROVED_FINGERPRINT", catalog.fingerprint(value))
    return value


def configured(robot):
    data = native.default_composition(robot, symbol="EURUSD", timeframe="M5").to_dict()
    data["strategy"]["params"].update(seed_start_broker="2026-11-01T00:00", entries_enabled=True)
    return Composition(data)


def test_default_is_unconfigured_unarmed_and_cannot_start_session(robot, monkeypatch):
    from mt5_extracao.presets import execution
    composition = native.default_composition(robot, symbol="EURUSD", timeframe="M5")
    out = native.native_adapter(composition, robot).describe("default")
    assert not out["configuration_ready"] and not out["expected_native_inputs"]["InpEntriesEnabled"]
    assert out["expected_native_inputs"]["InpSeedStart"] == "1970.01.01 00:00"
    assert "primeira barra real" in out["configuration_reason"]
    assert native.runtime_binding_required(robot)
    data = composition.to_dict(); data["strategy"]["params"]["entries_enabled"] = True
    with pytest.raises(ValueError, match="primeira barra real"):
        native.native_adapter(Composition(data), robot)
    # Compile/import alone remain available; no implicit seed is selected.
    from mt5_extracao.composition.adapter import compatibility
    assert compatibility(composition)[0].field == "strategy"


def test_complete_typed_package_broker_clock_absolute_stop_and_no_other_management(robot):
    composition = configured(robot); out = native.native_adapter(composition, robot).describe("manual_triplo")
    assert out["configuration_ready"] and len(out["parameters"]) == len(out["receipt_fields"]) == 13
    assert set(out["expected_native_inputs"]) == set(triple.INPUT_TYPES)
    assert out["expected_native_inputs"]["InpSeedStart"] == "2026.11.01 00:00"
    assert out["expected_native_inputs"]["InpRiskPct"] == .5
    assert out["fixed_constraints"]["stop_anchor"] == "absolute_confirmed_curve1_outward_tick"
    assert out["fixed_constraints"]["target_anchor"] == "disabled_required_zero_tp"
    assert not out["fixed_constraints"]["same_bar_reverse"] and not out["operational_acceptance"]
    from mt5_extracao.presets import NATIVE_PRESET_KEYS
    assert robot.key in NATIVE_PRESET_KEYS


@pytest.mark.parametrize("changes", [dict(seed_start_broker="2026-11-01T00:01"), dict(seed_start_broker="2026-11-01T00:00Z"),
    dict(initial_side=0), dict(mean=2), dict(deviation2=1), dict(period=10001), dict(deviation1=float("nan"))])
def test_incompatible_parameters_refuse_instead_of_reseed_or_conversion(robot, changes):
    data = configured(robot).to_dict(); data["strategy"]["params"].update(changes)
    with pytest.raises(ValueError): native.native_adapter(Composition(data), robot)


def test_source_gate_rechecks_closure_version_inputs_and_external_include(robot):
    for changed in (replace(robot, version="1.01"), replace(robot, inputs=robot.inputs[:-1]),
                    replace(robot, missing_includes=["Canvas/Other.mqh"])):
        with pytest.raises(ValueError): native.default_composition(changed, symbol="EURUSD", timeframe="M5")
    adapter = native.native_adapter(configured(robot), robot)
    Path(robot.path).write_text("// source revised\n")
    with pytest.raises(ValueError): adapter.describe("manual")


def test_ui_default_manual_explicit_seed_save_reopen_and_export(view):
    assert view.triplo and len(view.fields) == 8
    assert view.fields["seed_start_broker"].get() == "" and not view.fields["entries_enabled"].get()
    assert view.backtest.instate(["disabled"])
    own = named(view)
    view.fields["seed_start_broker"].set("2026-11-01T00:00")
    view.fields["entries_enabled"].set(True); view.fields["period"].set("25")
    view.save.invoke(); assert not view.dirty, view.status.get()
    view.reopen.invoke()
    assert view.current.identity.id == own.identity.id and view.current.identity.revision == 2
    assert view.fields["entries_enabled"].get() and view.fields["seed_start_broker"].get() == "2026-11-01T00:00"
    assert view.current.manifest["native"]["configuration_ready"]
    assert int(view.save.grid_info()["row"]) > int(view.entries[-1].grid_info()["row"])
    view.export.invoke(); assert "Pacote exportado" in view.status.get(), view.status.get()
