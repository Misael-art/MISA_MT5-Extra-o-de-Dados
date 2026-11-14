"""Perfil de apresentação versionado; preserva pacote Triplo1.00 sem migrar."""
from pathlib import Path
from dataclasses import replace

import pytest

from mt5_extracao.composition import native, Composition
from mt5_extracao.composition import super_atr_triple_native as triple
from mt5_extracao.robots import catalog, mql
from tests.test_default_presets import db
from tests.test_ui_preset_workflow import named, view


@pytest.fixture
def robot(tmp_path, monkeypatch):
    path=tmp_path/"HDK_SuperATRTriplo.mq5";path.write_text("// public synthetic plot-profile source\n")
    robot=catalog.Robot(triple.ROBOT_KEY,catalog.MQL5,"Triplo",version=triple.PLOT_ROBOT_VERSION,path=str(path),
        files=[str(path)],inputs=[mql.MqlInput(n,t,"0") for n,t in triple.PLOT_INPUT_TYPES.items()],missing_includes=["Canvas/Canvas.mqh"])
    monkeypatch.setattr(triple,"PLOT_APPROVED_FINGERPRINT",catalog.fingerprint(robot))
    return robot


def test_new_presentation_input_has_own_strategy_contract_and_complete_binding(robot):
    comp=native.default_composition(robot,symbol="EURUSD",timeframe="M5")
    assert comp.to_dict()["strategy"]["key"]=="super_atr_triple_plot"
    out=native.native_adapter(comp,robot).describe("plot_default")
    assert out["profile_version"]==2 and len(out["expected_native_inputs"])==14
    assert out["expected_native_inputs"]["InpPlotEnabled"] is False and not out["configuration_ready"]
    assert out["presentation"]["maximum_objects"]==768 and out["presentation"]["panel_toggle"]
    assert not out["presentation"]["native_visual_qualified"]
    data=comp.to_dict();data["strategy"]["params"].update(seed_start_broker="2026-11-01T00:00",plot_enabled=True)
    changed=native.native_adapter(Composition(data),robot).describe("plot_manual")
    assert changed["configuration_ready"] and changed["expected_native_inputs"]["InpPlotEnabled"] is True
    assert changed["fixed_constraints"]==out["fixed_constraints"]
    assert "strategy.params.plot_enabled" in changed["capabilities"]["bindings"]


def test_old_profile_keeps_frozen_inputs_pin_and_no_presentation_migration(robot,monkeypatch):
    old=replace(robot,version=triple.ROBOT_VERSION,inputs=[mql.MqlInput(n,t,"0") for n,t in triple.INPUT_TYPES.items()])
    monkeypatch.setattr(triple,"APPROVED_FINGERPRINT",catalog.fingerprint(old))
    comp=native.default_composition(old,symbol="EURUSD",timeframe="M5")
    assert "plot_enabled" not in comp.to_dict()["strategy"]["params"]
    installed=native.native_adapter(comp,old).describe("same_old")
    # O contrato público anterior permanece literal, inclusive seu pin/fingerprint.
    assert installed["profile_version"]==1 and "presentation" not in installed
    assert len(installed["parameters"])==13 and installed["hadouken_revision"]==triple.HADOUKEN_REVISION
    assert triple.ROBOT_VERSION=="1.00" and triple.HADOUKEN_REVISION=="42793107df40118080936e63196ad152f2bfb930"
    assert installed["parameters"]==dict(InpMagic="909202611",InpDeviationPoints="20",InpPresetId="same_old",
        InpConfigHash=comp.config_hash,InpRiskPct="0.500",InpSeedStart="1970.01.01 00:00",InpPeriod="20",InpMean="0",
        InpDeviation1="1.0",InpDeviation2="2.0",InpDeviation3="3.0",InpInitialSide="1",InpEntriesEnabled="false")


def test_corrected_panel_keeps_triple_plot_binding_with_a_new_source_identity(robot, monkeypatch):
    fixed = replace(robot, version=triple.PANEL_SCOPE_FIX_ROBOT_VERSION)
    monkeypatch.setattr(triple, "PANEL_SCOPE_FIX_FINGERPRINT", catalog.fingerprint(fixed))
    comp = native.default_composition(fixed, symbol="EURUSD", timeframe="M5")
    result = native.native_adapter(comp, fixed).describe("panel_scope_fix")
    assert result["profile_version"] == 3
    assert result["hadouken_revision"] == triple.PANEL_SCOPE_FIX_HADOUKEN_REVISION
    assert result["expected_native_inputs"]["InpPlotEnabled"] is False
    assert set(result["expected_native_inputs"]) == set(triple.PLOT_INPUT_TYPES)


def test_ui_plot_checkbox_saves_and_reopens_without_enabling_fixed_lots_or_partials(view):
    assert view.triplo and len(view.fields)==9 and not view.lots_enabled and not view.partials_enabled
    assert not view.fields["plot_enabled"].get()
    named(view);view.fields["seed_start_broker"].set("2026-11-01T00:00")
    view.fields["plot_enabled"].set(True);view.save.invoke();assert not view.dirty,view.status.get()
    view.reopen.invoke();assert view.fields["plot_enabled"].get()
    assert view.current.manifest["native"]["expected_native_inputs"]["InpPlotEnabled"] is True
    assert view.current.manifest["native"]["expected_native_inputs"]["InpEntriesEnabled"] is False
    entries=view.entries[:len(view.strategy_fields)]
    assert all(e.master is view.save.master for e in entries)
    assert int(view.save.grid_info()["row"])>max(int(e.grid_info()["row"]) for e in entries)
