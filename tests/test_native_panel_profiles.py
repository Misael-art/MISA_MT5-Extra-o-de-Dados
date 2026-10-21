"""Pins novos do painel; perfis antigos e restrições de execução preservados."""
from dataclasses import replace
from pathlib import Path
import pytest
from mt5_extracao.composition import native
from mt5_extracao.composition import pullback_native as pb
from mt5_extracao.robots import catalog
from tests.test_cooldown_native import robot as cooldown_robot, session_robot, atr_robot, dc_robot, er_robot, temporal_robot, partial_robot, lots_robot, legacy_robot
from tests.test_pullback_native_profile import robot as pullback_robot
from tests.test_ui_preset_workflow import view, named
from tests.test_volume_sizing import db


@pytest.mark.parametrize("family", ["s09", "pullback"])
def test_panel_profiles_preserve_operational_mapping_and_reject_changed_source(family, cooldown_robot, pullback_robot, tmp_path, monkeypatch):
    old = cooldown_robot if family == "s09" else pullback_robot
    module = native if family == "s09" else pb
    source = tmp_path / (family + "_panel.mq5")
    source.write_bytes(Path(old.path).read_bytes() + b"\n// new reviewed panel fixture\n")
    robot = replace(old, version=module.PANEL_ROBOT_VERSION, path=str(source), files=[str(source)],
                    missing_includes=["Canvas/Canvas.mqh"])
    monkeypatch.setattr(module, "PANEL_APPROVED_FINGERPRINT", catalog.fingerprint(robot))
    c = native.default_composition(robot, symbol="EURUSD", timeframe="H1")
    old_c = native.default_composition(old, symbol="EURUSD", timeframe="H1")
    assert c.to_dict() == old_c.to_dict()
    current = native.native_adapter(c, robot).describe("manual_panel")
    legacy = native.native_adapter(old_c, old).describe("manual_panel")
    assert current["parameters"] == legacy["parameters"]
    assert current["expected_native_inputs"] == legacy["expected_native_inputs"]
    assert current["capabilities"]["modules"] == legacy["capabilities"]["modules"]
    assert current["modality"] == "native_tester_demo_only"
    assert current["operational_acceptance"] is False
    assert current["hadouken_revision"] == module.PANEL_HADOUKEN_REVISION
    # O único include externo aprovado é o Canvas padrão; cabeçalho local ausente bloqueia.
    for missing in (["Framework/HADOUKEN/Safety/HDK_PanelHost.mqh"], ["Canvas/Outro.mqh"], ["../Canvas/Canvas.mqh"]):
        with pytest.raises(ValueError):
            native.native_adapter(c, replace(robot, missing_includes=missing))
    Path(robot.path).write_text("// changed after pin", encoding="utf-8")
    with pytest.raises(ValueError):
        native.native_adapter(c, robot)
    # Atualizar o perfil novo não substitui o pin/compatibilidade anterior.
    assert native.native_adapter(old_c, old).describe("old")["robot"]["version"] == old.version


@pytest.mark.parametrize("family", ["s09", "pullback"])
def test_unpinned_panel_fixture_is_denied(family, cooldown_robot, pullback_robot):
    old = cooldown_robot if family == "s09" else pullback_robot
    module = native if family == "s09" else pb
    unpinned = replace(old, version=module.PANEL_ROBOT_VERSION)
    with pytest.raises(ValueError):
        native.default_composition(unpinned, symbol="EURUSD", timeframe="H1")


@pytest.fixture(params=["s09", "pullback"])
def robot(request, cooldown_robot, pullback_robot, tmp_path, monkeypatch):
    old = cooldown_robot if request.param == "s09" else pullback_robot
    module = native if request.param == "s09" else pb
    path = tmp_path / (request.param + "_visual.mq5")
    path.write_bytes(Path(old.path).read_bytes() + b"\n// panel UI fixture\n")
    current = replace(old, version=module.PANEL_ROBOT_VERSION, path=str(path), files=[str(path)],
                      missing_includes=["Canvas/Canvas.mqh"])
    monkeypatch.setattr(module, "PANEL_APPROVED_FINGERPRINT", catalog.fingerprint(current))
    return current


def test_ui_new_profile_default_manual_reopen_and_export(view):
    assert view.lots_enabled and view.partials_enabled
    for name in ("temporal_enabled", "er_enabled", "donchian_enabled", "atr_enabled", "session_available", "cooldown_available"):
        assert getattr(view, name) is (view.robot.key == native.ROBOT_KEY), name
    default = view.current
    own = named(view)
    view.save.invoke()
    assert not view.dirty, view.status.get()
    view.reopen.invoke()
    assert view.current.identity.id == own.identity.id
    assert view.current.manifest["native"]["robot"]["fingerprint"] == catalog.fingerprint(view.robot)
    assert view.current.manifest["native"]["operational_acceptance"] is False
    view.export.invoke()
    assert "Pacote exportado" in view.status.get(), view.status.get()
    from mt5_extracao.presets import store
    assert store.get_preset(view.db, default.identity.id) == default


def test_ui_s09_panel_keeps_cooldown_and_partial_controls(view):
    if view.robot.key != native.ROBOT_KEY:
        assert view.pullback and not view.cooldown_available
        return
    from tests.test_volume_sizing import info
    view.ui.app.mt5_connector = type("Connector", (), {"get_symbol_info": lambda self, symbol: info()})()
    view.reopen.invoke()
    named(view)
    view.cooldown_seconds.set("3600")
    view.partial_mode.set("Fração (%)")
    view.partial_fields["trigger1_r"].set("0.5")
    view.partial_fields["quantity1"].set("50")
    view.partial_be.set(True)
    assert not view.preview_error, view.lot_preview.get()
    view.save.invoke()
    assert not view.dirty, view.status.get()
    view.reopen.invoke()
    assert view.cooldown_seconds.get() == "3600"
    assert view.current.manifest["native"]["expected_native_inputs"]["InpCooldownSeconds"] == 3600
    assert view.current.manifest["native"]["expected_native_inputs"]["InpPartialQuantity1"] == 0.5
    assert view.current.manifest["native"]["expected_native_inputs"]["InpPartialBreakeven"] is True
