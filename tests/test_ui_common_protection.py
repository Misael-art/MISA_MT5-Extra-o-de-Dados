"""E14: formulário real usa o mesmo compilador; nenhuma sessão MT5 neste teste."""
import json

import pytest

from tests.test_default_presets import db
from tests.test_ui_preset_workflow import view, named
from mt5_extracao.composition import native
from mt5_extracao.robots import catalog, mql


@pytest.fixture
def robot(tmp_path,monkeypatch):
    source=tmp_path/"HDK_S09_Referencia.mq5"
    source.write_text("// fixture de mapper/UI, não é evidência MT5\n")
    robot=catalog.Robot(native.ROBOT_KEY,catalog.MQL5,"S09",version=native.PROTECTION_ROBOT_VERSION,
                        path=str(source),files=[str(source)],inputs=[mql.MqlInput(name,kind,"0")
                        for name,kind in native.PROTECTION_INPUT_TYPES.items()])
    monkeypatch.setattr(native,"PROTECTION_APPROVED_FINGERPRINT",catalog.fingerprint(robot))
    monkeypatch.setattr(native,"PROTECTION_HADOUKEN_REVISION","e"*40)
    return robot


def test_default_has_no_financial_toggle_and_manual_export_binds_atr_trailing_be_and_target_r(view):
    assert view.common_protection_available and not view.common_on.get()
    assert not view.postfill_available and "R pós-fill" not in view.common_widgets["target_mode"].cget("values")
    assert view.common_toggle.instate(["disabled"])
    assert all(widget.instate(["disabled"]) for widget in view.common_widgets.values())
    named(view)
    view.common_on.set(True)
    view.common_fields["stop_mode"].set("ATR14")
    view.common_fields["target_mode"].set("R planejado")
    view.common_fields["target_r"].set("3")
    view.common_fields["trailing_mode"].set("ATR14")
    view.common_fields["trailing_atr"].set("1,5")
    view.common_fields["trailing_trigger_r"].set("0,5")
    view.common_fields["breakeven_trigger_r"].set("1")
    view.common_exits["exit_on_opposite"].set(True)
    assert view.dirty and view.backtest.instate(["disabled"])
    assert view.common_widgets["stop_points"].instate(["disabled"])
    assert view.common_widgets["stop_atr"].instate(["!disabled"])
    view.save.invoke()
    assert view.current.identity.revision==2,view.status.get()
    data=view.current.composition.to_dict()
    assert data["protection"]["params"]["trailing_atr"]==1.5
    assert data["protection"]["params"]["stop_points"]==0
    assert data["profit_exit"]=={"mode":"planned_r_v1","params":{"target_r":3}}
    assert "ATR14" in view.lot_preview.get() and "indisponível" in view.lot_preview.get()
    view.export.invoke()
    paths=list(__import__("pathlib").Path(view.export_dir.get()).rglob("*.manifest.json"))
    assert len(paths)==1,view.status.get()
    manifest=json.loads(paths[0].read_text())
    assert manifest["native"]["parameters"]["InpTrailingATR"]=="1.5"
    assert len(manifest["native"]["parameters"])==59
    assert manifest["native"]["parameters"]["InpExitOnOpposite"]=="true"
    view.reopen.invoke()
    assert view.common_on.get() and view.common_fields["trailing_mode"].get()=="ATR14"
    assert view.common_fields["target_mode"].get()=="R planejado"
    assert view.execution is None


def test_invalid_managed_input_preserves_saved_revision_and_inactive_fields_are_not_read(view):
    original=named(view)
    view.common_on.set(True)
    view.common_fields["stop_points"].set("0")
    view.save.invoke()
    assert view.current==original and view.dirty and view.backtest.instate(["disabled"])
    view.common_fields["stop_points"].set("100")
    view.common_fields["stop_atr"].set("texto inativo")
    view.save.invoke()
    assert view.current.identity.revision==2,view.status.get()
    assert view.current.composition.to_dict()["protection"]["params"]["stop_atr"]==0


def test_dynamic_target_and_session_exit_remain_explicit_financial_choices(view):
    original=named(view);view.common_on.set(True)
    view.common_fields["target_mode"].set("ATR virtual")
    view.common_exits["exit_on_session_end"].set(True)
    view.save.invoke()
    assert view.current==original and "session_v1" in view.status.get()
    view.session_on.set(True)
    view.save.invoke()
    assert view.current.identity.revision==2,view.status.get()
    assert view.current.composition.to_dict()["profit_exit"]=={
        "mode":"dynamic_atr_v1","params":dict(atr_multiplier=2,minimum_r=2,maximum_r=4)}
    assert view.common_widgets["target_points"].instate(["disabled"])
    assert view.common_widgets["target_atr"].instate(["!disabled"])


def test_inversion_is_saved_without_implicitly_enabling_trailing_or_be(view):
    named(view);assert not view.common_on.get()
    view.signal_orientation.set("Invertida");view.save.invoke()
    data=view.current.composition.to_dict()
    assert data["orientation"]["mode"]=="inverted"
    assert data["protection"]["mode"]=="strategy_default" and not view.common_on.get()
    view.reopen.invoke();assert view.signal_orientation.get()=="Invertida"


def test_confirmed_management_is_displayed_with_request_and_effect_and_clears_on_context_change(view):
    proof=dict(status="reconciled_backtest",trades=1,functional_cycle=False,mode="backtest",net_profit=0,
               ledger=[],signals=[],inputs=dict(values={}),protection_acceptance=False,
               protection_events=[dict(kind="protection",at=1200,position=9007199254740999,before=90,requested=102,tp=140,
                                       confirmed=0,remaining=2,request_id=55,effect=3)])
    view.show_evidence(proof)
    rows=view.protection_events.get_children();assert len(rows) == 1
    values=view.protection_events.item(rows[0],"values")
    assert values[2] == "9007199254740999" and values[-2:] == ("55","SL observado")
    assert "Aceite das modalidades ainda pendente" in view.result.get()
    view.clear_result();assert not view.protection_events.get_children()
