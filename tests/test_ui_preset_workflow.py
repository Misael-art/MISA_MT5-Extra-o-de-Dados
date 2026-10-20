"""T20.22: actions Tk connected to real preset services; native boundary is fake."""
import configparser
from pathlib import Path
from types import SimpleNamespace
import pytest
from tests.test_s09_native_profile import robot
from tests.test_default_presets import db
from mt5_extracao.presets import manual,store
from mt5_extracao.ui.robot_controls import CardControls

@pytest.fixture
def view(robot,db,tmp_path):
    import tkinter as tk
    from tkinter import ttk
    from mt5_extracao.ui.theme import Theme
    from mt5_extracao.ui.preset_workflow import PresetWorkflow
    try:root=tk.Tk()
    except tk.TclError as error:pytest.skip(f"sem display gráfico: {error}")
    callbacks=[]
    root.report_callback_exception=lambda kind,value,trace:callbacks.append(value)
    config=configparser.ConfigParser();config.read_string("[STRATEGY]\ninitial_equity=100000\n")
    def bg(action,done):
        try:value=action()
        except Exception as error:done(None,error)
        else:done(value,None)
    ui=SimpleNamespace(root=root,theme=Theme(root),app=SimpleNamespace(db_manager=db,config=config,offline=False),run_bg=bg)
    panel=SimpleNamespace(ui=ui,theme=ui.theme,frame=ttk.Frame(root),render=lambda:None,reload=lambda:None)
    controller=CardControls(panel);controller.chosen[robot.key]=("EURUSD","M5",None)
    window=controller.open_presets(robot)
    assert window is not None
    window.export_dir.set(str(tmp_path/"exports"));window.start.set("2026-10-14");window.end.set("2026-10-15")
    yield window
    root.destroy()
    assert not callbacks,callbacks
    import gc
    gc.collect()


def named(view):
    view.clone.invoke();dialog=view.name_dialog
    assert dialog.save.instate(["disabled"])
    dialog.name.set("Piloto S09");assert not dialog.save.instate(["disabled"])
    assert "__Manual__Piloto-S09__" in dialog.preview.cget("text")
    dialog.save.invoke()
    assert view.current.identity.label=="Piloto S09",view.status.get()
    return view.current


def test_default_readonly_manual_named_edit_reopen_rename_export(view):
    default=view.current
    assert default.identity.category=="Default" and all(e.instate(["disabled"]) for e in view.entries)
    own=named(view)
    assert all(not e.instate(["disabled"]) for e in view.entries)
    view.fields["fast"].set("21")
    assert view.dirty and view.backtest.instate(["disabled"]) and view.export.instate(["disabled"])
    view.save.invoke();assert view.current.identity.revision==2,view.status.get()
    view.reopen.invoke();assert view.current.identity.id==own.identity.id and view.fields["fast"].get()=="21"
    assert store.get_preset(view.db,default.identity.id)==default
    view.rename.invoke();view.name_dialog.name.set("Novo nome");view.name_dialog.save.invoke()
    assert view.current.identity.label=="Novo nome" and view.current.identity.revision==2
    view.export.invoke();assert "Pacote exportado" in view.status.get(),view.status.get()
    assert len(list(Path(view.export_dir.get()).rglob("*.manifest.json")))==1


def test_invalid_edit_preserves_saved_revision_and_error_is_actionable(view):
    own=named(view);view.fields["fast"].set("999");view.save.invoke()
    assert view.dirty and view.current==own
    assert manual.get_manual(view.db,own.identity.id)==own
    assert view.status.get() and view.backtest.instate(["disabled"])


def test_offline_and_stale_selection_do_not_launch_terminal(view,monkeypatch):
    from mt5_extracao.ui import preset_workflow
    calls=[]
    monkeypatch.setattr(preset_workflow.execution,"PresetExecution",lambda *a,**k:calls.append(k))
    view.ui.app.offline=True;view.update_buttons();assert view.backtest.instate(["disabled"])
    view.start_native("backtest");assert "sem --offline" in view.status.get()
    view.ui.app.offline=False;view.controller.chosen[view.robot.key]=("EURUSD","H1",None)
    view.start_native("backtest");assert "Ativo/TF mudou" in view.status.get() and not calls


def test_card_launch_reuses_workflow_window_and_blocks_live(view,monkeypatch):
    from mt5_extracao.ui import robot_controls
    calls=[];monkeypatch.setattr(robot_controls.messagebox,"showinfo",lambda *a,**k:calls.append(a))
    assert view.controller.launch(view.robot,"backtest") is view
    view.controller.launch(view.robot,"play");assert "Ao vivo indisponível" in calls[0][1]


def test_execution_registered_for_existing_stop_and_zero_activity_is_not_acceptance(view,monkeypatch):
    from mt5_extracao.ui import preset_workflow
    from tests.test_preset_execution import FakeSession
    from mt5_extracao.presets import execution
    from mt5_extracao.robots import store as robot_store
    named(view)
    original=execution.PresetExecution
    root=Path(view.export_dir.get()).parent/"sessions"
    monkeypatch.setattr(execution.sessions,"Session",FakeSession)
    # FakeSession has no UI lifecycle properties; explicitly model them here.
    monkeypatch.setattr(FakeSession,"active",property(lambda s:s.state in ("preparing","running")),raising=False)
    monkeypatch.setattr(FakeSession,"fault","zero")
    monkeypatch.setattr(preset_workflow.execution,"PresetExecution",lambda *a,**k:original(*a,root=root,**k))
    view.backtest.invoke()
    assert view.execution is not None,view.status.get()
    assert view.controller.sessions[view.robot.key] is view.execution.session
    assert view.execution.status=="contract_only",view.execution.error
    assert "nenhum negócio" in view.result.get() and not view.last_evidence["functional_cycle"]
    assert len(view.inputs.get_children())==10
    view.consult();assert "nenhum negócio" in view.result.get()
    proof=view.last_evidence
    view.execution=None;view.clear_result();view.consult()
    assert view.last_evidence==proof and view.artifacts.get("artifact_directory")
    assert not view.files.instate(["disabled"])


def test_actions_reflow_inside_small_viewport_without_clipping(view):
    view.window.geometry("650x550")
    for _ in range(5):view.window.update()
    for button in (view.reopen,view.clone,view.rename,view.export,view.import_button,view.backtest,view.replay,view.stop):
        assert button.winfo_manager()=="grid"
        assert button.winfo_width()>0
        assert button.winfo_x()+button.winfo_width()<=button.master.winfo_width()
    assert view.body.winfo_width()<=view.window.winfo_width()
