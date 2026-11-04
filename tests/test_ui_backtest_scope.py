"""Percurso All na GUI; dados próprios e nenhum terminal real."""
import time

from tests.test_ui_main_window import ui
from tests.test_ui_robot_controls import _tested
from tests.test_ui_preset_workflow import view
from tests.test_default_presets import db
from tests.test_s09_native_profile import robot
from tests.market_data import trend
from mt5_extracao.robots import store,backtest_scope as scope
from mt5_extracao.ui.robot_controls import SelectionDialog


def wait(ui, condition, seconds=30):
    deadline=time.monotonic()+seconds
    while not condition() and time.monotonic()<deadline:ui.pump(.05)
    assert condition()


def test_all_is_separate_from_terminal_tf_and_blocks_play_and_replay(ui,monkeypatch):
    panel,robot,preset=_tested(ui);calls=[]
    monkeypatch.setattr('mt5_extracao.ui.robot_controls.messagebox.showinfo',lambda *a,**k:calls.append(a))
    dialog=SelectionDialog(panel.controls,robot,'asset')
    assert dialog.tf_combo.cget('values')[-1]==scope.ALL
    dialog.tf.set(scope.ALL);dialog.apply()
    assert panel.controls.chosen[robot.key]==('WIN$N','H1',preset)
    assert robot.key in panel.controls.all_timeframes
    assert panel.controls.context(robot)['headline'] is None
    buttons=panel.controls.widgets[robot.key]['buttons']
    assert buttons['play'].instate(['disabled']) and buttons['replay'].instate(['disabled'])
    panel.controls.launch(robot,'play');panel.controls.launch(robot,'replay')
    assert len(calls)==2 and not panel.controls.sessions
    ui.toggle_theme();panel.render();assert robot.key in panel.controls.all_timeframes
    individual=SelectionDialog(panel.controls,robot,'asset');individual.tf.set('M5');individual.apply()
    assert robot.key not in panel.controls.all_timeframes and panel.controls.chosen[robot.key][1]=='M5'


def open_python(ui):
    ui.app.config.set('ROBOTS','batch_max_workers','1')
    ui.app.db_manager.save_ohlcv_data('WIN$N','1 hora',trend(350,freq='h'))
    ui.refresh_inventory();wait(ui,lambda:len(ui.symbol_series.get('WIN$N',[]))==2)
    panel=ui.robots_panel;panel.select('python:ema_atr');detail=panel.detail
    detail.timeframe_var.set(scope.ALL)
    dialog=detail.run_backtest()
    assert dialog is panel.controls.scope_windows['python:ema_atr']
    dialog.start.set('2020-01-01');dialog.end.set('2025-01-01')
    return dialog


def test_python_all_review_real_execution_progress_and_recorded_context(ui):
    dialog=open_python(ui)
    dialog.prepare();assert dialog.active
    wait(ui,lambda:not dialog.active)
    assert dialog.plan['eligible']==2 and not store.load_runs(ui.app.db_manager)
    assert not dialog.run_button.instate(['disabled'])
    ui.toggle_theme();dialog.execute()
    wait(ui,lambda:not dialog.active)
    wait(ui,lambda:all(dialog.tree.item(tf,'values')[2]=='Concluído' for tf in ('M1','H1')))
    rows=store.load_runs(ui.app.db_manager)
    assert len(rows)==2 and all(r['metrics']['backtest_scope']['batch_id']==dialog.plan['batch_id'] for r in rows)
    dialog.tree.selection_set('H1');dialog.show_detail()
    assert 'Run ' in dialog.detail.cget('text')
    ui.robots_panel.detail.show_run(next(r for r in rows if r['timeframe']=='H1'))
    assert dialog.plan['preset_identity']['content_hash'] in ui.robots_panel.detail.run_status.cget('text')
    dialog.close()


def test_partial_batch_resumes_after_catalog_refresh_without_repeating_runs(ui):
    dialog=open_python(ui);ui.app.config.set('ROBOTS','batch_max_jobs','1')
    dialog.prepare();wait(ui,lambda:not dialog.active)
    dialog.execute();wait(ui,lambda:not dialog.active)
    wait(ui,lambda:not ui.background_activity.pending)
    first=store.load_runs(ui.app.db_manager)
    assert len(first)==1 and dialog.run_button.cget('text')=='Retomar plano'
    assert ui.robots_panel.detail.robot is not dialog.robot
    assert dialog.is_current() and not dialog.run_button.instate(['disabled'])
    ui.app.config.set('ROBOTS','batch_max_jobs','2')
    dialog.execute();wait(ui,lambda:not dialog.active)
    final=store.load_runs(ui.app.db_manager)
    assert len(final)==2 and sum(r['run_id']==first[0]['run_id'] for r in final)==1
    assert scope.inspect_plan(ui.app.db_manager,dialog.plan['batch_id'])['status']=='completed'
    dialog.close()


def test_stale_context_refuses_execution_and_preview_can_be_cancelled(ui,monkeypatch):
    dialog=open_python(ui);dialog.prepare();wait(ui,lambda:not dialog.active)
    ui.robots_panel.detail.timeframe_var.set('1 hora');dialog.execute()
    assert 'mudou' in dialog.status.cget('text') and not store.load_runs(ui.app.db_manager)
    dialog.close()
    dialog=open_python(ui)
    def blocked(*args,guard,**kwargs):
        while True:
            guard();time.sleep(.01)
    monkeypatch.setattr(scope,'preview',blocked)
    dialog.prepare();assert dialog.active
    dialog.cancel_or_close();wait(ui,lambda:not dialog.active)
    assert dialog.exists() and 'cancelada' in dialog.status.cget('text')
    dialog.close()


def test_footer_remains_visible_at_two_scales_and_narrow_width(ui):
    dialog=open_python(ui);dialog.prepare();wait(ui,lambda:not dialog.active)
    original=float(ui.root.tk.call('tk','scaling'))
    try:
        for scale in (96/72,144/72):
            ui.root.tk.call('tk','scaling',scale)
            for size in ('600x500','980x680'):
                dialog.window.geometry(size);ui.toggle_theme();ui.pump(.2)
                dialog.viewport.canvas.yview_moveto(0);ui.pump(.1)
                left,top=dialog.window.winfo_rootx(),dialog.window.winfo_rooty()
                right,bottom=left+dialog.window.winfo_width(),top+dialog.window.winfo_height()
                for button in (dialog.prepare_button,dialog.run_button,dialog.cancel_button):
                    assert button.winfo_viewable()
                    assert left <= button.winfo_rootx() and button.winfo_rootx()+button.winfo_width() <= right
                    assert top <= button.winfo_rooty() and button.winfo_rooty()+button.winfo_height() <= bottom
                dialog.viewport.canvas.yview_moveto(1);ui.pump(.1)
                canvas=dialog.viewport.canvas
                assert dialog.tree.winfo_viewable() and dialog.tree.winfo_height() >= 40
                assert dialog.tree.winfo_rooty() < canvas.winfo_rooty()+canvas.winfo_height()
                assert dialog.tree.winfo_rooty()+dialog.tree.winfo_height() > canvas.winfo_rooty()
                assert dialog.run_button.winfo_rooty()+dialog.run_button.winfo_height() <= bottom
    finally:
        ui.root.tk.call('tk','scaling',original);dialog.close()


def test_default_manual_workflow_exports_bound_package_before_all_preview(view,monkeypatch):
    calls=[];view.controller.all_timeframes.add(view.robot.key)
    monkeypatch.setattr(view.controller,'open_scope',lambda *a,**k:calls.append(k))
    view.update_buttons();assert view.replay.instate(['disabled'])
    view.start_native('replay');assert 'individual' in view.status.get()
    view.start_native('backtest')
    assert len(calls)==1 and calls[0]['manifest_path'].is_file()
    assert calls[0]['start']=='2026-10-14' and calls[0]['end']=='2026-10-15'
