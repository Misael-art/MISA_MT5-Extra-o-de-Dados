"""Consulta, seleção, espera e alcance da comparação real na UI existente."""
import threading

from tests.test_ui_main_window import ui
from tests.test_ui_backtest_scope import wait
from tests.test_scope_comparison import completed_scope
from mt5_extracao import sdk
from mt5_extracao.robots import backtest
from mt5_extracao.ui.scope_comparison import ScopeComparisonDialog


def test_existing_scope_read_select_and_export_without_running(ui,completed_scope,tmp_path,monkeypatch):
    db,_,_,plan=completed_scope;monkeypatch.setattr(ui.app,'db_manager',db)
    monkeypatch.setattr(backtest,'run_python',lambda *a,**k:(_ for _ in ()).throw(AssertionError('UI executed strategy')))
    view=ScopeComparisonDialog(ui,plan['batch_id']);assert view.pending and view.progress.winfo_manager()
    wait(ui,lambda:not view.pending)
    assert view.result is not None,view.status.cget('text')
    assert view.result['counts']['completed']==2 and len(view.table.tree.get_children())==21
    ident=next(i for i,c in view.cells.items() if c['state']=='completed');cell=view.cells[ident]
    view.tabs.select(1);ui.pump(.2)
    from matplotlib.backend_bases import MouseEvent
    x,y=view._matrix_axis.transData.transform((view.result['timeframes'].index(cell['timeframe']),view.result['symbols'].index(cell['symbol'])))
    event=MouseEvent('button_press_event',view.matrix_chart.canvas,x,y,button=1)
    view.matrix_chart.canvas.callbacks.process('button_press_event',event)
    assert view.table.tree.selection()==(ident,) and view.tabs.index('current')==3
    view.table.tree.selection_set(ident);view.inspect(reveal=True);wait(ui,lambda:not ui.background_activity.pending)
    assert view.tabs.index('current')==3 and cell['run_id'] in view.detail.cget('text')
    assert '0.0' in view.detail.cget('text') or '0' in view.detail.cget('text')
    target=tmp_path/'scope.html';monkeypatch.setattr('mt5_extracao.ui.scope_comparison.filedialog.asksaveasfilename',lambda **k:str(target))
    view.export();wait(ui,lambda:not view.pending);assert target.exists()
    view.load();wait(ui,lambda:not view.pending);assert not view.receipt.get('1.0','end').strip()
    view.close();assert not view.alive()


def test_themes_scales_metrics_and_narrow_window_keep_actions_reachable(ui,completed_scope,monkeypatch):
    db,_,_,plan=completed_scope;monkeypatch.setattr(ui.app,'db_manager',db)
    view=ScopeComparisonDialog(ui,plan['batch_id']);wait(ui,lambda:not view.pending)
    original=float(ui.root.tk.call('tk','scaling'))
    try:
        for scale in (96/72,144/72):
            ui.root.tk.call('tk','scaling',scale)
            view.window.geometry('600x500');ui.toggle_theme();ui.pump(.2)
            left,top=view.window.winfo_rootx(),view.window.winfo_rooty();right,bottom=left+view.window.winfo_width(),top+view.window.winfo_height()
            assert view.refresh.winfo_viewable() and left<=view.refresh.winfo_rootx()<right
            assert top<=view.refresh.winfo_rooty() and view.refresh.winfo_rooty()+view.refresh.winfo_height()<=bottom
            for tab in range(4):
                view.tabs.select(tab);view.views[tab].canvas.yview_moveto(1);ui.pump(.1)
                if tab<3:
                    from matplotlib.colors import to_rgba
                    chart=(view.summary_chart,view.matrix_chart,view.curves)[tab]
                    expected=to_rgba(ui.theme.colors['surface'])
                    assert chart.widget.winfo_width()<=view.views[tab].canvas.winfo_width()
                    assert chart.figure.get_facecolor()==expected
                    assert tuple(chart.canvas.buffer_rgba().cast('B')[:4])==tuple(round(v*255) for v in expected)
                    if tab==1:
                        counts=sorted((t.get_window_extent(chart.canvas.get_renderer()) for t in chart.figure.axes[0].texts if t.get_text().startswith('n=')),key=lambda box:box.x0)
                        assert all(left.x1<=right.x0 for left,right in zip(counts,counts[1:]))
        view.tabs.select(0);view.metric.set('Fator de lucro');ui.pump(.1)
        assert not view.summary_chart.figure.axes[0].patches
    finally:
        ui.root.tk.call('tk','scaling',original);view.close()


def test_close_cancels_query_and_removes_theme_consumer(ui,completed_scope,monkeypatch):
    db,_,_,plan=completed_scope;monkeypatch.setattr(ui.app,'db_manager',db);entered=threading.Event();cancelled=threading.Event()
    def waiting(db,ident,*,guard):
        entered.set()
        try:
            while True:guard();cancelled.wait(.01)
        finally:cancelled.set()
    monkeypatch.setattr(sdk,'compare_backtest_scope',waiting)
    before=len(ui.theme._listeners);view=ScopeComparisonDialog(ui,plan['batch_id']);wait(ui,entered.is_set)
    view.close();wait(ui,cancelled.is_set)
    assert not view.alive() and len(ui.theme._listeners)==before


def test_legacy_run_without_scope_keeps_history_readable(ui,completed_scope,monkeypatch):
    from copy import deepcopy
    from mt5_extracao.robots import store
    db,_,_,plan=completed_scope;monkeypatch.setattr(ui.app,'db_manager',db)
    record=deepcopy(store.load_runs(db)[0]);record['metrics']['backtest_scope']=None
    ui.robots_panel.select('python:ema_atr');detail=ui.robots_panel.detail;detail.show_run(record)
    assert detail.shown_run['run_id']==record['run_id'] and detail.scope_compare_button.instate(['disabled'])
