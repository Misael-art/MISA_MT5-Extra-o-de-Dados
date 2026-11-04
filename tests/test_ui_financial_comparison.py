"""Seleção manual, espera e exportação na interface real."""
from tests.test_ui_laboratory import panel
from tests.test_laboratory_financial import financial
from tests.test_mentions_contract import db


def test_financial_view_selects_runs_and_exports_without_execution(panel,financial,tmp_path,monkeypatch):
    from mt5_extracao.ui import financial_comparison
    from mt5_extracao.robots import backtest
    database,cfg,runs=financial;panel.ui.app.db_manager=database
    monkeypatch.setattr(backtest,'run_python',lambda *a,**k:(_ for _ in ()).throw(AssertionError('UI executed strategy')))
    panel.open_financial_comparison();view=panel.financial_comparison
    panel.pump(lambda:not view.pending,seconds=30)
    ids=[r['run_id'] for r in runs[:2]];view.index.tree.selection_set(ids);view.add()
    view.mode.set('Mesmo preset entre ativos');view.compare();assert view.pending and view.progress.winfo_manager()
    panel.pump(lambda:not view.pending,seconds=30)
    assert view.result is not None,view.status.cget('text')
    assert view.result['compatible'] and len(view.results.tree.get_children())==2
    out=tmp_path/'financial.html';monkeypatch.setattr(financial_comparison.filedialog,'asksaveasfilename',lambda **k:str(out))
    view.export('html');panel.pump(lambda:not view.pending,seconds=30);assert out.exists()
    view.window.geometry('650x500');panel.ui.root.update()
    view.chosen.tree.selection_set(ids[0]);view.remove();assert view.result is None
    view.close();assert not view.alive()
