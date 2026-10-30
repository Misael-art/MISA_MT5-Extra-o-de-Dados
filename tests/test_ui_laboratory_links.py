"""Percurso Tk para vínculos, matrizes/candle e exportação; modelos/rede ausentes."""
import pytest

from tests.test_ui_laboratory import panel
from tests.test_ui_laboratory_series import open_series
from tests.test_laboratory_links import linked, analyze


def test_dialog_selects_explicit_sources_and_shows_frozen_candle_and_matrix(panel,linked,tmp_path,monkeypatch):
    from mt5_extracao.ui import laboratory_links
    panel.ui.app.db_manager=linked[0]
    series=open_series(panel)
    panel.pump(lambda:not getattr(series,'_loading_sources',True))
    series.symbol.set('X');series.set_report(analyze(linked,timeline_basis='publication'))
    series.open_links(candle=2);dialog=series.links_dialog
    panel.pump(lambda:not dialog.pending)
    assert 'Janelas de publicação' in dialog.candle_text.get('1.0','end')
    assert '2025-01-01T10:03' in dialog.candle_text.get('1.0','end')
    assert dialog.tabs.select()==str(dialog.candle_tab)
    assert dialog.inventory.get_children()
    dialog.inventory.selection_set('0');dialog.relationship.set('Associação escolhida em QA')
    dialog.add()
    assert series.linked_target=='X' and len(series.linked_selections)==1
    assert series.linked_selections[0]['symbol']=='TOPIC:QA'
    dialog.calculate();panel.pump(lambda:not dialog.pending,seconds=30)
    assert dialog.result['analysis_id']==series.report['analysis_id']
    assert len(dialog.matrix.get_children())==min(100,len(series.report['channels'])**2)
    out=tmp_path/'report.html';monkeypatch.setattr(laboratory_links.filedialog,'asksaveasfilename',lambda **k:str(out))
    dialog.export();panel.pump(lambda:not dialog.pending,seconds=30)
    assert out.exists() and 'Relatório salvo' in dialog.status.cget('text')
    panel.ui.root.geometry('820x680');dialog.window.geometry('650x600');panel.ui.root.update()
    dialog.close();assert not dialog.alive()


def test_cancellation_preserves_previous_crossing_result_and_mapped_target(panel,linked,monkeypatch):
    from mt5_extracao.analytics.indicator_materializer import Cancelled
    panel.ui.app.db_manager=linked[0];series=open_series(panel)
    panel.pump(lambda:not getattr(series,'_loading_sources',True))
    series.symbol.set('X');series.set_report(analyze(linked));series.open_links()
    dialog=series.links_dialog;panel.pump(lambda:not dialog.pending)
    dialog.calculate();panel.pump(lambda:not dialog.pending,seconds=30);before=dialog.result
    jobs=[];monkeypatch.setattr(panel.ui,'run_bg',lambda work,done:jobs.append((work,done)))
    dialog.calculate();assert dialog.pending
    dialog.cancel_event.set()
    with pytest.raises(Cancelled) as e:jobs[0][0]()
    jobs[0][1](None,e.value)
    assert dialog.result is before and not dialog.pending
    series.linked_selections=[dict(features=[['x','1']],relationship='QA')];series.linked_target='Y'
    dialog.inventory.selection_set('0');dialog.add()
    assert 'outro ativo' in dialog.status.cget('text')
    dialog.close()
