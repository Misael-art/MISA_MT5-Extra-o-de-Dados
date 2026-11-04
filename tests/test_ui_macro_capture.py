"""Cliente real: captura explícita, cancelamento, fonte congelada e faixa categórica."""
import configparser
import pytest

from mt5_extracao.analytics import feature_store
from mt5_extracao.ui.macro_capture import MacroCapture
from tests.test_ui_laboratory import panel
from tests.test_ui_laboratory_series import open_series
from tests.test_macro_sources import db,response
from tests.test_macro_laboratory import phase


def prepare(panel,db):
    panel.ui.app.db_manager=db
    if not hasattr(panel.ui.app,'config'):panel.ui.app.config=configparser.ConfigParser()
    busy=[];saved=[]
    form=MacroCapture(panel.ui,panel.frame,db=db,on_busy=lambda *args:busy.append(args),on_saved=saved.append)
    form.start.set('2025-01-02');form.end.set('2025-01-04')
    return form,busy,saved


def test_macro_form_collects_and_freezes_through_sdk(panel,db,response):
    form,busy,saved=prepare(panel,db);form.submit_button.invoke()
    assert form.pending and form.submit_button.instate(['disabled'])
    panel.pump(lambda:not form.pending,seconds=30)
    assert not form.alive() and saved[0]['observations']==2 and saved[0]['snapshot']['manifest_id']
    assert busy[0][0] is True and busy[-1][0] is False
    assert len(feature_store.query_history(db))==2


def test_close_cancels_only_pending_macro_work_without_publishing(panel,db,response,monkeypatch):
    form,busy,saved=prepare(panel,db);jobs=[]
    monkeypatch.setattr(panel.ui,'run_bg',lambda work,done:jobs.append((work,done)))
    form.submit();form.close();assert form.pending and form.cancel_event.is_set()
    from mt5_extracao.analytics.indicator_materializer import Cancelled
    with pytest.raises(Cancelled) as exc:jobs[0][0]()
    jobs[0][1](None,exc.value)
    assert not form.alive() and not saved and feature_store.query_history(db).empty


def test_macro_periods_are_drawn_as_category_bands_not_numeric_signals(panel,db,phase):
    panel.ui.app.db_manager=db;series=open_series(panel);panel.pump(lambda:not series._loading_sources)
    report=phase();series.set_report(report)
    category=next(c for c in report['channels'] if c.get('linked_source'))
    series.detail_channel.set(category['id']);series._channel_changed();series.redraw()
    axis=series.panels['price'].chart.figure.axes[0]
    assert any(p.get_hatch()=='////' for p in axis.patches)
    assert any('Recessão' in t.get_text() for t in axis.texts)
    assert category['values']==[None]*len(report['times'])
