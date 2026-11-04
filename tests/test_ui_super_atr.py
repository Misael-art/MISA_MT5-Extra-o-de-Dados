"""Fluxo Tk de materialização; adaptador controlado explicitamente sintético."""
import configparser
import pytest
from mt5_extracao.analytics import feature_store
from tests.test_super_atr_materializer import db,adapter,prices
from tests.test_ui_laboratory import panel
from tests.test_ui_laboratory_series import open_series


def prepare(panel,db):
    prices(db);panel.ui.app.db_manager=db
    if not hasattr(panel.ui.app,'config'):panel.ui.app.config=configparser.ConfigParser()
    series=open_series(panel);panel.pump(lambda:not series._loading_sources)
    series.symbol.set('QA');series._symbol_changed();series.open_links();view=series.links_dialog
    panel.pump(lambda:not view.pending);view.open_super_atr();form=view.super_atr_capture
    form.framework.set('CONTROLLED_TEST_SOURCE');form.period.set('3')
    return view,form


def test_materializes_and_freezes_visible_inventory_with_wait_indicator(panel,db,adapter):
    view,form=prepare(panel,db);form.submit_button.invoke()
    assert form.pending and view.pending and form.progress.winfo_manager()
    panel.pump(lambda:not view.pending,seconds=30)
    assert not form.alive(),form.status.cget('text')
    receipt=view.last_super_atr_receipt
    assert receipt['counts']['inserted']==24 and receipt['snapshot']['manifest_id']
    assert view.query.get()=='super' and len(view.inventory.get_children())==4
    assert feature_store.query_as_of(db,'2025-01-01T00:06Z').empty


def test_close_cancels_pending_materialization_without_false_facts(panel,db,adapter,monkeypatch):
    view,form=prepare(panel,db);jobs=[]
    monkeypatch.setattr(panel.ui,'run_bg',lambda work,done:jobs.append((work,done)))
    form.submit();form.close()
    from mt5_extracao.analytics.indicator_materializer import Cancelled
    with pytest.raises(Cancelled) as exc:jobs[0][0]()
    jobs[0][1](None,exc.value)
    assert not form.alive() and not view.pending and feature_store.query_history(db).empty
