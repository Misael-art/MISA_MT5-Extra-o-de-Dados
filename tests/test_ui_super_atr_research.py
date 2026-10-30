"""Geração explícita pela UI; arquivos preservados e cancelamento sem conta/ordem."""
from types import SimpleNamespace
import pytest
from mt5_extracao.ui.super_atr_research import SuperAtrResearch
from mt5_extracao.robots import catalog
from tests.test_ui_laboratory import panel
from tests.test_super_atr_research import framework


def form_for(panel,tmp_path,framework):
    reloads=[];owner=SimpleNamespace(frame=panel.frame,ui=panel.ui,robots=[],reload=lambda:reloads.append(True))
    form=SuperAtrResearch(owner);form.folder.set(str(tmp_path/'research'));form.framework.set(str(framework));form.seed.set('2025-01-01T00:00')
    return form,reloads


def test_research_form_generates_disabled_candidate_and_refreshes_catalog(panel,tmp_path,framework):
    form,reloads=form_for(panel,tmp_path,framework);form.submit_button.invoke()
    assert form.pending and form.progress.winfo_manager() and form.submit_button.instate(['disabled'])
    panel.pump(lambda:not form.pending,seconds=30)
    assert hasattr(form,'created_files'),form.status.cget('text')
    robot=catalog.robot_from_file(form.created_files[0]);assert next(i.default for i in robot.inputs if i.name=='InpResearchOrders')=='false'
    assert reloads and 'Entradas desligadas' in form.status.cget('text')


def test_close_cancels_before_generation_without_files(panel,tmp_path,framework,monkeypatch):
    form,reloads=form_for(panel,tmp_path,framework);jobs=[]
    monkeypatch.setattr(panel.ui,'run_bg',lambda work,done:jobs.append((work,done)))
    form.submit();form.close()
    from mt5_extracao.analytics.indicator_materializer import Cancelled
    with pytest.raises(Cancelled) as error:jobs[0][0]()
    jobs[0][1](None,error.value)
    assert not form.alive() and not reloads and not (tmp_path/'research/Experts').exists()
