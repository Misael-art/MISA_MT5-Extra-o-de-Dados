"""Consumo e publicação de uma janela real; nenhuma fórmula no widget."""
import pytest
from sqlalchemy import text

from mt5_extracao import sdk
from mt5_extracao.analytics import feature_store
from tests.test_ui_relations import relations_panel
from tests.test_ui_saved_relations import show_saved
from tests.test_reference_relation_windows import reference, phase
from tests.test_ui_relation_transfer import receiver


def count(db):
    with db.engine.connect() as c:return c.execute(text('SELECT COUNT(*) FROM _feature_relations')).scalar()


def prepare(panel,root,phase):
    db,image,cutoff=phase
    receipt=sdk.persist_feature_relation(db,image.manifest_id,left=('qa.x','1'),right=('qa.phase','1'),as_of=cutoff,
        bootstrap_replicates=29,synthetic_fixture=True)
    show_saved(panel,root,db);panel.saved_windows.invoke();root.update()
    viewer=panel._windows
    assert viewer.alive() and viewer.report is None
    viewer.options['window_bars'].set('80');viewer.options['stride_bars'].set('40')
    return viewer,receipt


def test_window_chart_and_table_show_sign_changes_without_automatic_persistence(relations_panel,phase):
    panel,root=relations_panel;viewer,_=prepare(panel,root,phase)
    before=count(phase[0]);viewer.calculate_button.invoke();root.update()
    assert len(viewer.table.tree.get_children())==3 and count(phase[0])==before
    assert [w['result']['relation']['effect'] for w in viewer.report['windows']]==pytest.approx([1.,0.,-1.],abs=1e-12)
    assert '+1,0000' in viewer.effect.cget('text') and 'Pares: 80' in viewer.sample.cget('text')
    assert 'p ajustado: 0,3.' in viewer.inference.cget('text')
    assert '3 testes' in viewer.population.cget('text') and 'Referência sintética' in viewer.inference.cget('text')
    viewer.table.tree.selection_set('window-2');viewer._selected(None);root.update()
    assert '−1' in viewer.effect.cget('text').replace('-','−') and viewer.selected['index']==2
    low,high=viewer._ax.get_ylim();assert low<=-1 and high>=1


def test_saved_window_has_exact_identity_and_range_in_parent(relations_panel,phase):
    panel,root=relations_panel;viewer,base=prepare(panel,root,phase)
    viewer.calculate();wanted=viewer.selected['analysis_id'];viewer.save_button.invoke();root.update()
    assert not viewer.alive() and viewer.saved_receipt['analysis_id']==wanted and count(phase[0])==2
    assert panel._saved_dto['relation_schema_version']==3 and panel._saved_dto['provenance']['analysis_id']==wanted
    assert 'Janela UTC' in panel.saved_scope.cget('text') and 'Pares: 80' in panel.saved_sample.cget('text')
    assert sdk.load_feature_relation(phase[0],base['analysis_id'])==base['relation']
    panel.open_relation_windows();root.update()
    assert panel._windows.recipe['decision_start']==viewer.saved_receipt['relation']['scope']['settings']['decision_start']


def test_changed_parameters_clear_previous_numbers_and_disable_save(relations_panel,phase):
    panel,root=relations_panel;viewer,_=prepare(panel,root,phase);viewer.calculate()
    viewer.options['seed'].set('8');root.update()
    assert viewer.report is None and viewer.selected is None and viewer.save_button.instate(['disabled'])
    assert viewer.effect.cget('text')=='Calcule as janelas' and not viewer.table.tree.get_children()
    assert count(phase[0])==1


def test_late_calculation_cannot_replace_new_parameters(relations_panel,phase):
    panel,root=relations_panel;viewer,_=prepare(panel,root,phase)
    queued=[];viewer.run_bg=lambda work,done:queued.append((work,done))
    viewer.calculate();viewer.calculate();assert len(queued)==1
    result,error=panel._call(queued[0][0]);viewer.options['seed'].set('8')
    queued[0][1](result,error);root.update()
    assert viewer.report is None and not viewer.pending and 'Parâmetros mudaram' in viewer.status.cget('text')
    assert count(phase[0])==1


def test_cancel_and_close_wait_for_query_confirmation(relations_panel,phase):
    panel,root=relations_panel;viewer,_=prepare(panel,root,phase)
    queued=[];viewer.run_bg=lambda work,done:queued.append((work,done))
    viewer.calculate();viewer.close()
    assert viewer.pending and viewer.alive() and viewer._cancel.is_set()
    queued[0][1](*panel._call(queued[0][0]));root.update()
    assert not viewer.alive() and count(phase[0])==1


def test_cancel_during_window_publication_rolls_back_only_new_artifact(relations_panel,phase,monkeypatch):
    panel,root=relations_panel;viewer,_=prepare(panel,root,phase);viewer.calculate()
    original=feature_store.save_pair_relation
    def stop(db,dto,**kwargs):return original(db,dto,**kwargs,transaction_hook=lambda conn:viewer._cancel.set())
    monkeypatch.setattr(feature_store,'save_pair_relation',stop)
    viewer.save();root.update()
    assert viewer.alive() and not viewer.pending and count(phase[0])==1
    assert 'cancelada' in viewer.status.cget('text') and viewer.selected is not None


def test_late_cancel_after_commit_preserves_and_displays_saved_window(relations_panel,phase,monkeypatch):
    panel,root=relations_panel;viewer,_=prepare(panel,root,phase);viewer.calculate()
    original=sdk.persist_feature_relation
    def committed(*args,**kwargs):result=original(*args,**kwargs);viewer._cancel.set();return result
    monkeypatch.setattr(sdk,'persist_feature_relation',committed)
    viewer.save();root.update()
    assert not viewer.alive() and count(phase[0])==2 and panel._saved_dto['relation_schema_version']==3


def test_resource_policy_is_fresh_and_retry_keeps_the_same_window_identity(relations_panel,phase):
    panel,root=relations_panel;viewer,_=prepare(panel,root,phase);viewer.calculate();wanted=viewer.selected['analysis_id']
    panel.write_guard_factory=lambda:lambda:'controlled quota'
    viewer.save();root.update()
    assert viewer.alive() and count(phase[0])==1 and 'controlled quota' in viewer.status.cget('text')
    panel.write_guard_factory=lambda:None
    viewer.save();root.update()
    assert viewer.saved_receipt['analysis_id']==wanted and count(phase[0])==2


def test_database_change_before_save_is_refused(relations_panel,phase,receiver):
    panel,root=relations_panel;viewer,_=prepare(panel,root,phase);viewer.calculate();panel.db=receiver
    viewer.save();root.update()
    assert viewer.alive() and not viewer.pending and count(phase[0])==1
    assert 'Base alterada' in viewer.status.cget('text')


def test_database_change_after_commit_does_not_apply_old_window_to_new_context(relations_panel,phase,receiver):
    panel,root=relations_panel;viewer,_=prepare(panel,root,phase);viewer.calculate()
    queued=[];viewer.run_bg=lambda work,done:queued.append((work,done))
    viewer.save();result,error=panel._call(queued[0][0]);panel.db=receiver
    queued[0][1](result,error);root.update()
    assert count(phase[0])==2 and panel._saved_dto is None and 'contexto original' in panel.saved_status.cget('text')


def test_insufficient_and_empty_windows_are_explicit_without_zero_imputation(relations_panel,phase):
    panel,root=relations_panel;viewer,_=prepare(panel,root,phase)
    viewer.options['window_bars'].set('40');viewer.calculate();root.update()
    assert len(viewer.report['windows'])==4 and viewer.effect.cget('text').startswith('Indisponível')
    assert all(w['result']['relation']['effect'] is None for w in viewer.report['windows'])
    viewer.options['window_bars'].set('200');viewer.calculate();root.update()
    assert not viewer.table.tree.get_children() and viewer.save_button.instate(['disabled'])
    assert 'não contém' in viewer.status.cget('text') and count(phase[0])==1


def test_invalid_field_does_not_dispatch_and_child_windows_do_not_overlap(relations_panel,phase):
    panel,root=relations_panel;viewer,_=prepare(panel,root,phase)
    queued=[];viewer.run_bg=lambda *args:queued.append(args)
    viewer.options['window_bars'].set('invalid');viewer.calculate()
    assert not queued and not viewer.pending and 'número válido' in viewer.status.cget('text')
    panel.open_relation_creator();assert panel._creator is None
    panel.open_relation_windows();assert panel._windows is viewer


def test_saving_preserves_the_execution_budget_used_to_calculate_the_window(relations_panel,phase,monkeypatch):
    panel,root=relations_panel;viewer,_=prepare(panel,root,phase)
    # Este caso verifica o encaminhamento do orçamento, independente do RSS herdado da suíte.
    from mt5_extracao.analytics import producer_jobs
    monkeypatch.setattr(producer_jobs,'current_rss_bytes',lambda:128*1024**2)
    viewer.options['timeout_seconds'].set('25');viewer.options['max_memory_mb'].set('1024');viewer.calculate()
    original=sdk.persist_feature_relation;seen=[]
    def publish(*args,**kwargs):seen.append(kwargs);return original(*args,**kwargs)
    monkeypatch.setattr(sdk,'persist_feature_relation',publish)
    viewer.save();root.update()
    assert seen[0]['timeout_seconds']==25 and seen[0]['max_memory_mb']==1024
    assert viewer.saved_receipt['relation']['provenance']['limits']['max_memory_mb']==1024
