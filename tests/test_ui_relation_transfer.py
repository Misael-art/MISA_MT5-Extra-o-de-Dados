"""Transferência pelo painel real; SDK, cancelamento e contexto único."""
import json

import pytest
from sqlalchemy import inspect, text

from mt5_extracao import sdk
from mt5_extracao.analytics import datasets, feature_store
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.ui import relations
from tests.test_ui_relations import relations_panel
from tests.test_ui_saved_relations import show_saved
from tests.test_saved_relation_queries import saved_base, saved_db


@pytest.fixture
def receiver(tmp_path):
    db=DatabaseManager(db_path=str(tmp_path/'receiver.sqlite'),broker_utc_offset=0)
    datasets.declare_source(db,dict(provider='destination',server='Synthetic destination'))
    try:yield db
    finally:db.close()


def archive(panel,root,db,path):
    show_saved(panel,root,db)
    dto=panel._saved_dto
    sdk.export_feature_relation(db,dto['provenance']['analysis_id'],path)
    return dto


def select_receiver(panel,root,db):
    panel.db=db;panel.load_saved_relations();root.update()
    assert panel._saved_dto is None


def artifact_count(db):
    with db.engine.connect() as c:
        return c.execute(text('SELECT COUNT(*) FROM _feature_relations')).scalar() if inspect(c).has_table('_feature_relations') else 0


def test_export_requires_verified_selection_and_uses_file_dialog(relations_panel,saved_db,tmp_path,monkeypatch):
    panel,root=relations_panel
    assert panel.saved_export.instate(['disabled']) and panel.saved_import.instate(['disabled'])
    show_saved(panel,root,saved_db)
    dto=panel._saved_dto;path=tmp_path/'selected.json';choices=[]
    def choose(**kwargs):choices.append(kwargs);return str(path)
    monkeypatch.setattr(relations.filedialog,'asksaveasfilename',choose)
    panel.saved_export.invoke();root.update()
    assert choices[0]['parent'] is panel.frame and choices[0]['confirmoverwrite'] is False
    assert json.loads(path.read_text(encoding='utf-8'))['receipt']['relation']==dto
    assert 'Exportação concluída' in panel.saved_status.cget('text') and not panel._transfer_pending
    assert panel._transfer_result['path']==str(path) and panel.saved_cancel.instate(['disabled'])


def test_cancelled_file_dialog_dispatches_nothing(relations_panel,saved_db,monkeypatch):
    panel,root=relations_panel;show_saved(panel,root,saved_db)
    queued=[];panel._run_bg=lambda *args:queued.append(args)
    monkeypatch.setattr(relations.filedialog,'askopenfilename',lambda **kwargs:'')
    monkeypatch.setattr(relations.filedialog,'asksaveasfilename',lambda **kwargs:'')
    panel.saved_import.invoke();panel.saved_export.invoke()
    assert not queued and not panel._transfer_pending


def test_import_selects_exact_receipt_and_repeat_keeps_original(relations_panel,saved_db,receiver,tmp_path,monkeypatch):
    panel,root=relations_panel;path=tmp_path/'pair.json';dto=archive(panel,root,saved_db,path)
    select_receiver(panel,root,receiver);origin=datasets.identity(receiver)
    monkeypatch.setattr(relations.filedialog,'askopenfilename',lambda **kwargs:str(path))
    panel.saved_import.invoke();root.update()
    first=panel._transfer_result['result']
    assert first['status']=='INSERTED' and first['relation']==panel._saved_dto==dto
    assert len(panel._saved_rows)==1 and 'importada selecionada' in panel.saved_status.cget('text')
    assert sdk.load_feature_relation(receiver,first['analysis_id'])==dto
    panel.import_saved_relation(path);root.update()
    second=panel._transfer_result['result']
    assert second==dict(first,status='UNCHANGED') and artifact_count(receiver)==1
    assert 'já existente' in panel.saved_status.cget('text') and datasets.identity(receiver)==origin


def test_failed_export_preserves_user_file_and_current_value(relations_panel,saved_db,tmp_path):
    panel,root=relations_panel;show_saved(panel,root,saved_db)
    value=panel.saved_effect.cget('text');path=tmp_path/'owner.json';path.write_bytes(b'owner original')
    panel.export_saved_relation(path);root.update()
    assert path.read_bytes()==b'owner original' and panel.saved_effect.cget('text')==value
    assert 'difere' in panel.saved_status.cget('text') and not panel._transfer_pending
    assert panel.saved_export.instate(['!disabled']) and panel.saved_import.instate(['!disabled'])


def test_invalid_import_does_not_replace_the_selected_result(relations_panel,saved_db,tmp_path):
    panel,root=relations_panel;show_saved(panel,root,saved_db)
    original=panel._saved_dto;path=tmp_path/'invalid.json';path.write_text('{}',encoding='utf-8')
    panel.import_saved_relation(path);root.update()
    assert 'Importação:' in panel.saved_status.cget('text') and panel._saved_dto==original
    assert artifact_count(saved_db)==3 and not panel._transfer_pending


def test_pending_transfer_blocks_duplicate_jobs_and_cancel_is_explicit(relations_panel,saved_db,receiver,tmp_path):
    panel,root=relations_panel;path=tmp_path/'pair.json';archive(panel,root,saved_db,path)
    select_receiver(panel,root,receiver)
    queued=[];panel._run_bg=lambda work,done:queued.append((work,done))
    panel.import_saved_relation(path)
    assert panel._transfer_pending and panel.saved_cancel.instate(['!disabled'])
    assert panel.saved_import.instate(['disabled']) and panel.saved_new.instate(['disabled'])
    assert all(e.instate(['disabled']) for e in panel._saved_filter_entries)
    assert str(panel.saved_table.tree.cget('selectmode'))=='none'
    panel.import_saved_relation(path);panel.load_saved_relations();panel.open_relation_creator()
    assert len(queued)==1 and panel._creator is None
    panel.saved_cancel.invoke()
    assert panel._transfer_pending and 'Cancelando' in panel.saved_status.cget('text')
    queued[0][1](*panel._call(queued[0][0]))
    assert not panel._transfer_pending and artifact_count(receiver)==0 and 'cancelada' in panel.saved_status.cget('text')
    assert panel.saved_import.instate(['!disabled']) and panel.saved_cancel.instate(['disabled'])
    assert str(panel.saved_table.tree.cget('selectmode'))==panel._saved_selectmode


def test_cancel_inside_transaction_rolls_back_all_imported_origins(relations_panel,saved_db,receiver,tmp_path,monkeypatch):
    panel,root=relations_panel;path=tmp_path/'pair.json';archive(panel,root,saved_db,path);select_receiver(panel,root,receiver)
    original=feature_store._save_pair_relation
    def stop(*args,**kwargs):return original(*args,**kwargs,transaction_hook=lambda conn:panel._transfer_cancel.set())
    monkeypatch.setattr(feature_store,'_save_pair_relation',stop)
    panel.import_saved_relation(path);root.update()
    assert 'cancelada' in panel.saved_status.cget('text') and artifact_count(receiver)==0
    with receiver.engine.connect() as c:
        for table in ('_dataset_blocks','_dataset_manifests','_dataset_references'):
            assert c.execute(text('SELECT COUNT(*) FROM '+table)).scalar()==0


def test_cancel_after_commit_keeps_confirmed_receipt_visible(relations_panel,saved_db,receiver,tmp_path,monkeypatch):
    panel,root=relations_panel;path=tmp_path/'pair.json';dto=archive(panel,root,saved_db,path);select_receiver(panel,root,receiver)
    original=sdk.import_feature_relation
    def commit_then_cancel(*args,**kwargs):
        result=original(*args,**kwargs);panel._transfer_cancel.set();return result
    monkeypatch.setattr(sdk,'import_feature_relation',commit_then_cancel)
    panel.import_saved_relation(path);root.update()
    assert artifact_count(receiver)==1 and panel._saved_dto==dto
    assert 'importada selecionada' in panel.saved_status.cget('text') and not panel._transfer_pending


def test_resource_policy_is_created_for_each_operation_and_can_retry(relations_panel,saved_db,receiver,tmp_path):
    panel,root=relations_panel;path=tmp_path/'pair.json';archive(panel,root,saved_db,path);select_receiver(panel,root,receiver)
    quota=['quota changed after opening'];calls=[]
    def factory():calls.append(True);return lambda:quota[0]
    panel.write_guard_factory=factory
    panel.import_saved_relation(path);root.update()
    assert 'quota changed' in panel.saved_status.cget('text') and artifact_count(receiver)==0
    quota[0]=None;panel.import_saved_relation(path);root.update()
    assert len(calls)==2 and artifact_count(receiver)==1 and panel._saved_dto is not None


def test_changed_filter_cancels_previous_transfer_without_restoring_old_selection(relations_panel,saved_db,receiver,tmp_path):
    panel,root=relations_panel;path=tmp_path/'pair.json';archive(panel,root,saved_db,path);select_receiver(panel,root,receiver)
    queued=[];panel._run_bg=lambda work,done:queued.append((work,done))
    panel.import_saved_relation(path);panel.saved_symbol.set('OTHER')
    queued[0][1](*panel._call(queued[0][0]));root.update()
    assert artifact_count(receiver)==0 and panel.saved_symbol.get()=='OTHER' and panel._saved_dto is None
    assert not panel._transfer_pending and not panel.saved_table.tree.get_children()


def test_changed_database_after_commit_does_not_apply_original_result(relations_panel,saved_db,receiver,tmp_path,monkeypatch):
    panel,root=relations_panel;path=tmp_path/'pair.json';archive(panel,root,saved_db,path);select_receiver(panel,root,receiver)
    original=sdk.import_feature_relation
    def commit_then_switch(*args,**kwargs):
        result=original(*args,**kwargs);panel.db=saved_db;return result
    monkeypatch.setattr(sdk,'import_feature_relation',commit_then_switch)
    panel.import_saved_relation(path);root.update()
    assert artifact_count(receiver)==1 and panel._saved_dto is None
    assert 'base original' in panel.saved_status.cget('text') and artifact_count(saved_db)==3


def test_deferred_worker_keeps_resource_guard_bound_to_chosen_database(relations_panel,saved_db,receiver,tmp_path):
    panel,root=relations_panel;path=tmp_path/'pair.json';archive(panel,root,saved_db,path);select_receiver(panel,root,receiver)
    queued=[];bound=[];panel._run_bg=lambda work,done:queued.append((work,done))
    def factory():
        target=panel.db;bound.append(target)
        return lambda:'receiver quota' if target is receiver else None
    panel.write_guard_factory=factory
    panel.import_saved_relation(path);panel.db=saved_db
    queued[0][1](*panel._call(queued[0][0]));root.update()
    assert bound==[receiver] and artifact_count(receiver)==0 and artifact_count(saved_db)==3
    assert 'contexto anterior' in panel.saved_status.cget('text') and 'receiver quota' in panel.saved_status.cget('text')


def test_guard_factory_failure_leaves_controls_idle_and_writes_nothing(relations_panel,saved_db,receiver,tmp_path):
    panel,root=relations_panel;path=tmp_path/'pair.json';archive(panel,root,saved_db,path);select_receiver(panel,root,receiver)
    def refused():raise ValueError('controlled policy error')
    panel.write_guard_factory=refused
    panel.import_saved_relation(path);root.update()
    assert not panel._transfer_pending and panel.saved_import.instate(['!disabled']) and artifact_count(receiver)==0
    assert 'controlled policy error' in panel.saved_status.cget('text')


def test_destroyed_panel_requests_cancel_and_ignores_late_callback(relations_panel,saved_db,receiver,tmp_path):
    panel,root=relations_panel;path=tmp_path/'pair.json';archive(panel,root,saved_db,path);select_receiver(panel,root,receiver)
    queued=[];panel._run_bg=lambda work,done:queued.append((work,done))
    panel.import_saved_relation(path);panel.frame.destroy()
    assert panel._transfer_cancel.is_set()
    queued[0][1](*panel._call(queued[0][0]));root.update()
    assert artifact_count(receiver)==0


def test_transfer_controls_fit_without_displacing_selected_value(relations_panel,saved_db):
    panel,root=relations_panel;show_saved(panel,root,saved_db)
    root.geometry('880x760');root.update()
    for widget in (panel.saved_import,panel.saved_export,panel.saved_cancel,panel.saved_effect):
        assert widget.winfo_ismapped()
        assert widget.winfo_rootx()+widget.winfo_width()<=root.winfo_rootx()+root.winfo_width()
        assert widget.winfo_rooty()+widget.winfo_height()<=root.winfo_rooty()+root.winfo_height()
