"""Criação pelo formulário real, persistência, cancelamento e paginação no serviço canônico."""
import pytest
from sqlalchemy import text

from mt5_extracao import sdk
from mt5_extracao.analytics import feature_store
from tests.test_ui_relations import relations_panel
from tests.test_ui_saved_relations import show_saved
from tests.test_saved_relation_queries import saved_base, saved_db


def count(db):
    with db.engine.connect() as conn:
        return conn.execute(text('SELECT COUNT(*) FROM _feature_relations')).scalar()


def prepare(panel,root,db):
    show_saved(panel,root,db)
    panel.open_relation_creator();root.update()
    creator=panel._creator
    assert creator.alive() and not creator.loading
    def label(ident):return next(k for k,v in creator._contracts.items() if v['feature_id']==ident)
    creator.left.set(label('qa.x'));creator.right.set(label('qa.positive'))
    creator.options['bootstrap_replicates'].set('29');creator.options['seed'].set('19')
    return creator,label


def test_create_and_select_exact_persisted_result(relations_panel,saved_db):
    panel,root=relations_panel
    creator,_=prepare(panel,root,saved_db)
    before=count(saved_db)
    creator.save_button.invoke();root.update()
    assert creator.saved_receipt['status']=='INSERTED' and count(saved_db)==before+1
    assert not creator.alive() and '+1,0000' in panel.saved_effect.cget('text')
    assert len(panel._saved_rows)==1 and next(iter(panel._saved_rows.values()))['analysis_id']==creator.saved_receipt['analysis_id']
    assert 'recém salvo' in panel.saved_status.cget('text')
    assert sdk.load_feature_relation(saved_db,creator.saved_receipt['analysis_id'])==creator.saved_receipt['relation']


def test_categorical_selection_and_same_series_without_lag_are_blocked(relations_panel,saved_db):
    panel,root=relations_panel
    creator,label=prepare(panel,root,saved_db)
    creator.right.set(label('qa.category'))
    assert creator.save_button.instate(['disabled']) and 'categóricos' in creator.compatibility.cget('text')
    creator.right.set(label('qa.x'))
    assert creator.save_button.instate(['disabled'])
    creator.lag.set('1');assert creator.save_button.instate(['!disabled'])


def test_cancel_before_dispatch_preserves_all_artifacts(relations_panel,saved_db):
    panel,root=relations_panel
    creator,_=prepare(panel,root,saved_db)
    queued=[];creator.run_bg=lambda work,done:queued.append((work,done))
    before=count(saved_db)
    creator.calculate();assert creator.pending and creator.save_button.instate(['disabled'])
    creator.cancel_button.invoke()
    result,error=panel._call(queued[0][0]);queued[0][1](result,error)
    assert not creator.pending and creator.alive() and count(saved_db)==before
    assert 'cancelada' in creator.status.cget('text')


def test_cancel_at_transaction_boundary_rolls_back_new_artifact(relations_panel,saved_db,monkeypatch):
    panel,root=relations_panel
    creator,_=prepare(panel,root,saved_db)
    original=feature_store.save_pair_relation
    def cancel_inside(db,dto,**kwargs):
        return original(db,dto,**kwargs,transaction_hook=lambda conn:creator._cancel.set())
    monkeypatch.setattr(feature_store,'save_pair_relation',cancel_inside)
    before=count(saved_db);creator.calculate()
    assert creator.alive() and not creator.pending and count(saved_db)==before
    assert 'cancelada' in creator.status.cget('text')


def test_cancel_after_commit_preserves_and_displays_confirmed_result(relations_panel,saved_db):
    panel,root=relations_panel
    creator,_=prepare(panel,root,saved_db)
    queued=[];creator.run_bg=lambda work,done:queued.append((work,done))
    before=count(saved_db);creator.calculate()
    result=queued[0][0]()
    assert count(saved_db)==before+1
    creator.cancel()
    queued[0][1](result,None);root.update()
    assert not creator.alive() and count(saved_db)==before+1
    assert creator.saved_receipt['analysis_id'] in panel._details['saved-relations'].get('1.0','end-1c')


def test_failed_new_capture_clears_previous_scope_and_choices(relations_panel,saved_db):
    panel,root=relations_panel
    creator,_=prepare(panel,root,saved_db)
    creator._captures['Ausente']='0'*64;creator.capture.set('Ausente');creator.load_capture()
    assert creator._contracts=={} and not creator.left_box.cget('values') and not creator.right_box.cget('values')
    assert 'Não foi possível conferir' in creator.scope.cget('text')
    assert creator.save_button.instate(['disabled'])


def test_obsolete_capture_response_cannot_restore_old_contracts(relations_panel,saved_db):
    panel,root=relations_panel
    creator,_=prepare(panel,root,saved_db)
    queued=[];creator.run_bg=lambda work,done:queued.append((work,done))
    creator.load_capture()
    creator._captures['Ausente']='0'*64;creator.capture.set('Ausente');creator.load_capture()
    queued[1][1](queued[1][0](),None);queued[0][1](queued[0][0](),None)
    assert creator._contracts=={} and creator.save_button.instate(['disabled'])
    assert 'Não foi possível conferir' in creator.scope.cget('text')


def test_write_refusal_keeps_form_and_does_not_publish(relations_panel,saved_db):
    panel,root=relations_panel
    creator,_=prepare(panel,root,saved_db)
    creator.write_guard=lambda:'limite de recursos QA'
    before=count(saved_db);creator.calculate()
    assert creator.alive() and count(saved_db)==before and 'limite de recursos QA' in creator.status.cget('text')
    assert str(creator.left_box.cget('state'))=='readonly'


def test_policy_factory_is_consulted_when_calculation_starts(relations_panel,saved_db):
    panel,root=relations_panel
    creator,_=prepare(panel,root,saved_db)
    state={'reason':None,'calls':0}
    def factory():
        state['calls']+=1
        return lambda:state['reason']
    creator.write_guard_factory=factory
    state['reason']='política alterada após abrir o formulário'
    before=count(saved_db);creator.calculate()
    assert state['calls']==1 and count(saved_db)==before
    assert state['reason'] in creator.status.cget('text')


def test_invalid_advanced_input_has_actionable_message_and_does_not_start_job(relations_panel,saved_db):
    panel,root=relations_panel
    creator,_=prepare(panel,root,saved_db)
    creator.options['minimum_pairs'].set('abc')
    before=count(saved_db);creator.calculate()
    assert not creator.pending and count(saved_db)==before
    assert 'Mínimo de pares precisa ser um número inteiro' in creator.status.cget('text')


def test_close_during_processing_requests_cancel_then_closes(relations_panel,saved_db):
    panel,root=relations_panel
    creator,_=prepare(panel,root,saved_db)
    queued=[];creator.run_bg=lambda work,done:queued.append((work,done))
    before=count(saved_db);creator.calculate();creator.close(cancel=True)
    assert creator.pending and creator.alive() and creator._cancel.is_set()
    result,error=panel._call(queued[0][0]);queued[0][1](result,error)
    assert not creator.alive() and count(saved_db)==before


def test_escape_requests_cancellation_before_closing_active_dialog(relations_panel,saved_db):
    panel,root=relations_panel
    creator,_=prepare(panel,root,saved_db)
    queued=[];creator.run_bg=lambda work,done:queued.append((work,done))
    before=count(saved_db);creator.calculate()
    creator.capture_box.focus_force();root.update()
    creator.capture_box.event_generate('<Escape>');root.update()
    assert creator.alive() and creator.pending and creator._cancel.is_set()
    result,error=panel._call(queued[0][0]);queued[0][1](result,error)
    assert not creator.alive() and count(saved_db)==before


def test_empty_context_guides_user_back_to_series(relations_panel):
    panel,root=relations_panel
    visits=[];panel.open_series=lambda:visits.append('series')
    panel.open_relation_creator();root.update()
    creator=panel._creator
    assert not creator._captures and creator.save_button.instate(['disabled']) and 'Congele' in creator.status.cget('text')
    creator.select_in_series()
    assert visits==['series'] and not creator.alive()


def test_real_page_buttons_round_trip_without_duplicate_records(relations_panel,saved_db,monkeypatch):
    panel,root=relations_panel
    original=sdk.query_saved_relations
    def one_per_page(db,**kwargs):return original(db,**dict(kwargs,page_size=1))
    monkeypatch.setattr(sdk,'query_saved_relations',one_per_page)
    show_saved(panel,root,saved_db)
    first=panel._saved_rows['saved-0']['analysis_id']
    assert panel.saved_next.instate(['!disabled']) and panel.saved_next.cget('text')=='Próxima'
    panel.saved_next.invoke();root.update()
    assert panel._saved_rows['saved-0']['analysis_id']!=first and panel.saved_previous.instate(['!disabled'])
    panel.saved_previous.invoke();root.update()
    assert panel._saved_rows['saved-0']['analysis_id']==first
