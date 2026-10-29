"""Seleção legível de artefatos e recusa de respostas obsoletas no painel real."""
import pytest
from mt5_extracao import sdk
from tests.test_ui_relations import relations_panel
from tests.test_saved_relation_queries import saved_base, saved_db


def show_saved(panel, root, db):
    panel.db=db
    panel.notebook.select(panel.pages['Matriz'])
    panel.matrix_notebook.select(panel.saved_frame)
    root.update()


def test_saved_result_has_explicit_versions_units_sample_and_fixture_scope(relations_panel,saved_db):
    panel,root=relations_panel
    show_saved(panel,root,saved_db)
    assert len(panel.saved_table.tree.get_children())==3
    selected=panel._saved_rows[panel.saved_table.tree.selection()[0]]
    assert panel._saved_number(selected['relation']['effect']) in panel.saved_effect.cget('text')
    assert '[fraction]' in panel.saved_scope.cget('text') and 'versão 1' in panel.saved_scope.cget('text')
    assert 'Pares:' in panel.saved_sample.cget('text') and 'fontes distintas:' in panel.saved_sample.cget('text')
    assert 'Referência sintética' in panel.saved_qualification.cget('text')
    text=panel._details['saved-relations'].get('1.0','end-1c')
    assert 'Origens conferidas:' in text and selected['analysis_id'] in text


def test_new_filter_clears_previous_value_and_empty_result_is_explicit(relations_panel,saved_db):
    panel,root=relations_panel
    show_saved(panel,root,saved_db)
    panel.saved_symbol.set('OTHER')
    assert not panel.saved_table.tree.get_children() and panel.saved_sample.cget('text')==''
    assert panel.saved_effect.cget('text')=='Selecione uma relação'
    panel.load_saved_relations();root.update()
    assert 'Nenhuma relação' in panel.saved_status.cget('text')


def test_outdated_filter_response_cannot_replace_current_empty_page(relations_panel,saved_db):
    panel,root=relations_panel
    show_saved(panel,root,saved_db)
    queued=[];panel._run_bg=lambda work,done:queued.append((work,done))
    panel.load_saved_relations()
    panel.saved_symbol.set('OTHER');panel.load_saved_relations()
    latest,old=queued[1],queued[0]
    latest[1](latest[0](),None);old[1](old[0](),None)
    assert not panel.saved_table.tree.get_children() and 'Nenhuma relação' in panel.saved_status.cget('text')


def test_late_selection_result_cannot_override_new_choice(relations_panel,saved_db):
    panel,root=relations_panel
    show_saved(panel,root,saved_db)
    queued=[];panel._run_bg=lambda work,done:queued.append((work,done))
    for key in ('saved-1','saved-2'):
        panel.saved_table.tree.selection_set(key);panel._saved_selected(None)
    assert len(queued)==2 and panel.saved_effect.cget('text')=='Aguarde…'
    queued[1][1](queued[1][0](),None)
    expected=panel.saved_effect.cget('text')
    queued[0][1](queued[0][0](),None)
    assert panel.saved_effect.cget('text')==expected
    assert panel._saved_rows['saved-2']['analysis_id'] in panel._details['saved-relations'].get('1.0','end-1c')


def test_missing_origin_does_not_leave_previous_numeric_value(relations_panel,saved_db,monkeypatch):
    panel,root=relations_panel
    show_saved(panel,root,saved_db)
    def refuse(*args):raise ValueError('Recupere o snapshot de origem')
    monkeypatch.setattr(sdk,'load_feature_relation',refuse)
    panel.saved_table.tree.selection_set('saved-1');panel._saved_selected(None)
    assert panel.saved_effect.cget('text')=='Indisponível'
    assert panel.saved_sample.cget('text')=='' and 'Recupere' in panel.saved_scope.cget('text')


def test_selection_notification_does_not_duplicate_source_reads(relations_panel,saved_db,monkeypatch):
    panel,root=relations_panel
    calls=[];original=sdk.load_feature_relation
    def counted(db,ident):calls.append(ident);return original(db,ident)
    monkeypatch.setattr(sdk,'load_feature_relation',counted)
    show_saved(panel,root,saved_db)
    panel._saved_selected(None);root.update()
    assert len(calls)==1


def test_saved_view_uses_its_own_scope_and_restores_analysis_context(relations_panel,saved_db):
    panel,root=relations_panel
    original=panel.synthetic_notice.message.cget('text')
    show_saved(panel,root,saved_db)
    assert not panel.scope_filter_bar.winfo_ismapped() and not panel.state_legend.winfo_ismapped()
    assert not panel.matrix_intro.winfo_ismapped() and not panel.correlation_legend.winfo_ismapped()
    assert 'Resultados exploratórios salvos' in panel.synthetic_notice.message.cget('text')
    value=panel.saved_effect.cget('text')
    panel.matrix_notebook.select(0);root.update()
    assert panel.scope_filter_bar.winfo_ismapped() and panel.state_legend.winfo_ismapped()
    assert panel.matrix_intro.winfo_ismapped() and panel.correlation_legend.winfo_ismapped()
    assert panel.synthetic_notice.message.cget('text')==original
    panel.matrix_notebook.select(panel.saved_frame);root.update()
    assert panel.saved_effect.cget('text')==value


def test_repeated_tab_events_do_not_duplicate_pending_list_request(relations_panel,saved_db):
    panel,root=relations_panel
    queued=[];panel._run_bg=lambda work,done:queued.append((work,done))
    show_saved(panel,root,saved_db)
    panel.matrix_notebook.event_generate('<<NotebookTabChanged>>');root.update()
    panel.matrix_notebook.event_generate('<<NotebookTabChanged>>');root.update()
    assert len(queued)==1 and panel._saved_loading
    assert panel.saved_refresh.instate(['disabled'])
