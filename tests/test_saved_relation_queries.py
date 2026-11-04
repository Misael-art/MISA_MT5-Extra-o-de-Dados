"""Índice BD19b: paginação estável, somente leitura e payloads limitados à página."""
import json
import sqlite3

import pytest
from sqlalchemy import event, inspect, text

from mt5_extracao import sdk
from mt5_extracao.analytics import datasets, feature_store
from mt5_extracao.database_manager import DatabaseManager
from tests.test_frozen_association_query import seed_reference


def seed_saved_relations(db):
    _, snapshot, cutoff = seed_reference(db)
    results = []
    for right in ('qa.positive', 'qa.negative', 'qa.missing'):
        results.append(sdk.persist_feature_relation(db, snapshot.manifest_id, left=('qa.x', '1'), right=(right, '1'),
            as_of=cutoff, bootstrap_replicates=29, synthetic_fixture=True))
    return snapshot, cutoff, results


@pytest.fixture(scope='module')
def saved_base(tmp_path_factory):
    path = tmp_path_factory.mktemp('saved-relations')/'base.sqlite'
    db = DatabaseManager(db_path=str(path), broker_utc_offset=0)
    try:
        seed_saved_relations(db)
        yield path
    finally:
        db.close()


@pytest.fixture
def saved_db(saved_base, tmp_path):
    path = tmp_path/'qa.sqlite'
    with sqlite3.connect(saved_base) as source, sqlite3.connect(path) as target:
        source.backup(target)
    db = DatabaseManager(db_path=str(path), broker_utc_offset=0)
    try:
        yield db
    finally:
        db.close()


def test_empty_base_query_does_not_create_schema(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path/'empty.sqlite'))
    try:
        before = set(inspect(db.engine).get_table_names())
        result = sdk.query_saved_relations(db)
        assert result['status'] == 'ok' and result['items'] == [] and result['matched'] == 0
        assert set(inspect(db.engine).get_table_names()) == before
    finally:
        db.close()


def test_pages_have_no_duplicates_and_load_only_selected_payloads(saved_db):
    statements = []
    def capture(conn, cursor, statement, parameters, context, executemany):
        if '_feature_relations' in statement:
            statements.append(statement)
    event.listen(saved_db.engine, 'before_cursor_execute', capture)
    try:
        result = sdk.query_saved_relations(saved_db, page_size=1)
    finally:
        event.remove(saved_db.engine, 'before_cursor_execute', capture)
    assert result['status'] == 'ok' and result['matched'] == 3 and len(result['items']) == 1
    assert len([s for s in statements if 'SELECT *' in s]) == 1
    assert all('analysis_id=' in s for s in statements if 'SELECT *' in s)
    assert any('LIMIT' in s for s in statements)
    ids = []
    while True:
        ids.extend(r['analysis_id'] for r in result['items'])
        cursor = result['page']['next_cursor']
        if cursor is None:
            break
        result = sdk.query_saved_relations(saved_db, page_size=1, cursor=cursor)
        assert result['status'] == 'ok'
    assert len(ids) == len(set(ids)) == 3
    assert {r['analysis_id'] for r in sdk.query_saved_relations(saved_db)['items']} == set(ids)


def test_filters_and_cursor_scope_are_exact(saved_db):
    first = sdk.query_saved_relations(saved_db, symbol='QA', timeframe='M1', page_size=1)
    assert first['matched'] == 3
    assert sdk.query_saved_relations(saved_db, symbol='qa')['matched'] == 0
    wrong = sdk.query_saved_relations(saved_db, symbol='OTHER', timeframe='M1', cursor=first['page']['next_cursor'])
    assert wrong['status'] == 'error' and wrong['errors'][0]['code'] == 'CURSOR_MISMATCH'


@pytest.mark.parametrize('change', ['insert', 'fork'])
def test_changed_acervo_or_base_invalidates_cursor(saved_db, change):
    first = sdk.query_saved_relations(saved_db, page_size=1)
    if change == 'fork':
        datasets.fork(saved_db)
    else:
        dto = sdk.load_feature_relation(saved_db, first['items'][0]['analysis_id'])
        sdk.persist_feature_relation(saved_db, dto['provenance']['snapshot_manifest_id'],
            left=(dto['left']['feature_id'],dto['left']['algorithm_version']),
            right=(dto['right']['feature_id'],dto['right']['algorithm_version']),
            as_of=dto['provenance']['as_of_utc'],seed=17,bootstrap_replicates=29,synthetic_fixture=True)
    result = sdk.query_saved_relations(saved_db, page_size=1, cursor=first['page']['next_cursor'])
    assert result['status'] == 'error' and result['items'] == [] and result['errors'][0]['code'] == 'STALE_CURSOR'


def test_corrupt_payload_does_not_return_a_partial_page(saved_db):
    first = sdk.query_saved_relations(saved_db)['items'][1]['analysis_id']
    with saved_db.engine.begin() as conn:
        conn.execute(text('UPDATE _feature_relations SET payload_json=:p WHERE analysis_id=:id'), {'p':'{}','id':first})
    result = sdk.query_saved_relations(saved_db)
    assert result['status'] == 'error' and result['items'] == []
    assert result['errors'][0]['code'] == 'INVALID_RELATION'


@pytest.mark.parametrize('options', [dict(page_size=0),dict(page_size=True),dict(symbol=[]),dict(timeframe=object()),dict(cursor='bad')])
def test_bad_inputs_return_json_safe_errors(saved_db, options):
    result = sdk.query_saved_relations(saved_db, **options)
    assert result['status'] == 'error' and result['items'] == []
    json.dumps(result, allow_nan=False)


def test_cli_uses_same_index_with_explicit_configuration(saved_db, tmp_path, capsys, monkeypatch):
    from mt5_extracao import cli
    config = tmp_path/'qa.ini'
    config.write_text(f'[DATABASE]\ntype=sqlite\npath={saved_db.db_path}\n[APP]\nbroker_utc_offset=0\n')
    monkeypatch.chdir(tmp_path)
    command = ['--config',str(config),'--json','analytics','features','relation-list']
    assert cli.main(command+['--symbol','QA','--tf','M1','--page-size','1']) == 0
    actual = json.loads(capsys.readouterr().out)['data']['query']
    assert actual == sdk.query_saved_relations(saved_db, symbol='QA', timeframe='M1', page_size=1)
    assert cli.main(command+['--page-size','0']) == 1
    assert json.loads(capsys.readouterr().out)['data']['query']['status'] == 'error'
