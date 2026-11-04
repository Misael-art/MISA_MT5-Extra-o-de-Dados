"""BD15b: artefato original, referências e publicação transacional."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import json

import pytest
from sqlalchemy import inspect, text

from mt5_extracao import sdk
from mt5_extracao.analytics import associations, datasets, feature_store
from tests.test_frozen_association_query import reference, query, materialized_reference


def publish(reference, **kwargs):
    db, snapshot, cutoff = reference
    return sdk.persist_feature_relation(db, snapshot.manifest_id, left=('qa.x', '1'), right=('qa.positive', '1'),
        as_of=cutoff, bootstrap_replicates=29, synthetic_fixture=True, **kwargs)


def stored_counts(db):
    with db.engine.connect() as conn:
        rows = conn.execute(text('SELECT COUNT(*) FROM _feature_relations')).scalar() if inspect(conn).has_table('_feature_relations') else 0
        refs = conn.execute(text("SELECT COUNT(*) FROM _dataset_references WHERE run_id LIKE 'relation:%'")).scalar()
    return rows, refs


def test_query_and_unknown_read_do_not_create_result_schema(reference):
    db, _, _ = reference
    query(reference)
    tables = set(inspect(db.engine).get_table_names())
    with pytest.raises(ValueError, match='não persistida'):
        sdk.load_feature_relation(db, 'a'*64)
    assert set(inspect(db.engine).get_table_names()) == tables and '_feature_relations' not in tables


def test_repeat_returns_the_original_timestamp_and_budget_receipt(reference, monkeypatch):
    db, _, _ = reference
    first = publish(reference, timeout_seconds=80)
    again = publish(reference, timeout_seconds=90)
    assert first['status'] == 'INSERTED' and again['status'] == 'UNCHANGED'
    assert {k:v for k,v in first.items() if k != 'status'} == {k:v for k,v in again.items() if k != 'status'}
    assert first['relation']['provenance']['limits']['timeout_seconds'] == 80
    assert stored_counts(db) == (1, 1)
    monkeypatch.setattr(associations, 'query_frozen_pair', lambda *a, **k: pytest.fail('read must not recalculate'))
    assert sdk.load_feature_relation(db, first['analysis_id']) == first['relation']


def test_reusing_identity_for_a_different_result_preserves_original(reference):
    db, _, _ = reference
    first = publish(reference)
    changed = deepcopy(first['relation']);changed['relation']['effect'] = .5
    with pytest.raises(ValueError, match='outro resultado'):
        feature_store.save_pair_relation(db, changed)
    assert sdk.load_feature_relation(db, first['analysis_id']) == first['relation']
    assert stored_counts(db) == (1, 1)


def test_changed_sample_cannot_keep_the_same_identity(reference):
    db, _, _ = reference
    dto = query(reference);dto['provenance']['sample_hash'] = '0'*64
    with pytest.raises(ValueError, match='Identidade'):
        feature_store.save_pair_relation(db, dto)
    assert stored_counts(db) == (0, 0)


@pytest.mark.parametrize('failure', [KeyboardInterrupt, RuntimeError])
def test_interruption_inside_transaction_rolls_back_artifact_and_references(reference, failure):
    db, _, _ = reference
    dto = query(reference)
    def interrupt(conn):
        assert conn.execute(text('SELECT COUNT(*) FROM _feature_relations')).scalar() == 1
        assert conn.execute(text("SELECT COUNT(*) FROM _dataset_references WHERE run_id LIKE 'relation:%'")).scalar() == 1
        raise failure('controlled interruption')
    with pytest.raises(failure):
        feature_store.save_pair_relation(db, dto, transaction_hook=interrupt)
    assert stored_counts(db) == (0, 0)
    assert feature_store.save_pair_relation(db, dto)['status'] == 'INSERTED'


def test_late_write_guard_rolls_back_inserted_result(reference):
    db, _, _ = reference
    dto = query(reference)
    def guard(conn):
        if conn is not None and conn.execute(text('SELECT COUNT(*) FROM _feature_relations')).scalar():
            raise RuntimeError('late resource refusal')
    with pytest.raises(RuntimeError, match='late resource'):
        feature_store.save_pair_relation(db, dto, transaction_guard=guard)
    assert stored_counts(db) == (0, 0)


def test_concurrent_publishers_keep_one_original_receipt(reference):
    db, _, _ = reference
    dto = query(reference)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: feature_store.save_pair_relation(db, dto), range(2)))
    assert sorted(r['status'] for r in results) == ['INSERTED', 'UNCHANGED']
    assert results[0]['payload_sha256'] == results[1]['payload_sha256'] and stored_counts(db) == (1, 1)


def test_changed_method_and_seed_produce_separate_artifacts(reference):
    db, _, _ = reference
    receipts = [publish(reference), publish(reference, seed=8), publish(reference, method='spearman')]
    assert len({r['analysis_id'] for r in receipts}) == 3 and stored_counts(db) == (3, 3)


def test_new_frozen_revision_keeps_the_previous_artifact_readable(reference):
    import pandas as pd
    db, snapshot, cutoff = reference
    original = publish(reference)
    feature_store.save_value(db, 'QA', 'M1', 'qa.positive', '1', '2025-01-20T09:07Z', 999.,
        available_at=cutoff, observed_at=cutoff)
    revised = datasets.capture(db, 'QA', 'M1', stored_features=[('qa.x','1'),('qa.positive','1')])
    after = sdk.persist_feature_relation(db, revised.manifest_id, left=('qa.x','1'), right=('qa.positive','1'),
        as_of=pd.Timestamp.now(tz='UTC'), bootstrap_replicates=29, synthetic_fixture=True)
    assert after['analysis_id'] != original['analysis_id'] and revised.manifest_id != snapshot.manifest_id
    assert stored_counts(db) == (2, 2)
    assert sdk.load_feature_relation(db, original['analysis_id']) == original['relation']
    assert sdk.load_feature_relation(db, after['analysis_id']) == after['relation']


@pytest.mark.parametrize('column,value', [('payload_json', '{}'), ('payload_sha256', 'x'), ('semantic_sha256', 'x'), ('symbol', 'OTHER')])
def test_corrupted_payload_or_index_is_refused(reference, column, value):
    db, _, _ = reference
    result = publish(reference)
    with db.engine.begin() as conn:
        conn.execute(text(f'UPDATE _feature_relations SET {column}=:v'), {'v':value})
    with pytest.raises(ValueError):
        sdk.load_feature_relation(db, result['analysis_id'])


def test_materialized_ancestry_is_retained_and_missing_reference_is_refused(reference):
    db, _, cutoff = reference
    pair, origin, snapshot = materialized_reference(reference)
    result = sdk.persist_feature_relation(db, snapshot.manifest_id, left=pair, right=('qa.positive','1'),
        as_of=cutoff, bootstrap_replicates=29, synthetic_fixture=True)
    assert origin.manifest_id in result['relation']['provenance']['lineage_manifests']
    assert stored_counts(db) == (1, 2)
    assert sdk.load_feature_relation(db, result['analysis_id']) == result['relation']
    with db.engine.begin() as conn:
        conn.execute(text('DELETE FROM _dataset_references WHERE manifest_id=:id'), {'id':origin.manifest_id})
    with pytest.raises(ValueError, match='Referência'):
        sdk.load_feature_relation(db, result['analysis_id'])


def test_cancellation_and_write_refusal_do_not_publish(reference):
    from mt5_extracao.analytics import indicator_materializer, producers
    db, _, _ = reference
    with pytest.raises(indicator_materializer.Cancelled):
        publish(reference, cancel_check=lambda: True)
    with pytest.raises(producers.WriteBlocked):
        publish(reference, write_guard=lambda: 'controlled resource refusal')
    assert stored_counts(db) == (0, 0)


def test_cli_publish_and_read_use_explicit_configuration(reference, tmp_path, capsys, monkeypatch):
    from mt5_extracao import cli
    db, snapshot, cutoff = reference
    config = tmp_path/'qa.ini'
    config.write_text(f'[DATABASE]\ntype=sqlite\npath={db.db_path}\n[APP]\nbroker_utc_offset=0\ntime_basis=broker\n')
    monkeypatch.chdir(tmp_path)
    command = ['--config',str(config),'--json','analytics','features']
    assert cli.main(command+['relation','--persist','--manifest',snapshot.manifest_id,'--left','qa.x','1',
        '--right','qa.positive','1','--as-of',cutoff,'--bootstrap-replicates','29','--synthetic-fixture']) == 0
    payload = json.loads(capsys.readouterr().out)['data']
    assert payload['persistence']['status'] == 'INSERTED'
    assert cli.main(command+['relation-result','--analysis-id',payload['persistence']['analysis_id']]) == 0
    assert json.loads(capsys.readouterr().out)['data']['relation'] == payload['relation']
