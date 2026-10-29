"""BD19c2: recibo original, origens completas e importação sem publicação parcial."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import json

import pytest
from sqlalchemy import inspect, text

from mt5_extracao import sdk
from mt5_extracao.analytics import associations, datasets, feature_store, indicator_materializer, producers, relation_transfer
from mt5_extracao.database_manager import DatabaseManager
from tests.test_frozen_association_query import reference, materialized_reference
from tests.test_persisted_feature_relations import publish


@pytest.fixture
def destination(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path/'destination.sqlite'), broker_utc_offset=0)
    datasets.declare_source(db, dict(provider='destination', server='Other synthetic'))
    try:yield db
    finally:db.close()


def package(reference, tmp_path, **kwargs):
    receipt = publish(reference, **kwargs)
    path = tmp_path/'pair.json'
    info = sdk.export_feature_relation(reference[0], receipt['analysis_id'], path)
    return receipt, path, info


def counts(db):
    with db.engine.connect() as conn:
        return {table: conn.execute(text('SELECT COUNT(*) FROM '+table)).scalar()
            if inspect(conn).has_table(table) else 0 for table in
            ('_dataset_blocks','_dataset_manifests','_dataset_references','_feature_relations')}


def rewrite(path, change):
    payload = json.loads(path.read_text(encoding='utf-8'));change(payload)
    payload['package_sha256'] = datasets._hash(datasets._json(relation_transfer._content(payload)))
    path.write_text(datasets._json(payload),encoding='utf-8')


def test_exact_receipt_and_scope_survive_roundtrip_without_live_data(reference, destination, tmp_path, monkeypatch):
    receipt, path, info = package(reference, tmp_path)
    before_source = counts(reference[0]);origin = datasets.identity(destination)
    monkeypatch.setattr(associations, 'query_frozen_pair', lambda *a, **k: pytest.fail('Import/read cannot calculate'))
    result = sdk.import_feature_relation(destination, path)
    assert result == receipt and info['snapshots'] == 1
    again = sdk.import_feature_relation(destination, path)
    assert again == dict(receipt, status='UNCHANGED')
    assert sdk.load_feature_relation(destination, receipt['analysis_id']) == receipt['relation']
    assert sdk.query_saved_relations(destination, analysis_id=receipt['analysis_id'])['matched'] == 1
    assert sdk.inspect_feature_snapshot(destination, receipt['relation']['provenance']['snapshot_manifest_id'])['features']
    assert datasets.identity(destination) == origin and counts(reference[0]) == before_source
    assert not inspect(destination.engine).has_table('_feature_values')
    assert receipt['relation']['synthetic_fixture'] and not receipt['relation']['qualification']['causal_evidence']
    exported = tmp_path/'reexport.json'
    sdk.export_feature_relation(destination, receipt['analysis_id'], exported)
    assert exported.read_bytes() == path.read_bytes()


def test_materialized_pair_transfers_all_immutable_ancestors(reference, destination, tmp_path):
    db, _, cutoff = reference;pair, origin, final = materialized_reference(reference)
    receipt = sdk.persist_feature_relation(db, final.manifest_id, left=pair, right=('qa.positive','1'),
        as_of=cutoff, bootstrap_replicates=29, synthetic_fixture=True)
    path = tmp_path/'composite.json'
    info = sdk.export_feature_relation(db, receipt['analysis_id'], path)
    assert info['snapshots'] == 2 and origin.manifest_id in receipt['relation']['provenance']['lineage_manifests']
    assert sdk.import_feature_relation(destination, path) == receipt
    assert sdk.load_feature_relation(destination, receipt['analysis_id']) == receipt['relation']
    with destination.engine.connect() as conn:
        assert conn.execute(text('SELECT COUNT(*) FROM _dataset_references')).scalar() == 2
    rewrite(path, lambda p:p['snapshots'].pop(origin.manifest_id))
    other = DatabaseManager(db_path=str(tmp_path/'missing.sqlite'), broker_utc_offset=0)
    try:
        with pytest.raises(ValueError, match='origens'):sdk.import_feature_relation(other, path)
        assert not any(counts(other).values())
    finally:other.close()


@pytest.mark.parametrize('right', ['qa.category','qa.missing','qa.constant'])
def test_unavailable_result_keeps_reason_and_qualification(reference, destination, tmp_path, right):
    db, snapshot, cutoff = reference
    original = sdk.persist_feature_relation(db, snapshot.manifest_id, left=('qa.x','1'), right=(right,'1'),
        as_of=cutoff, bootstrap_replicates=29, synthetic_fixture=True)
    path = tmp_path/'unavailable.json'
    sdk.export_feature_relation(db, original['analysis_id'], path)
    assert sdk.import_feature_relation(destination, path) == original
    assert sdk.load_feature_relation(destination, original['analysis_id'])['relation']['effect'] is None


@pytest.mark.parametrize('mutation', ['digest','block','receipt','definition','created','extra'])
def test_invalid_package_is_refused_before_any_rows_or_schema(reference, destination, tmp_path, mutation):
    _, path, _ = package(reference, tmp_path)
    def change(p):
        if mutation == 'digest':p['package_sha256'] = '0'*64
        elif mutation == 'block':
            b=next(iter(p['snapshots'].values()));b['blocks'][next(iter(b['blocks']))] += ' '
        elif mutation == 'receipt':p['receipt']['payload_sha256'] = '0'*64
        elif mutation == 'definition':
            dto=p['receipt']['relation'];dto['left']['definition']['formula']='A different frozen formula'
            p['receipt']['payload_sha256']=feature_store._relation_payload(dto)[2]
        elif mutation == 'created':p['receipt']['created_at_utc']='2026-01-01T00:00:00'
        elif mutation == 'extra':p['snapshots']['a'*64]=deepcopy(next(iter(p['snapshots'].values())))
    rewrite(path, change)
    if mutation == 'digest':
        p=json.loads(path.read_text(encoding='utf-8'));p['package_sha256']='0'*64;path.write_text(json.dumps(p),encoding='utf-8')
    before=counts(destination);tables=inspect(destination.engine).get_table_names()
    with pytest.raises(ValueError):sdk.import_feature_relation(destination, path)
    assert counts(destination)==before and inspect(destination.engine).get_table_names()==tables


@pytest.mark.parametrize('phase', ['blocks','after_receipt'])
def test_cancel_inside_import_rolls_back_snapshots_result_and_refs(reference, destination, tmp_path, phase, monkeypatch):
    receipt, path, _ = package(reference, tmp_path)
    before=counts(destination)
    if phase=='blocks':
        original=datasets._persist_validated
        def interrupt(conn, bundle, **kwargs):
            original(conn,bundle,**kwargs)
            raise indicator_materializer.Cancelled('controlled after snapshot writes')
        monkeypatch.setattr(datasets,'_persist_validated',interrupt)
    else:
        original=feature_store._save_pair_relation
        def interrupt(conn, *args, **kwargs):
            result=original(conn,*args,**kwargs)
            assert conn.execute(text('SELECT COUNT(*) FROM _feature_relations')).scalar()==1
            raise indicator_materializer.Cancelled('controlled after receipt write')
        monkeypatch.setattr(feature_store,'_save_pair_relation',interrupt)
    with pytest.raises(indicator_materializer.Cancelled):sdk.import_feature_relation(destination,path)
    assert counts(destination)==before
    monkeypatch.undo()
    assert sdk.import_feature_relation(destination,path)==receipt


def test_resource_guard_is_checked_late_and_rolls_back(reference, destination, tmp_path):
    receipt,path,_=package(reference,tmp_path)
    calls=0
    def policy():
        nonlocal calls
        calls+=1
        return 'controlled quota' if calls>=15 else None
    before=counts(destination)
    with pytest.raises(producers.WriteBlocked,match='controlled quota'):sdk.import_feature_relation(destination,path,write_guard=policy)
    assert calls>=15 and counts(destination)==before
    assert sdk.import_feature_relation(destination,path)==receipt


def test_conflicting_result_preserves_local_receipt_and_rolls_back(reference, destination, tmp_path):
    receipt,path,_=package(reference,tmp_path)
    assert sdk.import_feature_relation(destination,path)==receipt
    before=counts(destination)
    def change(p):
        dto=p['receipt']['relation'];dto['relation']['effect']=.5
        p['receipt']['payload_sha256']=feature_store._relation_payload(dto)[2]
    rewrite(path,change)
    with pytest.raises(ValueError,match='outro resultado'):sdk.import_feature_relation(destination,path)
    assert counts(destination)==before and sdk.load_feature_relation(destination,receipt['analysis_id'])==receipt['relation']


def test_semantic_repeat_preserves_first_destination_timestamp_and_budget(reference,destination,tmp_path):
    receipt,path,_=package(reference,tmp_path)
    assert sdk.import_feature_relation(destination,path)==receipt
    def change(p):
        p['receipt']['created_at_utc']='2026-01-01T00:00:00+00:00'
        dto=p['receipt']['relation'];dto['generated_at_utc']='2026-01-01T00:00:00.000000Z'
        dto['provenance']['limits']['timeout_seconds']=80
        p['receipt']['payload_sha256']=feature_store._relation_payload(dto)[2]
    rewrite(path,change)
    assert sdk.import_feature_relation(destination,path)==dict(receipt,status='UNCHANGED')


def test_existing_corrupt_block_is_not_overwritten(reference, destination, tmp_path):
    _,path,_=package(reference,tmp_path);p=json.loads(path.read_text(encoding='utf-8'))
    digest=next(iter(next(iter(p['snapshots'].values()))['blocks']))
    with destination.engine.begin() as c:c.execute(text('INSERT INTO _dataset_blocks VALUES (:id,:value)'),dict(id=digest,value='corrupt'))
    before=counts(destination)
    with pytest.raises(ValueError,match='Bloco existente'):sdk.import_feature_relation(destination,path)
    assert counts(destination)==before
    with destination.engine.connect() as c:assert c.execute(text('SELECT payload_json FROM _dataset_blocks WHERE block_id=:id'),dict(id=digest)).scalar()=='corrupt'


def test_concurrent_importers_publish_one_original_receipt(reference, destination, tmp_path):
    receipt,path,_=package(reference,tmp_path)
    with ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(lambda _:sdk.import_feature_relation(destination,path),range(2)))
    assert sorted(r['status'] for r in results)==['INSERTED','UNCHANGED']
    assert all(dict(r,status='INSERTED')==receipt for r in results)
    assert counts(destination)['_feature_relations']==1


def test_export_protects_destination_and_publishes_only_complete_files(reference, tmp_path, monkeypatch):
    receipt,path,_=package(reference,tmp_path)
    original=path.read_bytes();sdk.export_feature_relation(reference[0],receipt['analysis_id'],path)
    assert path.read_bytes()==original
    path.write_bytes(b'owner file')
    with pytest.raises(ValueError,match='existente'):sdk.export_feature_relation(reference[0],receipt['analysis_id'],path)
    assert path.read_bytes()==b'owner file'
    from mt5_extracao.presets import manual
    def failed_publish(source,target):
        assert not target.exists()
        raise OSError('controlled interruption before link')
    monkeypatch.setattr(manual.os,'link',failed_publish)
    fresh=tmp_path/'fresh.json'
    with pytest.raises(ValueError,match='atomicamente'):sdk.export_feature_relation(reference[0],receipt['analysis_id'],fresh)
    assert not fresh.exists() and not list(tmp_path.glob('.mt5x-*'))


def test_bounded_reader_and_duplicate_json_keys(reference,destination,tmp_path,monkeypatch):
    _,path,_=package(reference,tmp_path)
    monkeypatch.setattr(relation_transfer,'MAX_PACKAGE_BYTES',10)
    with pytest.raises(ValueError,match='excede'):sdk.import_feature_relation(destination,path)
    monkeypatch.undo()
    path.write_text('{"format":"a","format":"b"}',encoding='utf-8')
    with pytest.raises(ValueError,match='duplicada'):sdk.import_feature_relation(destination,path)
    assert not any(counts(destination).values())


def test_cli_export_import_lookup_share_sdk_contract_and_write_policy(reference,destination,tmp_path,capsys,monkeypatch):
    from mt5_extracao import cli
    receipt=publish(reference);path=tmp_path/'cli.json'
    def config(db,name,quota=0):
        f=tmp_path/name;f.write_text(f'[DATABASE]\ntype=sqlite\npath={db.db_path}\n[APP]\nbroker_utc_offset=0\n[RESOURCES]\nblock_db_gb={quota}\n',encoding='utf-8');return str(f)
    source_config=config(reference[0],'source.ini');dest=config(destination,'destination.ini',.000000001)
    monkeypatch.chdir(tmp_path)
    def run(cfg,args):
        status=cli.main(['--config',cfg,'--json','analytics','features',*args]);return status,json.loads(capsys.readouterr().out)
    code,data=run(source_config,['relation-export','--analysis-id',receipt['analysis_id'],'--out',str(path)])
    assert code==0 and data['data']['transfer']['snapshots']==1
    code,data=run(dest,['relation-import','--source',str(path)])
    assert code==1 and not data['ok'] and not any(counts(destination).values())
    dest=config(destination,'destination.ini')
    code,data=run(dest,['relation-import','--source',str(path)])
    assert code==0 and data['data']['relation']==receipt['relation']
    code,data=run(dest,['relation-result','--analysis-id',receipt['analysis_id']])
    assert code==0 and data['data']['relation']==sdk.load_feature_relation(destination,receipt['analysis_id'])
