"""Contrato congelado, compatibilidade e seleção precisa para a criação visual."""
import json

import pandas as pd
import pytest
from sqlalchemy import inspect, text

from mt5_extracao import sdk
from mt5_extracao.analytics import datasets, feature_store
from tests.test_frozen_association_query import reference
from tests.test_pair_relation_dto import payload


def test_snapshot_inspection_uses_frozen_versions_without_creating_schema(reference, monkeypatch):
    db, snapshot, _ = reference
    tables=set(inspect(db.engine).get_table_names())
    monkeypatch.setattr(feature_store,'list_definitions',lambda *a,**k:pytest.fail('no active catalog'))
    result=sdk.inspect_feature_snapshot(db,snapshot.manifest_id)
    assert result['status']=='ok' and len(result['features'])==9
    by_id={r['feature_id']:r for r in result['features']}
    assert by_id['qa.x']['numeric_contract_compatible'] and by_id['qa.x']['algorithm_version']=='1'
    assert not by_id['qa.category']['numeric_contract_compatible'] and by_id['qa.flag']['reason']=='UNSUPPORTED_VALUE_TYPE'
    assert result['scope']['symbol']=='QA' and result['scope']['timeframe']=='M1'
    assert set(inspect(db.engine).get_table_names())==tables
    json.dumps(result,allow_nan=False)


def test_snapshot_without_stored_versions_and_unavailable_id_are_explicit(reference):
    db,_,_=reference
    bare=datasets.capture(db,'QA','M1')
    result=sdk.inspect_feature_snapshot(db,bare.manifest_id)
    assert result['status']=='ok' and result['features']==[] and result['reasons']
    assert sdk.inspect_feature_snapshot(db,'not-an-id')['status']=='error'


def test_legacy_number_with_frozen_category_kind_cannot_receive_a_continuous_effect(reference):
    db,_,_=reference
    feature_store.register_definition(db,'qa.codes','Direction codes QA','1',{},[],family='qa',
        unit='direction_code_-1_0_1',value_type='NUMBER')
    with db.engine.connect() as conn:
        rows=conn.execute(text('SELECT * FROM _feature_values WHERE feature_id=:id'),{'id':'qa.x'}).mappings().all()
    feature_store.save_values(db,[dict(symbol='QA',timeframe='M1',feature_id='qa.codes',algorithm_version='1',
        observation_time=r['observation_time'],value=r['numeric_value'],available_at=r['available_at_utc'],
        observed_at=r['observed_at_utc'],valid_until=r['valid_until_utc']) for r in rows])
    snapshot=datasets.capture(db,'QA','M1',stored_features=[('qa.x','1'),('qa.codes','1')])
    from mt5_extracao.analytics import associations
    result=associations.query_frozen_pair(db,snapshot.manifest_id,left=('qa.x','1'),right=('qa.codes','1'),
        as_of=pd.Timestamp.now(tz='UTC'),bootstrap_replicates=29,synthetic_fixture=True)
    assert result['right']['value_type']=='NUMBER' and result['right']['definition']['output_kind']=='category'
    assert result['relation']['effect'] is None and result['relation']['reason']=='UNSUPPORTED_VALUE_TYPE'
    choices=sdk.inspect_feature_snapshot(db,snapshot.manifest_id)
    assert next(r for r in choices['features'] if r['feature_id']=='qa.codes')['reason']=='UNSUPPORTED_VALUE_TYPE'


def test_dto_cannot_restore_a_category_kind_as_a_number():
    from mt5_extracao.analytics import relation_dto
    args=payload();args['left']['definition']['output_kind']='category'
    with pytest.raises(ValueError,match='saída numérica'):
        relation_dto.build_pair_relation_dto(**args)


def test_exact_saved_result_lookup_is_part_of_the_cursor_selection(reference):
    db,snapshot,cutoff=reference
    first=sdk.persist_feature_relation(db,snapshot.manifest_id,left=('qa.x','1'),right=('qa.positive','1'),
        as_of=cutoff,bootstrap_replicates=29,synthetic_fixture=True)
    second=sdk.persist_feature_relation(db,snapshot.manifest_id,left=('qa.x','1'),right=('qa.negative','1'),
        as_of=cutoff,bootstrap_replicates=29,synthetic_fixture=True)
    found=sdk.query_saved_relations(db,analysis_id=second['analysis_id'])
    assert found['status']=='ok' and found['matched']==1 and found['items'][0]['analysis_id']==second['analysis_id']
    assert sdk.query_saved_relations(db,analysis_id='bad')['status']=='error'
    page=sdk.query_saved_relations(db,page_size=1)
    wrong=sdk.query_saved_relations(db,analysis_id=first['analysis_id'],cursor=page['page']['next_cursor'])
    assert wrong['status']=='error' and wrong['errors'][0]['code']=='CURSOR_MISMATCH'


def test_cli_persistence_respects_the_existing_resource_policy(reference,tmp_path,capsys,monkeypatch):
    from mt5_extracao import cli
    db,snapshot,cutoff=reference
    config=tmp_path/'qa.ini'
    config.write_text(f'[DATABASE]\ntype=sqlite\npath={db.db_path}\n[APP]\nbroker_utc_offset=0\n'
        '[RESOURCES]\nblock_db_gb=0.000000001\n')
    monkeypatch.chdir(tmp_path)
    assert cli.main(['--config',str(config),'--json','analytics','features','relation','--persist',
        '--manifest',snapshot.manifest_id,'--left','qa.x','1','--right','qa.positive','1','--as-of',cutoff,
        '--bootstrap-replicates','29','--synthetic-fixture'])==1
    response=json.loads(capsys.readouterr().out)
    assert not response['ok']
    assert not inspect(db.engine).has_table('_feature_relations')
