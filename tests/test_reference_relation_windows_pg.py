"""Mesma campanha/recorte, sem fatos ativos, no schema PostgreSQL de QA."""
import pytest
from sqlalchemy import inspect
from tests.test_analytics_profiles_pg import PG_URL
from tests.test_composite_materialization_pg import pg
from tests.test_reference_relation_windows import reference,phase
from mt5_extracao import sdk
from mt5_extracao.analytics import datasets

pytestmark=pytest.mark.skipif(not PG_URL,reason='defina MT5X_TEST_PG_URL para testar com PostgreSQL')


def test_rolling_windows_and_dto3_roundtrip_in_postgresql_without_live_facts(phase,pg,tmp_path):
    source,image,cutoff=phase
    path=tmp_path/'image.json';datasets.export_snapshot(source,image.manifest_id,path)
    datasets.import_snapshot(pg,path)
    result=sdk.query_feature_relation_windows(pg,image.manifest_id,left=('qa.x','1'),right=('qa.phase','1'),as_of=cutoff,
        window_bars=80,stride_bars=40,bootstrap_replicates=29,synthetic_fixture=True)
    assert [w['result']['relation']['effect'] for w in result['windows']]==pytest.approx([1.,0.,-1.],abs=1e-12)
    selected=result['windows'][1]
    receipt=sdk.persist_feature_relation(pg,image.manifest_id,**selected['result']['scope']['settings'])
    assert receipt['analysis_id']==selected['analysis_id'] and receipt['relation']['relation_schema_version']==3
    assert sdk.persist_feature_relation(pg,image.manifest_id,**selected['result']['scope']['settings'])==dict(receipt,status='UNCHANGED')
    assert sdk.load_feature_relation(pg,receipt['analysis_id'])==receipt['relation']
    assert sdk.query_saved_relations(pg,analysis_id=receipt['analysis_id'])['matched']==1
    exported=tmp_path/'window.json';sdk.export_feature_relation(pg,receipt['analysis_id'],exported)
    assert sdk.import_feature_relation(pg,exported)==dict(receipt,status='UNCHANGED')
    assert not inspect(pg.engine).has_table('_feature_values')
