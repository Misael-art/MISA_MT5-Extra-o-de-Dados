"""BD15a: consulta real de pares congelados, calendário conhecido e tipos explícitos."""
import json

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import text

from mt5_extracao.analytics import associations, calendar, datasets, feature_store, features, relation_dto
from mt5_extracao.database_manager import DatabaseManager

VERSION='1'
IDS=('qa.x','qa.positive','qa.negative','qa.independent','qa.constant','qa.missing','qa.category','qa.flag','qa.lagged')


def seed_reference(db):
    datasets.declare_source(db,dict(provider='qa',server='Synthetic'))
    times=pd.DatetimeIndex([pd.Timestamp('2025-01-01T09:00')+pd.Timedelta(days=day,minutes=bar)
        for day in range(20) for bar in range(8)])
    bars=pd.DataFrame(dict(time=times,open=100.,high=101.,low=99.,close=100.,tick_volume=10.,real_volume=0.,spread=1.))
    assert db.save_ohlcv_data('QA','1 minuto',bars)
    _,sessions=calendar.sessionize_m1(bars,'QA',broker_utc_offset=0)
    calendar.save_sessions(db,sessions)
    for ident in IDS:
        kind='CATEGORY' if ident=='qa.category' else 'BOOLEAN' if ident=='qa.flag' else 'NUMBER'
        features.register(db,ident,ident,VERSION,dependencies=['bars:M1'],unit='price_units' if ident=='qa.negative' else
            'category' if kind=='CATEGORY' else 'boolean' if kind=='BOOLEAN' else 'fraction',value_type=kind,
            formula='Declared mathematical reference',warmup_count=0,warmup_unit='bars',source_id='synthetic_reference',
            timeframe='M1',availability_rule='closed_bar_received; expires at next nominal close')
    rows=[];xs=np.tile([-1.,-1.,1.,1.],40);independent=np.tile([-1.,1.,-1.,1.],40)
    for n,stamp in enumerate(times):
        observation=stamp.tz_localize('UTC');available=observation+pd.Timedelta(minutes=1)
        values=dict(zip(IDS,(float(xs[n]),float(2*xs[n]+1),float(-3*xs[n]+7),float(independent[n]),2.,None,'1',True,
                            float(xs[n-1]) if n%8 else None)))
        for ident,value in values.items():
            if value is None:continue
            rows.append(dict(symbol='QA',timeframe='M1',feature_id=ident,algorithm_version=VERSION,
                observation_time=observation,available_at=available,observed_at=available,valid_until=available+pd.Timedelta(minutes=1),value=value))
    feature_store.save_values(db,rows)
    snapshot=datasets.capture(db,'QA','M1',stored_features=[(ident,VERSION) for ident in IDS])
    cutoff=pd.Timestamp.now(tz='UTC').isoformat()
    return db,snapshot,cutoff


@pytest.fixture
def reference(tmp_path):
    db=DatabaseManager(db_path=str(tmp_path/'reference.sqlite'),broker_utc_offset=0)
    try:
        yield seed_reference(db)
    finally:
        db.close()


def query(reference,right='qa.positive',**kwargs):
    db,snapshot,cutoff=reference
    return associations.query_frozen_pair(db,snapshot.manifest_id,left=('qa.x',VERSION),right=(right,VERSION),
        as_of=cutoff,bootstrap_replicates=29,synthetic_fixture=True,**kwargs)


@pytest.mark.parametrize('method',['pearson','spearman'])
@pytest.mark.parametrize('right,expected',[('qa.positive',1.),('qa.negative',-1.),('qa.independent',0.)])
def test_reference_pair_effects_are_computed_from_frozen_facts_and_remain_descriptive(reference,method,right,expected):
    db,snapshot,cutoff=reference
    with db.engine.connect() as conn:
        before=conn.execute(text('SELECT * FROM _feature_values ORDER BY feature_id,observation_time')).all()
    result=query(reference,right,method=method)
    assert result['relation_schema_version']==2 and result['kind']=='FEATURE_PAIR_RELATION'
    assert result['relation']['effect']==pytest.approx(expected,abs=1e-12)
    assert result['relation']['n_pairs']==160 and result['relation']['n_sessions']==20
    assert result['relation']['evidence_state']=='EXPLORATORY' and result['relation']['utility_state']=='INCONCLUSIVE'
    assert result['population']['n_distinct_source_pairs']==160 and result['population']['exclusions']=={}
    assert result['provenance']['snapshot_manifest_id']==snapshot.manifest_id and result['provenance']['sample_hash']
    assert not result['qualification']['causal_evidence'] and not result['qualification']['operational_acceptance']
    assert json.loads(relation_dto.dumps(result))==result
    with db.engine.connect() as conn:
        assert conn.execute(text('SELECT * FROM _feature_values ORDER BY feature_id,observation_time')).all()==before


@pytest.mark.parametrize('right,reason',[('qa.constant','CONSTANT'),('qa.missing','RIGHT_UNAVAILABLE'),('qa.category','UNSUPPORTED'),('qa.flag','UNSUPPORTED')])
def test_unavailable_degenerate_or_categorical_inputs_do_not_receive_numeric_effects(reference,right,reason):
    result=query(reference,right)
    assert result['relation']['effect'] is None and reason in result['relation']['reason']
    assert result['relation']['evidence_state']=='INSUFFICIENT'
    population=result['population']
    assert population['n_total']==population['n_eligible']+sum(population['exclusions'].values())


def test_nominal_lag_does_not_compress_overnight_gaps_or_change_session(reference):
    result=query(reference,'qa.lagged',lag_bars=1)
    assert result['relation']['effect']==pytest.approx(1.,abs=1e-12)
    assert result['relation']['n_pairs']==140 and result['population']['exclusions']=={'NO_REFERENCE_BAR':20}
    assert 'nominal' in result['scope']['lag_rule']


def test_live_correction_does_not_change_frozen_analysis_identity(reference):
    db,snapshot,cutoff=reference;before=query(reference)
    feature_store.save_value(db,'QA','M1','qa.positive',VERSION,'2025-01-20T09:07Z',-999.,
        available_at=cutoff,observed_at=cutoff)
    after=query(reference)
    assert before['relation']==after['relation'] and before['population']==after['population']
    assert before['provenance']['analysis_id']==after['provenance']['analysis_id']


def test_calendar_not_known_by_cut_is_not_invented_from_dates(reference):
    db,snapshot,cutoff=reference
    result=associations.query_frozen_pair(db,snapshot.manifest_id,left=('qa.x',VERSION),right=('qa.positive',VERSION),
        as_of='2025-02-01T00:00Z',bootstrap_replicates=9,synthetic_fixture=True)
    assert result['relation']['effect'] is None
    assert result['population']['exclusions']=={'SESSION_UNAVAILABLE_AS_OF':160}


def test_explicit_multiple_comparison_count_and_seed_are_part_of_identity(reference):
    first=query(reference,tests_counted=10);second=query(reference,tests_counted=20)
    assert first['relation']['p_adjusted']==min(1.,10*first['relation']['p_raw'])
    assert first['provenance']['analysis_id']!=second['provenance']['analysis_id']


def test_unknown_version_future_cut_and_cancellation_do_not_return_partial_artifacts(reference):
    db,snapshot,cutoff=reference
    with pytest.raises(ValueError,match='não congelada'):
        associations.query_frozen_pair(db,snapshot.manifest_id,left=('qa.x','absent'),right=('qa.positive',VERSION),as_of=cutoff)
    with pytest.raises(ValueError,match='Corte futuro'):
        associations.query_frozen_pair(db,snapshot.manifest_id,left=('qa.x',VERSION),right=('qa.positive',VERSION),as_of='2099-01-01T00:00Z')
    with pytest.raises(RuntimeError,match='cancelada'):
        query(reference,cancel_check=lambda:True)


def materialized_reference(reference,*,pivot=False):
    db,snapshot,cutoff=reference
    parent='qa.x'
    if pivot:
        parent='qa.pivot'
        features.register(db,parent,'Pivô de referência','1',unit='fraction',value_type='NUMBER',family='structure')
        history=feature_store.query_history(db,feature_id='qa.x',algorithm_version='1')
        feature_store.save_values(db,[dict(symbol='QA',timeframe='M1',feature_id=parent,algorithm_version='1',
            observation_time=r.observation_time,available_at=r.available_at_utc,observed_at=r.observed_at_utc,
            valid_until=r.valid_until_utc,value=r.numeric_value,metadata={'provisional':True}) for r in history.itertuples()])
    difference=('qa.difference','1');absolute=('qa.absolute','1')
    features.register_composite(db,difference[0],'Diferença',difference[1],operation='difference',inputs=[(parent,'1'),('qa.positive','1')])
    features.register_composite(db,absolute[0],'Módulo',absolute[1],operation='absolute',inputs=[difference])
    origin=datasets.capture(db,'QA','M1',stored_features=[(parent,'1'),('qa.positive','1'),difference,absolute])
    result=features.materialize_snapshot(db,origin.manifest_id,targets=[absolute],as_of=cutoff)
    pair=next((r['feature_id'],r['algorithm_version']) for r in result['outputs'] if r['feature_id']==absolute[0])
    final=datasets.capture(db,'QA','M1',stored_features=[pair,('qa.positive','1')])
    return pair,origin,final


def test_materialized_composite_reads_immutable_ancestry_and_does_not_create_a_new_run(reference):
    db,snapshot,cutoff=reference;pair,origin,final=materialized_reference(reference)
    with db.engine.connect() as conn:count=conn.execute(text('SELECT COUNT(*) FROM _analytics_producer_runs')).scalar_one()
    result=associations.query_frozen_pair(db,final.manifest_id,left=pair,right=('qa.positive','1'),as_of=cutoff,
        bootstrap_replicates=29,synthetic_fixture=True)
    assert result['relation']['effect']==pytest.approx(1.,abs=1e-12)
    assert origin.manifest_id in result['provenance']['lineage_manifests']
    assert result['population']['n_eligible']==160
    with db.engine.connect() as conn:assert conn.execute(text('SELECT COUNT(*) FROM _analytics_producer_runs')).scalar_one()==count


def test_unqualified_pivot_is_blocked_before_composition_and_in_direct_relations(reference):
    db,snapshot,cutoff=reference
    with pytest.raises(ValueError,match='Pivôs armazenados exigem qualificação'):
        materialized_reference(reference,pivot=True)
    final=datasets.capture(db,'QA','M1',stored_features=[('qa.pivot','1'),('qa.positive','1')])
    result=associations.query_frozen_pair(db,final.manifest_id,left=('qa.pivot','1'),right=('qa.positive','1'),as_of=cutoff,
        bootstrap_replicates=29,synthetic_fixture=True)
    assert result['relation']['effect'] is None and result['relation']['reason']=='UNVERIFIED_STORED_EVENT'
    assert result['population']['exclusions']=={'UNVERIFIED_STORED_EVENT':160}


def test_missing_materialization_origins_are_not_completed_from_live_values(reference,tmp_path):
    db,snapshot,cutoff=reference;pair,origin,final=materialized_reference(reference)
    exported=tmp_path/'root.json';datasets.export_snapshot(db,final.manifest_id,exported)
    foreign=DatabaseManager(db_path=str(tmp_path/'foreign.sqlite'),broker_utc_offset=0)
    try:
        datasets.import_snapshot(foreign,exported)
        result=associations.query_frozen_pair(foreign,final.manifest_id,left=pair,right=('qa.positive','1'),as_of=cutoff,
            bootstrap_replicates=9,synthetic_fixture=True)
        assert result['relation']['effect'] is None and result['relation']['reason']=='UNVERIFIED_MATERIALIZED_LINEAGE'
        assert result['provenance']['lineage_diagnostics']
        origin_export=tmp_path/'origin.json';datasets.export_snapshot(db,origin.manifest_id,origin_export)
        datasets.import_snapshot(foreign,origin_export)
        restored=associations.query_frozen_pair(foreign,final.manifest_id,left=pair,right=('qa.positive','1'),as_of=cutoff,
            bootstrap_replicates=9,synthetic_fixture=True)
        assert restored['relation']['effect']==pytest.approx(1.,abs=1e-12)
    finally:foreign.close()


def test_same_id_different_frozen_versions_do_not_collide(reference):
    db,snapshot,cutoff=reference
    features.register(db,'qa.x','Segunda versão','2',unit='fraction',value_type='NUMBER')
    history=feature_store.query_history(db,feature_id='qa.x',algorithm_version='1')
    feature_store.save_values(db,[dict(symbol='QA',timeframe='M1',feature_id='qa.x',algorithm_version='2',
        observation_time=r.observation_time,available_at=r.available_at_utc,observed_at=r.observed_at_utc,
        valid_until=r.valid_until_utc,value=2*r.numeric_value+1) for r in history.itertuples()])
    final=datasets.capture(db,'QA','M1',stored_features=[('qa.x','1'),('qa.x','2')])
    result=associations.query_frozen_pair(db,final.manifest_id,left=('qa.x','1'),right=('qa.x','2'),as_of=cutoff,
        bootstrap_replicates=9,synthetic_fixture=True)
    assert result['relation']['effect']==pytest.approx(1.) and result['right']['algorithm_version']=='2'


def test_query_does_not_read_live_catalog_or_history(reference,monkeypatch):
    def forbidden(*a,**kw):pytest.fail('sem leitura live')
    monkeypatch.setattr(feature_store,'query_history',forbidden)
    monkeypatch.setattr(feature_store,'list_definitions',forbidden)
    assert query(reference)['relation']['effect']==pytest.approx(1.)


def test_sdk_and_cli_delegate_with_own_configuration(reference,tmp_path,capsys):
    from mt5_extracao import cli,sdk
    db,snapshot,cutoff=reference
    config=tmp_path/'qa.ini'
    config.write_text(f'[DATABASE]\ntype=sqlite\npath={db.db_path}\n[APP]\nbroker_utc_offset=0\n',encoding='utf-8')
    assert cli.main(['--config',str(config),'--json','analytics','features','relation','--manifest',snapshot.manifest_id,
        '--left','qa.x','1','--right','qa.positive','1','--as-of',cutoff,'--bootstrap-replicates','29','--synthetic-fixture'])==0
    result=json.loads(capsys.readouterr().out)['data']['relation']
    expected=sdk.query_feature_relation(db,snapshot.manifest_id,left=('qa.x','1'),right=('qa.positive','1'),as_of=cutoff,
        bootstrap_replicates=29,synthetic_fixture=True)
    assert result['relation']==expected['relation'] and result['provenance']['analysis_id']==expected['provenance']['analysis_id']
