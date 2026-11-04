"""Lotes de observações equivalem à API escalar, sem cruzar barras/validade."""
import json

import pandas as pd
import pytest
from sqlalchemy import text

from mt5_extracao.analytics import feature_store, features, indicator_context, indicator_materializer, producer_jobs
from tests.test_feature_compositions import A, ABS, ALL, B, define, freeze, put, source, db
from tests.test_frozen_observation_context import frozen, LATE
from tests.test_composite_materialization import materialize, output_facts


def batch_context(snapshot, decisions, observations, targets=(ABS,)):
    plan=features._composition_plan(snapshot,targets)
    history=snapshot.feature_history
    def read(decisions, ids, version, observation):
        return indicator_context._snapshot_context(decisions,manifest_id=snapshot.manifest_id,
            manifest=snapshot.manifest,definitions=plan['definitions'],history=history,
            window=snapshot.feature_window_utc,feature_ids=ids,algorithm_version=version,observation_times=observations)
    return features._compose_plan(snapshot,decisions,plan,read_context=read)


def test_mixed_unsorted_repeated_observations_and_missing_leaves_match_scalar_api(source,frozen):
    put(source,A,20.,observation='2025-01-01T00:02Z',available='2025-01-01T00:03Z')
    snapshot=freeze(source)
    observations=['2025-01-01T00:00Z','2025-01-01T00:01Z','2025-01-01T00:02Z',
        '2025-01-01T00:00Z','2025-01-01T00:01Z','2025-01-01T00:00Z','2025-01-01T00:00Z','2025-01-01T00:00Z']
    times=['2025-01-01T00:01Z','2025-01-01T00:01:10Z','2025-01-01T00:03:10Z',
        '2025-01-01T00:01:30Z','2025-01-01T00:03:15Z','2025-01-01T00:03Z',LATE,'2025-01-01T00:03Z']
    decisions=pd.DataFrame(dict(decision_time=times,label=range(len(times))),index=range(20,28))
    rows=batch_context(snapshot,decisions,observations)['context']
    expected=[features.compose_snapshot(snapshot,decisions.iloc[[i]],targets=[ABS],observation_time=obs)['context'][0]
              for i,obs in enumerate(observations)]
    assert rows==expected
    assert [r['qa.absolute::value'] for r in rows]==[2.,None,None,None,26.,6.,3.,6.]
    assert [r['label'] for r in rows]==list(range(8))


def test_different_leaf_versions_share_the_same_row_observation_without_backfill(source):
    alternate=('qa.b','v2');difference=('qa.mixed_difference','1');absolute=('qa.mixed_absolute','1')
    features.register(source,*alternate[:1],name='Outro pai',algorithm_version='v2',unit='fraction',value_type='NUMBER')
    features.register_composite(source,difference[0],'Diferença',difference[1],operation='difference',inputs=[A,alternate])
    features.register_composite(source,absolute[0],'Módulo',absolute[1],operation='absolute',inputs=[difference])
    put(source,A,1.);put(source,alternate,4.)
    put(source,A,10.,observation='2025-01-01T00:01Z',available='2025-01-01T00:02Z')
    snapshot=freeze(source,[A,alternate,difference,absolute])
    observations=['2025-01-01T00:00Z','2025-01-01T00:01Z','2025-01-01T00:00Z']
    decisions=pd.DataFrame(dict(decision_time=['2025-01-01T00:02Z']*3))
    rows=batch_context(snapshot,decisions,observations,[absolute])['context']
    expected=[features.compose_snapshot(snapshot,decisions.iloc[[i]],targets=[absolute],observation_time=obs)['context'][0]
              for i,obs in enumerate(observations)]
    assert rows==expected and [r[absolute[0]+'::value'] for r in rows]==[3.,None,3.]


def test_late_receipt_scope_of_one_observation_does_not_extend_another(source,frozen):
    decisions=pd.DataFrame(dict(decision_time=[LATE,LATE]))
    observations=['2025-01-01T00:00Z','2025-01-01T00:01Z']
    assert features.compose_snapshot(frozen,decisions.iloc[[0]],targets=[ABS],observation_time=observations[0])
    with pytest.raises(ValueError,match='fora da janela'):
        batch_context(frozen,decisions,observations)


def test_selected_observation_column_cannot_disagree_with_batch_scope(source,frozen):
    decisions=pd.DataFrame(dict(decision_time=['2025-01-01T00:02Z'],selected_observation_time=['2025-01-01T00:01Z']))
    with pytest.raises(ValueError,match='outra origem/seleção'):
        batch_context(frozen,decisions,['2025-01-01T00:00Z'])


def test_integer_revision_identity_survives_missing_observation_without_float_rounding(source):
    define(source);put(source,A,1.);put(source,B,3.)
    put(source,B,9.,observation='2025-01-01T00:01Z',available='2025-01-01T00:02Z')
    snapshot=freeze(source);history=snapshot.feature_history
    large=2**53+1
    history.loc[history.feature_id==A[0],'revision']=large
    frame=indicator_context._snapshot_context(pd.DataFrame(dict(decision_time=['2025-01-01T00:02Z']*2)),
        manifest_id=snapshot.manifest_id,manifest=snapshot.manifest,definitions=snapshot.feature_definitions,
        history=history,window=snapshot.feature_window_utc,feature_ids=[A[0]],algorithm_version=A[1],
        observation_times=['2025-01-01T00:00Z','2025-01-01T00:01Z'])
    rows=indicator_context.records(frame)
    assert rows[0][A[0]+'::revision']==large and isinstance(rows[0][A[0]+'::revision'],int)
    assert rows[1][A[0]+'::revision'] is None and rows[1][A[0]+'::value'] is None


def test_complete_identity_join_matches_legacy_expansion_and_independent_point_in_time_oracle():
    rows=[]
    def add(symbol,ident,version,observation,available,value,revision=1,observed=None,until=None):
        stamp=lambda time:pd.Timestamp('2025-01-01T'+time+'Z')
        rows.append(dict(symbol=symbol,tf='M1',feature_id=ident,algorithm_version=version,
            observation_time=stamp(observation),available_at_utc=stamp(available),
            observed_at_utc=stamp(observed or available),valid_until_utc=stamp(until) if until else pd.NaT,
            numeric_value=float(value),categorical_value=None,quality='VALID',revision=revision,
            source_revision=f'{symbol}:{ident}:{version}:{observation}:{revision}'))
    add('QA','x','1','00:00','00:01',1.,until='00:03')
    add('QA','x','1','00:01','00:02',10.,until='00:03')
    add('QA','x','1','00:00','00:02:30',999.,revision=2)
    add('QA','x','1','00:01','00:04',12.,revision=2)
    add('QB','x','1','00:00','00:02',1.,observed='00:01:30')
    add('QB','x','1','00:00','00:02',9.,revision=2,observed='00:01')
    add('QA','y','1','00:00','00:01',3.,until='00:02')
    add('QA','x','2','00:00','00:01',4.)
    history=pd.DataFrame(rows).sample(frac=1,random_state=17)
    requests=[]
    for symbol,ident,version in [('QA','x','1'),('QB','x','1'),('QA','y','1'),('QA','x','2'),('QA','absent','1')]:
        for time in ['00:00:59.999999999','00:01','00:02','00:02:30','00:03','00:04','00:05']:
            requests.append(dict(symbol=symbol,tf='M1',feature_id=ident,algorithm_version=version,
                                 decision_time=pd.Timestamp('2025-01-01T'+time+'Z'),label=len(requests)))
    decisions=pd.DataFrame(requests).sample(frac=1,random_state=19)
    fast=indicator_context.records(feature_store.asof_join(decisions,history))
    expanded=indicator_context.records(feature_store.asof_join(decisions.drop(columns='tf'),history))
    assert fast==expanded
    assert [r['label'] for r in fast]==[r['label'] for r in decisions.to_dict('records') if r['feature_id']!='absent']
    for result in fast:
        decision=next(r for r in requests if r['label']==result['label'])
        available=[r for r in rows if all(r[k]==decision[k] for k in ('symbol','tf','feature_id','algorithm_version'))
                   and r['available_at_utc']<=decision['decision_time']]
        chosen=max(available,key=lambda r:(r['observation_time'],r['available_at_utc'],r['observed_at_utc'],r['revision'])) if available else None
        if chosen and pd.notna(chosen['valid_until_utc']) and chosen['valid_until_utc']<=decision['decision_time']:
            chosen=None
        for field in ('numeric_value','quality','revision','source_revision','observation_time','available_at_utc'):
            expected=chosen[field] if chosen else None
            if isinstance(expected,pd.Timestamp):expected=expected.isoformat()
            assert result.get(field)==expected


def test_crossing_calculation_batch_boundary_keeps_held_branch_deduplicated(source,monkeypatch):
    define(source)
    held=('qa.absolute_a','1')
    features.register_composite(source,held[0],'Módulo do pai',held[1],operation='absolute',inputs=[A])
    put(source,A,1.)
    rows=[]
    for n in range(260):
        stamp=pd.Timestamp('2025-01-01T00:01Z')+pd.Timedelta(seconds=n)
        rows.append(dict(symbol='QA',timeframe='M1',feature_id=B[0],algorithm_version=B[1],
            observation_time='2025-01-01T00:00Z',available_at=stamp,observed_at=stamp,value=float(n%3)))
    feature_store.save_values(source,rows)
    snapshot=freeze(source,ALL+[held]);calls=[];compose=features._compose_plan
    def count(*args,**kwargs):
        calls.append(len(args[1]));return compose(*args,**kwargs)
    monkeypatch.setattr(features,'_compose_plan',count)
    result=features.materialize_snapshot(source,snapshot.manifest_id,targets=[ABS,held],as_of='2026-01-01T00:00Z')
    assert result['inserted']+result['revised']==521 and calls==[256,4,256,4]
    history=feature_store.query_history(source)
    assert len(history[history.feature_id==held[0]])==1
    absolute=history[history.feature_id==ABS[0]].sort_values('available_at_utc')
    assert absolute.numeric_value.tolist()==[abs(1.-n%3) for n in range(260)]


def test_many_observations_use_bounded_shared_execution_and_independent_values(source,monkeypatch):
    define(source)
    source.save_ohlcv_data('QA','1 minuto',pd.DataFrame(dict(time=pd.date_range('2025-01-01',periods=260,freq='min'),
        open=100.,high=101.,low=99.,close=100.,tick_volume=10.,real_volume=0.,spread=1.)))
    rows=[]
    for n in range(260):
        observation=pd.Timestamp('2025-01-01T00:00Z')+pd.Timedelta(minutes=n)
        for pair,value in ((A,float(n+1)),(B,float(2*n+3))):
            rows.append(dict(symbol='QA',timeframe='M1',feature_id=pair[0],algorithm_version=pair[1],
                observation_time=observation,available_at=observation+pd.Timedelta(minutes=1),
                observed_at=observation+pd.Timedelta(minutes=1),value=value))
    feature_store.save_values(source,rows);snapshot=freeze(source)
    calls=[];compose=features._compose_plan
    def count(*args,**kwargs):
        calls.append(len(args[1]));return compose(*args,**kwargs)
    monkeypatch.setattr(features,'_compose_plan',count)
    result=materialize(source,snapshot)
    assert result['inserted']==520 and calls==[256,4,256,4]
    history=feature_store.query_history(source,feature_id=ABS[0]).sort_values('observation_time')
    assert history.numeric_value.tolist()==[float(n+2) for n in range(260)]


def test_in_place_operator_code_change_after_last_data_commit_prevents_false_complete(source,frozen,monkeypatch):
    function=features._COMPOSITE_OPERATORS['absolute']['evaluate'];original_code=function.__code__
    save=feature_store.save_values;changed=[]
    def mutate(manager,records,**kwargs):
        result=save(manager,records,**kwargs)
        if records and not changed:
            changed.append(True)
            monkeypatch.setattr(function,'__code__',(lambda values:abs(values[0])+1.).__code__)
        return result
    monkeypatch.setattr(feature_store,'save_values',mutate)
    with pytest.raises(indicator_materializer.SourceChanged,match='cálculo mudou'):
        materialize(source,frozen)
    with source.engine.connect() as conn:
        assert conn.execute(text('SELECT status FROM _analytics_producer_runs')).scalar_one()=='FAILED'
        assert conn.execute(text('SELECT status FROM _analytics_producer_checkpoints')).scalar_one()=='RUNNING'
    assert len(output_facts(source))==10
    monkeypatch.setattr(function,'__code__',original_code);monkeypatch.setattr(feature_store,'save_values',save)
    assert materialize(source,frozen)['job']['status']=='SKIPPED' and len(output_facts(source))==10
