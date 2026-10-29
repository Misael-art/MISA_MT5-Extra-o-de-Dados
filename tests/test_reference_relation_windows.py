"""BD16: fronteiras, amostra e orçamento de janelas pelo cálculo canônico."""
from copy import deepcopy
import json

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import inspect, text

from mt5_extracao import sdk
from mt5_extracao.analytics import associations, datasets, feature_store, features, indicator_materializer, query_service
from mt5_extracao.database_manager import DatabaseManager
from tests.test_frozen_association_query import reference


@pytest.fixture
def phase(reference):
    db,_,_=reference
    features.register(db,'qa.phase','Fases de referência','1',unit='fraction',value_type='NUMBER',timeframe='M1',
        formula='QA x in first10 sessions and -x in last10 sessions',dependencies=['bars:M1'])
    history=feature_store.query_history(db,feature_id='qa.x',algorithm_version='1')
    feature_store.save_values(db,[dict(symbol='QA',timeframe='M1',feature_id='qa.phase',algorithm_version='1',
        observation_time=r.observation_time,available_at=r.available_at_utc,observed_at=r.observed_at_utc,
        valid_until=r.valid_until_utc,value=float(r.numeric_value)*(1 if pd.Timestamp(r.observation_time).day<=10 else -1))
        for r in history.itertuples()])
    image=datasets.capture(db,'QA','M1',stored_features=[('qa.x','1'),('qa.phase','1')])
    return db,image,pd.Timestamp.now(tz='UTC').isoformat()


def rolling(reference,**kwargs):
    db,image,cutoff=reference
    options=dict(left=('qa.x','1'),right=('qa.positive','1'),as_of=cutoff,window_bars=80,stride_bars=40,
        bootstrap_replicates=29,synthetic_fixture=True)
    options.update(kwargs)
    return sdk.query_feature_relation_windows(db,image.manifest_id,**options)


def db_rows(db):
    with db.engine.connect() as c:
        return {t:c.execute(text('SELECT * FROM "'+t+'"')).all() for t in inspect(c).get_table_names()}


def stable(dto):
    dto=deepcopy(dto);dto.pop('generated_at_utc');return dto


@pytest.mark.parametrize('method',['pearson','spearman'])
def test_rolling_exposes_known_sign_changes_hidden_by_zero_aggregate(phase,method):
    db,image,cutoff=phase;before=db_rows(db)
    aggregate=sdk.query_feature_relation(db,image.manifest_id,left=('qa.x','1'),right=('qa.phase','1'),as_of=cutoff,
        method=method,bootstrap_replicates=29,synthetic_fixture=True)
    result=rolling(phase,right=('qa.phase','1'),method=method)
    xs=np.tile([-1.,-1.,1.,1.],40);ys=xs*np.repeat([1.,-1.],80)
    oracle=[float(np.corrcoef(xs[a:a+80],ys[a:a+80])[0,1]) for a in [0,40,80]]
    assert aggregate['relation']['effect']==pytest.approx(0.,abs=1e-12)
    assert oracle==pytest.approx([1.,0.,-1.],abs=1e-12)
    assert [w['result']['relation']['effect'] for w in result['windows']]==pytest.approx(oracle,abs=1e-12)
    for window in result['windows']:
        dto=window['result'];row=dto['relation']
        assert dto['relation_schema_version']==3 and dto['kind']=='FEATURE_PAIR_WINDOW_RELATION'
        assert row['n_pairs']==dto['population']['n_total']==80 and row['n_sessions']==10
        assert dto['population']['exclusions']=={} and dto['scope']['settings']['tests_counted']==3
        assert row['p_adjusted']==pytest.approx(min(1.,row['p_raw']*3))
        assert not dto['qualification']['financial_utility_certified'] and dto['synthetic_fixture']
    assert result['population']['overlapping_windows'] and result['population']['pair_evaluations']==240
    assert result['correction']['tests_counted']==3 and result['correction']['other_research_trials_accounted'] is False
    assert db_rows(db)==before and '_feature_relations' not in inspect(db.engine).get_table_names()
    json.dumps(result,allow_nan=False)


def test_campaign_prepares_root_and_both_channels_once(reference,monkeypatch):
    loads=[];contexts=[];original_load=datasets.load;original_context=associations._pair_context
    def load(db,ident):loads.append(ident);return original_load(db,ident)
    def context(*args,**kwargs):contexts.append(True);return original_context(*args,**kwargs)
    monkeypatch.setattr(datasets,'load',load);monkeypatch.setattr(associations,'_pair_context',context)
    result=rolling(reference)
    assert len(result['windows'])==3 and len(contexts)==2 and loads==[reference[1].manifest_id]


def test_each_window_is_the_same_canonical_query_and_can_be_persisted_and_transferred(reference,tmp_path):
    db,image,_=reference;report=rolling(reference)
    for window in report['windows']:
        direct=sdk.query_feature_relation(db,image.manifest_id,**window['result']['scope']['settings'])
        assert stable(direct)==stable(window['result'])
    window=report['windows'][1]
    receipt=sdk.persist_feature_relation(db,image.manifest_id,**window['result']['scope']['settings'])
    assert receipt['analysis_id']==window['analysis_id'] and stable(receipt['relation'])==stable(window['result'])
    assert sdk.persist_feature_relation(db,image.manifest_id,**window['result']['scope']['settings'])==dict(receipt,status='UNCHANGED')
    path=tmp_path/'window.json';sdk.export_feature_relation(db,receipt['analysis_id'],path)
    other=DatabaseManager(db_path=str(tmp_path/'other.sqlite'),broker_utc_offset=0)
    try:
        assert sdk.import_feature_relation(other,path)==receipt
        assert sdk.load_feature_relation(other,receipt['analysis_id'])==receipt['relation']
    finally:other.close()


def test_lag_cannot_borrow_left_close_before_window_or_compress_session_gaps(reference):
    db,image,cutoff=reference
    result=sdk.query_feature_relation(db,image.manifest_id,left=('qa.x','1'),right=('qa.lagged','1'),as_of=cutoff,
        lag_bars=1,decision_start='2025-01-06T09:02Z',decision_end='2025-01-15T09:08Z',
        bootstrap_replicates=29,synthetic_fixture=True)
    assert result['population']['n_total']==79 and result['relation']['n_pairs']==69
    assert result['population']['exclusions']=={'NO_REFERENCE_BAR':9,'REFERENCE_OUTSIDE_WINDOW':1}
    assert result['relation']['effect']==pytest.approx(1.,abs=1e-12)
    assert result['scope']['decision_window']['reference_rule']=='both_closes_inside_window'


def test_empty_and_insufficient_windows_keep_reasons_and_do_not_lower_minima(reference):
    report=rolling(reference,window_bars=40,stride_bars=40)
    assert len(report['windows'])==4
    for w in report['windows']:
        assert w['result']['relation']['effect'] is None
        assert 'MINIMUM_SESSIONS' in w['result']['relation']['reason']
        assert w['result']['scope']['settings']['minimum_sessions']==10
    empty=rolling(reference,window_bars=200)
    assert empty['status']=='insufficient' and not empty['windows']
    assert empty['population']['decisions_not_in_any_window']==empty['population']['uncovered_tail_decisions']==160
    replay=sdk.query_feature_relation_windows(reference[0],reference[1].manifest_id,**empty['selection'])
    assert replay['provenance']['analysis_id']==empty['provenance']['analysis_id']
    db,image,cutoff=reference
    blank=sdk.query_feature_relation(db,image.manifest_id,left=('qa.x','1'),right=('qa.positive','1'),as_of=cutoff,
        decision_start='2025-02-01T00:00Z',decision_end='2025-02-02T00:00Z',bootstrap_replicates=29)
    assert blank['relation']['effect'] is None and blank['relation']['reason']=='NO_DECISIONS_IN_WINDOW'


def test_tail_and_internal_uncovered_decisions_are_explicit(reference):
    tail=rolling(reference,window_bars=100,stride_bars=100)
    assert tail['population']['uncovered_tail_decisions']==tail['population']['decisions_not_in_any_window']==60
    gaps=rolling(reference,window_bars=30,stride_bars=80)
    assert len(gaps['windows'])==2 and gaps['population']['decisions_not_in_any_window']==100
    assert gaps['population']['uncovered_tail_decisions']==50


@pytest.mark.parametrize('changes',[
    dict(window_bars=True),dict(window_bars=2),dict(stride_bars=0),dict(max_windows=65),dict(tests_counted=2),
    dict(decision_start='2025-01-01T09:01Z'),
    dict(decision_start='2025-01-01T09:01',decision_end='2025-01-10T09:08Z'),
    dict(decision_start='2025-01-10T09:01Z',decision_end='2025-01-01T09:08Z'),
])
def test_invalid_selection_does_not_publish_partial_results(reference,changes):
    before=db_rows(reference[0])
    with pytest.raises(ValueError):rolling(reference,**changes)
    assert db_rows(reference[0])==before


def test_window_and_pair_visit_caps_refuse_without_truncation_or_inference(reference,monkeypatch):
    monkeypatch.setattr(associations,'analyze_association',lambda *a,**k:pytest.fail('No inference beyond budget'))
    with pytest.raises(ValueError,match='max_windows'):rolling(reference,max_windows=2)
    monkeypatch.setattr(query_service,'MAX_SELECTION_ROWS',2000)
    with pytest.raises(ValueError,match='orçamento'):rolling(reference,stride_bars=4)


def test_family_may_be_explicitly_larger_and_execution_budget_does_not_change_identity(reference):
    first=rolling(reference,tests_counted=10)
    second=rolling(reference,tests_counted=10,max_windows=3,timeout_seconds=100,max_memory_mb=1024)
    assert first['provenance']['analysis_id']==second['provenance']['analysis_id']
    for window in first['windows']:
        row=window['result']['relation'];assert row['p_adjusted']==pytest.approx(min(1.,10*row['p_raw']))


def test_fixed_cutoff_cannot_use_calendar_received_after_that_cutoff(reference):
    report=rolling(reference,as_of='2025-01-10T09:08Z')
    assert report['population']['closed_decisions']==80 and len(report['windows'])==1
    row=report['windows'][0]['result']
    assert row['relation']['effect'] is None and row['population']['exclusions']=={'SESSION_UNAVAILABLE_AS_OF':80}


def test_cancellation_between_windows_and_limits_return_no_partial_campaign(reference,monkeypatch):
    stopped=[False];original=associations.analyze_association
    def analyze(*args,**kwargs):result=original(*args,**kwargs);stopped[0]=True;return result
    monkeypatch.setattr(associations,'analyze_association',analyze)
    before=db_rows(reference[0])
    with pytest.raises(indicator_materializer.Cancelled):rolling(reference,cancel_check=lambda:stopped[0])
    assert db_rows(reference[0])==before
    with pytest.raises(MemoryError):rolling(reference,max_memory_mb=.000001)
    with pytest.raises(TimeoutError):rolling(reference,timeout_seconds=.000001)


def test_window_contract_refuses_scope_or_cutoff_mismatch_before_persistence(reference):
    db,_,_=reference;dto=rolling(reference)['windows'][0]['result']
    changed=deepcopy(dto);changed['scope']['decision_window']['end_inclusive_utc']='2025-01-20T09:08:00.000000Z'
    with pytest.raises(ValueError,match='Janela'):feature_store.save_pair_relation(db,changed)
    changed=deepcopy(dto);changed['scope']['settings']['as_of']='2025-02-01T00:00Z'
    with pytest.raises(ValueError,match='Janela'):feature_store.save_pair_relation(db,changed)


@pytest.mark.parametrize('changes',[
    dict(decision_start='2025-01-01T09:01:00.000000001Z',decision_end='2025-01-10T09:08Z'),
    dict(as_of='2026-10-29T00:00:00.000000001Z'),
])
def test_window_precision_is_not_silently_rounded(reference,changes):
    with pytest.raises(ValueError,match='microssegundos'):rolling(reference,**changes)


def test_global_dto2_remains_compatible_and_window_dto3_is_explicit(reference):
    from mt5_extracao.analytics import relation_dto
    db,image,cutoff=reference
    dto=sdk.query_feature_relation(db,image.manifest_id,left=('qa.x','1'),right=('qa.positive','1'),as_of=cutoff,
        bootstrap_replicates=29)
    assert dto['relation_schema_version']==2 and dto['kind']=='FEATURE_PAIR_RELATION'
    old=deepcopy(dto);old['scope']['settings']=None
    payload={k:old[k] for k in ('left','right','relation','scope','population','provenance','generated_at_utc','synthetic_fixture')}
    assert relation_dto.build_pair_relation_dto(**payload)==old
    window=rolling(reference)['windows'][0]['result']
    assert window['relation_schema_version']==3 and json.loads(relation_dto.dumps(window))==window
    changed=deepcopy(window);changed['scope']['settings']['decision_start']='2025-01-01T09:01:00.000000001Z'
    with pytest.raises(ValueError,match='Janela'):feature_store.save_pair_relation(db,changed)
