"""Ledger real, expurgo temporal, família completa e leitura sem inferência."""
from copy import deepcopy
import json

import numpy as np
import pandas as pd
import pytest

from mt5_extracao import sdk,cli
from mt5_extracao.analytics import datasets
from mt5_extracao.experiments import relations,store
from mt5_extracao.laboratory import service,report as portable
from tests.test_mentions_contract import db


def definition(report):
    t=report['times'];n=len(t);edges=[0,n//3,n*2//3,n]
    return dict(name='qa.relations',revision=1,channels=[c['id'] for c in report['channels']],horizons=[1,6],
        windows={role:dict(start=t[a],end=t[b] if b<n else (pd.Timestamp(t[-1])+pd.Timedelta(nanoseconds=1)).isoformat()) for role,a,b in zip(relations.ROLES,edges[:-1],edges[1:])},
        session_basis='utc_day_exploratory',method='spearman',bootstrap_replicates=99,seed=7)


@pytest.fixture
def research(db):
    rng=np.random.default_rng(192);prices=100+np.cumsum(rng.normal(0,.2,365))
    db.save_ohlcv_data('X','1 dia',pd.DataFrame(dict(time=pd.date_range('2024-01-01',periods=365,freq='D'),open=prices,high=prices+.5,low=prices-.5,close=prices,tick_volume=rng.integers(5,80,365),real_volume=100,spread=1)))
    r=service.analyze(service.capture_selection(db,symbol='X',timeframes_selected=['D1']),feature_ids=['price.return_1','volume.ticks'],as_of='2025-01-02T00:00Z')
    return db,r,definition(r)


def test_registered_family_purges_boundaries_and_counts_all_windows(research):
    db,r,spec=research;p=sdk.register_laboratory_protocol(db,r,spec)
    result=sdk.evaluate_laboratory_protocol(db,r,p['protocol_id'],key='first')
    assert result['tests_counted']==12 and len(result['results'])==12
    assert all(row['multiple_testing']['tests_counted']==12 for row in result['results'])
    assert all(row['purged_boundary']==row['horizon'] for row in result['results'] if row['role']!='holdout')
    assert all(row['period_end_utc']<spec['windows'][row['role']]['end'] for row in result['results'])
    assert not result['qualified_final_holdout'] and not result['predictive_qualification']
    assert store.status(db,p['protocol_id'])['exposure']['holdout']['recorded_accesses']==1
    assert relations.validate_receipt(r,result)==result
    html=portable.render(r,evaluation=result);assert 'evaluation-results' in html and 'Bonferroni' in html
    altered=deepcopy(result);altered['results'][0]['p_adjusted']=0
    with pytest.raises(ValueError,match='alterado'):portable.render(r,evaluation=altered)


def test_invalid_window_overlap_and_implicit_calendar_are_refused(research):
    db,r,spec=research
    overlap=deepcopy(spec);overlap['windows']['validation']['start']=overlap['windows']['train']['start']
    with pytest.raises(ValueError,match='sobreposição'):relations.register(db,r,overlap)
    spec['session_basis']='frozen_calendar'
    with pytest.raises(ValueError,match='calendário'):relations.register(db,r,spec)


def test_late_feature_clocks_excluded_and_invalid_tests_stay_in_family(research):
    db,r,spec=research
    r['channels'][0]['source_ends']=['2026-01-01T00:00Z']*len(r['times'])
    p=relations.register(db,r,spec);result=relations.evaluate(db,r,p['protocol_id'],key='late')
    rows=[v for v in result['results'] if v['channel_id']==r['channels'][0]['id']]
    assert all(v['n_pairs']==0 and v['n_temporal_rejected']>0 and v['p_adjusted'] is None for v in rows)
    assert result['tests_counted']==12


def test_protocol_immutable_cached_read_and_cancelled_exposure(research,monkeypatch):
    db,r,spec=research;p=relations.register(db,r,spec);pid=p['protocol_id']
    from mt5_extracao.experiments import service as strategy_experiments
    with pytest.raises(ValueError,match='associações'):strategy_experiments.request(db,pid,'wrong-executor',{})
    altered=deepcopy(spec);altered['seed']=8
    with pytest.raises(ValueError,match='imutável'):relations.register(db,r,altered)
    result=relations.evaluate(db,r,pid,key='complete')
    monkeypatch.setattr(relations.associations,'analyze_association',lambda *a,**k:(_ for _ in ()).throw(AssertionError('read recomputed')))
    assert relations.evaluate(db,r,pid,key='complete')==result
    def cancelled(*a,**k):raise relations.indicator_materializer.Cancelled('QA')
    monkeypatch.setattr(relations,'_evaluate',cancelled)
    with pytest.raises(relations.indicator_materializer.Cancelled):relations.evaluate(db,r,pid,key='cancelled')
    status=store.status(db,pid);assert status['exposure']['holdout']['recorded_accesses']==2
    assert any(e['kind']=='relations_evaluation_failed' and e['payload']['cancelled'] for e in status['events'])
    with pytest.raises(ValueError,match='admitida'):relations.evaluate(db,r,pid,key='cancelled')


def test_cli_register_evaluate_and_export_same_receipt(research,tmp_path,capsys):
    db,r,spec=research
    cfg=tmp_path/'qa.ini';cfg.write_text(f'[DATABASE]\ntype=sqlite\npath={db.db_path}\n[APP]\nbroker_utc_offset=0\n')
    source=tmp_path/'analysis.json';source.write_text(json.dumps(r));definition_path=tmp_path/'definition.json';definition_path.write_text(json.dumps(spec))
    prefix=['--config',str(cfg),'--json','laboratory']
    assert cli.main(prefix+['protocol-register','--analysis',str(source),'--definition',str(definition_path)])==0
    p=json.loads(capsys.readouterr().out)['data'];out=tmp_path/'result.json'
    assert cli.main(prefix+['protocol-evaluate','--analysis',str(source),'--protocol',p['protocol_id'],'--key','cli','--out',str(out)])==0
    capsys.readouterr();result=json.loads(out.read_text());assert result==relations.receipt(db,p['protocol_id'],'cli')
    assert cli.main(prefix+['report','--analysis',str(source),'--out',str(tmp_path/'report.html'),'--evaluation',str(out)])==0
