"""MEN-03: API injetada com os mesmos contratos de rede/publicação da produção."""
from copy import deepcopy
import json
import threading

import pytest
import requests
import pandas as pd
from sqlalchemy import text

from mt5_extracao import sdk
from mt5_extracao.analytics import datasets, feature_store, indicator_materializer, mention_sources, mentions, producer_jobs
from tests.test_mentions_contract import db, selection


def hn_selection():
    options=selection();options['source'].update(provider='hackernews',corpus=mention_sources.HN_CORPUS,parameters={'panel_size':2})
    options['query']['languages']=[];options['classifier']=None;options['universe']='sample'
    return options


class Response:
    def __init__(self,payload,status=200,headers=None,raw=None):
        self.payload=payload;self.status_code=status;self.headers=headers or {};self.raw=raw
    def __enter__(self):return self
    def __exit__(self,*args):return False
    def iter_content(self,size):yield self.raw if self.raw is not None else json.dumps(self.payload).encode()


class Session:
    def __init__(self,handler):self.handler=handler;self.headers={};self.calls=[]
    def __enter__(self):return self
    def __exit__(self,*args):return False
    def get(self,url,**kwargs):
        self.calls.append((url,kwargs));return self.handler(url)


def document(ident):
    return dict(id=ident,type='story',time=1735725600,title='Empresa QA publica relatório',
                text='<p>Empresa QA &amp; conteúdo local.</p><script>não ler</script>',by='public_author',url='https://example.invalid/not-fetched')


def transport(monkeypatch,handler=None):
    def normal(url):
        if url.endswith('newstories.json'):return Response([1,2,3])
        return Response(document(int(url.rsplit('/',1)[-1].split('.')[0])))
    session=Session(handler or normal);monkeypatch.setattr(mention_sources.requests,'Session',lambda:session)
    return session


def collect(db,options=None,**kwargs):
    return sdk.collect_mentions(db,selection=options or hn_selection(),start='2025-01-01T10:00Z',end='2025-01-01T10:01Z',**kwargs)


def test_verified_api_panel_counts_only_selected_publications_and_no_external_articles(db,monkeypatch):
    session=transport(monkeypatch);before=datasets.identity(db);result=collect(db)
    assert result['receipt']['quantity']==2 and result['receipt']['source_verified']
    assert result['receipt']['acquisition']['selected_items']==2 and not result['receipt']['text_retained']
    assert len(session.calls)==3 and all('example.invalid' not in url for url,_ in session.calls)
    assert all(args['stream'] and args['timeout']<=5 for _,args in session.calls)
    records=feature_store.query_history(db)
    assert records.loc[records.feature_id=='mentions.quantity','numeric_value'].tolist()==[2.]
    assert records.loc[records.feature_id!='mentions.quantity','quality'].tolist()==['UNAVAILABLE','UNAVAILABLE']
    assert 'conteúdo local' not in str(records.metadata_json.tolist()) and datasets.identity(db)==before
    assert result['acquisition']['requests']==3
    with db.engine.connect() as conn:
        configuration=json.loads(conn.execute(text('SELECT configuration_json FROM _analytics_producer_runs WHERE run_id=:id'),{'id':result['job']['run_id']}).scalar_one())
    assert configuration['input_manifest']['ids']==[1,2]


def test_retry_preserves_original_facts_and_receipt_clock(db,monkeypatch):
    transport(monkeypatch);first=collect(db);before=feature_store.query_history(db)
    second=collect(db)
    assert first['receipt']==second['receipt'] and second['counts']==dict(inserted=0,revised=0,unchanged=3)
    pd.testing.assert_frame_equal(feature_store.query_history(db),before)


def test_partial_network_failure_never_publishes_zero_and_resumes_frozen_roster(db,monkeypatch):
    def broken(url):
        if url.endswith('newstories.json'):return Response([1,2])
        return Response(document(1) if url.endswith('/1.json') else None)
    transport(monkeypatch,broken)
    with pytest.raises(ValueError,match='ausente'):collect(db)
    assert feature_store.query_history(db).empty
    descriptor=sdk.describe_mention_series(db,**hn_selection());checkpoint=producer_jobs.get_checkpoint(db,'QA','M1',descriptor['job_family'])
    assert checkpoint['status']=='RUNNING' and checkpoint['state']['pending']['ids']==[1,2] and checkpoint['source_sequence']==0
    assert producer_jobs.latest_run(db,'QA','M1',descriptor['job_family'])['status']=='FAILED'
    recovered=transport(monkeypatch);result=collect(db)
    assert result['acquisition']['resumed_panel'] and result['counts']['inserted']==3
    assert not any(url.endswith('newstories.json') for url,_ in recovered.calls)
    assert 'pending' not in producer_jobs.get_checkpoint(db,'QA','M1',descriptor['job_family'])['state']


def test_explicit_fresh_panel_replaces_failed_cohort_without_erasing_old_manifest(db,monkeypatch):
    def broken(url):return Response([1,2]) if url.endswith('newstories.json') else Response(None)
    transport(monkeypatch,broken)
    with pytest.raises(ValueError):collect(db)
    def new(url):return Response([3,4]) if url.endswith('newstories.json') else Response(document(int(url.rsplit('/',1)[-1].split('.')[0])))
    transport(monkeypatch,new);result=collect(db,resume=False)
    assert not result['acquisition']['resumed_panel']
    with db.engine.connect() as conn:
        configs=[json.loads(r[0]) for r in conn.execute(text('SELECT configuration_json FROM _analytics_producer_runs ORDER BY started_at_utc'))]
    assert [c['input_manifest']['ids'] for c in configs]==[[1,2],[3,4]]


def test_429_retry_after_is_bounded_and_does_not_duplicate_items(db,monkeypatch):
    count=[]
    def handler(url):
        if url.endswith('newstories.json'):
            count.append(1)
            if len(count)==1:return Response(None,429,{'Retry-After':'0'})
            return Response([1,2])
        return Response(document(int(url.rsplit('/',1)[-1].split('.')[0])))
    transport(monkeypatch,handler);result=collect(db)
    assert result['acquisition']['retries']==1 and result['acquisition']['requests']==4
    assert len(feature_store.query_history(db))==3


@pytest.mark.parametrize('response',[
    Response(None,403),Response(None,429,{'Retry-After':'3600'}),Response(None,200,raw=b'<html>not JSON</html>'),
    Response(None,200,raw=b' '*(mention_sources.MAX_RESPONSE_BYTES+1)),Response(['not-an-id']),
])
def test_failed_or_oversized_response_keeps_capture_incomplete_without_facts(db,monkeypatch,response):
    transport(monkeypatch,lambda url:response)
    with pytest.raises((ValueError,RuntimeError)):collect(db)
    assert feature_store.query_history(db).empty


def test_timeout_is_retried_finitely_and_cancel_closes_owner_run(db,monkeypatch):
    calls=[]
    def fail(url):calls.append(url);raise requests.Timeout('controlled')
    transport(monkeypatch,fail);monkeypatch.setattr(mention_sources,'_wait',lambda seconds,guard:guard())
    with pytest.raises(RuntimeError,match='três tentativas'):collect(db)
    assert len(calls)==3 and feature_store.query_history(db).empty
    stop=threading.Event()
    def cancel(url):stop.set();return Response([1,2])
    transport(monkeypatch,cancel)
    with pytest.raises(indicator_materializer.Cancelled):collect(db,cancel_check=stop.is_set)
    descriptor=sdk.describe_mention_series(db,**hn_selection())
    assert producer_jobs.latest_run(db,'QA','M1',descriptor['job_family'])['status']=='CANCELLED'


def test_database_is_not_in_network_transaction_and_changed_namespace_refuses_publish(db,monkeypatch):
    switched=[]
    def handler(url):
        if url.endswith('newstories.json'):return Response([1,2])
        # A second connection can write while HTTP is in progress.
        if not switched:switched.append(True);datasets.fork(db)
        return Response(document(int(url.rsplit('/',1)[-1].split('.')[0])))
    transport(monkeypatch,handler)
    with pytest.raises(indicator_materializer.SourceChanged):collect(db)
    assert feature_store.query_history(db).empty


def test_tombstone_without_original_publication_clock_is_explicitly_excluded(db,monkeypatch):
    def handler(url):
        if url.endswith('newstories.json'):return Response([1,2])
        return Response(dict(id=1,deleted=True)) if url.endswith('/1.json') else Response(document(2))
    transport(monkeypatch,handler);result=collect(db)
    assert result['receipt']['quantity']==1 and result['receipt']['exclusions']['DELETED']==1


def test_capture_owner_cannot_be_stolen_by_second_collector(db,monkeypatch):
    attempted=[]
    def handler(url):
        if url.endswith('newstories.json'):
            if not attempted:
                attempted.append(True)
                with pytest.raises(RuntimeError,match='captura ativa'):collect(db)
            return Response([1,2])
        return Response(document(int(url.rsplit('/',1)[-1].split('.')[0])))
    transport(monkeypatch,handler)
    assert collect(db)['receipt']['quantity']==2


@pytest.mark.parametrize('configuration',[None,{}])
def test_run_input_manifest_can_only_be_attached_once_while_running(db,configuration):
    run=producer_jobs.start_run(db,'QA','M1','qa-inputs','1','cfg',0,configuration=configuration)
    try:
        assert producer_jobs.attach_run_inputs(db,run,{'ids':[1,2]})
        assert not producer_jobs.attach_run_inputs(db,run,{'ids':[1,2]})
        with pytest.raises(ValueError,match='fixado'):producer_jobs.attach_run_inputs(db,run,{'ids':[3]})
        producer_jobs.finish_run(db,run,source_sequence=0,status='COMPLETE')
        with pytest.raises(ValueError,match='ativo'):producer_jobs.attach_run_inputs(db,run,{'ids':[1,2]})
    finally:run['rss_sampler'].stop()


@pytest.mark.parametrize('change',['provider','corpus','size','filter','open_window'])
def test_unsupported_source_or_selection_never_probes_network(db,monkeypatch,change):
    monkeypatch.setattr(mention_sources.requests,'Session',lambda:pytest.fail('network'))
    options=hn_selection();kwargs={}
    if change=='provider':options['source']['provider']='not-integrated'
    elif change=='corpus':options['source']['corpus']='all-internet'
    elif change=='size':options['source']['parameters']['panel_size']=1000
    elif change=='filter':options['query']['languages']=['pt']
    else:kwargs={'start':'2100-01-01T00:00Z','end':'2100-01-01T00:01Z'}
    with pytest.raises(ValueError):
        if kwargs:sdk.collect_mentions(db,selection=options,**kwargs)
        else:collect(db,options)
