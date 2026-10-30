"""Painéis próprios: autenticação, IDs congelados, perda de acesso e nenhum censo fictício."""
import copy
import json
import subprocess
import sys
import threading
import time

import pandas as pd
import pytest

from mt5_extracao import sdk
from mt5_extracao.analytics import datasets,feature_store,mention_social,mention_stackexchange,producer_jobs
from tests.test_mentions_contract import db,selection


def options(provider):
    s=selection();s['source'].update(provider=provider,corpus=mention_social.CORPORA[provider],
        access_profile='composio-read-approved',parameters={'panel_size':2},publication_types=['comment'] if provider=='youtube' else ['original'])
    s['query']['languages']=[];s['query']['communities']=['qa_community'] if provider=='reddit' else []
    if provider=='youtube':s['source']['parameters']['video_id']='qa_video_01'
    s['classifier']=None;s['universe']='sample';return s


@pytest.fixture
def transport(monkeypatch):
    start=pd.Timestamp.now(tz='UTC').floor('min')-pd.Timedelta(minutes=3);calls=[]
    stamp=start+pd.Timedelta(seconds=10)
    reddit=[dict(name='t3_qa1',title='Empresa QA é excelente',selftext='',created_utc=stamp.timestamp(),subreddit='qa_community',author='qa'),
            dict(name='t3_qa2',title='Empresa QA é terrível',selftext='',created_utc=stamp.timestamp()+1,subreddit='qa_community',author='qa')]
    tweets=[dict(id=str(100+i),text='Empresa QA is excellent',created_at=(stamp+pd.Timedelta(seconds=i)).isoformat(),lang='en',author_id='qa') for i in range(2)]
    comments=[dict(id='qa'+str(i),snippet=dict(videoId='qa_video_01',channelId='qa_channel',textOriginal='Empresa QA é excelente',publishedAt=(stamp+pd.Timedelta(seconds=i)).isoformat())) for i in range(2)]
    def proxy(provider,path,parameters,*a,**kw):
        calls.append((provider,path,parameters))
        if provider=='reddit':return {'kind':'Listing','data':{'children':[{'kind':'t3','data':r} for r in reddit],'after':None}}
        if provider=='twitter':return {'data':tweets,'meta':{'result_count':2}}
        if path=='/commentThreads':return {'items':[{'snippet':{'topLevelComment':c}} for c in comments]}
        return {'items':comments}
    monkeypatch.setattr(mention_social,'proxy',proxy)
    # O transporte da fixture não consome quota: orçamento canônico é testado separadamente.
    monkeypatch.setattr(mention_stackexchange,'_budget',lambda *a,**k:0)
    return start,calls


@pytest.mark.parametrize('provider',['reddit','twitter','youtube'])
def test_social_panel_uses_existing_store_and_is_not_a_census(db,transport,provider):
    start,calls=transport;before=datasets.identity(db)
    result=sdk.collect_mentions(db,selection=options(provider),start=start,end=start+pd.Timedelta(minutes=1))
    assert result['receipt']['quantity']==2 and result['receipt']['universe']=='sample'
    assert len(calls)==2 and all(c[0]==provider for c in calls)
    assert datasets.identity(db)==before and len(feature_store.query_history(db))==3
    assert result['receipt']['source_verified']
    facts=feature_store.query_history(db)
    assert all('Empresa QA é excelente' not in m for m in facts.metadata_json)
    assert result['job']['status']=='COMPLETE'


@pytest.mark.parametrize('provider',['reddit','twitter','youtube'])
def test_lost_document_cannot_become_zero_and_roster_remains_pinned(db,transport,monkeypatch,provider):
    start,calls=transport;original=mention_social.proxy
    def proxy(p,path,*a,**k):
        if path in ('/api/info','/tweets','/comments'):
            raise RuntimeError('owned QA access failure')
        return original(p,path,*a,**k)
    monkeypatch.setattr(mention_social,'proxy',proxy)
    s=options(provider)
    with pytest.raises(RuntimeError,match='QA access'):sdk.collect_mentions(db,selection=s,start=start,end=start+pd.Timedelta(minutes=1))
    contract=sdk.describe_mention_series(db,**s)
    pending=producer_jobs.get_checkpoint(db,'QA','M1',contract['job_family'])['state']['pending']
    assert len(pending['ids'])==2 and feature_store.query_history(db).empty
    monkeypatch.setattr(mention_social,'proxy',original);calls.clear()
    result=sdk.collect_mentions(db,selection=s,start=start,end=start+pd.Timedelta(minutes=1))
    assert result['acquisition']['resumed_panel'] and result['receipt']['quantity']==2
    assert len(calls)==1


def test_provider_budgets_are_isolated_in_existing_journal(db):
    producer_jobs.ensure_schema(db)
    from mt5_extracao.analytics.mention_stackexchange import _budget
    for provider in ['reddit','twitter','youtube']:
        assert _budget(db,lambda:None,method='/qa',url='https://qa.invalid',claim=True,provider=provider)==0
    assert _budget(db,lambda:None,method='/qa',url='https://qa.invalid',claim=True,provider='reddit')>0
    for provider in ['reddit','twitter','youtube']:
        assert producer_jobs.get_checkpoint(db,'@mentions.http','M1','mentions:http:'+provider+':global')


def test_access_and_unsupported_filters_fail_before_capture(db,transport):
    start,calls=transport;s=options('reddit');s['source']['access_profile']='not-authorized'
    with pytest.raises(ValueError,match='Autorize'):sdk.collect_mentions(db,selection=s,start=start,end=start+pd.Timedelta(minutes=1))
    assert not calls and feature_store.query_history(db).empty
    s=options('youtube');s['query']['region']='BR'
    with pytest.raises(ValueError,match='região'):sdk.collect_mentions(db,selection=s,start=start,end=start+pd.Timedelta(minutes=1))


def test_x_missing_list_is_not_an_empty_success():
    with pytest.raises(ValueError):mention_social._posts({'meta':{'result_count':1}})
    assert mention_social._posts({'meta':{'result_count':0}})==[]


def test_read_only_cli_transport_is_bounded_and_cancels_only_its_process(monkeypatch):
    original=subprocess.Popen;children=[]
    monkeypatch.setattr(mention_social.shutil,'which',lambda _:sys.executable)
    def spawn(command,**kw):
        assert command[command.index('--method')+1]=='GET'
        p=original([sys.executable,'-c',"import time;print('{}',flush=True);time.sleep(10)"],**kw);children.append(p);return p
    monkeypatch.setattr(mention_social.subprocess,'Popen',spawn)
    started=time.monotonic()
    def guard():
        if time.monotonic()-started>.2:raise RuntimeError('owned cancellation')
    with pytest.raises(RuntimeError,match='cancellation'):mention_social.proxy('reddit','/qa',{},guard,5)
    assert children and all(p.poll() is not None for p in children)
