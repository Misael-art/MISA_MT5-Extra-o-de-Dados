"""Segundo publicador: mesmo contrato/store/journal e quota sem transação na rede."""
from datetime import datetime,timedelta,timezone
from urllib.parse import urlsplit,parse_qs

import pytest
from sqlalchemy import text

from mt5_extracao import sdk
from mt5_extracao.analytics import datasets,feature_store,mention_sources,mention_stackexchange as source,producer_jobs
from tests.test_mentions_contract import db
from tests.test_mention_sources import hn_selection,transport,Response,collect


def selection():
    s=hn_selection();s['source'].update(provider='stackexchange',corpus=source.CORPUS);return s


def question(ident):
    return dict(question_id=ident,creation_date=1735725600,title='Empresa QA is helpful',body='<p>Empresa QA is a useful tool.</p>',
                content_license='CC BY-SA 4.0',owner={'user_id':ident+10},score=999)


@pytest.fixture
def clock(monkeypatch):
    state={'offset':0.};base=datetime.now(timezone.utc)
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return (base+timedelta(seconds=state['offset'])).astimezone(tz) if tz else (base+timedelta(seconds=state['offset'])).replace(tzinfo=None)
    monkeypatch.setattr(source,'datetime',Clock)
    def wait(seconds,guard):guard();state['offset']+=seconds;guard()
    monkeypatch.setattr(mention_sources,'_wait',wait)
    return state


def network(monkeypatch,handler=None):
    def normal(url):
        path=urlsplit(url).path;query=parse_qs(urlsplit(url).query)
        assert query['site']==['stackoverflow'] and not any(k in query for k in ('key','access_token'))
        if path.endswith('/questions'):return Response(dict(items=[{'question_id':1},{'question_id':2}],has_more=True,quota_remaining=299))
        assert query['filter']==[source.QUESTION_FILTER]
        ids=list(map(int,path.rsplit('/',1)[-1].split(';')))
        return Response(dict(items=[question(i) for i in ids],has_more=False,quota_remaining=298))
    return transport(monkeypatch,handler or normal)


def test_independent_source_uses_same_count_contract_replays_and_preserves_price_origin(db,monkeypatch,clock):
    session=network(monkeypatch);before=datasets.identity(db);s=selection();first=collect(db,s)
    assert first['receipt']['quantity']==2 and first['receipt']['source_verified']
    assert first['receipt']['acquisition']['licenses_observed']==['CC BY-SA 4.0']
    assert len(session.calls)==2 and 'score' not in str(feature_store.query_history(db).metadata_json.tolist())
    assert not any('useful tool' in m for m in feature_store.query_history(db).metadata_json)
    second=collect(db,s);assert second['counts']==dict(inserted=0,revised=0,unchanged=3)
    assert clock['offset']>=60 and datasets.identity(db)==before
    assert feature_store.query_history(db,feature_id='mentions.positive').quality.tolist()==['UNAVAILABLE']
    checkpoint=producer_jobs.get_checkpoint(db,'QA','M1',first['job_family']);assert checkpoint['source_sequence']==1


def test_source_queries_and_http_quota_checkpoints_do_not_collide(db,monkeypatch,clock):
    network(monkeypatch);first=collect(db,selection());s=selection();s['query']['id']='second-query';second=collect(db,s)
    assert first['algorithm_version']!=second['algorithm_version'] and len(feature_store.query_history(db))==6
    assert clock['offset']>=60
    for result in [first,second]:assert producer_jobs.get_checkpoint(db,'QA','M1',result['job_family'])['state']['capture_id']==result['receipt']['capture_id']
    with db.engine.connect() as c:assert c.execute(text('SELECT COUNT(*) FROM _analytics_producer_checkpoints WHERE symbol=:symbol'),{'symbol':source.SYMBOL}).scalar()>=4


def test_missing_id_leaves_same_roster_for_resume_not_a_smaller_zero_population(db,monkeypatch,clock):
    def incomplete(url):return Response(dict(items=[{'question_id':1},{'question_id':2}] if urlsplit(url).path.endswith('/questions') else [question(1)],has_more=False,quota_remaining=299))
    network(monkeypatch,incomplete)
    with pytest.raises(ValueError,match='omitiu'):collect(db,selection())
    assert feature_store.query_history(db).empty
    session=network(monkeypatch);done=collect(db,selection())
    assert done['acquisition']['resumed_panel'] and len(session.calls)==1 and done['receipt']['quantity']==2


@pytest.mark.parametrize('kind',['body_missing','unknown_license','bad_id','bad_time','duplicate','has_more'])
def test_incomplete_or_unsupported_documents_never_publish_zero(db,monkeypatch,clock,kind):
    def response(url):
        if urlsplit(url).path.endswith('/questions'):return Response(dict(items=[{'question_id':1},{'question_id':2}],has_more=False,quota_remaining=299))
        items=[question(1),question(2)]
        if kind=='body_missing':items[0].pop('body')
        if kind=='unknown_license':items[0]['content_license']='unknown'
        if kind=='bad_id':items[0]['question_id']=999
        if kind=='bad_time':items[0]['creation_date']=True
        if kind=='duplicate':items[1]=items[0]
        return Response(dict(items=items,has_more=kind=='has_more',quota_remaining=298))
    network(monkeypatch,response)
    with pytest.raises(ValueError):collect(db,selection())
    assert feature_store.query_history(db).empty


def test_backoff_and_retry_after_persist_across_runs_and_respect_remaining_budget(db,monkeypatch,clock):
    session=network(monkeypatch,lambda u:Response({},status=429,headers={'Retry-After':'300'}))
    with pytest.raises(RuntimeError):collect(db,selection(),timeout_seconds=1)
    assert len(session.calls)==1 and feature_store.query_history(db).empty
    session=network(monkeypatch)
    with pytest.raises(RuntimeError,match='Quota/backoff'):collect(db,selection(),timeout_seconds=1)
    assert not session.calls


def test_successful_empty_panel_is_zero_but_error_or_truncated_roster_is_not(db,monkeypatch,clock):
    network(monkeypatch,lambda u:Response(dict(items=[],has_more=False,quota_remaining=299)))
    assert collect(db,selection())['receipt']['quantity']==0
    for payload in [dict(items=[],has_more=True,quota_remaining=299),dict(error_id=502,items=[],has_more=False,quota_remaining=299),dict(items=[],has_more=False)]:
        network(monkeypatch,lambda u,p=payload:Response(p))
        with pytest.raises((ValueError,RuntimeError)):collect(db,selection())


def test_wrapper_backoff_and_zero_quota_are_honored_before_next_method_call(db,monkeypatch,clock):
    network(monkeypatch,lambda u:Response(dict(items=[],has_more=False,quota_remaining=0,backoff=600)))
    assert collect(db,selection())['receipt']['quantity']==0
    session=network(monkeypatch)
    with pytest.raises(RuntimeError,match='Quota/backoff'):collect(db,selection(),timeout_seconds=1)
    assert not session.calls


def test_http_error_wrapper_quota_also_blocks_later_requests(db,monkeypatch,clock):
    session=network(monkeypatch,lambda u:Response(dict(quota_remaining=0,backoff=600,error_id=502),status=400))
    with pytest.raises(RuntimeError):collect(db,selection())
    assert len(session.calls)==1
    session=network(monkeypatch)
    with pytest.raises(RuntimeError,match='Quota/backoff'):collect(db,selection(),timeout_seconds=1)
    assert not session.calls and feature_store.query_history(db).empty


@pytest.mark.parametrize('recent',[True,False])
def test_missing_license_uses_published_policy_only_for_recent_contributions(db,monkeypatch,clock,recent):
    def response(url):
        if urlsplit(url).path.endswith('/questions'):return Response(dict(items=[{'question_id':1},{'question_id':2}],has_more=False,quota_remaining=299))
        items=[question(1),question(2)]
        for item in items:
            item.pop('content_license')
            if not recent:item['creation_date']=1000000000
        return Response(dict(items=items,has_more=False,quota_remaining=298))
    network(monkeypatch,response)
    if not recent:
        with pytest.raises(ValueError,match='licença'):collect(db,selection())
    else:
        result=collect(db,selection());licensing=result['receipt']['acquisition']['licensing']
        assert licensing['api_observed']==[] and licensing['policy_inferred_items']==2
        assert licensing['policy_inferred_license']=='CC BY-SA 4.0' and licensing['policy_reference']==source.LICENSE_POLICY


@pytest.mark.parametrize('provider',['hackernews','stackexchange'])
def test_unknown_region_filter_is_refused_before_http(db,monkeypatch,provider):
    session=network(monkeypatch);s=hn_selection() if provider=='hackernews' else selection();s['query']['region']='BR'
    with pytest.raises(ValueError,match='região'):collect(db,s)
    assert not session.calls and feature_store.query_history(db).empty


def test_cancel_inside_quota_checkpoint_rolls_back_its_reservation(db,monkeypatch,clock):
    session=network(monkeypatch);state={'cancel':False};original=producer_jobs.advance_checkpoint
    def save(conn,**kw):
        result=original(conn,**kw)
        if kw['family'].startswith(source.FAMILY):state['cancel']=True
        return result
    monkeypatch.setattr(producer_jobs,'advance_checkpoint',save)
    from mt5_extracao.analytics import indicator_materializer
    with pytest.raises(indicator_materializer.Cancelled):collect(db,selection(),cancel_check=lambda:state['cancel'])
    assert not session.calls and feature_store.query_history(db).empty
    with db.engine.connect() as c:assert c.execute(text('SELECT COUNT(*) FROM _analytics_producer_checkpoints WHERE symbol=:symbol'),{'symbol':source.SYMBOL}).scalar()==0


def test_classifier_uses_full_body_not_provider_vote_score(db,monkeypatch,clock):
    from mt5_extracao.analytics import mention_classifier
    def response(url):
        if urlsplit(url).path.endswith('/questions'):return Response(dict(items=[{'question_id':1},{'question_id':2}],has_more=False,quota_remaining=299))
        items=[question(1),question(2)]
        for item in items:
            item.update(title='No opinion in this title',body='<p>Empresa QA is a wonderful and helpful product for our entire team. I love Empresa QA because the service works perfectly.</p>')
        return Response(dict(items=items,has_more=False,quota_remaining=298))
    network(monkeypatch,response);s=selection();s['classifier']=mention_classifier.descriptor()
    result=collect(db,s,classify=True)
    assert result['classification']['receipt']['label_counts']=={'positive':2}
    rows=feature_store.query_as_of(db,datetime.now(timezone.utc).isoformat(),algorithm_version=result['algorithm_version'])
    assert rows[rows.feature_id=='mentions.positive'].iloc[0].numeric_value==2
    assert not any('999'==str(v) for v in rows.numeric_value)
