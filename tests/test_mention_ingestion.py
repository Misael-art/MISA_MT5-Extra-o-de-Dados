"""MEN-02: universo verificado, deduplicação e publicação/cursor atômicos."""
from copy import deepcopy
import json
import threading

import pytest
from sqlalchemy import inspect, text

from mt5_extracao import sdk
from mt5_extracao.analytics import datasets, feature_store, features, indicator_materializer, mention_ingestion, mentions, producer_jobs, producers
from tests.test_mentions_contract import db, selection


def batch(db, options=None):
    options=options or selection();contract=sdk.describe_mention_series(db,**options)
    documents=[dict(id=str(i),revision='1',published_at=f'2025-01-01T10:00:{i:02d}Z',
        text='Empresa QA e Empresa QB. Texto próprio de teste, Empresa QA Empresa QA.',kind='original',language='pt') for i in range(10)]
    labels=['positive']*3+['negative']*2+['neutral','neutral','mixed','abstained','neutral']
    annotations={str(i):{'company:qa':label,'company:qb':'negative' if label=='positive' else 'positive'} for i,label in enumerate(labels)}
    return dict(capture_schema_version=1,scope_hash=contract['scope_hash'],start='2025-01-01T10:00Z',end='2025-01-01T10:01Z',
        captured_at='2025-01-01T10:20Z',coverage='complete',documents=documents,annotations=annotations,
        classified_at='2025-01-01T10:22Z',cursor='qa-cursor-1')


def ingest(db,data=None,options=None,**kwargs):
    options=options or selection()
    return sdk.ingest_mention_batch(db,selection=options,batch=data or batch(db,options),synthetic_fixture=True,**kwargs)


def values(db,cut='2025-01-01T11:00Z',version=None):
    frame=feature_store.query_as_of(db,cut,algorithm_version=version)
    return {r.feature_id:r for r in frame.itertuples()}


def test_q10_p3_n2_deduplicates_transport_and_repeated_words_without_storing_text(db):
    data=batch(db);data['documents'] += [deepcopy(data['documents'][0])]*2
    result=ingest(db,data);assert result['counts']==dict(inserted=3,revised=0,unchanged=0)
    assert result['duplicates']==2 and result['receipt']['unique_documents']==result['receipt']['eligible_documents']==10
    current=values(db);assert {k:r.numeric_value for k,r in current.items()}=={'mentions.quantity':10.,'mentions.positive':3.,'mentions.negative':2.}
    assert result['receipt']['synthetic_fixture'] and not result['receipt']['source_verified'] and not result['receipt']['text_retained']
    for row in current.values():
        assert 'Texto próprio' not in row.metadata_json
        assert json.loads(row.metadata_json)['capture']['capture_id']==result['receipt']['capture_id']
    checkpoint=producer_jobs.get_checkpoint(db,'QA','M1',result['job_family'])
    assert checkpoint['status']=='COMPLETE' and checkpoint['state']['cursor']=='qa-cursor-1'
    assert checkpoint['source_sequence']==1


def test_retry_with_new_receipt_clock_and_transport_cursor_keeps_original_values(db):
    data=batch(db);first=ingest(db,data);before=feature_store.query_history(db).to_dict('records')
    data.update(captured_at='2025-01-01T10:30Z',classified_at='2025-01-01T10:32Z',cursor='qa-cursor-2')
    repeated=ingest(db,data)
    assert repeated['counts']==dict(inserted=0,revised=0,unchanged=3)
    assert repeated['receipt']['capture_id']==first['receipt']['capture_id']
    assert feature_store.query_history(db).to_dict('records')==before
    assert producer_jobs.get_checkpoint(db,'QA','M1',first['job_family'])['source_sequence']==1
    with db.engine.connect() as conn:
        sequences=conn.execute(text('SELECT source_sequence_start,source_sequence_end FROM _analytics_producer_runs '
            'WHERE family=:family ORDER BY started_at_utc'),{'family':first['job_family']}).all()
    assert sequences==[(0,1),(1,1)]


def test_multiobject_labels_are_per_subject_and_not_a_global_post_sentiment(db):
    first=ingest(db);options=selection();options['subject'].update(id='company:qb',label='Empresa QB',aliases=['Empresa QB'])
    second=ingest(db,options=options)
    assert first['algorithm_version']!=second['algorithm_version']
    a=values(db,version=first['algorithm_version']);b=values(db,version=second['algorithm_version'])
    assert a['mentions.positive'].numeric_value==3 and b['mentions.negative'].numeric_value==3
    assert b['mentions.positive'].numeric_value==7 and b['mentions.quantity'].numeric_value==10


def test_pending_and_late_classification_never_add_future_metadata_to_quantity(db):
    complete=batch(db);pending=deepcopy(complete);pending.pop('annotations');pending.pop('classified_at')
    ingest(db,pending);result=ingest(db,complete)
    assert result['counts']==dict(inserted=0,revised=2,unchanged=1)
    old=values(db,'2025-01-01T10:21Z');now=values(db,'2025-01-01T10:23Z')
    assert old['mentions.quantity'].numeric_value==10 and old['mentions.positive'].quality=='UNAVAILABLE'
    assert now['mentions.positive'].numeric_value==3
    assert feature_store.query_history(db,feature_id='mentions.quantity').revision.tolist()==[1]
    assert 'label_counts' not in json.loads(now['mentions.quantity'].metadata_json)


def test_edit_and_deletion_append_consistent_populations_without_overwriting_old_rows(db):
    data=batch(db);first=ingest(db,data)
    data['documents'][0].update(deleted=True,revision='2');data.update(captured_at='2025-01-01T10:30Z',classified_at='2025-01-01T10:32Z')
    second=ingest(db,data);assert first['receipt']['capture_id']!=second['receipt']['capture_id']
    assert second['counts']['revised']==3 and second['receipt']['exclusions']['DELETED']==1
    assert values(db,'2025-01-01T10:25Z')['mentions.quantity'].numeric_value==10
    current=values(db);assert current['mentions.quantity'].numeric_value==9 and current['mentions.positive'].numeric_value==2
    assert len({r.source_revision for r in current.values()})==1


def test_incomplete_attempt_publishes_absence_and_preserves_last_complete_cursor(db):
    initial=ingest(db);data=batch(db);data.update(documents=[],coverage='unavailable',reason='SOURCE_UNAVAILABLE',captured_at='2025-01-01T10:30Z')
    data.pop('annotations');data.pop('classified_at')
    result=ingest(db,data);checkpoint=producer_jobs.get_checkpoint(db,'QA','M1',result['job_family'])
    assert result['job']['status']=='INCOMPLETE'
    assert all(r.quality=='UNAVAILABLE' for r in values(db).values())
    assert checkpoint['state']['cursor']=='qa-cursor-1' and checkpoint['source_sequence']==1
    assert checkpoint['status']=='INCOMPLETE'


def test_closed_window_excludes_final_boundary_reposts_and_filtered_publications(db):
    data=batch(db);data['documents'][0]['published_at']='2025-01-01T10:01Z';data['documents'][1]['kind']='repost'
    data['documents'][2]['text']='Sem o objeto configurado';result=ingest(db,data)
    assert result['receipt']['quantity']==7
    assert result['receipt']['exclusions']=={'OUTSIDE_WINDOW':1,'EXCLUDED_PUBLICATION_TYPE':1,'OBJECT_NOT_MATCHED':1}


def test_zero_requires_a_successfully_covered_empty_population(db):
    data=batch(db);data.update(documents=[],annotations={})
    result=ingest(db,data);assert result['receipt']['quantity']==0
    assert [r.numeric_value for r in values(db).values()]==[0.,0.,0.]


def test_partial_capture_does_not_present_fixture_labels_as_population_counts(db):
    data=batch(db);data.update(coverage='partial',reason='PAGE_LIMIT')
    ingest(db,data)
    assert all(r.quality=='UNAVAILABLE' and 'label_counts' not in json.loads(r.metadata_json) for r in values(db).values())


@pytest.mark.parametrize('change',['conflicting_duplicate','unknown_label','missing_label','wrong_scope','too_many','score_aggregate','real_annotations','unknown_language_filter'])
def test_invalid_batches_cannot_publish_counts(db,change):
    options=selection();data=batch(db,options);synthetic=True
    if change=='conflicting_duplicate':
        doc=deepcopy(data['documents'][0]);doc['text']='Outra revisão não reconciliada';data['documents'].append(doc)
    elif change=='unknown_label':data['annotations']['0']['company:qa']='bullish_probability'
    elif change=='missing_label':data['annotations'].pop('0')
    elif change=='wrong_scope':data['scope_hash']='a'*64
    elif change=='too_many':data['documents']*=101
    elif change=='score_aggregate':
        options['source']['mode']='aggregate';options['universe']='provider_total';data=batch(db,options)
        data.update(documents=[],aggregate={'quantity':1000,'positive':.7});data.pop('annotations')
    elif change=='real_annotations':synthetic=False
    elif change=='unknown_language_filter':data['documents'][0].pop('language')
    with pytest.raises(ValueError):sdk.ingest_mention_batch(db,selection=options,batch=data,synthetic_fixture=synthetic)
    assert not inspect(db.engine).has_table('_feature_values') or feature_store.query_history(db).empty


def test_provider_total_and_document_sample_are_never_extrapolated(db):
    options=selection();options['source']['mode']='aggregate';options['universe']='provider_total'
    total=batch(db,options);total.update(documents=[],aggregate={'quantity':1000});total.pop('annotations');total.pop('classified_at')
    a=ingest(db,total,options);current=values(db,version=a['algorithm_version'])
    assert current['mentions.quantity'].numeric_value==1000 and current['mentions.positive'].quality=='UNAVAILABLE'
    options=selection();options['universe']='sample';data=batch(db,options)
    data['documents']=[dict(data['documents'][0],id=str(i)) for i in range(100)]
    data['annotations']={str(i):{'company:qa':'positive' if i<30 else 'negative' if i<50 else 'neutral'} for i in range(100)}
    b=ingest(db,data,options);current=values(db,version=b['algorithm_version'])
    assert [current[k].numeric_value for k in ['mentions.quantity','mentions.positive','mentions.negative']]==[100,30,20]


def test_cancellation_before_commit_rolls_back_values_and_checkpoint_and_closes_sampler(db,monkeypatch):
    event=threading.Event();original=producer_jobs.advance_checkpoint
    def cancel(conn,**kwargs):original(conn,**kwargs);event.set()
    monkeypatch.setattr(producer_jobs,'advance_checkpoint',cancel)
    with pytest.raises(indicator_materializer.Cancelled):ingest(db,cancel_check=event.is_set)
    assert feature_store.query_history(db).empty
    descriptor=sdk.describe_mention_series(db,**selection())
    assert producer_jobs.get_checkpoint(db,'QA','M1',descriptor['job_family']) is None
    assert producer_jobs.latest_run(db,'QA','M1',descriptor['job_family'])['status']=='CANCELLED'


def test_late_cancel_after_commit_preserves_successful_capture(db,monkeypatch):
    event=threading.Event();original=feature_store.save_values
    def committed(*args,**kwargs):result=original(*args,**kwargs);event.set();return result
    monkeypatch.setattr(feature_store,'save_values',committed)
    assert ingest(db,cancel_check=event.is_set)['job']['status']=='COMPLETE'
    assert len(feature_store.query_history(db))==3


def test_resource_block_and_namespace_change_refuse_publication(db,monkeypatch):
    with pytest.raises(producers.WriteBlocked):ingest(db,write_guard=lambda:'quota de QA')
    assert not inspect(db.engine).has_table('_feature_values')
    original=features.register;changed=[]
    def forked(db_,**definition):
        result=original(db_,**definition)
        if not changed:changed.append(True);datasets.fork(db_)
        return result
    monkeypatch.setattr(features,'register',forked)
    with pytest.raises(indicator_materializer.SourceChanged):ingest(db)
    assert feature_store.query_history(db).empty


def test_source_capabilities_are_passive_documentation_not_fake_connectors(monkeypatch):
    import requests
    monkeypatch.setattr(requests,'get',lambda *a,**k:pytest.fail('network on capability read'))
    items=sdk.mention_source_capabilities()
    assert [i['id'] for i in items]==['F01','F02','F03','F04','F05','F06','F07','F08','F09']
    assert all(not i['connector_implemented'] and i['qualification']=='DOCUMENTED_ONLY' for i in items)


@pytest.mark.parametrize('kind,ident,aliases,expression,inclusions,texts',[
    ('topic','metal:gold',['ouro'],'ouro',['metal'],['Ouro é um metal.','Medalha de ouro.','Ouro da medalha de metal.']),
    ('company','company:vale',['Vale S.A.','$VALE'],'Vale',[],['Vale S.A. divulga relatório.','Isso vale a pena.','O vale da montanha.']),
    ('instrument','index:ibovespa',['índice Ibovespa'],'Ibovespa',[],['Índice Ibovespa em análise.','ETF Ibovespa anunciado.','Futuro de Ibovespa.']),
])
def test_explicit_object_filters_distinguish_metal_medal_company_verb_index_and_products(db,kind,ident,aliases,expression,inclusions,texts):
    options=selection();options['subject'].update(id=ident,kind=kind,aliases=aliases,include_terms=inclusions,exclude_terms=['medalha'])
    options['query']['expression']=expression;data=batch(db,options)
    data['documents']=[dict(data['documents'][0],id=str(i),text=body) for i,body in enumerate(texts)]
    data['annotations']={str(i):{ident:'neutral'} for i in range(len(texts))}
    result=ingest(db,data,options)
    assert result['receipt']['quantity']==1 and sum(result['receipt']['exclusions'].values())==2
