"""MEN-01: contrato e isolamento exercitados no store/journal reais de QA."""
from copy import deepcopy

import pytest
from sqlalchemy import text

from mt5_extracao.analytics import datasets, feature_store, features, mentions, producer_jobs, query_service
from mt5_extracao.database_manager import DatabaseManager


@pytest.fixture
def db(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path/'mentions.sqlite'), broker_utc_offset=0)
    datasets.declare_source(db, {"provider": "qa_prices", "server": "Synthetic"})
    yield db
    db.close()


def selection(**changes):
    result = dict(symbol="QA", timeframe="M1",
        subject=dict(id="company:qa", kind="company", label="Empresa QA", aliases=["Empresa QA", "$QA"], mapping_version="1"),
        source=dict(provider="qa.public-a", corpus="owned_test_documents", access_profile="public", terms_version="qa1",
                    mode="documents", retention="no_raw_text_export"),
        query=dict(id="qa-query", version="1", expression="Empresa QA", languages=["pt"]),
        classifier=dict(id="qa.manual", version="1", languages=["pt"]))
    result.update(changes)
    return result


def contract(db, **changes):
    return mentions.describe(db, **selection(**changes))


def register(db, descriptor):
    for definition in descriptor['definitions']:
        features.register(db, **definition)


def records(descriptor, **changes):
    options = dict(start="2025-01-01T10:00:00Z", end="2025-01-01T10:01:00Z",
        captured_at="2025-01-01T10:20:00Z", processed_at="2025-01-01T10:20:00Z",
        population_revision="population:1", quantity=10, positive=3, negative=2,
        classification="complete", classified_at="2025-01-01T10:22:00Z")
    options.update(changes)
    return mentions.value_records(descriptor, **options)


def test_three_numeric_counts_reuse_catalog_and_preserve_price_source(db):
    original = datasets.identity(db);descriptor = contract(db)
    register(db, descriptor);register(db, descriptor)
    assert [d['feature_id'] for d in descriptor['definitions']] == ['mentions.quantity','mentions.positive','mentions.negative']
    assert all(d['unit']=='publication/window' and d['value_type']=='NUMBER' for d in descriptor['definitions'])
    assert feature_store.save_values(db, records(descriptor)) == dict(inserted=3,revised=0,unchanged=0)
    actual = feature_store.query_history(db)
    assert dict(zip(actual.feature_id, actual.numeric_value)) == {'mentions.quantity':10.,'mentions.positive':3.,'mentions.negative':2.}
    assert len(query_service.definition_catalog(db)) >= 3
    assert datasets.identity(db) == original and original['provider']=='qa_prices'


def test_two_sources_and_two_queries_isolate_values_and_cursors(db):
    descriptors=[]
    for source_name in ['qa.public-a','qa.public-b']:
        for query_name in ['qa-query','qa-other']:
            options=selection();options['source']['provider']=source_name;options['query']['id']=query_name
            descriptor=mentions.describe(db,**options);descriptors.append(descriptor);register(db,descriptor)
            feature_store.save_values(db, records(descriptor,quantity=len(descriptors)+5))
    assert len({d['algorithm_version'] for d in descriptors})==len({d['job_family'] for d in descriptors})==4
    assert len(feature_store.query_history(db))==12
    producer_jobs.ensure_schema(db)
    for i,d in enumerate(descriptors):
        with db.engine.begin() as conn:
            producer_jobs.advance_checkpoint(conn,symbol='QA',timeframe='M1',family=d['job_family'],
                algorithm_version=d['algorithm_version'],config_hash=d['scope_hash'],source_sequence=i,
                last_bar_start='2025-01-01T10:00:00Z',state={'cursor':'cursor-'+str(i)})
    for i,d in enumerate(descriptors):
        checkpoint=producer_jobs.get_checkpoint(db,'QA','M1',d['job_family'])
        assert checkpoint['source_sequence']==i and checkpoint['state']['cursor']=='cursor-'+str(i)
        assert feature_store.query_history(db,feature_id='mentions.quantity',algorithm_version=d['algorithm_version']).numeric_value.tolist()==[i+6.]


def test_base_namespace_and_object_kind_are_identity_not_symbol_equivalence(db,tmp_path):
    other=DatabaseManager(db_path=str(tmp_path/'other.sqlite'),broker_utc_offset=0)
    try:
        assert contract(db)['algorithm_version']!=contract(other)['algorithm_version']
        first=contract(db);subject=selection()['subject'];subject['kind']='topic'
        assert contract(db,subject=subject)['scope_hash']!=first['scope_hash']
        changed=deepcopy(subject);changed['mapping_version']='2'
        assert contract(db,subject=changed)['scope_hash']!=contract(db,subject=subject)['scope_hash']
    finally:other.close()


def test_scope_is_stable_for_set_order_and_export_does_not_contain_private_query(db):
    options=selection();options['query'].update(expression='private phrase',authors=['author_b','author_a'])
    first=mentions.describe(db,**options);options['subject']['aliases'].reverse();options['query']['authors'].reverse()
    assert mentions.describe(db,**options)==first
    for d in first['definitions']:
        assert 'private phrase' not in str(d) and 'author_a' not in str(d) and '$QA' not in str(d)
    options['query']['expression']='another phrase'
    assert mentions.describe(db,**options)['scope_hash']!=first['scope_hash']


def test_late_classification_preserves_quantity_revision_and_old_asof(db):
    descriptor=contract(db);register(db,descriptor)
    pending=records(descriptor,positive=None,negative=None,classification='pending',classified_at=None)
    feature_store.save_values(db,pending)
    completed=records(descriptor)
    assert pending[0]==completed[0]  # no future classification metadata in Q
    assert feature_store.save_values(db,completed)==dict(inserted=0,revised=2,unchanged=1)
    before=feature_store.query_as_of(db,'2025-01-01T10:21:00Z')
    after=feature_store.query_as_of(db,'2025-01-01T10:23:00Z')
    assert before.loc[before.feature_id=='mentions.quantity','numeric_value'].tolist()==[10.]
    assert before.loc[before.feature_id!='mentions.quantity','quality'].tolist()==['UNAVAILABLE','UNAVAILABLE']
    assert sorted(after.numeric_value.tolist())==[2.,3.,10.]
    assert feature_store.query_history(db,feature_id='mentions.quantity').revision.tolist()==[1]
    assert feature_store.save_values(db,completed)==dict(inserted=0,revised=0,unchanged=3)


@pytest.mark.parametrize('value',[True,-1,.7,1.0,float('nan'),float('inf'),2**53,'3'])
def test_scores_and_inexact_counts_are_refused(db,value):
    with pytest.raises(ValueError,match='contagem inteira'):records(contract(db),quantity=value)


def test_max_exact_integer_roundtrips_without_loss(db):
    descriptor=contract(db);register(db,descriptor)
    feature_store.save_values(db,records(descriptor,quantity=mentions.MAX_EXACT_COUNT,positive=0,negative=0))
    value=feature_store.query_history(db,feature_id='mentions.quantity').numeric_value.iloc[0]
    assert int(value)==mentions.MAX_EXACT_COUNT


def test_zero_is_observed_and_missing_classifier_is_not_two_zeroes(db):
    zero=records(contract(db),quantity=0,positive=0,negative=0)
    assert [r['value'] for r in zero]==[0,0,0] and all(r['quality']=='VALID' for r in zero)
    pending=records(contract(db),quantity=0,positive=None,negative=None,classification='pending',classified_at=None)
    assert [r['value'] for r in pending]==[0,None,None]
    with pytest.raises(ValueError,match='não autoriza'):records(contract(db),positive=0,negative=0,classification='pending',classified_at=None)
    with pytest.raises(ValueError,match='classificador'):records(contract(db,classifier=None))


def test_total_and_sample_have_distinct_population_identity(db):
    options=selection();options['source']['mode']='aggregate';options['universe']='provider_total'
    total=mentions.describe(db,**options)
    total_records=records(total,quantity=1000,positive=None,negative=None,classification='unavailable',classified_at=None)
    sample=contract(db,universe='sample')
    sampled=records(sample,quantity=100,positive=30,negative=20)
    assert total['scope_hash']!=sample['scope_hash']
    assert [r['value'] for r in total_records]==[1000,None,None]
    assert [r['value'] for r in sampled]==[100,30,20]


@pytest.mark.parametrize('changes',[
    dict(positive=9,negative=2), dict(classified_at='2025-01-01T10:19Z'),
    dict(captured_at='2025-01-01T10:00:30Z'),dict(start='2025-01-01T10:00:01Z'),
    dict(start='2025-01-01T10:00:00'),dict(start='2025-01-01T10:00:00.000000001Z'),
    dict(coverage='partial'),dict(coverage='unavailable'),dict(classified_at=None),
])
def test_invalid_population_clocks_and_coverage_cannot_be_published(db,changes):
    with pytest.raises(ValueError):records(contract(db),**changes)


def test_incomplete_capture_has_three_explicit_absences_and_reason(db):
    unavailable=records(contract(db),quantity=None,positive=None,negative=None,coverage='unavailable',
        classification='unavailable',classified_at=None,reason='SOURCE_UNAVAILABLE')
    assert all(r['value'] is None and r['quality']=='UNAVAILABLE' for r in unavailable)
    assert all(r['metadata']['reason']=='SOURCE_UNAVAILABLE' for r in unavailable)


def test_unrecognized_filters_post_types_calendar_and_mutated_contract_are_refused(db):
    options=selection();options['query']['secret_token']='not a credential fixture'
    with pytest.raises(ValueError,match='Campos inválidos'):mentions.describe(db,**options)
    options=selection();options['source']['publication_types']=['repost']
    with pytest.raises(ValueError,match='repostagem'):mentions.describe(db,**options)
    with pytest.raises(ValueError,match='calendário'):contract(db,timeframe='MN1')
    descriptor=contract(db);descriptor['scope']['query']['expression']='changed'
    with pytest.raises(ValueError,match='diverge'):records(descriptor)


def test_existing_schema_and_legacy_facts_survive_contract_registration(db):
    features.register(db,'qa.legacy','Legacy QA','1')
    feature_store.save_value(db,'QA','M1','qa.legacy','1','2025-01-01T00:00Z',17.,
        observed_at='2025-01-01T00:01Z',available_at='2025-01-01T00:01Z')
    with db.engine.connect() as c:before=c.execute(text("SELECT * FROM _feature_values")).all()
    register(db,contract(db));feature_store.ensure_schema(db);feature_store.ensure_schema(db)
    with db.engine.connect() as c:assert c.execute(text("SELECT * FROM _feature_values")).all()==before
