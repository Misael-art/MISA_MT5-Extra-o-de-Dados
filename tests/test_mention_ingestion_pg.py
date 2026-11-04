"""MEN-02 no schema PostgreSQL de QA: transação e repetição concorrente."""
from concurrent.futures import ThreadPoolExecutor
import threading

import pytest

from mt5_extracao import sdk
from mt5_extracao.analytics import feature_store, features, indicator_materializer, producer_jobs
from tests.test_analytics_profiles_pg import PG_URL
from tests.test_composite_materialization_pg import pg
from tests.test_mentions_contract import selection
from tests.test_mention_ingestion import batch, ingest, values

pytestmark=pytest.mark.skipif(not PG_URL,reason='defina MT5X_TEST_PG_URL para testar com PostgreSQL')


def test_postgresql_mention_ingestion_uses_one_population_and_exact_repeat(pg):
    result=ingest(pg);assert result['counts']['inserted']==3
    assert {k:r.numeric_value for k,r in values(pg).items()}=={'mentions.quantity':10.,'mentions.positive':3.,'mentions.negative':2.}
    assert ingest(pg)['counts']==dict(inserted=0,revised=0,unchanged=3)
    assert producer_jobs.get_checkpoint(pg,'QA','M1',result['job_family'])['source_sequence']==1


def test_postgresql_cancelled_checkpoint_rolls_back_all_three_counts(pg,monkeypatch):
    stop=threading.Event();original=producer_jobs.advance_checkpoint
    def cancel(conn,**kwargs):original(conn,**kwargs);stop.set()
    monkeypatch.setattr(producer_jobs,'advance_checkpoint',cancel)
    with pytest.raises(indicator_materializer.Cancelled):ingest(pg,cancel_check=stop.is_set)
    assert feature_store.query_history(pg).empty
    descriptor=sdk.describe_mention_series(pg,**selection())
    assert producer_jobs.get_checkpoint(pg,'QA','M1',descriptor['job_family']) is None


def test_postgresql_concurrent_mention_writers_keep_first_receipt_and_cursor(pg):
    # Registry initialization is separate from value publication; definitions
    # exist before concurrent writers, as in the other canonical producer tests.
    descriptor=sdk.describe_mention_series(pg,**selection())
    for definition in descriptor['definitions']:features.register(pg,**definition)
    data=batch(pg);ready=threading.Barrier(2)
    def write():ready.wait(timeout=10);return ingest(pg,data)
    with ThreadPoolExecutor(max_workers=2) as pool:
        a,b=list(pool.map(lambda _:write(),range(2)))
    assert sorted(r['counts']['inserted'] for r in [a,b])==[0,3]
    assert len(feature_store.query_history(pg))==3
    assert producer_jobs.get_checkpoint(pg,'QA','M1',descriptor['job_family'])['source_sequence']==1
