"""Contrato MEN no PostgreSQL existente; nenhum banco/fontes privados como fixture."""
import pytest

from mt5_extracao import sdk
from mt5_extracao.analytics import datasets, feature_store, producer_jobs
from tests.test_analytics_profiles_pg import PG_URL
from tests.test_composite_materialization_pg import pg
from tests.test_mentions_contract import selection, register, records

pytestmark = pytest.mark.skipif(not PG_URL, reason="defina MT5X_TEST_PG_URL para testar com PostgreSQL")


def test_scoped_mentions_values_and_checkpoints_do_not_collide_in_postgresql(pg):
    original=datasets.identity(pg);producer_jobs.ensure_schema(pg)
    for i,provider in enumerate(['qa.public-a','qa.public-b']):
        options=selection();options['source']['provider']=provider
        descriptor=sdk.describe_mention_series(pg,**options);register(pg,descriptor)
        counts=feature_store.save_values(pg,records(descriptor,quantity=10+i))
        assert counts==dict(inserted=3,revised=0,unchanged=0)
        assert feature_store.save_values(pg,records(descriptor,quantity=10+i))==dict(inserted=0,revised=0,unchanged=3)
        with pg.engine.begin() as conn:
            producer_jobs.advance_checkpoint(conn,symbol='QA',timeframe='M1',family=descriptor['job_family'],
                algorithm_version=descriptor['algorithm_version'],config_hash=descriptor['scope_hash'],
                source_sequence=i,last_bar_start='2025-01-01T10:00:00Z',state={'cursor':str(i)})
        assert producer_jobs.get_checkpoint(pg,'QA','M1',descriptor['job_family'])['state']['cursor']==str(i)
    assert len(feature_store.query_history(pg))==6
    assert datasets.identity(pg)==original
