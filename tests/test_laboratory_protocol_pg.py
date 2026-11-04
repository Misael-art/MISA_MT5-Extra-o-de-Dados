"""Publicação e exposição no ledger PostgreSQL real, sem dados pessoais."""
import os
import pytest
from mt5_extracao.analytics import datasets
from mt5_extracao.experiments import relations,store
from tests.postgres_helpers import isolated_postgresql_database
from tests.test_laboratory_protocol import research

PG_URL=os.environ.get('MT5X_TEST_PG_URL')
pytestmark=pytest.mark.skipif(not PG_URL,reason='defina MT5X_TEST_PG_URL para PostgreSQL real')


def test_registered_relation_result_and_exposure_postgresql():
    with isolated_postgresql_database(PG_URL,broker_utc_offset=0) as db:
        datasets.declare_source(db,dict(provider='qa_prices',server='Synthetic'))
        _,report,spec=research.__wrapped__(db)
        p=relations.register(db,report,spec);result=relations.evaluate(db,report,p['protocol_id'],key='pg')
        assert result==relations.receipt(db,p['protocol_id'],'pg')
        assert result['tests_counted']==12
        assert store.status(db,p['protocol_id'])['exposure']['holdout']['recorded_accesses']==1
