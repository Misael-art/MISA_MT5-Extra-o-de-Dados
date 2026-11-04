"""Consulta de tentativas/publicação no PostgreSQL real em schema próprio."""
import os
import pytest
from mt5_extracao import sdk
from mt5_extracao.robots import backtest_scope,store
from tests.postgres_helpers import isolated_postgresql_database
from tests.test_scope_comparison import prepare

PG_URL=os.environ.get('MT5X_TEST_PG_URL')
pytestmark=pytest.mark.skipif(not PG_URL,reason='defina MT5X_TEST_PG_URL para PostgreSQL real')


def test_same_revision_across_timeframes_postgresql():
    with isolated_postgresql_database(PG_URL,broker_utc_offset=0) as db:
        cfg,robot,plan=prepare(db)
        batch=backtest_scope.create_runner(db,cfg,robot,plan['batch_id']).start()
        assert batch.join(30) and not batch.error,batch.error
        before=[r['run_id'] for r in store.load_runs(db)]
        value=sdk.compare_backtest_scope(db,plan['batch_id'])
        assert value['compatible'],value['reasons']
        assert value['counts']=={'completed':2,'excluded':19}
        assert [r['run_id'] for r in store.load_runs(db)]==before
