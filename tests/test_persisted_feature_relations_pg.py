"""Publicação e leitura no schema PostgreSQL de QA existente."""
import pytest
from tests.test_analytics_profiles_pg import PG_URL
from tests.test_composite_materialization_pg import pg
from tests.test_frozen_association_query import seed_reference, query
from mt5_extracao import sdk
from mt5_extracao.analytics import feature_store
from tests.test_persisted_feature_relations import stored_counts

pytestmark = pytest.mark.skipif(not PG_URL, reason='defina MT5X_TEST_PG_URL para testar com PostgreSQL')


def test_postgresql_pair_persistence_repeats_and_reads_original(pg):
    _, snapshot, cutoff = seed_reference(pg)
    first = sdk.persist_feature_relation(pg, snapshot.manifest_id, left=('qa.x','1'), right=('qa.positive','1'),
        as_of=cutoff, bootstrap_replicates=29, synthetic_fixture=True)
    dto = first['relation']
    assert first['status'] == 'INSERTED'
    assert feature_store.save_pair_relation(pg, dto)['status'] == 'UNCHANGED'
    assert sdk.load_feature_relation(pg, first['analysis_id']) == first['relation']
    assert stored_counts(pg) == (1, 1)


def test_postgresql_relation_rollback_and_concurrent_publication(pg):
    from concurrent.futures import ThreadPoolExecutor
    dto = query(seed_reference(pg))
    def interrupt(conn):
        raise RuntimeError('controlled transaction interruption')
    with pytest.raises(RuntimeError, match='interruption'):
        feature_store.save_pair_relation(pg, dto, transaction_hook=interrupt)
    assert stored_counts(pg) == (0, 0)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: feature_store.save_pair_relation(pg, dto), range(2)))
    assert sorted(row['status'] for row in results) == ['INSERTED', 'UNCHANGED']
    assert stored_counts(pg) == (1, 1)
