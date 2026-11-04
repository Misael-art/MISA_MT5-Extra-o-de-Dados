"""Contratos congelados e resultado exato no backend PostgreSQL de QA."""
import pytest
from mt5_extracao import sdk
from tests.test_analytics_profiles_pg import PG_URL
from tests.test_composite_materialization_pg import pg
from tests.test_frozen_association_query import seed_reference

pytestmark=pytest.mark.skipif(not PG_URL,reason='defina MT5X_TEST_PG_URL para testar com PostgreSQL')


def test_creator_contracts_and_exact_result_lookup_in_postgresql(pg):
    _,snapshot,cutoff=seed_reference(pg)
    contract=sdk.inspect_feature_snapshot(pg,snapshot.manifest_id)
    assert contract['status']=='ok' and len(contract['features'])==9
    assert not next(r for r in contract['features'] if r['feature_id']=='qa.category')['numeric_contract_compatible']
    receipt=sdk.persist_feature_relation(pg,snapshot.manifest_id,left=('qa.x','1'),right=('qa.positive','1'),
        as_of=cutoff,bootstrap_replicates=29,synthetic_fixture=True)
    listed=sdk.query_saved_relations(pg,analysis_id=receipt['analysis_id'])
    assert listed['status']=='ok' and listed['matched']==1
    assert listed['items'][0]['analysis_id']==receipt['analysis_id']
