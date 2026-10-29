"""Mesmo índice/cursores no backend PostgreSQL oficial de QA."""
import pytest
from mt5_extracao import sdk
from tests.test_analytics_profiles_pg import PG_URL
from tests.test_composite_materialization_pg import pg
from tests.test_saved_relation_queries import seed_saved_relations

pytestmark=pytest.mark.skipif(not PG_URL,reason='defina MT5X_TEST_PG_URL para testar com PostgreSQL')


def test_saved_relation_pages_are_stable_in_postgresql(pg):
    seed_saved_relations(pg)
    first=sdk.query_saved_relations(pg,page_size=1)
    assert first['status']=='ok' and first['matched']==3
    second=sdk.query_saved_relations(pg,page_size=1,cursor=first['page']['next_cursor'])
    assert second['status']=='ok' and second['items'][0]['analysis_id']!=first['items'][0]['analysis_id']
    assert sdk.load_feature_relation(pg,second['items'][0]['analysis_id'])['relation']==second['items'][0]['relation']
