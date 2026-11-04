"""Mesmo pacote e transação no schema PostgreSQL de QA existente."""
import pytest
from concurrent.futures import ThreadPoolExecutor
from tests.test_analytics_profiles_pg import PG_URL
from tests.test_composite_materialization_pg import pg
from tests.test_frozen_association_query import reference
from tests.test_relation_transfer import package, counts
from mt5_extracao import sdk
from mt5_extracao.analytics import feature_store

pytestmark=pytest.mark.skipif(not PG_URL,reason='defina MT5X_TEST_PG_URL para testar com PostgreSQL')


def test_postgresql_import_receipt_is_exact_and_rolls_back_ancestry(reference,pg,tmp_path,monkeypatch):
    receipt,path,_=package(reference,tmp_path);before=counts(pg)
    original=feature_store._save_pair_relation
    def interrupt(*args,**kwargs):
        original(*args,**kwargs)
        raise RuntimeError('controlled after all writes')
    monkeypatch.setattr(feature_store,'_save_pair_relation',interrupt)
    with pytest.raises(RuntimeError,match='controlled'):sdk.import_feature_relation(pg,path)
    assert counts(pg)==before
    monkeypatch.undo()
    assert sdk.import_feature_relation(pg,path)==receipt
    assert sdk.import_feature_relation(pg,path)==dict(receipt,status='UNCHANGED')
    assert sdk.load_feature_relation(pg,receipt['analysis_id'])==receipt['relation']


def test_postgresql_concurrent_importers_keep_first_receipt(reference,pg,tmp_path):
    receipt,path,_=package(reference,tmp_path)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results=list(pool.map(lambda _:sdk.import_feature_relation(pg,path),range(2)))
    assert sorted(r['status'] for r in results)==['INSERTED','UNCHANGED']
    assert all(dict(r,status='INSERTED')==receipt for r in results)
    assert counts(pg)['_feature_relations']==1
