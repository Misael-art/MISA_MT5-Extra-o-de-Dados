"""Publicação/leitura/transporte dos blocos no PostgreSQL real, schema próprio."""
import os
import pandas as pd
import pytest

from mt5_extracao.analytics import datasets
from mt5_extracao.database_manager import DatabaseManager
from tests.postgres_helpers import isolated_postgresql_database
from tests.test_dataset_block_decoding import prepare

PG_URL=os.environ.get('MT5X_TEST_PG_URL')
pytestmark=pytest.mark.skipif(not PG_URL,reason='defina MT5X_TEST_PG_URL para PostgreSQL real')


def test_daily_blocks_keep_exact_snapshot_after_postgresql_to_sqlite_transfer(tmp_path):
    with isolated_postgresql_database(PG_URL,broker_utc_offset=0) as db:
        snapshot=prepare(db);before=datasets._bundle(db,snapshot.manifest_id)
        assert len(before['manifest']['frames']['bars'])==31
        loaded=datasets.load(db,snapshot.manifest_id)
        pd.testing.assert_frame_equal(loaded.bars,snapshot.bars,check_exact=True)
        assert datasets._bundle(db,snapshot.manifest_id)==before
        exported=tmp_path/'snapshot.json';datasets.export_snapshot(db,snapshot.manifest_id,exported)
    target=DatabaseManager(db_path=str(tmp_path/'target.sqlite'),broker_utc_offset=0)
    try:
        imported=datasets.import_snapshot(target,exported)
        assert imported.manifest_id==snapshot.manifest_id and imported.manifest==snapshot.manifest
        pd.testing.assert_frame_equal(imported.bars,snapshot.bars,check_exact=True)
    finally:target.close()
