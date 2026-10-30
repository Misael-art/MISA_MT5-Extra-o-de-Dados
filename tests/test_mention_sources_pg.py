"""Captura HN: contrato de rede injetado, transações e manifesto PostgreSQL."""
import json

import pytest
from sqlalchemy import text

from mt5_extracao.analytics import feature_store, producer_jobs
from tests.test_analytics_profiles_pg import PG_URL
from tests.test_composite_materialization_pg import pg
from tests.test_mention_sources import collect,transport,Response,document

pytestmark=pytest.mark.skipif(not PG_URL,reason='defina MT5X_TEST_PG_URL para testar com PostgreSQL')


def test_postgresql_public_api_panel_and_first_receipt_are_preserved(pg,monkeypatch):
    transport(monkeypatch);first=collect(pg);assert first['receipt']['quantity']==2
    second=collect(pg);assert second['counts']==dict(inserted=0,revised=0,unchanged=3)
    assert first['receipt']['capture_id']==second['receipt']['capture_id']
    assert len(feature_store.query_history(pg))==3


def test_postgresql_failed_item_reuses_input_roster_without_network_transaction(pg,monkeypatch):
    def broken(url):return Response([1,2]) if url.endswith('newstories.json') else Response(None)
    transport(monkeypatch,broken)
    with pytest.raises(ValueError):collect(pg)
    assert feature_store.query_history(pg).empty
    session=transport(monkeypatch);result=collect(pg)
    assert result['acquisition']['resumed_panel'] and not any(u.endswith('newstories.json') for u,_ in session.calls)
    checkpoint=producer_jobs.get_checkpoint(pg,'QA','M1',result['job_family'])
    assert checkpoint['status']=='COMPLETE' and 'pending' not in checkpoint['state']


def test_postgresql_run_input_attachment_handles_null_configuration_and_is_immutable(pg):
    run=producer_jobs.start_run(pg,'QA','M1','qa-input-manifest','1','cfg',0)
    try:
        assert producer_jobs.attach_run_inputs(pg,run,{'ids':[1,2]})
        with pytest.raises(ValueError):producer_jobs.attach_run_inputs(pg,run,{'ids':[3]})
        producer_jobs.finish_run(pg,run,source_sequence=0,status='COMPLETE')
        with pg.engine.connect() as conn:
            config=json.loads(conn.execute(text('SELECT configuration_json FROM _analytics_producer_runs WHERE run_id=:id'),{'id':run['run_id']}).scalar_one())
        assert config['input_manifest']['ids']==[1,2]
    finally:run['rss_sampler'].stop()
