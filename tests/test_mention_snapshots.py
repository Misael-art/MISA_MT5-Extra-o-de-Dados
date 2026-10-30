"""MEN-05: temas sem OHLCV no snapshot canônico, revisões/cortes e transporte."""
from copy import deepcopy
from datetime import datetime,timezone
import json

import pandas as pd
import pytest
from sqlalchemy import inspect,text

from mt5_extracao import cli,sdk
from mt5_extracao.analytics import datasets,feature_store,indicator_context,indicator_materializer
from mt5_extracao.database_manager import DatabaseManager
from tests.test_mentions_contract import db
from tests.test_mention_classification import prepared,classify


def freeze(db,s,**changes):
    version=sdk.describe_mention_series(db,**s)['algorithm_version']
    args=dict(symbol=s['symbol'],timeframe=s['timeframe'],start='2025-01-01T10:00Z',end='2025-01-01T10:01Z',
        stored_features=[('mentions.'+key,version) for key in ['quantity','positive','negative']])
    return sdk.capture_feature_snapshot(db,**dict(args,**changes))


def test_topic_snapshot_needs_no_market_timezone_or_fake_prices(db,monkeypatch):
    s,b=prepared(db);db.offset_declared=False
    monkeypatch.setattr(db,'load_ohlcv',lambda *a,**k:(_ for _ in ()).throw(AssertionError('topic read OHLCV')))
    original=datasets.identity(db);result=freeze(db,s);snap=datasets.load(db,result['manifest_id'])
    assert snap.manifest['schema_version']==datasets.FEATURE_ONLY_VERSION==5
    assert set(snap.manifest['frames'])=={'feature_definitions','feature_history'}
    assert snap.bars.empty and len(snap.feature_history)==3 and snap.manifest['specification'] is None
    assert snap.feature_window_utc==(pd.Timestamp('2025-01-01T10:00Z'),pd.Timestamp('2025-01-01T10:01Z'))
    assert snap.quality['evidence_basis']=='feature_only_no_market_data' and not snap.spec.known
    with pytest.raises(ValueError,match='somente séries'):snap.view().load_ohlcv('QA','M1')
    assert datasets.identity(db)==original
    assert sdk.inspect_feature_snapshot(db,snap.manifest_id)['status']=='ok'
    assert {d['unit'] for d in snap.feature_definitions}=={'publication/window'}
    assert snap.manifest_id==freeze(db,s)['manifest_id']


def test_frozen_before_and_after_late_classification_and_population_revision_are_immutable(db,tmp_path,monkeypatch):
    s,b=prepared(db);first=freeze(db,s);old=datasets.load(db,first['manifest_id']).feature_history
    classify(db,s,b);second=freeze(db,s);assert first['manifest_id']!=second['manifest_id']
    assert len(datasets.load(db,second['manifest_id']).feature_history)==5
    b['documents'][0].update(revision='2',text='Acme is awful.');b['captured_at']=datetime.now(timezone.utc).isoformat()
    sdk.ingest_mention_batch(db,selection=s,batch=b,synthetic_fixture=True);classify(db,s,b)
    third=freeze(db,s);assert third['manifest_id'] not in (first['manifest_id'],second['manifest_id'])
    pd.testing.assert_frame_equal(datasets.load(db,first['manifest_id']).feature_history,old)
    path=tmp_path/'frozen.json';sdk.export_feature_snapshot(db,second['manifest_id'],path)
    raw=path.read_text();assert all(d['text'] not in raw for d in b['documents'])
    other=DatabaseManager(db_path=str(tmp_path/'destination.sqlite'))
    try:
        original=datasets.identity(other);imported=sdk.import_feature_snapshot(other,path)
        assert imported['manifest_id']==second['manifest_id'] and datasets.identity(other)==original
        assert feature_store.query_history(other).empty
        monkeypatch.setattr(db,'load_ohlcv',lambda *a,**k:(_ for _ in ()).throw(AssertionError('live access')))
        snap=datasets.load(other,imported['manifest_id'])
        context=indicator_context.from_snapshot(snap,pd.DataFrame(dict(decision_time=[snap.feature_history.available_at_utc.max()])),
            feature_ids=['mentions.quantity','mentions.positive','mentions.negative'],observation_time=b['start'])
        assert context['mentions.positive::value'].iloc[0]==3
        out=tmp_path/'again.json';sdk.export_feature_snapshot(other,imported['manifest_id'],out)
        assert path.read_bytes()==out.read_bytes()
    finally:other.close()


@pytest.mark.parametrize('changes',[
    dict(start=None),dict(end=None),dict(start='2025-01-01T10:00'),
    dict(end='2025-01-01T10:00Z'),dict(end='2099-01-01T00:00Z'),dict(stored_features=[]),dict(feature_only='yes')])
def test_feature_only_requires_explicit_finite_closed_utc_bounds_and_versions(db,changes):
    s,b=prepared(db)
    with pytest.raises(ValueError):freeze(db,s,**changes)
    with db.engine.connect() as c:assert c.execute(text('SELECT COUNT(*) FROM _dataset_manifests')).scalar()==0


def test_empty_window_preserves_missing_and_half_open_observation_boundaries(db):
    s,b=prepared(db)
    empty=freeze(db,s,start='2025-01-01T09:59Z',end='2025-01-01T10:00Z')
    snap=datasets.load(db,empty['manifest_id']);assert snap.feature_history.empty
    assert len(snap.feature_definitions)==3


def test_cancellation_at_manifest_insert_rolls_back_all_new_blocks(db,monkeypatch):
    s,b=prepared(db);original=datasets._persist_validated
    def wrapped(conn,bundle,**kw):
        original(conn,bundle,**kw);raise indicator_materializer.Cancelled('QA cancellation after insert')
    monkeypatch.setattr(datasets,'_persist_validated',wrapped)
    with pytest.raises(indicator_materializer.Cancelled):freeze(db,s)
    with db.engine.connect() as c:
        assert c.execute(text('SELECT COUNT(*) FROM _dataset_manifests')).scalar()==0
        assert c.execute(text('SELECT COUNT(*) FROM _dataset_blocks')).scalar()==0


def test_cli_capture_export_import_and_context_are_passive(db,tmp_path,capsys):
    s,b=prepared(db);classify(db,s,b);version=sdk.describe_mention_series(db,**s)['algorithm_version']
    cfg=tmp_path/'qa.ini';cfg.write_text(f'[DATABASE]\ntype=sqlite\npath={db.db_path}\n[APP]\nbroker_utc_offset=0\n')
    prefix=['--config',str(cfg),'--json','analytics','features']
    args=['snapshot-capture','--symbol','QA','--tf','M1','--start',b['start'],'--end',b['end']]
    for key in ['quantity','positive','negative']:args+=['--stored-feature','mentions.'+key,version]
    assert cli.main(prefix+args)==0
    manifest=json.loads(capsys.readouterr().out)['data']['snapshot']['manifest_id']
    target=tmp_path/'snapshot.json'
    assert cli.main(prefix+['snapshot-export','--manifest',manifest,'--out',str(target)])==0;capsys.readouterr()
    assert cli.main(prefix+['snapshot-import','--source',str(target)])==0
    assert json.loads(capsys.readouterr().out)['data']['snapshot']['manifest_id']==manifest
    assert cli.main(prefix+args+['--max-memory-mb','1'])==1
    error=json.loads(capsys.readouterr().out)
    assert not error['ok'] and 'orçamento' in str(error['errors'])


@pytest.mark.parametrize('change',['market_claim','extra_selection','offset','period','observation'])
def test_rehashed_feature_only_packet_cannot_invent_market_or_change_scope(db,change):
    s,b=prepared(db);result=freeze(db,s);bundle=deepcopy(datasets._bundle(db,result['manifest_id']))
    manifest=bundle['manifest']
    if change=='market_claim':manifest['derivation']['market_data_included']=True
    elif change=='extra_selection':manifest['selection']['expression']='private query must not be a selection slot'
    elif change=='offset':manifest['time']['broker_utc_offset_hours']=3
    elif change=='period':manifest['period']['rows']=999
    else:
        manifest['selection']['start']='2025-01-01T10:01:00+00:00'
        manifest['selection']['end']='2025-01-01T10:02:00+00:00'
        manifest['period'].update(first=manifest['selection']['start'],last=manifest['selection']['end'])
    bundle['manifest_id']=datasets._hash(datasets._json(manifest))
    with pytest.raises(ValueError,match='integridade'):datasets._validated(bundle)
