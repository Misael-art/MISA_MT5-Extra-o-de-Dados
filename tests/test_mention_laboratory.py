"""MEN-06: projeção por janela no SDK, médias canônicas e estados sem preço."""
from datetime import datetime,timezone
import json

import pytest

from mt5_extracao import sdk
from mt5_extracao.analytics import datasets,feature_store,indicator_materializer
from tests.test_mentions_contract import db,contract,register,records
from tests.test_mention_classification import prepared,classify
from tests.test_mention_snapshots import freeze
from tests.test_frozen_association_query import reference


def test_topic_is_discovered_and_known_abstentions_remain_visible_without_model_or_network(db,monkeypatch):
    s,b=prepared(db);classify(db,s,b)
    sources=sdk.stored_series_sources(db);assert sources==[dict(symbol='QA',timeframe='M1',rows=1,start='2025-01-01T10:00:00+00:00',end='2025-01-01T10:01:00+00:00')]
    frozen=freeze(db,s)
    from mt5_extracao.analytics import mention_classifier
    monkeypatch.setattr(mention_classifier,'classify_document',lambda *a,**k:(_ for _ in ()).throw(AssertionError('model on read')))
    report=sdk.query_stored_observations(db,frozen['manifest_id'],as_of=datetime.now(timezone.utc).isoformat(),mean_period=1)
    channels={c['feature_id']:c for c in report['channels']}
    assert channels['mentions.quantity']['values']==[10.] and channels['mentions.positive']['values']==[3.]
    assert channels['mentions.negative']['metadata'][0]['label_counts']['abstained']==1
    assert channels['mentions.quantity']['mean']==[10.]
    assert channels['mentions.quantity']['mean_implementation']['function'].endswith('.sma')
    assert report['temporal_basis']=='publication_windows_seen_retrospectively_at_explicit_cut'
    json.dumps(report,allow_nan=False)


def windows(db):
    c=contract(db);register(db,c)
    for minute,q in [(0,1),(1,2),(2,3),(4,5)]:
        rows=records(c,start=f'2025-01-01T10:{minute:02d}Z',end=f'2025-01-01T10:{minute+1:02d}Z',
            quantity=q,positive=0,negative=0,population_revision=str(minute))
        for r in rows:r['metadata']['synthetic_fixture']=True
        feature_store.save_values(db,rows)
    snap=sdk.capture_feature_snapshot(db,symbol='QA',timeframe='M1',start='2025-01-01T10:00Z',end='2025-01-01T10:05Z',
        stored_features=[(d['feature_id'],c['algorithm_version']) for d in c['definitions']])
    return c,snap


def test_mean_uses_consecutive_windows_no_carry_no_zero_and_keeps_source_availability(db):
    c,snap=windows(db)
    report=sdk.query_stored_observations(db,snap['manifest_id'],as_of='2025-01-01T11:00Z',mean_period=3)
    q=next(c for c in report['channels'] if c['feature_id']=='mentions.quantity')
    assert q['values']==[1.,2.,3.,None,5.]
    assert q['mean']==[None,None,2.,None,None]
    assert q['mean_available_at'][2]=='2025-01-01T10:20:00+00:00'
    assert q['qualities'][3]=='NOT_AVAILABLE_AT_CUT'


def test_late_revision_does_not_change_frozen_projection_or_old_cut(db):
    s,b=prepared(db);before=freeze(db,s);classify(db,s,b);after=freeze(db,s)
    old=sdk.query_stored_observations(db,after['manifest_id'],as_of='2025-01-01T10:21Z',mean_period=1)
    assert next(c for c in old['channels'] if c['feature_id']=='mentions.positive')['values']==[None]
    late=sdk.query_stored_observations(db,before['manifest_id'],as_of=datetime.now(timezone.utc).isoformat(),mean_period=1)
    assert next(c for c in late['channels'] if c['feature_id']=='mentions.positive')['values']==[None]


@pytest.mark.parametrize('options',[dict(mean_period=0),dict(mean_period=True),dict(as_of='2099-01-01T00:00Z'),dict(as_of='2025-01-01'),dict(max_memory_mb=1)])
def test_bad_period_clock_budget_and_cancel_are_explicit(db,options):
    c,snap=windows(db)
    with pytest.raises((ValueError,MemoryError)):
        sdk.query_stored_observations(db,snap['manifest_id'],**{'as_of':'2025-01-01T11:00Z',**options})
    with pytest.raises(indicator_materializer.Cancelled):
        sdk.query_stored_observations(db,snap['manifest_id'],as_of='2025-01-01T11:00Z',cancel_check=lambda:True)


def test_ui_value_format_preserves_observed_zero_and_legible_exact_counts():
    from mt5_extracao.ui.series_observations import value_text
    assert [value_text(v) for v in [None,0,1234567,2.5]]==['—','0','1.234.567','2,5000']


@pytest.mark.parametrize('right,expected',[('qa.x',1.),('qa.independent',0.)])
def test_mentions_counts_use_existing_relation_engine_for_known_strong_and_null_indicator(reference,right,expected):
    from mt5_extracao.analytics import mentions
    from tests.test_mentions_contract import selection
    db,base,cut=reference;c=mentions.describe(db,**selection());register(db,c)
    for row in feature_store.query_history(db,feature_id='qa.x',algorithm_version='1').itertuples():
        start=row.observation_time;end=start+__import__('pandas').Timedelta(minutes=1)
        prepared=mentions.value_records(c,start=start,end=end,captured_at=end,processed_at=end,
            population_revision=start.isoformat(),quantity=1000000+int(row.numeric_value),positive=250000,negative=50000,
            classification='complete',classified_at=end)
        for item in prepared:item['metadata']['synthetic_fixture']=True
        feature_store.save_values(db,prepared)
    pairs=[('mentions.quantity',c['algorithm_version']),(right,'1')]
    frozen=datasets.capture(db,'QA','M1',stored_features=pairs)
    result=sdk.query_feature_relation(db,frozen.manifest_id,left=pairs[0],right=pairs[1],as_of=datetime.now(timezone.utc).isoformat(),
        bootstrap_replicates=9,synthetic_fixture=True)
    assert result['relation']['effect']==pytest.approx(expected,abs=1e-12)
    assert result['relation']['n_pairs']==160 and result['relation']['n_sessions']==20
    assert not result['qualification']['financial_utility_certified']


def test_cli_stored_source_discovery_and_frozen_projection_equal_sdk(db,tmp_path,capsys):
    from mt5_extracao import cli
    c,snap=windows(db);cfg=tmp_path/'qa.ini'
    cfg.write_text(f'[DATABASE]\ntype=sqlite\npath={db.db_path}\n[APP]\nbroker_utc_offset=0\n')
    prefix=['--config',str(cfg),'--json','analytics','features']
    assert cli.main(prefix+['stored-sources'])==0
    assert json.loads(capsys.readouterr().out)['data']['sources']==sdk.stored_series_sources(db)
    assert cli.main(prefix+['stored-observations','--manifest',snap['manifest_id'],'--as-of','2025-01-01T11:00Z'])==0
    assert json.loads(capsys.readouterr().out)['data']['observations']==sdk.query_stored_observations(db,snap['manifest_id'],as_of='2025-01-01T11:00Z')
