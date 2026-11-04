"""Vínculo explícito e relógios: fatos próprios, separados do preço e sem dados live."""
import copy
import json

import pandas as pd
import pytest

from mt5_extracao.analytics import datasets, feature_store, mentions
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.laboratory import service
from tests.test_mentions_contract import selection, register


@pytest.fixture
def linked(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path/'links.sqlite'), broker_utc_offset=0)
    datasets.declare_source(db, {'provider':'qa_prices', 'server':'Synthetic'})
    for tf, count, step in [('1 minuto',18,1),('3 minutos',6,3)]:
        x = list(range(count));close = [100.+i*.2 for i in x]
        db.save_ohlcv_data('X', tf, pd.DataFrame(dict(
            time=pd.date_range('2025-01-01T10:00',periods=count,freq=f'{step}min'),
            open=close,high=[v+.1 for v in close],low=[v-.1 for v in close],close=close,
            tick_volume=[10+i for i in x],real_volume=[100+i for i in x],spread=1)))
    market = service.capture_selection(db,symbol='X',timeframes_selected=['M1','M3'])
    options = selection();options.update(symbol='TOPIC:QA',timeframe='M3')
    contract = mentions.describe(db,**options);register(db,contract)
    for i in range(5):
        start=pd.Timestamp('2025-01-01T10:00Z')+pd.Timedelta(minutes=3*i);end=start+pd.Timedelta(minutes=3)
        rows=mentions.value_records(contract,start=start,end=end,captured_at=end,processed_at=end+pd.Timedelta(minutes=1),
            classified_at=end+pd.Timedelta(minutes=2),population_revision=str(i),quantity=10*(i+1),positive=2*(i+1),negative=i,
            classification='complete')
        for row in rows:row['metadata']['synthetic_fixture']=True
        feature_store.save_values(db,rows)
    pairs=[(d['feature_id'],contract['algorithm_version']) for d in contract['definitions']]
    snap=datasets.capture(db,'TOPIC:QA','M3','2025-01-01T10:00Z','2025-01-01T10:15Z',stored_features=pairs,feature_only=True)
    source=dict(snapshot=snap,features=pairs,target_symbol='X',relationship='Vínculo próprio de QA, não equivalência de instrumento')
    yield db,market,source,contract
    db.close()


def analyze(linked,**kwargs):
    db,market,source,contract=linked
    return service.analyze(market,feature_ids=['volume.ticks'],as_of='2025-01-01T10:18Z',linked_sources=[source],**kwargs)


def channel(report,feature):
    return next(c for c in report['channels'] if c.get('linked_source') and c['feature']==feature)


def test_receipt_is_a_pulse_and_classification_is_not_backdated(linked):
    report=analyze(linked)
    q,p=channel(report,'mentions.quantity'),channel(report,'mentions.positive')
    assert [i for i,v in enumerate(q['values']) if v is not None]==[3,6,9,12,15]
    assert [i for i,v in enumerate(p['values']) if v is not None]==[4,7,10,13,16]
    assert q['values'][2] is p['values'][3] is None
    assert q['available']==p['available']==5 and q['total']==18
    assert q['source_rows']==5 and q['states']['NO_NEW_OBSERVATION']==13
    assert q['directions']==[None]*18
    assert report['qualification']['retrospective_publication'] is False
    assert service.load_report(json.dumps(report))==report


def test_publication_is_explicit_retrospective_and_numeric_crossings_use_existing_matrix(linked):
    report=analyze(linked,timeline_basis='publication')
    q,p=channel(report,'mentions.quantity'),channel(report,'mentions.positive')
    assert [i for i,v in enumerate(q['values']) if v is not None]==[2,5,8,11,14]
    assert q['values']==[None,None,10.,None,None,20.,None,None,30.,None,None,40.,None,None,50.,None,None,None]
    assert q['receipt_events'][0]['available_at']=='2025-01-01T10:04:00+00:00'
    assert p['receipt_events'][0]['available_at']=='2025-01-01T10:05:00+00:00'
    assert report['qualification']['retrospective_publication']
    matrix=service.relation_matrix(report)
    qi,pi=[next(i for i,c in enumerate(report['channels']) if c['id']==ch['id']) for ch in [q,p]]
    assert matrix['cells'][qi][pi]['effect']==pytest.approx(1.)
    assert matrix['cells'][qi][pi]['n_pairs']==5
    # Recebimentos separados não são fabricados como pares simultâneos.
    causal=analyze(linked);cm=service.relation_matrix(causal)
    assert cm['cells'][qi][pi]['effect'] is None
    assert cm['cells'][qi][pi]['n_pairs']==0


def test_mapping_scope_version_and_content_are_immutable(linked):
    db,market,source,contract=linked;before=analyze(linked)
    source['target_symbol']='Y'
    with pytest.raises(ValueError,match='ativo estudado'):analyze(linked)
    source['target_symbol']='X';source['relationship']='Associação alternativa explicitamente escolhida'
    after=analyze(linked);assert after['analysis_id']!=before['analysis_id']
    altered=copy.deepcopy(after);altered['channels'][0]['values'][0]=999
    with pytest.raises(ValueError,match='alterado'):service.load_report(json.dumps(altered))
    source['features'].append(source['features'][0])
    with pytest.raises(ValueError,match='duplicad'):analyze(linked)


def test_cancellation_and_unknown_perspective_do_not_publish_a_report(linked):
    db,market,source,contract=linked
    from mt5_extracao.analytics.indicator_materializer import Cancelled
    with pytest.raises(Cancelled):
        service.analyze(market,feature_ids=[],linked_sources=[source],as_of='2025-01-01T10:18Z',cancel_check=lambda:True)
    with pytest.raises(ValueError,match='Perspectiva'):analyze(linked,timeline_basis='guess')


def test_cut_does_not_expose_future_prices_or_classification(linked):
    db,market,source,contract=linked
    report=service.analyze(market,feature_ids=['volume.ticks'],linked_sources=[source],as_of='2025-01-01T10:04Z')
    assert len(report['times'])==len(report['prices'])==4
    assert channel(report,'mentions.quantity')['values'][-1]==10.
    assert channel(report,'mentions.positive')['values']==[None]*4
    assert service.candle_context(report,3)['received_events'][0]['available_at']=='2025-01-01T10:04:00+00:00'
    assert service.candle_context(report,3)['calendar_basis']=='NO_FROZEN_CALENDAR'
    assert service.target_matrix(report,horizon=1)['cells'][0][0]['n_pairs']==3


def test_portable_report_and_candle_read_are_passive_and_escape_source_text(linked,tmp_path,monkeypatch):
    import requests
    from mt5_extracao import sdk
    from mt5_extracao.analytics import mention_classifier
    db,market,source,contract=linked
    source['relationship']='Objeto </script><script>throw Error("injected")</script>'
    report=analyze(linked,timeline_basis='publication')
    refused=lambda *a,**k:(_ for _ in ()).throw(AssertionError('network/model on frozen report'))
    monkeypatch.setattr(requests.sessions.Session,'request',refused)
    monkeypatch.setattr(mention_classifier,'classify_document',refused)
    out=tmp_path/'portable.html';receipt=sdk.export_laboratory_report(report,out)
    html=out.read_text(encoding="utf-8");assert '</script><script>throw Error("injected")' not in html
    assert '\\u003c/script\\u003e' in html and 'Content-Security-Policy' in html
    assert receipt['network_required'] is False
    with pytest.raises(ValueError,match='novo'):sdk.export_laboratory_report(report,out)
    assert sdk.laboratory_candle(report,2)['publication_windows']


def test_new_cli_analysis_and_report_use_same_sdk_envelope(linked,tmp_path,capsys):
    from mt5_extracao import cli,sdk
    db,market,source,contract=linked
    cfg=tmp_path/'qa.ini';cfg.write_text(f'[DATABASE]\ntype=sqlite\npath={db.db_path}\n[APP]\nbroker_utc_offset=0\n')
    config=[dict(manifest_id=source['snapshot'].manifest_id,features=source['features'],relationship=source['relationship'])]
    links=tmp_path/'links.json';links.write_text(json.dumps(config));out=tmp_path/'analysis.json'
    args=['--config',str(cfg),'--json','laboratory','analyze','--as-of','2025-01-01T10:18Z','--links',str(links),
          '--feature','volume.ticks','--timeline-basis','publication','--out',str(out)]
    for snap in market:args+=['--manifest',snap.manifest_id]
    assert cli.main(args)==0
    report=json.loads(capsys.readouterr().out)['data']
    expected=sdk.analyze_laboratory(db,manifest_ids=[s.manifest_id for s in market],linked_selections=config,
        timeline_basis='publication',feature_ids=['volume.ticks'],as_of='2025-01-01T10:18Z')
    assert report==expected
    prefix=['--config',str(cfg),'--json','laboratory']
    assert cli.main(prefix+['candle','--analysis',str(out),'--index','2'])==0
    assert json.loads(capsys.readouterr().out)['data']==sdk.laboratory_candle(report,2)
    assert cli.main(prefix+['report','--analysis',str(out),'--out',str(tmp_path/'report.html')])==0
    assert not json.loads(capsys.readouterr().out)['data']['network_required']


def test_reopen_uses_frozen_crossings_and_does_not_rerun_statistics(linked,monkeypatch):
    report=analyze(linked,timeline_basis='publication')
    frozen=service.load_report(json.dumps(report))
    expected=copy.deepcopy(frozen['crossing_result'])
    monkeypatch.setattr(service,'_rank_effect',lambda *a,**k:(_ for _ in ()).throw(AssertionError('statistics recomputed')))
    assert service.crossings(frozen)==expected
    assert service.relation_matrix(frozen)==expected['matrices'][0]
    assert service.target_matrix(frozen)==expected['price_targets'][0]
    changed=service.crossings(frozen);changed['matrices'][0]['cells'][0][0]['effect']=999
    assert frozen['crossing_result']==expected


def test_batch_of_observations_is_not_summed_into_a_candle(linked):
    db,market,source,contract=linked
    history=source['snapshot'].feature_history.copy()
    # Recria duas janelas recebidas juntas: a projeção não inventa soma nem duas amostras do candle.
    ident=contract['definitions'][0]['feature_id'];version=contract['algorithm_version']
    rows=history[(history.feature_id==ident)&history.observation_time.isin(pd.to_datetime(['2025-01-01T10:00Z','2025-01-01T10:03Z']))]
    for row in rows.itertuples():
        feature_store.save_value(db,'TOPIC:QA','M3',ident,version,row.observation_time,row.numeric_value,
            available_at='2025-01-01T10:10Z',observed_at='2025-01-01T10:10Z',source_revision='late-batch',metadata=dict(synthetic_fixture=True))
    source['snapshot']=datasets.capture(db,'TOPIC:QA','M3','2025-01-01T10:00Z','2025-01-01T10:15Z',stored_features=source['features'],feature_only=True)
    report=analyze(linked);c=channel(report,ident)
    assert c['values'][9] is None and c['per_bar_states'][9]=='MULTIPLE_OBSERVATIONS_IN_CANDLE'
    assert len([e for e in c['receipt_events'] if e['receipt_bar_index']==9])>=2


def test_analysis_refuses_implicit_source_capture(linked,monkeypatch):
    from mt5_extracao import sdk
    db,market,source,contract=linked
    monkeypatch.setattr(sdk,'capture_feature_snapshot',lambda **k:pytest.fail('analysis wrote a snapshot'))
    raw=dict(symbol='TOPIC:QA',timeframe='M3',start='2025-01-01T10:00Z',end='2025-01-01T10:15Z',
        features=source['features'],relationship='QA')
    for choices in ([raw],None,[None]):
        with pytest.raises(ValueError):
            sdk.analyze_laboratory(db,manifest_ids=[s.manifest_id for s in market],linked_selections=choices)


def test_linked_memory_budget_does_not_change_legacy_analysis_contract(linked,monkeypatch):
    from mt5_extracao.analytics import producer_jobs
    db,market,source,contract=linked
    monkeypatch.setattr(producer_jobs,'current_rss_bytes',lambda:2*1024**3)
    legacy=service.analyze(market,feature_ids=['volume.ticks'],as_of='2025-01-01T10:18Z')
    assert legacy['laboratory_version']==1
    with pytest.raises(MemoryError,match='1024'):
        analyze(linked)
