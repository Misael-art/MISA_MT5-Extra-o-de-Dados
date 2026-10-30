"""Observações próprias por transporte injetado; relógio local, revisão e categoria."""
from datetime import datetime,timezone
import json

import pandas as pd
import pytest

from mt5_extracao import sdk,cli
from mt5_extracao.analytics import macro_sources,feature_store,datasets
from tests.test_mentions_contract import db
from tests.test_mention_sources import Response


@pytest.fixture
def response(monkeypatch):
    payload=[{'data':'02/01/2025','valor':'0'},{'data':'03/01/2025','valor':'0.1'}]
    state={'payload':payload,'status':200,'raw':None}
    monkeypatch.setattr(macro_sources.requests,'get',lambda *a,**k:Response(state['payload'],status=state['status'],raw=state['raw']))
    return state


def collect(db,**kw):
    return sdk.collect_macro_observations(db,source_id='bcb.sgs.11',start='2025-01-02',end='2025-01-04',**kw)


def test_macro_zero_is_valid_and_receipt_is_not_backdated(db,response):
    identity=datasets.identity(db);started=pd.Timestamp.now(tz='UTC');r=collect(db)
    assert r['observations']==2 and r['counts']==dict(inserted=2,revised=0,unchanged=0)
    history=feature_store.query_history(db);assert list(history.numeric_value)==[0.,.1]
    assert history.available_at_utc.min()>=started and history.numeric_value.notna().all()
    assert feature_store.query_as_of(db,'2025-01-04T00:00Z').empty
    assert datasets.identity(db)==identity
    assert collect(db)['counts']==dict(inserted=0,revised=0,unchanged=2)


def test_macro_revision_preserves_old_cut_and_unrelated_observation(db,response):
    r=collect(db);cut=pd.Timestamp.now(tz='UTC');before=feature_store.query_as_of(db,cut)
    response['payload'][0]['valor']='0.2'
    assert collect(db)['counts']==dict(inserted=0,revised=1,unchanged=1)
    pd.testing.assert_frame_equal(feature_store.query_as_of(db,cut),before)


def test_cycle_is_a_cited_category_and_snapshot_preserves_it(db,response):
    response['raw']=b'observation_date,USREC\n2020-01-01,0\n2020-02-01,1\n2020-03-01,.\n'
    r=sdk.collect_macro_observations(db,source_id='fred.usrec',start='2020-01-01',end='2020-04-01')
    h=feature_store.query_history(db);assert h.numeric_value.isna().all()
    assert list(h.categorical_value[:2])==['expansion','recession'] and h.quality.iloc[-1]=='UNAVAILABLE'
    snap=datasets.capture(db,r['symbol'],'D1','2020-01-01T00:00Z','2020-04-01T00:00Z',stored_features=r['features'],feature_only=True)
    definition=snap.feature_definitions[0]
    assert definition['value_type']=='CATEGORY' and 'Citation required' in definition['parameters']['attribution']
    assert json.loads(snap.feature_history.metadata_json.iloc[0])['historical_publication_available_at'] is None


@pytest.mark.parametrize('raw',[b'{"error":"qa"}',b'not json',b'[{"data":"02/01/2025","valor":"nan"}]',b'[{"data":"02/01/2025","valor":"0"},{"data":"02/01/2025","valor":"0"}]'])
def test_invalid_macro_response_is_not_zero(db,response,raw):
    response['raw']=raw
    with pytest.raises(ValueError):collect(db)
    assert feature_store.query_history(db).empty


def test_macro_access_failure_and_cancellation_do_not_publish(db,response):
    response['status']=403
    with pytest.raises(RuntimeError,match='HTTP403'):collect(db)
    from mt5_extracao.analytics.indicator_materializer import Cancelled
    with pytest.raises(Cancelled):collect(db,cancel_check=lambda:True)
    assert feature_store.query_history(db).empty


def test_macro_network_timeout_has_actionable_failure_and_no_facts(db,monkeypatch):
    def timeout(*a,**k):raise macro_sources.requests.Timeout('controlled QA timeout')
    monkeypatch.setattr(macro_sources.requests,'get',timeout)
    with pytest.raises(RuntimeError,match='retome explicitamente'):collect(db)
    assert feature_store.query_history(db).empty


def test_cli_and_sdk_share_macro_contract_and_passive_catalog(db,response,tmp_path,capsys):
    cfg=tmp_path/'qa.ini';cfg.write_text(f'[DATABASE]\ntype=sqlite\npath={db.db_path}\n[APP]\nbroker_utc_offset=0\n',encoding='utf-8')
    prefix=['--config',str(cfg),'--json','analytics','features']
    assert cli.main(prefix+['macro-sources'])==0
    assert json.loads(capsys.readouterr().out)['data']['sources']==sdk.macro_source_capabilities()
    assert cli.main(prefix+['macro-collect','--source','bcb.sgs.11','--start','2025-01-02','--end','2025-01-04'])==0
    r=json.loads(capsys.readouterr().out)['data']['capture'];assert r['observations']==2
    assert collect(db)['features']==r['features']
