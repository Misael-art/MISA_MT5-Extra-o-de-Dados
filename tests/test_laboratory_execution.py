"""Eventos observados do motor, conciliados com snapshot/ledger canônicos."""
from copy import deepcopy
import json
import pandas as pd
import pytest

from mt5_extracao import sdk,cli
from mt5_extracao.analytics import datasets
from mt5_extracao.laboratory import service,execution,report as portable
from mt5_extracao.robots import backtest
from tests.test_laboratory_financial import financial
from tests.test_mentions_contract import db


def analyze(db,run,cut='2026-10-29T00:00Z'):
    return service.analyze([datasets.load(db,run['data_manifest_id'])],feature_ids=['price.return_1','volume.ticks'],as_of=cut)


def test_execution_link_uses_observations_not_intentions_and_preserves_capital(financial,monkeypatch):
    db,_,runs=financial;r=analyze(db,runs[0])
    monkeypatch.setattr(backtest,'run_python',lambda *a,**k:pytest.fail('reader ran strategy'))
    value=sdk.link_laboratory_execution(db,r,runs[0]['run_id']);context=sdk.laboratory_candle(r,300,execution=value)
    observed=context['execution'];assert observed['events'] and any(e['fill_price']==100 for e in observed['events'])
    assert all(e['initial_stop_price']==90 and e['initial_target_price'] is None for e in observed['events'])
    assert observed['observation_basis']=='modeled_execution_order' and not observed['historical_receipt_proven']
    finish=sdk.laboratory_candle(r,320,execution=value)['execution']
    assert finish['capital']['account_balance']==10007 and finish['capital']['floating_equity']==10007
    assert any(e['net_pnl']==7 for e in finish['events'])
    assert value==execution.validate(r,value)
    html=portable.render(r,execution=value);assert 'execution-events' in html
    altered=deepcopy(value);altered['capital_by_index'][320]['floating_equity']=1e9
    with pytest.raises(ValueError,match='alterado'):portable.render(r,execution=altered)


def test_wrong_asset_snapshot_absent_receipts_and_earlier_cut_do_not_invent_execution(financial,monkeypatch):
    db,_,runs=financial;r=analyze(db,runs[0])
    with pytest.raises(ValueError,match='mesmo ativo'):execution.link(db,r,runs[1]['run_id'])
    early=analyze(db,runs[0],cut='2025-01-01T04:00Z');receipt=execution.link(db,early,runs[0]['run_id'])
    assert not any(receipt['events_by_index']) and receipt['outside_or_after_cut']>0
    monkeypatch.setattr(execution.trajectories,'load',lambda *a:None)
    with pytest.raises(ValueError,match='não serão inferidos'):execution.link(db,r,runs[0]['run_id'])


def test_cli_freezes_and_reads_same_execution_receipt(financial,tmp_path,monkeypatch,capsys):
    db,_,runs=financial;r=analyze(db,runs[0]);source=tmp_path/'analysis.json';source.write_text(json.dumps(r),encoding='utf-8')
    monkeypatch.setattr(cli.Context,'db',property(lambda _:db));out=tmp_path/'execution.json'
    prefix=['--json','laboratory']
    assert cli.main(prefix+['execution-link','--analysis',str(source),'--run',runs[0]['run_id'],'--out',str(out)])==0
    capsys.readouterr();receipt=json.loads(out.read_text(encoding='utf-8'))
    assert receipt==sdk.link_laboratory_execution(db,r,runs[0]['run_id'])
    assert cli.main(prefix+['candle','--analysis',str(source),'--index','300','--execution',str(out)])==0
    assert json.loads(capsys.readouterr().out)['data']==sdk.laboratory_candle(r,300,execution=receipt)
