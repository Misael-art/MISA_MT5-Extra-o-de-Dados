"""Execuções reais do motor sobre preços próprios; comparação não roda backtest."""
from copy import deepcopy
import configparser
import json
from types import SimpleNamespace

import pandas as pd
import pytest

from mt5_extracao import cli,sdk
from mt5_extracao.analytics import datasets
from mt5_extracao.composition import base_composition,Composition
from mt5_extracao.laboratory import preset_assets,report
from mt5_extracao.robots import backtest,catalog,store
from mt5_extracao.strategies import ExitRules
from mt5_extracao.strategies.ema_atr import EmaAtrTrend
from mt5_extracao.strategies.specs import spec_from_symbol_info
from tests.test_mentions_contract import db
from tests.test_backtester import bars


def prepare(db,monkeypatch):
    def signals(self,frame,out):
        out.loc[299,'entry_long']=True;out.loc[319,'exit_long']=True;out['stop_long']=90.;out['stop_short']=110.
    monkeypatch.setattr(EmaAtrTrend,'_signals',signals);monkeypatch.setattr(EmaAtrTrend,'exit_rules',lambda self:ExitRules())
    data=bars([[100,101,99,100]]*330,start='2025-01-01',freq='min');data['tick_volume']=100;data['real_volume']=0
    data.loc[310,['high','low','close']]=[101,94,95];data.loc[320,['open','high','low','close']]=[103,104,102,103]
    for symbol in ('QA_X','QA_Y'):
        assert db.save_ohlcv_data(symbol,'1 minuto',data)
        db.save_symbol_spec(symbol,spec_from_symbol_info(SimpleNamespace(point=1,trade_tick_size=1,trade_tick_value=1,volume_min=.01,volume_step=.01)),source='qa.owned.spec')
    cfg=configparser.ConfigParser();cfg['STRATEGY']=dict(initial_equity='10000',account_currency='USD',commission_per_lot='.1')
    robot=catalog.load_catalog(None,db);robot=next(r for r in robot if r.key=='python:ema_atr')
    comps=[base_composition('ema_atr',symbol=s,timeframe='M1',risk_fraction=.0025) for s in ('QA_X','QA_Y')]
    changed=comps[0].to_dict();changed['strategy']['params']['fast']=21;comps.append(Composition(changed))
    runs=[backtest.run_python(db,cfg,robot,c.to_dict()['context']['symbol'],'M1',composition=c,real_spread=False) for c in comps]
    return cfg,runs


@pytest.fixture
def financial(db,monkeypatch):return db,*prepare(db,monkeypatch)


@pytest.fixture
def financial_timeframes(db):
    symbol = 'QA_TF'
    for tf, freq, size in [('1 minuto', 'min', 640), ('2 minutos', '2min', 320)]:
        frame = bars([[100, 102, 98, 101]] * size, start='2025-01-01 09:00', freq=freq)
        frame['tick_volume'] = 20 if size == 640 else 40
        frame['real_volume'] = 0
        assert db.save_ohlcv_data(symbol, tf, frame)
    db.save_symbol_spec(symbol, spec_from_symbol_info(SimpleNamespace(
        point=1, trade_tick_size=1, trade_tick_value=1, volume_min=.01, volume_step=.01)), source='qa.owned.spec')
    config = configparser.ConfigParser()
    config['STRATEGY'] = dict(initial_equity='10000', account_currency='USD', commission_per_lot='.1')
    robot = next(robot for robot in catalog.python_robots() if robot.key == 'python:ema_atr')
    runs = [backtest.run_python(db, config, robot, symbol, tf, real_spread=False,
                               composition=base_composition('ema_atr', symbol=symbol, timeframe=tf))
            for tf in ('M1', 'M2')]
    return db, config, runs


def test_same_strategy_between_timeframes_compares_frozen_window_and_exports(financial_timeframes, tmp_path, monkeypatch, capsys):
    db, _, runs = financial_timeframes
    ids = [run['run_id'] for run in runs]
    monkeypatch.setattr(backtest, 'run_python', lambda *a, **k: pytest.fail('comparison ran backtest'))
    result = sdk.compare_laboratory_runs(db, ids, mode='same_strategy')
    assert result['compatible'], result['reasons']
    left, right = result['rows']
    assert left['observed_interval'] == right['observed_interval']
    assert left['interval']['bars'] == 640 and right['interval']['bars'] == 320
    assert left['body_hash'] == right['body_hash'] and left['composition_hash'] != right['composition_hash']
    assert not result['opened_backtest'] and not result['financial_qualification']
    monkeypatch.setattr(cli.Context, 'db', property(lambda _: db))
    path = tmp_path / 'timeframes.html'
    assert cli.main(['--json', 'laboratory', 'compare-runs', '--mode', 'same_strategy', '--run', ids[0],
                     '--run', ids[1], '--html', str(path)]) == 0
    assert json.loads(capsys.readouterr().out)['data'] == result
    html = path.read_text(encoding='utf-8')
    assert 'Mesma estratégia entre tempos gráficos' in html and 'id="curve-filter"' in html


def test_same_strategy_keeps_different_parameters_and_windows_incompatible(financial_timeframes):
    db, config, runs = financial_timeframes
    base, second = runs
    changed = deepcopy(second)
    changed['run_id'] += '-params'
    changed['params']['fast'] += 1
    store.save_run(db, changed)
    result = sdk.compare_laboratory_runs(db, [base['run_id'], changed['run_id']], mode='same_strategy')
    assert not result['compatible'] and 'Estratégia ou parâmetros diferem além do timeframe' in result['reasons']

    robot = next(robot for robot in catalog.python_robots() if robot.key == 'python:ema_atr')
    truncated = backtest.run_python(db, config, robot, second['symbol'], 'M2', start='2025-01-01 09:02:00',
                                    real_spread=False, composition=Composition(second['metrics']['composition']))
    result = sdk.compare_laboratory_runs(db, [base['run_id'], truncated['run_id']], mode='same_strategy')
    assert result['configuration_compatible'] and not result['compatible']
    assert any(reason.startswith('Coberturas temporais diferentes') for reason in result['reasons'])


def test_compatible_runs_preserve_full_financial_metrics_and_clocks(financial,monkeypatch):
    db,cfg,runs=financial
    monkeypatch.setattr(backtest,'run_python',lambda *a,**k:pytest.fail('comparison ran backtest'))
    result=sdk.compare_laboratory_runs(db,[runs[0]['run_id'],runs[2]['run_id']],mode='same_asset')
    assert result['compatible'],result['reasons']
    assert not result['financial_qualification'] and not result['opened_backtest'] and not result['ranked']
    row=result['rows'][0];assert row['summary']['trades']==runs[0]['metrics']['trades']
    assert row['summary']['net_profit']==runs[0]['metrics']['net_profit']
    assert row['summary']['max_floating_drawdown_pct']>row['summary']['max_drawdown_pct']
    assert row['context']['settings']['commission_per_lot']==.1 and row['currency']=='USD'
    assert row['data_manifest_id']==runs[0]['data_manifest_id']
    assert row['curves']['floating_equity'] and row['curves']['account_balance']


def test_same_preset_between_assets_and_currency_mismatch_stays_separate(financial):
    db,_,runs=financial;ids=[r['run_id'] for r in runs[:2]]
    result=preset_assets.compare_runs(db,ids,mode='same_preset');assert result['compatible'],result['reasons']
    changed=store.load_runs(db,run_id=ids[1])[0];changed['run_id']+='-currency-fixture';changed['metrics']['currency']='EUR';changed['metrics']['comparison_context']['currency']='EUR';store.save_run(db,changed)
    result=preset_assets.compare_runs(db,[ids[0],changed['run_id']],mode='same_preset')
    assert not result['compatible'] and 'Moeda da conta diferente' in result['reasons']
    assert [r['currency'] for r in result['rows']]==['USD','EUR']


def test_cost_revision_and_missing_receipt_are_not_inferred(financial):
    db,cfg,runs=financial;ids=[r['run_id'] for r in runs[:2]]
    result=preset_assets.compare_runs(db,[ids[0],runs[2]['run_id']],mode='same_preset')
    assert 'Presets diferem além do ativo' in result['reasons']
    changed=store.load_runs(db,run_id=ids[1])[0];changed['run_id']+='-legacy-fixture';changed['metrics'].pop('comparison_context');store.save_run(db,changed)
    result=preset_assets.compare_runs(db,[ids[0],changed['run_id']],mode='same_preset')
    assert not result['compatible'] and any('recibo de motor' in r for r in result['reasons'])
    cfg['STRATEGY']['commission_per_lot']='.2';robot=next(r for r in catalog.load_catalog(None,db) if r.key=='python:ema_atr')
    cost=backtest.run_python(db,cfg,robot,'QA_X','M1',composition=Composition(runs[0]['metrics']['composition']),real_spread=False)
    result=preset_assets.compare_runs(db,[ids[0],cost['run_id']],mode='same_asset')
    assert 'Configuração de capital/risco/custos diferente' in result['reasons']
    with pytest.raises(ValueError,match='distintos'):preset_assets.compare_runs(db,[ids[0],ids[0]],mode='same_asset')


def test_frozen_export_detects_changes_and_cli_reads_same_receipt(financial,tmp_path,monkeypatch,capsys):
    db,_,runs=financial;ids=[r['run_id'] for r in runs[:2]]
    result=sdk.compare_laboratory_runs(db,ids,mode='same_preset')
    path=tmp_path/'financial.html';sdk.export_laboratory_runs(result,path)
    assert 'Patrimônio a mercado' in path.read_text(encoding='utf-8')
    changed=deepcopy(result);changed['rows'][0]['summary']['net_profit']=1e9
    with pytest.raises(ValueError,match='alterada'):report.render_runs(changed)
    monkeypatch.setattr(cli.Context,'db',property(lambda _:db))
    assert cli.main(['--json','laboratory','compare-runs','--mode','same_preset','--run',ids[0],'--run',ids[1]])==0
    assert json.loads(capsys.readouterr().out)['data']==result
    index=store.run_index(db,limit=1,offset=0);second=store.run_index(db,limit=1,offset=1)
    assert len(index)==len(second)==1 and index[0]['run_id']!=second[0]['run_id'] and 'equity' not in index[0]
