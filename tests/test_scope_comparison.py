"""Mesmo lote/revisão entre TFs; motor real em base compacta própria, leitura sem executar."""
from copy import deepcopy
import configparser
import json
from types import SimpleNamespace
from contextlib import contextmanager

import pytest
from sqlalchemy import text

from mt5_extracao import sdk,cli
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.robots import backtest_scope as scope, budget, backtest, catalog, store
from mt5_extracao.laboratory import preset_assets,report
from mt5_extracao.strategies.specs import spec_from_symbol_info
from tests.market_data import trend


def prepare(db, assets=1, *, same_window=True):
    config=configparser.ConfigParser();config.read_dict({'ROBOTS':{'batch_max_workers':'1'},'STRATEGY':{'account_currency':'USD'}})
    robot=next(r for r in catalog.python_robots() if r.key=='python:ema_atr')
    for i in range(assets):
        symbol=f'QA_{i}'
        series=[('1 minuto','min',502),('2 minutos','2min',251)] if same_window else [('5 minutos','5min',251),('1 hora','h',251)]
        for tf,freq,n in series:db.save_ohlcv_data(symbol,tf,trend(n,freq=freq))
        db.save_symbol_spec(symbol,spec_from_symbol_info(SimpleNamespace(point=.0001,trade_tick_size=.0001,trade_tick_value=1,volume_min=.01,volume_step=.01)),source='qa.owned.spec')
    plan=scope.preview(db,config,robot,symbols=[f'QA_{i}' for i in range(assets)],start='2020-01-01',end='2020-02-01')
    return config,robot,plan


@pytest.fixture(scope='module')
def completed_scope(tmp_path_factory):
    db=DatabaseManager(db_path=str(tmp_path_factory.mktemp('scope-comparison')/'own.sqlite'))
    cfg,robot,plan=prepare(db)
    batch=scope.create_runner(db,cfg,robot,plan['batch_id']).start();assert batch.join(30) and not batch.error,batch.error
    yield db,cfg,robot,plan
    db.close()


@contextmanager
def edit_run(db,ident,change):
    original=store.load_runs(db,run_id=ident)[0];changed=deepcopy(original);change(changed)
    if changed['data_manifest_id']!=original['data_manifest_id']:changed['metrics']['data_manifest_id']=changed['data_manifest_id']
    with db.engine.begin() as conn:
        raw=conn.execute(text('SELECT params_json,metrics_json FROM _robot_runs WHERE run_id=:r'),{'r':ident}).mappings().one()
        conn.execute(text('UPDATE _robot_runs SET params_json=:p,metrics_json=:m WHERE run_id=:r'),dict(r=ident,p=json.dumps(changed['params']),m=json.dumps(changed['metrics'])))
    try:yield
    finally:
        with db.engine.begin() as conn:conn.execute(text('UPDATE _robot_runs SET params_json=:p,metrics_json=:m WHERE run_id=:r'),dict(r=ident,p=raw['params_json'],m=raw['metrics_json']))


def test_exact_ids_zero_and_undefined_are_distinct_and_opening_never_runs(completed_scope,monkeypatch):
    db,cfg,robot,plan=completed_scope
    monkeypatch.setattr(backtest,'run_python',lambda *a,**k:pytest.fail('comparison executed'))
    before=budget.checkpoint(db,plan['batch_id']);result=sdk.compare_backtest_scope(db,plan['batch_id'])
    assert result['compatible'],result['reasons']
    assert result['counts']=={'completed':2,'excluded':19}
    assert [c['timeframe'] for c in result['cells']][:5]==['M1','M2','M3','M4','M5']
    assert result['preset_identity']==plan['preset_identity'] and result['coverage']['overlaps']
    assert not result['opened_backtest'] and not result['ranked'] and not result['financial_qualification']
    assert budget.checkpoint(db,plan['batch_id'])==before
    for row in result['rows']:
        saved=store.load_runs(db,run_id=row['run_id'])[0]
        assert row['data_manifest_id']==saved['data_manifest_id'] and row['effective_params']==saved['params']
        assert row['summary']['net_profit']==saved['metrics']['net_profit']
        assert row['summary']['trades']==0 and row['summary']['profit_factor'] is None
        assert row['profit_factor_status']=='no_trades' and row['profit_factor_note']


@pytest.mark.parametrize('change',[
    lambda r:r['metrics']['backtest_scope']['preset_identity'].update(revision=999),
    lambda r:r['metrics']['backtest_scope'].update(plan_signature='another'),
    lambda r:r['params'].update(fast=7),
    lambda r:r['params'].update(fast=20.0),
    lambda r:r.update(data_manifest_id='another'),
])
def test_altered_identity_parameters_and_dataset_are_separate_cells(completed_scope,change):
    db,_,_,plan=completed_scope;ident=scope.inspect_plan(db,plan['batch_id'])['executions'][0]['run_id']
    with edit_run(db,ident,change):
        value=sdk.compare_backtest_scope(db,plan['batch_id'])
        assert value['counts']['inconsistent']==1 and value['counts']['completed']==1
        cell=next(c for c in value['cells'] if c['run_id']==ident);assert cell['result'] is None and cell['reason']


def test_later_unrelated_run_is_never_selected(completed_scope):
    db,_,_,plan=completed_scope;before=sdk.compare_backtest_scope(db,plan['batch_id'])
    unrelated=deepcopy(store.load_runs(db,run_id=before['run_ids'][0])[0]);unrelated['run_id']='unrelated-later';unrelated['created_at']='2099-01-01';unrelated['params']['fast']=3;store.save_run(db,unrelated)
    after=sdk.compare_backtest_scope(db,plan['batch_id']);assert before==after and 'unrelated-later' not in after['run_ids']


def test_negative_values_cost_mismatch_and_integrity_are_preserved(completed_scope,tmp_path,monkeypatch,capsys):
    db,_,_,plan=completed_scope;ident=sdk.compare_backtest_scope(db,plan['batch_id'])['run_ids'][0]
    def change(r):r['metrics']['net_profit']=-7;r['metrics']['return_pct']=-.00007;r['metrics']['comparison_context']['settings']['commission_per_lot']=99
    with edit_run(db,ident,change):
        value=sdk.compare_backtest_scope(db,plan['batch_id'])
        assert not value['compatible'] and 'Configuração de capital/risco/custos diferente' in value['reasons']
        assert next(r for r in value['rows'] if r['run_id']==ident)['summary']['net_profit']==-7
        path=tmp_path/'scope.html';sdk.export_laboratory_runs(value,path);assert 'Matriz ativo × TF' in path.read_text()
        altered=deepcopy(value);altered['cells'][0]['state']='failed'
        with pytest.raises(ValueError,match='alterada'):report.render_runs(altered)
    monkeypatch.setattr(cli.Context,'db',property(lambda _:db))
    assert cli.main(['--json','robots','backtest-plan-compare','--batch-id',plan['batch_id']])==0
    assert json.loads(capsys.readouterr().out)['data']['comparison']==sdk.compare_backtest_scope(db,plan['batch_id'])


def test_pending_interruption_resume_and_failed_attempt_stay_distinct(tmp_path):
    db=DatabaseManager(db_path=str(tmp_path/'own.sqlite'))
    try:
        cfg,robot,plan=prepare(db);batch_id=plan['batch_id']
        pending=sdk.compare_backtest_scope(db,batch_id);assert pending['counts']['pending']==2 and not pending['rows']
        cfg['ROBOTS']['batch_max_jobs']='1';batch=scope.create_runner(db,cfg,robot,batch_id).start();assert batch.join(30)
        partial=sdk.compare_backtest_scope(db,batch_id);assert partial['counts']['completed']==1 and partial['counts']['pending']==1
        kept=partial['run_ids'];cfg['ROBOTS']['batch_max_jobs']='2'
        batch=scope.create_runner(db,cfg,robot,batch_id).start();assert batch.join(30) and not batch.error
        final=sdk.compare_backtest_scope(db,batch_id);assert final['counts']['completed']==2 and set(kept)<=set(final['run_ids'])
        with db.engine.begin() as conn:conn.execute(text("UPDATE _robot_batch_jobs SET status='failed' WHERE run_id=:r"),{'r':kept[0]})
        failed=sdk.compare_backtest_scope(db,batch_id);assert failed['counts']['failed']==1 and kept[0] not in failed['run_ids']
    finally:db.close()


def test_missing_published_result_and_broken_snapshot_binding_are_explicit(completed_scope):
    db,_,_,plan=completed_scope;value=sdk.compare_backtest_scope(db,plan['batch_id']);ident=value['run_ids'][0]
    with db.engine.begin() as conn:conn.execute(text("UPDATE _robot_batch_jobs SET run_id='missing-qa-result' WHERE run_id=:r"),{'r':ident})
    try:
        missing=sdk.compare_backtest_scope(db,plan['batch_id']);assert missing['counts']['missing_result']==1 and not missing['compatible']
    finally:
        with db.engine.begin() as conn:conn.execute(text("UPDATE _robot_batch_jobs SET run_id=:r WHERE run_id='missing-qa-result'"),{'r':ident})
    with db.engine.begin() as conn:
        manifest=conn.execute(text('SELECT manifest_id FROM _dataset_references WHERE run_id=:r'),{'r':ident}).scalar_one()
        conn.execute(text("UPDATE _dataset_references SET manifest_id='missing-qa-snapshot' WHERE run_id=:r"),{'r':ident})
    try:
        broken=sdk.compare_backtest_scope(db,plan['batch_id']);assert not broken['compatible'] and any('referência congelada' in r.lower() for r in broken['reasons'])
    finally:
        with db.engine.begin() as conn:conn.execute(text('UPDATE _dataset_references SET manifest_id=:m WHERE run_id=:r'),{'m':manifest,'r':ident})


def test_cancellation_guard_keeps_existing_journal(completed_scope):
    db,_,_,plan=completed_scope;before=budget.checkpoint(db,plan['batch_id']);calls=[]
    def guard():
        calls.append(True)
        if len(calls)==4:raise ValueError('own QA cancellation')
    with pytest.raises(ValueError,match='QA cancellation'):sdk.compare_backtest_scope(db,plan['batch_id'],guard=guard)
    assert budget.checkpoint(db,plan['batch_id'])==before


def test_economic_specification_change_is_not_hidden_by_bar_audit_normalization(completed_scope):
    db,_,_,plan=completed_scope;ident=sdk.compare_backtest_scope(db,plan['batch_id'])['run_ids'][0]
    def change(r):r['metrics']['comparison_context']['instrument_spec']['tick_value']=99
    with edit_run(db,ident,change):
        value=sdk.compare_backtest_scope(db,plan['batch_id'])
        assert not value['configuration_compatible'] and not value['compatible']
        assert 'Especificação do instrumento difere entre TFs do mesmo ativo' in value['reasons']


def test_same_settings_with_different_actual_windows_are_not_called_comparable(tmp_path):
    db=DatabaseManager(db_path=str(tmp_path/'own.sqlite'))
    try:
        cfg,robot,plan=prepare(db,same_window=False)
        batch=scope.create_runner(db,cfg,robot,plan['batch_id']).start();assert batch.join(30) and not batch.error
        value=sdk.compare_backtest_scope(db,plan['batch_id'])
        assert value['configuration_compatible'] and not value['compatible']
        assert any('Coberturas temporais diferentes' in r for r in value['reasons'])
        assert value['coverage']['overlaps'] and not value['coverage']['covers_requested_window']
    finally:db.close()


@pytest.mark.parametrize('assets',[3,6])
def test_multi_asset_scope_reads_all_cells_in_one_frozen_revision(tmp_path,assets):
    db=DatabaseManager(db_path=str(tmp_path/'own.sqlite'))
    try:
        cfg,robot,plan=prepare(db,assets);batch=scope.create_runner(db,cfg,robot,plan['batch_id']).start();assert batch.join(45) and not batch.error
        value=sdk.compare_backtest_scope(db,plan['batch_id']);assert value['counts']['completed']==assets*2
        assert len(value['cells'])==assets*21 and len({r['run_id'] for r in value['rows']})==assets*2
    finally:db.close()
