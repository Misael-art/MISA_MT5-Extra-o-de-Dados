"""T19.33b: plano persistido e motor real, sem MT5/conta do proprietário."""
import configparser
import threading
from types import SimpleNamespace
from dataclasses import replace

import pytest
from sqlalchemy import text

from mt5_extracao import sdk, cli
from mt5_extracao.analytics import datasets
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.robots import backtest, backtest_scope as scope, budget, catalog, runner, store
from tests.market_data import trend
from tests.test_s09_native_profile import robot as native_robot


@pytest.fixture
def source(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path/'scope.sqlite'))
    for tf, freq, n in [('1 minuto','min',10),('5 minutos','5min',350),('1 hora','h',350)]:
        db.save_ohlcv_data('QA',tf,trend(n,freq=freq))
    robot = next(r for r in catalog.python_robots() if r.key == 'python:ema_atr')
    config = configparser.ConfigParser()
    config.read_dict({'ROBOTS': {'batch_max_workers':'1'}})
    yield db, robot, config
    db.close()


def preview(source, **kwargs):
    db, robot, config = source
    return scope.preview(db,config,robot,'QA',start='2020-01-01',end='2020-02-01',**kwargs)


def test_preview_lists_exclusions_and_stages_without_admission_or_execution(source,monkeypatch):
    db,robot,config = source
    monkeypatch.setattr(backtest,'run_python',lambda *a,**k:pytest.fail('preview executou'))
    plan = preview(source)
    assert plan['status'] == 'pending' and plan['eligible'] == 2 and plan['excluded'] == 19
    assert [r['timeframe'] for r in plan['rows'] if r['eligible']] == ['M5','H1']
    assert 'mínimo 250' in plan['rows'][0]['reason']
    assert 'Sem série local' in next(r for r in plan['rows'] if r['timeframe']=='M15')['reason']
    saved = budget.checkpoint(db,plan['batch_id'])
    assert saved['owner'] == {} and saved['jobs'] == [] and not store.load_runs(db)
    assert len(saved['plan']['jobs']) == 2
    assert sdk.inspect_backtest_scope(db,plan['batch_id']) == plan
    unclaimed=budget.Budget(db,config,batch_id=plan['batch_id'])
    with pytest.raises(ValueError,match='Assuma'):unclaimed.admit(saved['plan']['jobs'][0]['key'])


def test_real_batch_freezes_data_and_params_and_resume_never_repeats(source,monkeypatch):
    db,robot,config = source
    robot.presets = {'Longa': {'fast':'21'}}
    calls=[];original=backtest.run_python
    def observe(*a,**kwargs):
        assert a[4] != scope.ALL
        calls.append((a[3],a[4]));return original(*a,**kwargs)
    monkeypatch.setattr(backtest,'run_python',observe)
    plan = preview(source,preset='Longa')
    changed = db.load_ohlcv('QA','1 hora');changed.loc[0,'close'] += .01
    db.save_ohlcv_data('QA','1 hora',changed)
    batch=sdk.create_backtest_scope_runner(db,config,robot,plan['batch_id']).start()
    assert batch.join(30) and not batch.error, batch.error
    assert len(batch.results)==2 and all(item['run'] for item in batch.results)
    assert calls==[('QA','M5'),('QA','H1')]
    rows=store.load_runs(db)
    assert len(rows)==2 and all(r['params']['fast']==21 for r in rows)
    refs={r['timeframe']:r['data_manifest_id'] for r in plan['rows'] if r['eligible']}
    for record in rows:
        assert record['data_manifest_id']==refs[record['timeframe']]
        receipt=record['metrics']['backtest_scope']
        assert receipt['batch_id']==plan['batch_id'] and receipt['plan_signature']==plan['signature']
        assert receipt['preset_identity']==plan['preset_identity']
    again=scope.create_runner(db,config,robot,plan['batch_id']).start()
    assert again.join(30) and not again.error
    assert len(calls)==2 and len(store.load_runs(db))==2
    assert budget.checkpoint(db,plan['batch_id'])['status']=='completed'


@pytest.mark.parametrize('change',['preset','costs','source','base'])
def test_changed_review_is_rejected_before_any_execution(source,change):
    db,robot,config=source;robot.presets={'Longa':{'fast':'21'}}
    plan=preview(source,preset='Longa')
    if change=='preset':robot.presets['Longa']['fast']='22'
    elif change=='costs':config.read_dict({'STRATEGY':{'commission_per_lot':'3'}})
    elif change=='source':robot.inputs[0]=replace(robot.inputs[0],default='22')
    else:datasets.fork(db)
    with pytest.raises(ValueError):scope.create_runner(db,config,robot,plan['batch_id'])
    assert not store.load_runs(db)


def test_budget_exhaustion_and_resume_keep_exact_results(source):
    db,robot,config=source;config['ROBOTS']['batch_max_jobs']='1'
    plan=preview(source)
    first=scope.create_runner(db,config,robot,plan['batch_id']).start()
    assert first.join(30) and len(store.load_runs(db))==1
    assert budget.checkpoint(db,plan['batch_id'])['status']=='interrupted'
    config['ROBOTS']['batch_max_jobs']='2'
    resumed=scope.create_runner(db,config,robot,plan['batch_id']).start()
    assert resumed.join(30) and not resumed.error
    assert len(store.load_runs(db))==2 and budget.checkpoint(db,plan['batch_id'])['status']=='completed'


def test_empty_and_cancelled_previews_do_not_start_jobs(source):
    db,robot,config=source
    plan=scope.preview(db,config,robot,'EMPTY',start='2020-01-01',end='2020-02-01')
    assert not plan['eligible'] and all(row['reason'] for row in plan['rows'])
    with pytest.raises(ValueError,match='Nenhuma'):scope.create_runner(db,config,robot,plan['batch_id'])
    def cancel():raise budget.Interrupted('cancelled')
    with pytest.raises(budget.Interrupted):preview(source,guard=cancel)
    assert not store.load_runs(db)


def test_source_change_during_execution_prevents_publication(source,monkeypatch):
    db,robot,config=source;robot.presets={'Longa':{'fast':'21'}}
    original=backtest.run_python
    def changed(*args,**kwargs):
        robot.presets['Longa']['fast']='22'
        return original(*args,**kwargs)
    monkeypatch.setattr(backtest,'run_python',changed)
    plan=preview(source,preset='Longa')
    batch=scope.create_runner(db,config,robot,plan['batch_id']).start()
    assert batch.join(30) and all(item['run'] is None for item in batch.results)
    assert not store.load_runs(db) and budget.checkpoint(db,plan['batch_id'])['status']=='failed'


def test_two_executors_cannot_claim_the_same_review(source,monkeypatch):
    db,robot,config=source;entered,release=threading.Event(),threading.Event();original=backtest.run_python
    def held(*args,**kwargs):
        entered.set();assert release.wait(15)
        return original(*args,**kwargs)
    monkeypatch.setattr(backtest,'run_python',held)
    plan=preview(source);first=scope.create_runner(db,config,robot,plan['batch_id']).start()
    try:
        assert entered.wait(10)
        second=scope.create_runner(db,config,robot,plan['batch_id']).start()
        assert second.join(10) and 'ativo' in second.error and not second.results
    finally:
        release.set();assert first.join(30)
    assert len(store.load_runs(db))==2 and not first.error


def test_captured_manifest_must_match_job_and_window(source):
    db,robot,config=source
    snapshot=datasets.capture(db,'QA','M5')
    control=budget.Budget(db,config)
    key=budget.job_key(robot.key,'QA','H1')
    with pytest.raises(ValueError,match='Revisão congelada'):
        control.prepare([(robot,'QA','H1')],manifest_ids={key:snapshot.manifest_id},claim=False)
    assert budget.checkpoint(db,control.batch_id) is None


def test_native_batches_reuse_private_session_and_preserve_model_and_checkpoint(source,tmp_path,monkeypatch):
    db,_,config=source
    path=tmp_path/'ScopeEA.mq5';path.write_text('#property strict\ninput int InpPeriod=20;\nvoid OnTick() {}\n',encoding='utf-8')
    robot=catalog.robot_from_file(path);robot.timeframes=('H1','M5')
    native_source=(db,robot,config)
    from mt5_extracao.robots import sessions,mt5_tools
    calls=[]
    monkeypatch.setattr(mt5_tools,'common_files_dir',lambda config:tmp_path/'owned-common')
    monkeypatch.setattr(backtest,'run_mql5',lambda *a,**k:pytest.fail('usou terminal compartilhado'))
    class Session:
        def __init__(self,db,config,robot,symbol,timeframe,**kwargs):
            self.args=(db,robot,symbol,timeframe);self.options=kwargs['native_options'];self.kwargs=kwargs
            self.trades_file='own.csv';self.exception=None;self.state='created';self.error='';self.record=None
        def run(self):
            db,robot,symbol,tf=self.args;control=self.options['control'];calls.append((tf,self.options['model']))
            self.record=dict(run_id=control.run_id,robot_key=robot.key,source='strategy tester',symbol=symbol,timeframe=tf,
                data_start=self.kwargs['start'].strftime('%Y-%m-%d %H:%M:%S'),data_end=self.kwargs['end'].strftime('%Y-%m-%d %H:%M:%S'),
                bars=0,params=self.options['params'],metrics={'source_fingerprint':catalog.fingerprint(robot)},
                equity=[],created_at='2020-02-01 00:00:00')
            store.save_run(db,self.record,control=control);self.state='completed'
    monkeypatch.setattr(sessions,'Session',Session)
    plan=preview(native_source)
    assert plan['eligible']==2 and plan['native_history_revision']=='unavailable'
    assert plan['model']=='4' and plan['native_download_notice']
    batch=scope.create_runner(db,config,robot,plan['batch_id']).start()
    assert batch.join(30) and not batch.error
    assert calls==[('M5','4'),('H1','4')] and len(store.load_runs(db))==2
    again=scope.create_runner(db,config,robot,plan['batch_id']).start()
    assert again.join(30) and not again.error and len(calls)==2


def test_cli_review_signature_and_actual_execution_share_sdk_plan(source,tmp_path,capsys,monkeypatch):
    db,robot,config=source
    cfg=tmp_path/'qa.ini';cfg.write_text(f'[DATABASE]\ntype=sqlite\npath={db.db_path}\n[ROBOTS]\nbatch_max_workers=1\n',encoding='utf-8')
    monkeypatch.chdir(tmp_path);monkeypatch.setattr(cli.Context,'db',property(lambda _:db))
    args=['--config',str(cfg),'--json','robots']
    assert cli.main(args+['backtest-plan',robot.key,'--symbol','QA','--from','2020-01-01','--to','2020-02-01'])==0
    import json
    plan=json.loads(capsys.readouterr().out)['data']['backtest_plan']
    assert plan==scope.inspect_plan(db,plan['batch_id'])
    assert cli.main(args+['backtest-plan-run',robot.key,'--batch-id',plan['batch_id'],'--confirm-plan','incorrect'])!=0
    capsys.readouterr();assert not store.load_runs(db)
    assert cli.main(args+['backtest-plan-run',robot.key,'--batch-id',plan['batch_id'],'--confirm-plan',plan['signature']])==0
    result=json.loads(capsys.readouterr().out)['data']
    assert len(result['results'])==2 and all(item['run_id'] for item in result['results'])
    assert result['backtest_plan']['status']=='completed'
    assert len(result['backtest_plan']['executions'])==2
    assert result['backtest_plan']['measurements']['sampled_rss_peak']>0


@pytest.mark.parametrize('category',['Default','Manual'])
def test_native_package_keeps_revision_hash_and_internal_tf(source,native_robot,tmp_path,category):
    from mt5_extracao.presets import defaults,manual
    from mt5_extracao.presets.compiler import TestContext
    db,_,config=source
    record=defaults.ensure_default(db,native_robot,symbol='QA',timeframe='M5')
    if category=='Manual':record=manual.save_as_manual(db,native_robot,record.identity.id,'Revisão preservada')
    exported=manual.export_preset(db,native_robot,record.identity.id,tmp_path/'package',
                                 test_context=TestContext('2020-01-01','2020-02-01',deposit=100000,model='0'))
    plan=scope.preview(db,config,native_robot,'QA',start='2020-01-01',end='2020-02-01',manifest_path=exported['manifest_path'])
    assert plan['eligible']==1 and [r['timeframe'] for r in plan['rows'] if r['eligible']]==['M5']
    assert all('Preset congelado' in r['reason'] for r in plan['rows'] if not r['eligible'])
    assert plan['preset_identity']['id']==record.identity.id and plan['preset_identity']['revision']==record.identity.revision
    assert plan['preset_identity']['category']==category
    assert plan['preset_identity']['content_hash']==exported['package'].package_hash
    assert scope.create_runner(db,config,native_robot,plan['batch_id']).jobs[0].timeframe=='M5'
    exported['set_path'].write_bytes(exported['set_path'].read_bytes()+b'\n')
    with pytest.raises(ValueError):scope.create_runner(db,config,native_robot,plan['batch_id'])
    assert not store.load_runs(db)


def test_native_package_execution_and_lost_proof_never_duplicate_tester(source,native_robot,tmp_path,monkeypatch):
    from mt5_extracao.presets import defaults,manual,execution
    from mt5_extracao.presets.compiler import TestContext,read_package
    db,_,config=source;calls=[]
    record=defaults.ensure_default(db,native_robot,symbol='QA',timeframe='M5')
    exported=manual.export_preset(db,native_robot,record.identity.id,tmp_path/'package',
        test_context=TestContext('2020-01-01','2020-02-01',deposit=100000,model='0'))
    class NativeBoundary:
        def __init__(self,db,config,robot,path,*,control):
            self.package=read_package(path,robot);self.control=control;self.evidence=None;self.error='';self.status='created'
        def run(self):
            calls.append(self.package.package_hash)
            record=dict(run_id=self.control.run_id,robot_key=native_robot.key,source='strategy tester',symbol='QA',timeframe='M5',
                data_start='2020-01-01 00:00:00',data_end='2020-02-01 00:00:00',bars=0,params={},
                metrics={'source_fingerprint':catalog.fingerprint(native_robot)},equity=[],created_at='2020-02-01 00:00:00')
            store.save_run(db,record,control=self.control)
            self.status='contract_only';self.evidence=dict(run_id=record['run_id'],execution_hash=self.package.execution_hash,
                mode='backtest',status=self.status,created_at='2020-02-01 00:00:00')
            execution._save_evidence(db,self.evidence);self.session=SimpleNamespace(record=record)
            return self
    monkeypatch.setattr(execution,'PresetExecution',NativeBoundary)
    plan=scope.preview(db,config,native_robot,'QA',start='2020-01-01',end='2020-02-01',manifest_path=exported['manifest_path'])
    first=scope.create_runner(db,config,native_robot,plan['batch_id']).start()
    assert first.join(30) and not first.error and first.results[0]['run']
    again=scope.create_runner(db,config,native_robot,plan['batch_id']).start()
    assert again.join(30) and not again.error and len(calls)==1
    with db.engine.begin() as conn:conn.execute(text('DELETE FROM _preset_native_runs'))
    refused=scope.create_runner(db,config,native_robot,plan['batch_id']).start()
    assert refused.join(30) and 'Conferência nativa pendente' in refused.error
    assert len(calls)==1 and len(store.load_runs(db))==1
