"""T20.25b: referências no journal e consumidores reais da limpeza, em dados próprios."""
import json
import os
from pathlib import Path
import subprocess
import sys
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import inspect, text

from mt5_extracao.resources import actions, files, storage
from mt5_extracao.robots import budget, backtest_scope as scope, sessions, mt5_tools
from tests.test_backtest_scope import empty_source, native_robot
from tests.market_data import trend


def configured(source, tmp_path, monkeypatch):
    db, _, config = source
    config['RESOURCES'] = dict(archive_dir=str(tmp_path/'maintenance'), exports_keep_days='1', logs_keep_days='1')
    root = tmp_path/'exports'; root.mkdir()
    monkeypatch.setitem(storage.EXTRA_DIRS, 'Exportações', lambda *a:root)
    monkeypatch.setitem(storage.EXTRA_DIRS, 'Logs', lambda *a:tmp_path/'logs')
    return db, config, root


def aged(path):
    path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(b'own QA receipt')
    stamp = (datetime.now()-timedelta(days=30)).timestamp(); os.utime(path,(stamp,stamp))
    return path


def test_pending_package_and_parameters_survive_real_cleanup_and_relative_cwd(empty_source, native_robot, tmp_path, monkeypatch):
    from mt5_extracao.presets import defaults, manual
    from mt5_extracao.presets.compiler import TestContext
    db, config, root = configured(empty_source, tmp_path, monkeypatch)
    db.save_ohlcv_data('QA', '5 minutos', trend(251, freq='5min'))
    record = defaults.ensure_default(db, native_robot, symbol='QA', timeframe='M5')
    exported = manual.export_preset(db,native_robot,record.identity.id,root/'reviewed',
        test_context=TestContext('2020-01-01','2020-02-01',deposit=100000,model='0'))
    monkeypatch.chdir(tmp_path)
    package = Path(exported['manifest_path'])
    plan = scope.preview(db,config,native_robot,'QA',start='2020-01-01',end='2020-02-01',
        manifest_path=package.relative_to(tmp_path))
    assert Path(plan['manifest_path']).is_absolute() and plan['status']=='pending'
    original = {p:p.read_bytes() for p in package.parent.rglob('*') if p.is_file()}
    old = (datetime.now()-timedelta(days=30)).timestamp()
    for path in original:os.utime(path,(old,old))
    orphan = aged(root/'unreferenced.csv')
    elsewhere=tmp_path/'elsewhere';elsewhere.mkdir();monkeypatch.chdir(elsewhere)
    assert [Path(v['path']) for v in files.old_files(db,config)]==[orphan]
    result=files.clean_files(db,config)
    assert result['ok'] and result['removed']==1 and not orphan.exists()
    assert all(path.read_bytes()==value for path,value in original.items())
    assert plan['retained_artifacts'][0]['basis']=='frozen_package'
    batch=scope.create_runner(db,config,native_robot,plan['batch_id'])
    assert [(job.symbol,job.timeframe) for job in batch.jobs]==[('QA','M5')]
    assert budget.checkpoint(db,plan['batch_id'])['owner']=={}  # nenhum Tester


def admitted(source):
    db,robot,config=source
    db.save_ohlcv_data('QA','5 minutos',trend(251,freq='5min'))
    parent=budget.Budget(db,config);entries=parent.prepare([(robot,'QA','M5')])
    return parent,parent.admit(entries[0]['key'])


def closed_session(db, root, name):
    directory=root/name;directory.mkdir(parents=True)
    (directory/'relatorio.json').write_text(json.dumps(dict(artifact_owner='mt5x-session',artifact_version=1,
        owner_store=files.session_owner_key(db),session=name,state='failed',resources_released=True,
        finished_at='2020-02-01T00:00:00',wine_prefix='')),encoding='utf-8')
    (directory/'receipt.txt').write_bytes(b'own native boundary receipt')
    return directory


def test_failure_links_preserved_and_unreferenced_profile_quarantine_is_reversible(empty_source,tmp_path,monkeypatch):
    db,config,_=configured(empty_source,tmp_path,monkeypatch)
    root=tmp_path/'profiles';monkeypatch.setattr(sessions,'session_root',lambda _:root)
    monkeypatch.setattr(mt5_tools,'_native_context_processes',lambda _:[])
    parent,control=admitted(empty_source)
    referenced=closed_session(db,root,'a'*32);free=closed_session(db,root,'b'*32)
    control.retain_artifacts(directories=[str(referenced)])
    control.retain_artifacts(files=[str(referenced/'receipt.txt')])
    saved=budget.checkpoint(db,parent.batch_id)
    assert len(saved['jobs'][0]['history'])==1 and parent._admitted==1
    control.result('failed','controlled failure');parent.finish(failed=True)
    assert budget.retained_artifacts(db,parent.batch_id)[0]['directories']==[str(referenced)]
    items={v['session']:v for v in files.list_sessions(db,config)}
    assert not items['a'*32]['eligible'] and 'lote' in items['a'*32]['reason']
    assert items['b'*32]['eligible']
    assert not files.move_session(db,config,'a'*32)['ok']
    before={p.name:p.read_bytes() for p in free.iterdir()}
    assert files.move_session(db,config,'b'*32)['ok']
    assert files.move_session(db,config,'b'*32,restore=True)['ok']
    assert {p.name:p.read_bytes() for p in free.iterdir()}==before
    with pytest.raises(ValueError):control.retain_artifacts(files=[str(free/'late.txt')])


def test_session_links_directory_and_uuid_channels_before_any_copy(empty_source,native_robot,tmp_path,monkeypatch):
    db,config,_=configured(empty_source,tmp_path,monkeypatch)
    parent=budget.Budget(db,config);entry=parent.prepare([(native_robot,'QA','M5')])[0];control=parent.admit(entry['key'])
    common=tmp_path/'common';monkeypatch.setattr(mt5_tools,'common_files_dir',lambda _:common)
    session=sessions.Session(db,config,native_robot,'QA','M5',root=tmp_path/'profiles',native_options={'control':control})
    assert not session.directory.exists()
    refs=budget.retained_artifacts(db,parent.batch_id)
    assert refs[0]['directories']==[str(session.directory)]
    assert set(refs[0]['files'])=={str(common/name) for name in
        (session.telemetry_file,session.trades_file,session.input_receipt_file,session.lease_file)}
    parent.cancel();parent.finish()


@pytest.mark.parametrize('malformed',['plan','history','relative'])
def test_bad_journal_refuses_age_cleanup_without_deleting_candidate(empty_source,tmp_path,monkeypatch,malformed):
    db,config,root=configured(empty_source,tmp_path,monkeypatch);parent,control=admitted(empty_source)
    receipt=aged(root/'receipt.txt');control.retain_artifacts(files=[str(receipt)])
    with db.engine.begin() as conn:
        if malformed=='plan':conn.execute(text('UPDATE _robot_batch_queue SET plan_json=:p'),{'p':'broken'})
        elif malformed=='history':conn.execute(text('UPDATE _robot_batch_jobs SET history_json=:p'),{'p':'null'})
        else:
            history=budget.checkpoint(db,parent.batch_id)['jobs'][0]['history']
            history[0]['artifacts']['files']=['relative.txt']
            conn.execute(text('UPDATE _robot_batch_jobs SET history_json=:p'),{'p':json.dumps(history)})
    with pytest.raises(ValueError,match='lotes'):files.clean_files(db,config)
    assert receipt.read_bytes()==b'own QA receipt'


def test_cleanup_lease_refuses_new_reference_without_partial_attachment(empty_source,tmp_path,monkeypatch):
    db,config,root=configured(empty_source,tmp_path,monkeypatch);parent,control=admitted(empty_source)
    with actions.exclusive(tmp_path/'maintenance','controlled cleanup'):
        with pytest.raises(actions.Busy):control.retain_artifacts(files=[str(root/'future.txt')])
    assert budget.retained_artifacts(db,parent.batch_id)==[]
    control.result('failed','no files created');parent.finish(failed=True)


def test_reading_empty_retention_projection_creates_no_journal(empty_source):
    db,_,_=empty_source
    assert budget.retained_artifacts(db)==[]
    assert not inspect(db.engine).has_table('_robot_batch_queue')


def test_resource_kernel_lease_survives_age_and_recovers_after_own_process_exit(tmp_path,monkeypatch):
    from mt5_extracao.bootstrap import paths
    state=tmp_path/'own-state';monkeypatch.setattr(paths,'state_dir',lambda:state/'mt5-extracao')
    folder=tmp_path/'archive'
    code="from pathlib import Path;from mt5_extracao.resources.actions import exclusive;import sys,time\nwith exclusive(Path(sys.argv[1]),'own QA child'):\n print('locked',flush=True)\n time.sleep(40)\n"
    environment=dict(os.environ,XDG_STATE_HOME=str(state),LOCALAPPDATA=str(state),APPDATA=str(state))
    child=subprocess.Popen([sys.executable,'-c',code,str(folder)],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,env=environment)
    try:
        import threading
        output=[];reader=threading.Thread(target=lambda:output.append(child.stdout.readline()),daemon=True);reader.start();reader.join(15)
        assert output==['locked\n'] and child.poll() is None
        path=folder/actions.LOCK_NAME;stamp=(datetime.now()-timedelta(days=100)).timestamp();os.utime(path,(stamp,stamp))
        with pytest.raises(actions.Busy):
            with actions.exclusive(folder):pytest.fail('stole live lease')
        with pytest.raises(actions.Busy):
            with actions.exclusive(tmp_path/'different-config'):pytest.fail('configuration bypassed lease')
    finally:
        child.terminate();child.communicate(timeout=10)
    with actions.exclusive(folder,'recovered own crashed child'):
        assert json.loads(path.read_text(encoding='utf-8'))['state']=='active'
    assert path.exists() and json.loads(path.read_text(encoding='utf-8'))['state']=='released'


def test_legacy_unproven_lock_is_not_removed_for_age(tmp_path):
    folder=tmp_path/'archive';folder.mkdir();path=folder/actions.LOCK_NAME;path.write_text('unknown old owner')
    stamp=(datetime.now()-timedelta(days=100)).timestamp();os.utime(path,(stamp,stamp))
    with pytest.raises(actions.Busy):
        with actions.exclusive(folder):pytest.fail('unproven lock removed')
    assert path.read_text()=='unknown old owner'


def test_windows_kernel_adapter_locks_exact_byte_and_unlocks(tmp_path,monkeypatch):
    calls=[]
    monkeypatch.setitem(sys.modules,'msvcrt',SimpleNamespace(LK_NBLCK=7,LK_UNLCK=8,
        locking=lambda fd,mode,count:calls.append((mode,count))))
    monkeypatch.setattr(actions,'os',SimpleNamespace(name='nt'))
    with (tmp_path/'lock').open('w+b') as handle:
        actions._lock_resource_file(handle);actions._lock_resource_file(handle,release=True)
    assert calls==[(7,1),(8,1)]


def test_bridge_control_files_and_resource_marker_are_not_age_candidates(empty_source,tmp_path,monkeypatch):
    db,config,root=configured(empty_source,tmp_path,monkeypatch)
    logs=tmp_path/'logs';names=('mt5_bridge.log','mt5_bridge.pid','mt5_bridge_process.json','mt5_bridge.lock')
    kept=[aged(logs/name) for name in names];garbage=aged(logs/'unreferenced-old.log')
    config['RESOURCES']['archive_dir']=str(root/'maintenance')
    with actions.exclusive(root/'maintenance'):
        marker=root/'maintenance'/actions.LOCK_NAME
        stamp=(datetime.now()-timedelta(days=30)).timestamp();os.utime(marker,(stamp,stamp))
        assert [Path(v['path']) for v in files.old_files(db,config)]==[garbage]
    assert files.clean_files(db,config)['removed']==1
    assert all(p.read_bytes()==b'own QA receipt' for p in kept) and marker.is_file()


def test_foreign_active_owner_cannot_be_reclaimed_from_metadata_only(tmp_path):
    folder=tmp_path/'archive';folder.mkdir();path=folder/actions.LOCK_NAME
    raw=json.dumps(dict(resource_lock_version=2,state='active',identity={'host':'another-QA-host'},action='foreign QA'))
    path.write_text(raw)
    with pytest.raises(actions.Busy):
        with actions.exclusive(folder):pytest.fail('foreign owner reclaimed')
    assert path.read_text()==raw


def test_package_changed_during_capture_is_not_published_as_reviewed(empty_source,native_robot,tmp_path,monkeypatch):
    from mt5_extracao.presets import defaults,manual
    from mt5_extracao.presets.compiler import TestContext
    from mt5_extracao.analytics import datasets
    db,config,root=configured(empty_source,tmp_path,monkeypatch)
    db.save_ohlcv_data('QA','5 minutos',trend(251,freq='5min'))
    record=defaults.ensure_default(db,native_robot,symbol='QA',timeframe='M5')
    exported=manual.export_preset(db,native_robot,record.identity.id,root/'reviewed',
        test_context=TestContext('2020-01-01','2020-02-01',deposit=100000,model='0'))
    original=datasets.capture
    def change(*args,**kwargs):
        snapshot=original(*args,**kwargs)
        exported['set_path'].write_bytes(exported['set_path'].read_bytes()+b'\n')
        return snapshot
    monkeypatch.setattr(datasets,'capture',change)
    with pytest.raises(ValueError):
        scope.preview(db,config,native_robot,'QA',start='2020-01-01',end='2020-02-01',manifest_path=exported['manifest_path'])
    assert not inspect(db.engine).has_table('_robot_batch_queue')


def test_plan_staging_waits_for_same_resource_lease_without_publishing_partial_plan(empty_source,tmp_path,monkeypatch):
    db,config,_=configured(empty_source,tmp_path,monkeypatch);robot=empty_source[1]
    db.save_ohlcv_data('QA','5 minutos',trend(251,freq='5min'))
    with actions.exclusive(tmp_path/'maintenance','controlled cleanup'):
        with pytest.raises(actions.Busy):scope.preview(db,config,robot,'QA',start='2020-01-01',end='2020-02-01')
    assert not inspect(db.engine).has_table('_robot_batch_queue')
