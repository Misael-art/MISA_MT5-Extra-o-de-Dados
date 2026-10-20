"""Historical result imported: release own children before the next Tester."""
import configparser
import json
from pathlib import Path
import pytest
from mt5_extracao.robots import sessions,mt5_tools,backtest
from tests.test_s09_native_profile import robot

@pytest.mark.parametrize("mode",["backtest","replay"])
@pytest.mark.parametrize("native_options",[None,{}],ids=["card","preset"])
def test_completed_report_closes_own_children_before_release_but_keeps_result(robot,tmp_path,monkeypatch,mode,native_options):
    cfg=configparser.ConfigParser();cfg.read_dict({"MT5":{"path":str(tmp_path/"main"),"wine_prefix":str(tmp_path/"wine")}})
    job=sessions.Session(None,cfg,robot,"EURUSD","M5",mode=mode,root=tmp_path/"sessions",native_options=native_options)
    events=[];record={"run_id":"preserved","metrics":{"trades":2},"trades":[["native",1,1]]}
    alive=[]
    def prepare(**kwargs):
        job.directory.mkdir(parents=True)
        job.child_config=sessions._private_config(cfg,job.directory)
        mt5_tools.register_native_context(job)
        return job.directory/"wrapper.mq5"
    def native(*args,**kwargs):
        alive.append(object());events.append("imported");return record
    def close():
        assert job.record is record
        alive.clear();events.append("close-own")
    original_release=mt5_tools.release_native_context
    def release(session):
        events.append("released");return original_release(session)
    monkeypatch.setattr(job,"_prepare_expert",prepare)
    monkeypatch.setattr(job,"_stop_owned",close)
    monkeypatch.setattr(backtest,"run_mql5",native)
    monkeypatch.setattr(mt5_tools,"common_files_dir",lambda cfg:tmp_path/"common")
    monkeypatch.setattr(mt5_tools,"_native_context_processes",lambda context:list(alive))
    monkeypatch.setattr(mt5_tools,"release_native_context",release)
    job.run()
    assert job.state=="completed" and job.record is record
    assert events==["imported","close-own","released"]
    report=json.loads(job.report.read_text())
    assert report["resources_released"] and report["run"]==record
    assert not (mt5_tools.paths.state_dir()/"native-active.json").exists()
    with mt5_tools.tester_lease():
        assert not alive


@pytest.mark.parametrize("mode",["backtest","replay"])
def test_failed_history_keeps_failure_and_uses_existing_owned_cleanup(robot,tmp_path,monkeypatch,mode):
    cfg=configparser.ConfigParser();cfg.read_dict({"MT5":{"path":str(tmp_path/"main"),"wine_prefix":str(tmp_path/"wine")}})
    job=sessions.Session(None,cfg,robot,"EURUSD","M5",mode=mode,root=tmp_path/"sessions",native_options={})
    stopped=[]
    def prepare(**kwargs):
        job.directory.mkdir(parents=True);job.child_config=sessions._private_config(cfg,job.directory)
        return job.directory/"wrapper.mq5"
    def native(**kwargs):raise RuntimeError("relatório ausente")
    monkeypatch.setattr(job,"_prepare_expert",prepare)
    monkeypatch.setattr(job,"_stop_owned",lambda:stopped.append(1))
    monkeypatch.setattr(backtest,"run_mql5",native)
    monkeypatch.setattr(mt5_tools,"release_native_context",lambda s:True)
    job.run()
    assert job.state=="failed" and job.error=="relatório ausente" and job.record is None and stopped==[1]


@pytest.mark.parametrize("mode",["backtest","replay"])
def test_surviving_child_keeps_owner_and_blocks_next_tester_without_losing_result(robot,tmp_path,monkeypatch,mode):
    cfg=configparser.ConfigParser();cfg.read_dict({"MT5":{"path":str(tmp_path/"main"),"wine_prefix":str(tmp_path/"wine")}})
    job=sessions.Session(None,cfg,robot,"EURUSD","M5",mode=mode,root=tmp_path/"sessions",native_options={})
    record={"run_id":"preserved","metrics":{"trades":2}}
    alive=[];stopped=[]
    def prepare(**kwargs):
        job.directory.mkdir(parents=True);job.child_config=sessions._private_config(cfg,job.directory)
        mt5_tools.register_native_context(job)
        return job.directory/"wrapper.mq5"
    def native(**kwargs):alive.append(object());return record
    monkeypatch.setattr(job,"_prepare_expert",prepare)
    monkeypatch.setattr(job,"_stop_owned",lambda:stopped.append(1))
    monkeypatch.setattr(backtest,"run_mql5",native)
    monkeypatch.setattr(mt5_tools,"_native_context_processes",lambda context:list(alive))
    job.run()
    report=json.loads(job.report.read_text())
    marker=mt5_tools.paths.state_dir()/"native-active.json"
    assert stopped==[1] and job.state=="completed" and job.record is record
    assert report["resources_released"] is False and report["run"]==record
    assert json.loads(marker.read_text())["session"]==job.id
    with pytest.raises(RuntimeError,match="anterior ainda"):
        with mt5_tools.tester_lease():
            pytest.fail("Filho ativo não pode liberar o próximo Tester")
    assert marker.is_file()
    alive.clear()
    with mt5_tools.tester_lease():
        assert not marker.exists()
