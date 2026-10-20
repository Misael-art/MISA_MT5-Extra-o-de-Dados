"""Launcher finished before native viewer: release own profile after imported report."""
import configparser
import json
from pathlib import Path
import pytest
from mt5_extracao.robots import sessions,mt5_tools,backtest
from tests.test_s09_native_profile import robot

@pytest.mark.parametrize("mode,stops",[("backtest",0),("replay",1)])
def test_completed_report_closes_own_replay_before_release_but_keeps_result(robot,tmp_path,monkeypatch,mode,stops):
    cfg=configparser.ConfigParser();cfg.read_dict({"MT5":{"path":str(tmp_path/"main"),"wine_prefix":str(tmp_path/"wine")}})
    job=sessions.Session(None,cfg,robot,"EURUSD","M5",mode=mode,root=tmp_path/"sessions",native_options={})
    events=[];record={"run_id":"preserved","metrics":{"trades":2},"trades":[["native",1,1]]}
    def prepare(**kwargs):
        job.directory.mkdir(parents=True)
        job.child_config=sessions._private_config(cfg,job.directory)
        return job.directory/"wrapper.mq5"
    def native(**kwargs):events.append("imported");return record
    def close():
        assert job.record is record
        events.append("close-own")
    def release(session):events.append("released");return True
    monkeypatch.setattr(job,"_prepare_expert",prepare)
    monkeypatch.setattr(job,"_stop_owned",close)
    monkeypatch.setattr(backtest,"run_mql5",native)
    monkeypatch.setattr(mt5_tools,"release_native_context",release)
    job.run()
    assert job.state=="completed" and job.record is record
    assert events==["imported"]+(["close-own"] if stops else [])+["released"]
    report=json.loads(job.report.read_text())
    assert report["resources_released"] and report["run"]==record


def test_failed_replay_keeps_failure_and_uses_existing_owned_cleanup(robot,tmp_path,monkeypatch):
    cfg=configparser.ConfigParser();cfg.read_dict({"MT5":{"path":str(tmp_path/"main"),"wine_prefix":str(tmp_path/"wine")}})
    job=sessions.Session(None,cfg,robot,"EURUSD","M5",mode="replay",root=tmp_path/"sessions",native_options={})
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
