"""Consent real e lifecycle por SDK, com processo/posição substituídos."""
import configparser
from pathlib import Path
import threading
import time
from types import SimpleNamespace
import uuid

import pytest

from mt5_extracao.automation.sessions import SessionController
from mt5_extracao.robots import controls, sessions
from tests.test_robot_controls import robot


class FakeSession:
    def __init__(self,db,config,robot,symbol,timeframe,preset=None,**kw):
        self.id=uuid.uuid4().hex;self.robot=robot;self.symbol=symbol;self.timeframe=timeframe;self.preset=preset
        self.mode=kw["mode"];self.visible=kw.get("visible",True);self.state="idle";self.error=""
        self.kw=kw;self.config=config;self.record=None;self.report=Path("report");self.window_monitor=None
        self.done=threading.Event();self.instrumented=True
    @property
    def active(self):return self.state in ("preparing","running")
    def run(self,**kw):
        self.state="running";self.done.wait(4);self.state="cancelled"
    def cancel(self):self.done.set()
    def telemetry(self):return "waiting",0,0


@pytest.fixture
def controller(robot,monkeypatch):
    account=dict(login=123,server="Demo",demo=True,balance=10000,currency="USD")
    c=SessionController(None,configparser.ConfigParser(),lambda:dict(account))
    monkeypatch.setattr(c,"_robot",lambda key:robot)
    monkeypatch.setattr(controls,"play_issues",lambda *args:[])
    monkeypatch.setattr(sessions,"Session",FakeSession)
    monkeypatch.setattr(c,"_magic",lambda robot,preset:"42")
    yield c,account
    c.close()


def review(c):return c.review(dict(robot="any",symbol="EURUSD",timeframe="H1",preset="A"))
def open_play(c,r,**overrides):return c.open(dict(mode="play",review=r["review"],accept_review=True,confirm_account=123,accept_negative=True,**overrides))


def test_play_requires_both_confirms_negative_and_binds_current_context(controller):
    c,account=controller;r=review(c)
    assert r["can_start"] and r["loss_scenario"]
    for params in (dict(mode="play",review=r["review"]),dict(mode="play",review=r["review"],accept_review=True,confirm_account=123)):
        with pytest.raises(ValueError):c.open(params)
    for field,value in [("login",124),("server","Changed"),("demo",False)]:
        previous=account[field];account[field]=value
        with pytest.raises(ValueError):open_play(c,r)
        account[field]=previous
    started=open_play(c,r)
    assert c.job(started["session"]).kw["chart_control"] is True
    assert c.wait(dict(session=started["session"],timeout=1))["state"]=="running"
    with pytest.raises(ValueError,match="ausente"):open_play(c,r)
    with pytest.raises(ValueError,match="Magic"):open_play(c,review(c))
    stopped=c.stop(dict(session=started["session"]))
    assert "não liquida" in stopped["stop_semantics"]
    assert c.wait(dict(session=started["session"],until="finished",timeout=1))["state"]=="cancelled"


def test_changed_source_or_expired_review_refuses_before_start(controller,robot):
    c,_=controller;r=review(c)
    Path(robot.path).write_text(Path(robot.path).read_text()+"// changed")
    with pytest.raises(ValueError,match="mudou"):open_play(c,r)
    r=review(c);c.reviews[r["review"]]["consent"].issued_at-=181
    with pytest.raises(ValueError,match="expirou"):open_play(c,r)
    assert not c.jobs


def test_multiple_histories_and_close_only_own_sessions(controller):
    c,_=controller
    ids=[]
    for _ in range(2):
        row=c.open(dict(mode="replay",robot="any",symbol="EURUSD",timeframe="M5",start="2026-10-01",end="2026-10-02"))
        ids.append(row["session"])
    assert len(set(ids))==2
    with pytest.raises(ValueError,match="pertence"):c.stop(dict(session="foreign"))
    with pytest.raises(ValueError,match="sempre visível"):
        c.open(dict(mode="replay",visible=False))
    assert len(c.close()["sessions"])==2 and all(not c.job(x).active for x in ids)


def test_native_profiles_cannot_bypass_package_or_play_gates(controller,robot):
    c,_=controller;robot.key="mql5:hdk_stochema9"
    from mt5_extracao.presets import NATIVE_PRESET_KEYS
    robot.key=next(iter(NATIVE_PRESET_KEYS))
    with pytest.raises(ValueError,match="PLAY"):review(c)
    with pytest.raises(ValueError,match="pacote"):
        c.open(dict(mode="replay",robot=robot.key,symbol="EURUSD",timeframe="M5",start="2026-10-01",end="2026-10-02"))
    assert not c.jobs
