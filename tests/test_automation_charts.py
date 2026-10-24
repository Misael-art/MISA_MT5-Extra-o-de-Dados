"""Canal de apresentação simulado: ownership, timeout, ACK e captura próprios."""
import json
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from mt5_extracao.automation import charts
from mt5_extracao.robots import mt5_tools, sessions
from tests.test_robot_controls import robot


@pytest.mark.parametrize("method,params", [
 ("panel.set",dict(field="buy",value=True)), ("panel.set",dict(field="theme",value=True)),
 ("panel.set",dict(field="timer",value="true")), ("indicator.add",dict(kind="shell")),
 ("indicator.add",dict(kind="sma",period=1)), ("indicator.add",dict(kind="sma",mean=1)), ("chart.open",dict(symbol="EURUSD",timeframe="M7")),
 ("chart.style",dict(style="dark",extra=True)), ("chart.snapshot",dict(width=0)),
 ("chart.list",dict(chart=123)), ("chart.list",dict(chart="９")),
 ("chart.list",dict(chart="9223372036854775808")), ("chart.open",dict(symbol="X\tY",timeframe="M5"))])
def test_invalid_graphics_do_not_write_requests(method,params):
    with pytest.raises(ValueError):charts.validate(method,params)


def test_valid_types_and_no_silent_ignored_parameters():
    row=charts.validate("indicator.add",dict(kind="super_atr_10",period=20,mean=0,deviation=1.5))
    assert row[0]=="indicator.add" and row[3:6]==["20","0","1.5"] and len(row)==8
    assert charts.validate("panel.set",dict(field="timer",value=False))[3]=="0"


@pytest.fixture
def channel(tmp_path,monkeypatch):
    root=tmp_path/"common";root.mkdir()
    monkeypatch.setattr(mt5_tools,"common_files_dir",lambda config:root)
    job=SimpleNamespace(config={},id="a"*32,directory=tmp_path/"session",state="running",instrumented=True)
    job.directory.mkdir()
    return charts.Channel(job),root


def respond(channel,root,payload):
    request=root/("mt5x_control_"+channel.session.id+".request.tsv")
    deadline=time.monotonic()+3
    while not request.exists() and time.monotonic()<deadline:time.sleep(.005)
    assert request.exists()
    cells=request.read_text().strip("\n").split("\t")
    assert len(cells)==9
    data=payload(cells) if callable(payload) else payload
    if isinstance(data,dict):data=dict(id=cells[0],ok=True,error="",**data)
    request.unlink()
    (root/("mt5x_control_"+channel.session.id+".response.json")).write_text(json.dumps(data))


def test_success_exact_tsv_and_snapshot_ack(channel):
    ch,root=channel
    def snapshot(cells):
        assert cells[1:5]==["chart.snapshot","0","1280","800"]
        p=ch.session.directory/"MQL5/Files"/("mt5xa_"+cells[0]+".png");p.parent.mkdir(parents=True)
        p.write_bytes(b"\x89PNG\r\n\x1a\nrest")
        return dict(data=dict(file=p.name))
    thread=threading.Thread(target=respond,args=(ch,root,snapshot));thread.start()
    data=ch.request("chart.snapshot",{},timeout=2);thread.join()
    assert Path(data["path"]).is_file()
    assert ch.receipt()["state"]=="responded"


def test_timeout_blocks_new_effect_and_late_ack_can_be_read_without_resend(channel):
    ch,root=channel
    with pytest.raises(RuntimeError,match="incerto"):ch.request("chart.list",{},timeout=.1)
    request=root/("mt5x_control_"+ch.session.id+".request.tsv");before=request.read_bytes()
    with pytest.raises(RuntimeError,match="anterior"):ch.request("chart.list",{},timeout=.1)
    assert request.read_bytes()==before and ch.receipt()["state"]=="unconfirmed"
    respond(ch,root,dict(data=dict(charts=[])))
    assert ch.receipt()["receipt"]["data"]==dict(charts=[]) and not ch.unconfirmed


@pytest.mark.parametrize("payload", [[], dict(data=[]), dict(data={},ok="true")])
def test_malformed_ack_is_uncertain_and_cannot_trigger_retry(channel,payload):
    ch,root=channel
    def response(cells):
        if isinstance(payload,dict):
            return dict(id=cells[0],error="",**payload)
        return payload
    # Escrever aqui evita que o helper normalize intencionalmente o ACK malformado.
    def native():
        req=root/("mt5x_control_"+ch.session.id+".request.tsv")
        while not req.exists():time.sleep(.005)
        cells=req.read_text().strip("\n").split("\t");req.unlink()
        (root/("mt5x_control_"+ch.session.id+".response.json")).write_text(json.dumps(response(cells)))
    t=threading.Thread(target=native);t.start()
    with pytest.raises((ValueError,RuntimeError)):ch.request("chart.list",{},timeout=2)
    t.join();assert ch.unconfirmed


def test_capture_outside_sandbox_never_accepted(channel):
    ch,root=channel
    t=threading.Thread(target=respond,args=(ch,root,dict(data=dict(file="../../other.png"))));t.start()
    with pytest.raises(RuntimeError,match="diverge"):ch.request("chart.snapshot",{},timeout=2)
    t.join();assert ch.unconfirmed


def test_control_opt_in_preserves_original_events_timer_and_source(robot):
    old=sessions.telemetry_source(robot,"state.txt")
    assert old==sessions.telemetry_source(robot,"state.txt",chart_control=None)
    assert "MT5X_AgentPoll" not in old
    new=sessions.telemetry_source(robot,"state.txt",chart_control="a"*32)
    assert "MT5X_AgentPoll();" in new and "void OnTick()" in new
    assert "#define OnTick MT5X_OnTick" in new
    assert "OrderSend" not in charts.source("a"*32)
    assert "#define MT5X_AGENT_SUPER_ATR" not in new
    with pytest.raises(ValueError):charts.source("../bad")


def test_native_channel_is_packaged_and_keeps_ambiguous_ownership_closed():
    root=Path(__file__).resolve().parents[1]
    assert '"automation/*.mqh"' in (root/"pyproject.toml").read_text()
    body=charts.source("a"*32,super_atr=True)
    assert "#define MT5X_AGENT_SUPER_ATR" in body and "@SESSION@" not in body
    assert "IndicatorRelease(" not in body
    assert "matches!=1 || found<0" in body
    assert "chart==ChartID()" in body and "MT5X_AgentOwn(chart)" in body


def test_ready_requires_actual_callback_marker_and_running_instrumented_session(channel):
    ch,root=channel
    assert ch.state()["ready"] is False
    marker=root/("mt5x_control_"+ch.session.id+".ready.json")
    marker.write_text(json.dumps(dict(session=ch.session.id,chart="123",ready=True)))
    assert ch.state()["ready"] is True
    ch.session.state="preparing"
    assert ch.state()["ready"] is False
    ch.session.state="running";ch.session.instrumented=False
    assert ch.state()["ready"] is False


def test_progress_receipt_does_not_repeat_pending_effect(channel):
    ch,root=channel
    with pytest.raises(RuntimeError):ch.request("indicator.add",dict(kind="sma"),timeout=.1)
    path=root/("mt5x_control_"+ch.session.id+".progress.json")
    path.write_text(json.dumps(dict(id=ch.last_id,data=dict(stage="indicator_calculation",calculated=-1))))
    assert ch.receipt()["progress"]["calculated"]==-1 and ch.unconfirmed
