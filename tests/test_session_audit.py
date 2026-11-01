"""Auditoria de sessão (Tester/DEMO) com dados sintéticos de novembro, fora da janela histórica."""
from datetime import datetime, timezone
import json
import csv
import io
from pathlib import Path
import pytest

from mt5_extracao.robots import session_audit


def bar(stamp):
    return str(int(datetime.strptime(stamp, "%Y.%m.%d %H:%M").replace(tzinfo=timezone.utc).timestamp()) - 300)


def decision(stamp, side=1, eligible=1, orders=1):
    return f"{stamp}:00   SAREF_DECISION decision={stamp} side={side} close=1.1 atr=0.0002 stop=1.09 run=150 eligible={eligible} orders={orders}"


def signal(stamp, side=1, opened=1, order=11, deal=21, reason=""):
    return f"{stamp}:00   HDK_STRATEGY_SIGNAL event_at={bar(stamp)} direction={side} opened={opened} execution_state={4 if opened else 1} order={order} deal={deal} reason={reason}"


def row(position_id, entry_deal, entry_order, symbol="EURUSD", reason="tp"):
    return dict(position_id=str(position_id), entry_deal=str(entry_deal), entry_order=str(entry_order), exit_deal="99",
                time="2026.11.03 12:00:00", entry_time="2026.11.03 10:00:01", exit_reason=reason, result="10", symbol=symbol)


def audit(lines, rows, magic=540121):
    text = "\n".join(lines + [f"2026.11.03 10:00:00   HDK#{magic} INF ok"]) + "\n"
    return session_audit.audit_session(text, rows, session_id="qa-novembro", symbol="EURUSD", magic=magic)


def test_executed_chain_links_by_deal_even_when_ticket_differs_from_position():
    report = audit([decision("2026.11.03 10:00"), signal("2026.11.03 10:00", order=11, deal=21)], [row(777, 21, 11)])
    chain = report["chains"][0]
    assert chain["state"] == "executed" and chain["position_id"] == "777" and chain["order_matches_position"] is False
    assert report["interrupt"] == [] and report["identity"]["interval"]["first_decision"] == "2026.11.03 10:00"


def test_refused_not_attempted_blocked_and_ineligible_stay_visible():
    lines = [decision("2026.11.03 10:00"), signal("2026.11.03 10:00", opened=0, order=0, deal=0, reason="limite"),
             decision("2026.11.03 10:30"),
             "2026.11.03 10:30:00   HDK#540121 AVS VENDA recusada: risco da ordem maior que o que resta hoje",
             decision("2026.11.03 10:30", side=-1).replace("close=1.1", "close=1.2"),
             decision("2026.11.03 11:00", eligible=0),
             "2026.11.03 11:05:00   SAREF_BLOCKED decision=2026.11.03 11:05 reason=bars_since_seed_above_20000"]
    report = audit(lines, [])
    states = {(c["decision"], c["side"]): c["state"] for c in report["chains"]}
    assert states[("2026.11.03 10:00", "1")] == "refused"
    assert states[("2026.11.03 10:30", "1")] == "ambiguous"
    assert states[("2026.11.03 10:30", "-1")] == "ambiguous"                     # conflito de decisões no mesmo instante
    assert states[("2026.11.03 11:00", "1")] == "not_armed_or_ineligible"
    assert report["blocked"] == [dict(decision="2026.11.03 11:05", reason="bars_since_seed_above_20000")]


def test_orphan_duplicate_and_foreign_identity_trigger_interruption():
    lines = [decision("2026.11.03 10:00"), signal("2026.11.03 10:00", deal=21)]
    report = audit(lines, [row(1, 21, 11), row(2, 21, 11), row(3, 55, 33, symbol="GBPUSD")])
    assert report["chains"][0]["state"] == "duplicate_position"
    assert report["orphans"] and report["orphans"][0]["position_id"] == "3"
    assert len(report["interrupt"]) == 3
    foreign = session_audit.audit_session(decision("2026.11.03 10:00") + "\n2026.11.03 10:00:00   HDK#999999 INF x\n", [],
                                          session_id="s", symbol="EURUSD", magic=540121)
    assert foreign["identity"]["foreign_magics"] == ["999999"] and foreign["interrupt"]


def test_open_entry_without_exported_close_is_not_called_executed():
    report = audit([decision("2026.11.03 10:00"), signal("2026.11.03 10:00", deal=21)], [])
    assert report["chains"][0]["state"] == "open_or_unexported"


def test_copies_between_log_sources_do_not_duplicate_attempts():
    block = [decision("2026.11.03 10:00"), signal("2026.11.03 10:00", deal=21)]
    text = "\n".join(["===== Tester/Agent/logs/a.log =====", *block, "===== Tester/logs/b.log =====", *block,
                      "2026.11.03 10:00:00   HDK#540121 INF ok"]) + "\n"
    report = session_audit.audit_session(text, [row(777, 21, 11)], session_id="s", symbol="EURUSD", magic=540121)
    assert report["chains"][0]["state"] == "executed" and not report["interrupt"]


def deal(ticket=21,order=11,pid=777,entry=0,magic=540121,volume=.1,side=0):
    return dict(ticket=ticket,order=order,position_id=pid,entry=entry,magic=magic,symbol="EURUSD",volume=volume,type=side)


def test_raw_deals_link_partial_and_manual_exit_without_ticket_equality():
    lines="\n".join([decision("2026.11.03 10:00"),signal("2026.11.03 10:00")])
    ds=[deal(),deal(ticket=22,entry=1,magic=0,volume=.04,side=1)]
    kw=dict(session_id="s",symbol="EURUSD",magic=540121,deals=ds,positions=[],orders=[])
    r=session_audit.audit_session(lines,[],**kw)
    assert r["chains"][0]["state"]=="open_position" and r["interrupt"]
    ds.append(deal(ticket=23,entry=1,magic=0,volume=.06,side=1))
    r=session_audit.audit_session(lines,[],**kw)
    assert r["chains"][0]["state"]=="executed" and len(r["chains"][0]["exits"])==2
    assert not r["interrupt"] and r["final_state"]=="flat_snapshot"


def test_unmatched_attempt_and_orphan_deal_survive_and_foreign_exposure_is_preserved():
    r=session_audit.audit_session(signal("2026.11.03 10:00"),[],session_id="s",symbol="EURUSD",magic=540121,
        deals=[deal(),deal(ticket=100,pid=999,magic=123)],positions=[dict(symbol="EURUSD",magic=123,identifier=999)],
        orders=[dict(symbol="EURUSD",magic=540121,ticket=88)])
    assert len(r["unmatched_signals"])==len(r["unmatched_deals"])==len(r["foreign_deals"])==1
    assert r["final_state"]=="exposed" and not r["open_positions"]


def test_duplicate_decision_does_not_reuse_single_attempt_to_confirm():
    line=decision("2026.11.03 10:00")
    r=audit([line,line,signal("2026.11.03 10:00")],[row(777,21,11)])
    assert [c["state"] for c in r["chains"]]==["ambiguous","ambiguous"]


def test_conflicting_order_in_history_does_not_confirm():
    lines="\n".join([decision("2026.11.03 10:00"),signal("2026.11.03 10:00")])
    r=session_audit.audit_session(lines,[],session_id="s",symbol="EURUSD",magic=540121,deals=[deal(order=999)])
    assert r["chains"][0]["state"]=="ambiguous"


def test_missing_snapshot_is_unknown_not_flat():
    assert audit([],[])["final_state"]=="unknown"


@pytest.fixture
def november_bundle(tmp_path):
    sid="a"*32
    source=tmp_path/"Synthetic.mq5"
    source.write_text("input long InpMagicNumber=540121;\ninput int Period=20;\nint OnInit(){return INIT_SUCCEEDED;}\nvoid OnTick(){}\n")
    preset=tmp_path/"session.set";preset.write_text("InpMagicNumber=540121\nPeriod=21\n")
    wrapper=tmp_path/"mt5x_session.mq5"
    wrapper.write_text('#include "Synthetic.mq5"\n// mt5x_session_'+sid+'_inputs.csv\n// ACCOUNT_LOGIN)==42 ACCOUNT_TRADE_MODE_DEMO AccountInfoString(ACCOUNT_SERVER)=="Synthetic-Demo"\n')
    binary=tmp_path/"mt5x_session.ex5";binary.write_bytes(b"synthetic binary fixture - not native qualification")
    inputs=tmp_path/"inputs.csv"
    stream=io.StringIO();csv.writer(stream,delimiter=";").writerows([
        ["mt5x-native-inputs","1",sid,"EURUSD","PERIOD_M5","0","0","1793692800","2"],
        ["input","InpMagicNumber","int","540121"],["input","Period","int","21"],["end","2"]])
    inputs.write_bytes(stream.getvalue().encode("utf-16"))
    log=tmp_path/"session.log";log.write_text("\n".join([
        f"MT5X_NATIVE_MODE session={sid} tester=0 visual=0 demo=1",
        decision("2026.11.03 10:00"),signal("2026.11.03 10:00")]))
    snapshot=tmp_path/"snapshot.json";snapshot.write_text(json.dumps(dict(session_id=sid,symbol="EURUSD",magic="540121",
        account=dict(login=42,server="Synthetic-Demo",demo=True),observed_at="2026-11-03T13:01:00Z",
        history_interval=dict(start="2026-11-03T12:00:00Z",end="2026-11-03T13:01:00Z"),
        deals=[deal(),deal(ticket=22,entry=1,magic=0,side=1)],positions=[],orders=[])))
    return dict(session_id=sid,symbol="EURUSD",magic=540121,timeframe="M5",logs=[log],preset=preset,
        source=source,wrapper=wrapper,binary=binary,inputs=inputs,fields=dict(InpMagicNumber="int",Period="int"),snapshot=snapshot)


def test_full_november_file_audit_checks_effective_inputs_and_identity(november_bundle):
    r=session_audit.audit_files(**november_bundle)
    assert not r["interrupt"] and r["identity"]["inputs"]["preset_verified"]
    assert r["identity"]["interval"]["first_decision"].startswith("2026.11.")
    assert r["chains"][0]["position_id"]=="777" and r["chains"][0]["order"]=="11"
    assert r["final_state"]=="flat_snapshot" and r["operational_acceptance"] is False
    assert all(r["identity"][k]["sha256"] for k in ("source","binary","wrapper","preset","snapshot"))


@pytest.mark.parametrize("change",["session","preset","source","log","account"])
def test_file_audit_refuses_identity_mismatch(november_bundle,change):
    b=november_bundle
    if change=="session":
        data=json.loads(b["snapshot"].read_text());data["session_id"]="foreign";b["snapshot"].write_text(json.dumps(data))
    elif change=="preset":b["preset"].write_text("InpMagicNumber=540121\nPeriod=22\n")
    elif change=="source":b["wrapper"].write_text("// different source")
    elif change=="log":b["logs"][0].write_text(decision("2026.11.03 10:00"))
    elif change=="account":
        data=json.loads(b["snapshot"].read_text());data["account"]["demo"]=False;b["snapshot"].write_text(json.dumps(data))
    with pytest.raises(ValueError):session_audit.audit_files(**b)


def test_capture_does_not_turn_failed_history_into_empty_portfolio(monkeypatch):
    from types import SimpleNamespace
    from mt5_extracao.robots import controls
    monkeypatch.setattr(controls,"account_identity",lambda _:dict(demo=True))
    backend=SimpleNamespace(positions_get=lambda:[],orders_get=lambda:[],history_deals_get=lambda *_:None)
    with pytest.raises(ValueError,match="deals"):
        session_audit.capture_snapshot(backend,session_id="s",symbol="EURUSD",magic=540121,
            start=datetime(2026,11,3,tzinfo=timezone.utc),end=datetime(2026,11,4,tzinfo=timezone.utc))


@pytest.mark.parametrize("state",[0,1,2,3,5,6])
def test_accepted_partial_uncertain_are_not_called_refused_or_confirmed(state):
    text="\n".join([decision("2026.11.03 10:00"),signal("2026.11.03 10:00").replace("execution_state=4",f"execution_state={state}")])
    r=session_audit.audit_session(text,[],session_id="s",symbol="EURUSD",magic=540121)
    assert r["chains"][0]["state"]=="ambiguous" and r["interrupt"]


def test_live_refusal_without_broker_date_stays_unmatched_instead_of_assuming_clock():
    line="0 10:00:00.000 SAREF (EURUSD,M5) HDK#540121 AVS COMPRA recusada: sem cotação atual"
    r=audit([line],[])
    assert r["unmatched_refusals"][0]["reason"]=="sem cotação atual"
    assert r["unmatched_refusals"][0]["minute"]==""
