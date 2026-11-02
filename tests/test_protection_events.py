"""Contratos E14 com diário/fills sintéticos, nunca aceite de broker."""
from datetime import datetime, timezone
import math

import pytest

from mt5_extracao.presets import protection_events as events
from mt5_extracao.presets.execution import first_difference
from tests.test_native_journal import CONTEXT, POSITION, fixture, read


@pytest.fixture
def prepared(tmp_path):
    journal=read(fixture());journal.update(reconciled=True,final_at=2000)
    inputs=dict(InpTrailingMode=1,InpTrailingPoints=2,InpTrailingTriggerR=.25,InpBreakevenTriggerR=1)
    signals=[dict(opened=True,deal_ticket=78,order_ticket=77,direction=1,filled_volume=2)]
    deals=[dict(entry=0,position=POSITION,deal=78,at_msc=1000000,price=100,volume=2),
           dict(entry=1,position=POSITION,deal=79,at_msc=1900000,price=140,volume=2)]
    ledger=[dict(entry_time=datetime.fromtimestamp(1000,timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),direction=1,entry_price=100,stop_price=90,target_price=140)]
    path=tmp_path/"Tester/Agent-QA/logs/test.log";path.parent.mkdir(parents=True)
    path.write_text(f"HDK_PROTECTION position={POSITION} at=1200 intent=trailing-{POSITION}-1100 direction=1 bar_at=1100 sampled=1 executable=104 point=1 tick=0.25 minimum=0.5 atr=unavailable initial_entry=100 initial_risk=10 initial_volume=2 before=90 requested=102 acknowledged=1 observed=1 tp_before=140 tp=140\n")
    return tmp_path,path,inputs,journal,signals,deals,ledger


def proof(case,*,strict=False):
    root,_,inputs,journal,signals,deals,ledger=case
    return events.read_events(root,inputs,journal,signals,deals,ledger,require_initial_protection=strict)


def test_preset_decision_request_and_effect_have_one_binding(prepared):
    rows=proof(prepared)
    assert len(rows) == 1 and rows[0]["requested"] == 102 and rows[0]["tp"] == 140
    assert rows[0]["request_id"] == 55 and rows[0]["effect"] == 3
    assert rows[0]["initial_risk"] == 10 and rows[0]["position"] > 2**53
    semantic=events.semantic(rows)
    assert "position" not in semantic[0] and "intent" not in semantic[0]
    reference=dict(ledger=[],signals=[],protection_events=rows)
    assert first_difference(reference,[],[],protection=rows) is None
    changed=[dict(rows[0],requested=103)]
    assert first_difference(reference,[],[],protection=changed)["section"] == "protection_events"


@pytest.mark.parametrize("fault",["pending","unreconciled","different_position","initial_risk","loosen","tp","unobserved","unacknowledged","repeat","intent","time","unconfigured","changed_journal","unknown_field"])
def test_logs_alone_cannot_approve_management(prepared,fault):
    _,path,inputs,journal,_,_,_=prepared;text=path.read_text()
    if fault=="pending":journal["all_settled"]=False
    elif fault=="unreconciled":journal["reconciled"]=False
    elif fault=="different_position":text=text.replace(f"position={POSITION}","position=99")
    elif fault=="initial_risk":text=text.replace("initial_risk=10","initial_risk=2")
    elif fault=="loosen":text=text.replace("requested=102","requested=89")
    elif fault=="tp":text=text.replace("tp=140","tp=150")
    elif fault=="unobserved":text=text.replace("observed=1","observed=0")
    elif fault=="unacknowledged":text=text.replace("acknowledged=1","acknowledged=0")
    elif fault=="repeat":text+=text
    elif fault=="intent":text=text.replace("intent=trailing-","intent=other-")
    elif fault=="time":text=text.replace("at=1200","at=1201")
    elif fault=="unconfigured":inputs["InpTrailingMode"]=0
    elif fault=="changed_journal":journal["actions"][0]["stop"]=103
    elif fault=="unknown_field":text=text.rstrip()+" extra=1\n"
    path.write_text(text)
    with pytest.raises(ValueError):proof(prepared)


def test_an_unobserved_journal_action_cannot_be_treated_as_no_opportunity(prepared):
    prepared[1].write_text("")
    with pytest.raises(ValueError,match="Ação financeira"):proof(prepared)


def close_case(case):
    _,_,_,journal,_,deals,_=case
    journal["actions"]=[dict(journal["actions"][0],kind=1,effect=1,at=1500,response_at=1500,before=2,requested=2,
                             stop=0,tp=0,confirmed=2,remaining=0,response_deal=79,
                             intent_hash=events._close_intent(CONTEXT,POSITION))]
    deals[1]["at_msc"]=1500000


@pytest.mark.parametrize("kind",["target","signal","session"])
def test_dynamic_signal_and_session_close_require_the_same_durable_action_and_fill(prepared,kind):
    close_case(prepared);_,path,inputs,_,_,_,_=prepared
    if kind=="target":
        inputs.update(InpTargetMode=2,InpDynamicTargetMinR=2,InpDynamicTargetMaxR=4,InpDynamicTargetATR=3)
        line=f"HDK_TARGET position={POSITION} at=1500 direction=1 initial_entry=100 initial_risk=10 closed_atr=8 target=124 executable=130 acknowledged=1"
    elif kind=="signal":
        inputs["InpExitOnOpposite"]=True
        line=f"HDK_EXIT kind=signal position={POSITION} event_at=1400 at=1500 direction=-1 acknowledged=1"
    else:
        inputs.update(InpExitOnSessionEnd=True,InpSessionEnabled=True,InpSessionStartMinute=0,InpSessionEndMinute=1,
                      InpSessionAnchorMinute=0,InpSessionWeekdays=127,InpSessionExcludeStartMinute=0,InpSessionExcludeEndMinute=0)
        line=f"HDK_EXIT kind=session position={POSITION} at=1500 decision=outside_window acknowledged=1"
    path.write_text(line+"\n");rows=proof(prepared)
    assert rows[0]["confirmed"] == 2 and rows[0]["remaining"] == 0
    assert rows[0]["kind"] == ("target" if kind=="target" else "exit")
    path.write_text(line.replace("acknowledged=1","acknowledged=0")+"\n")
    with pytest.raises(ValueError):proof(prepared)


@pytest.mark.parametrize("fault",["wrong_target","not_crossed","no_atr","unconfigured","no_fill","wrong_fill","pending_effect","duplicate_exit"])
def test_dynamic_target_is_never_a_fill_proof_by_itself(prepared,fault):
    close_case(prepared);_,path,inputs,journal,_,deals,_=prepared
    inputs.update(InpTargetMode=2,InpDynamicTargetMinR=2,InpDynamicTargetMaxR=4,InpDynamicTargetATR=3)
    line=f"HDK_TARGET position={POSITION} at=1500 direction=1 initial_entry=100 initial_risk=10 closed_atr=8 target=124 executable=130 acknowledged=1"
    if fault=="wrong_target":line=line.replace("target=124","target=123")
    elif fault=="not_crossed":line=line.replace("executable=130","executable=123")
    elif fault=="no_atr":line=line.replace("closed_atr=8","closed_atr=0")
    elif fault=="unconfigured":inputs["InpTargetMode"]=0
    elif fault=="no_fill":deals.pop()
    elif fault=="wrong_fill":deals[1]["volume"]=1
    elif fault=="pending_effect":journal["actions"][0]["effect"]=0
    elif fault=="duplicate_exit":line+="\n"+line
    path.write_text(line+"\n")
    with pytest.raises(ValueError):proof(prepared)


def test_no_opportunity_is_reported_per_mode_not_as_combination_acceptance():
    inputs=dict(InpTrailingMode=2,InpBreakevenTriggerR=1,InpTargetMode=2,InpExitOnOpposite=True,InpExitOnSessionEnd=True)
    result=events.acceptance([],inputs,active=True,replay_match=True)
    assert all(row["configured"] and not row["exercised"] and not row["replay_qualified"] for row in result.values())


def test_sell_trailing_uses_the_original_r_and_preserves_physical_target(prepared):
    _,path,_,journal,signals,_,ledger=prepared
    journal.clear();journal.update(read(fixture(-1)),reconciled=True,final_at=2000)
    signals[0]["direction"]=-1;ledger[0].update(direction=-1,stop_price=110,target_price=60)
    text=path.read_text().replace("direction=1","direction=-1").replace("executable=104","executable=96").replace("before=90","before=110").replace("requested=102","requested=98").replace("tp_before=140 tp=140","tp_before=60 tp=60")
    path.write_text(text)
    row=proof(prepared)[0]
    assert row["cause"] == "trailing" and row["requested"] == 98 and row["initial_risk"] == 10


def test_atr_trailing_and_trigger_breakeven_have_distinct_observed_causes(prepared):
    _,path,inputs,journal,_,_,_=prepared
    inputs.update(InpTrailingMode=2,InpTrailingATR=1)
    path.write_text(path.read_text().replace("atr=unavailable","atr=2"))
    assert proof(prepared)[0]["cause"] == "trailing"
    key=f"breakeven-{POSITION}";journal.clear();journal.update(read(fixture(action_stop=100,intent=key)),reconciled=True,final_at=2000)
    inputs.update(InpTrailingMode=0,InpBreakevenTriggerR=.4)
    text=path.read_text().replace(f"intent=trailing-{POSITION}-1100",f"intent={key}").replace("requested=102","requested=100")
    path.write_text(text)
    row=proof(prepared)[0]
    assert row["cause"] == "breakeven" and row["requested"] == 100 and row["initial_risk"] == 10


def test_no_actions_and_no_opportunity_is_valid_but_unqualified(prepared):
    _,path,_,journal,_,_,_=prepared
    journal["actions"]=[];path.write_text("")
    assert proof(prepared) == []


def test_original_r_uses_the_native_weighted_entry_even_for_one_fill(prepared):
    close_case(prepared);_,path,inputs,journal,signals,deals,ledger=prepared
    price=1.4;volume=.05;anchor=price*volume/volume;risk=anchor-1.3
    assert anchor != price  # diferença real de binary64, não tolerância de preço arbitrária
    signals[0]["filled_volume"]=volume
    deals[0].update(price=price,volume=volume);deals[1].update(price=1.6,volume=volume)
    ledger[0].update(entry_price=price,stop_price=1.3,target_price=1.8)
    journal["entries"][0].update(initial_stop=1.3,initial_target=1.8,requested_volume=volume)
    journal["actions"][0].update(before=volume,requested=volume,confirmed=volume)
    inputs.update(InpTargetMode=2,InpDynamicTargetMinR=1,InpDynamicTargetMaxR=4,InpDynamicTargetATR=2)
    target=anchor+.2
    path.write_text(f"HDK_TARGET position={POSITION} at=1500 direction=1 initial_entry={anchor:.16e} initial_risk={risk:.16e} closed_atr=0.1 target={target:.16e} executable=1.6 acknowledged=1\n")
    assert proof(prepared)[0]["initial_risk"] == risk


def normalized_initial_protection(case,*,broker_normalized=True):
    """Synthetic decimal quote: requested binary64 differs by one ulp."""
    close_case(case)
    _,path,inputs,journal,signals,deals,ledger=case
    price,volume,stop,target=1.0004,.05,1.,1.002
    requested_stop=math.nextafter(stop,0.)
    observed_stop=stop if broker_normalized else requested_stop
    anchor=price*volume/volume;risk=anchor-observed_stop
    assert anchor-stop != anchor-requested_stop
    signals[0]["filled_volume"]=volume
    deals[0].update(price=price,volume=volume,digits=5,order=77,entry_sl=observed_stop,entry_tp=target,
                    order_sl=observed_stop,order_tp=target,order_state=4,order_position=POSITION)
    deals[1].update(price=1.001,volume=volume,digits=5,order=78,entry_sl=observed_stop,entry_tp=target,
                    order_sl=observed_stop,order_tp=target,order_state=4,order_position=POSITION)
    ledger[0].update(entry_price=price,stop_price=stop,target_price=target)
    journal["entries"][0].update(initial_stop=requested_stop,initial_target=math.nextafter(target,0.),requested_volume=volume)
    journal["actions"][0].update(before=volume,requested=volume,confirmed=volume)
    inputs.update(InpTargetMode=2,InpDynamicTargetMinR=1,InpDynamicTargetMaxR=4,InpDynamicTargetATR=2)
    virtual_target=anchor+.0006
    path.write_text(f"HDK_TARGET position={POSITION} at=1500 direction=1 initial_entry={anchor:.16e} initial_risk={risk:.16e} closed_atr=0.0003 target={virtual_target:.16e} executable=1.001 acknowledged=1\n")
    return risk


def test_original_r_uses_observed_initial_stop_after_decimal_normalization(prepared):
    risk=normalized_initial_protection(prepared)
    row=proof(prepared)[0]
    assert row["initial_risk"] == risk
    assert row["initial_risk"] != row["initial_entry"]-prepared[3]["entries"][0]["initial_stop"]


def test_original_r_keeps_lossless_deal_sl_when_csv_has_the_same_rounded_price(prepared):
    risk=normalized_initial_protection(prepared,broker_normalized=False)
    row=proof(prepared,strict=True)[0]
    assert row["initial_risk"] == risk
    assert risk != row["initial_entry"]-prepared[6][0]["stop_price"]


@pytest.mark.parametrize("field",["order","entry_sl","entry_tp","order_sl","order_tp","order_state","order_position"])
def test_lossless_profile_requires_complete_historical_entry_proof(prepared,field):
    normalized_initial_protection(prepared)
    prepared[5][0].pop(field)
    with pytest.raises(ValueError,match="SL/TP históricos"):proof(prepared,strict=True)


def test_lossless_profile_requires_complete_exit_history_and_response_order(prepared):
    normalized_initial_protection(prepared)
    prepared[3]["actions"][0]["response_order"]=78
    assert proof(prepared,strict=True)
    prepared[5][1]["order"]=99
    with pytest.raises(ValueError):proof(prepared,strict=True)
    prepared[5][1]["order"]=78
    prepared[5][1].pop("entry_tp")
    with pytest.raises(ValueError,match="todos os fills"):proof(prepared,strict=True)


@pytest.mark.parametrize("field,value",[("order",99),("order_position",99),("order_state",3),("entry_sl",.9),("entry_tp",1.003),("order_sl",.9),("order_tp",1.003)])
def test_lossless_history_must_match_the_original_order_position_and_quotes(prepared,field,value):
    normalized_initial_protection(prepared)
    prepared[5][0][field]=value
    with pytest.raises(ValueError):proof(prepared,strict=True)


@pytest.mark.parametrize("fault",["stop_point","target_point","many_ulps","unknown_digits","bool_digits"])
def test_normalized_initial_protection_never_accepts_another_price_level(prepared,fault):
    normalized_initial_protection(prepared)
    entry=prepared[3]["entries"][0];opened=prepared[5][0]
    if fault=="stop_point":entry["initial_stop"]=1.-.00001
    elif fault=="target_point":entry["initial_target"]=1.002+.00001
    elif fault=="many_ulps":entry["initial_stop"]=1.-32*math.ulp(1.)
    elif fault=="unknown_digits":opened.pop("digits")
    else:opened["digits"]=True
    with pytest.raises(ValueError):proof(prepared)


def test_trailing_keeps_the_observed_tp_when_request_had_one_ulp_rounding(prepared):
    journal=prepared[3];prepared[5][0]["digits"]=2
    journal["entries"][0].update(initial_stop=math.nextafter(90.,0.),initial_target=math.nextafter(140.,0.))
    row=proof(prepared)[0]
    assert row["initial_risk"] == 10 and row["tp"] == 140
