"""Decisões da janela nativa: fixture Decimal e recusas de trace/recibo incompletos."""
from decimal import Decimal, localcontext
from pathlib import Path
import copy
import pytest
from mt5_extracao.presets.stoch_events import validate
from mt5_extracao.presets import execution

PARAMS=dict(k_period=2,slowing=1,d_period=2,ema_period=1000,upper_level=75.,lower_level=25.,stop_offset_points=50,target_points=120)


def context(window, *, reset, params=PARAMS):
    with localcontext() as ctx:
        ctx.prec=50
        alpha=Decimal(2)/Decimal(params["ema_period"]+1);ema=Decimal(str(window[0][-1]))
        for bar in window[1:]:ema=alpha*Decimal(str(bar[-1]))+(1-alpha)*ema
        current,previous,older=[Decimal(str(row[-1])) for row in window[-1:-4:-1]]
        k,kp=5*(current-90),5*(previous-90)
        d,dp=(k+kp)/2,(kp+5*(older-90))/2
        long=kp<=dp and k>d and k>params["upper_level"] and current>ema
        short=kp>=dp and k<d and k<params["lower_level"] and current<ema
        event=window[-1][0]
        offset=Decimal(params["stop_offset_points"])*Decimal(".01");target=Decimal(params["target_points"])*Decimal(".01")
        levels=dict(stop_long=float(ema-offset),stop_short=float(ema+offset),
                    target_long=float(current+target) if target else 0,target_short=float(current-target) if target else 0)
        return dict(event_at=event,confirmed_at=event+300,available_at=event+300,count=len(window),reset=int(reset),point=.01,tick=.25,
                    long=int(long),short=int(short),**levels,guard_long=levels["stop_long"],guard_short=levels["stop_short"])


def trace(tmp_path, *, params=PARAMS, first_at=1792454400):
    bars=[(first_at+i*300,100.,110.,90.,100.) for i in range(302)]
    bars[299]=(*bars[299][:-1],106.);bars[300]=(*bars[300][:-1],94.)
    first=bars[:300];second=bars[:301];third=bars[1:302]
    third[0]=(*third[0][:-1],104.)  # revisão antiga muda a semente: exige janela completa
    contexts=[context(first,reset=True,params=params),context(second,reset=False,params=params),context(third,reset=True,params=params)]
    lines=[]
    for row,window in zip(contexts,[first,[second[-1]],third]):
        lines.append("HDK_STRATEGY_CONTEXT "+" ".join(f"{key}={value}" for key,value in row.items()))
        for at,op,high,low,close in window:
            lines.append(f"HDK_STRATEGY_BAR event_at={row['event_at']} at={at} open={op} high={high} low={low} close={close}")
    path=tmp_path/"Tester/Agent-1/logs/test.log";path.parent.mkdir(parents=True,exist_ok=True);path.write_text("\n".join(lines))
    signals=[dict(event_at=row["event_at"],confirmed_at=row["confirmed_at"],available_at=row["available_at"],direction=side)
             for row,side in zip(contexts[:2],(1,-1))]
    return path,contexts,signals


def test_reconstructs_append_and_revised_seed_on_both_sides(tmp_path):
    path,contexts,signals=trace(tmp_path)
    proof=validate(tmp_path,PARAMS,signals)
    assert proof["contexts"]==3 and [r["direction"] for r in proof["decisions"]]==[1,-1]
    assert len(proof["trace_sha256"])==64
    assert proof["external_parity"] is False and proof["move_guard_execution_qualified"] is False
    params={**PARAMS,"target_points":0};trace(tmp_path,params=params)
    assert all(r["target_long"]==r["target_short"]==0 for r in validate(tmp_path,params,signals)["decisions"])


@pytest.mark.parametrize("fault",["no_context","missing_bar","duplicate_bar","bad_bar","bad_clock","bad_stop","wrong_side","false_append","orphan_bar","missing_key","nonfinite","bad_count","malformed_extra_bar","trailing_bar","malformed_extra_context"])
def test_rejects_incomplete_changed_or_invalid_context(tmp_path,fault):
    path,rows,signals=trace(tmp_path);text=path.read_text()
    if fault=="no_context":text=""
    elif fault=="missing_bar":text="\n".join(text.splitlines()[1:])
    elif fault=="duplicate_bar":text+="\n"+text.splitlines()[1]
    elif fault=="bad_bar":text=text.replace("high=110.0","high=80.0",1)
    elif fault=="bad_clock":text=text.replace(f"confirmed_at={rows[0]['confirmed_at']}",f"confirmed_at={rows[0]['event_at']}",1)
    elif fault=="bad_stop":text=text.replace(f"stop_long={rows[0]['stop_long']}","stop_long=1",1)
    elif fault=="wrong_side":signals[0]["direction"]=-1
    elif fault=="false_append":text=text.replace("reset=1","reset=0",1)
    elif fault=="orphan_bar":text+="\nHDK_STRATEGY_BAR event_at=1 at=1 open=100 high=110 low=90 close=100"
    elif fault=="missing_key":text=text.replace("tick=0.25 ","",1)
    elif fault=="nonfinite":text=text.replace("point=0.01","point=nan",1)
    elif fault=="bad_count":text=text.replace("count=300","count=299",1)
    elif fault=="malformed_extra_bar":text+="\nHDK_STRATEGY_BAR event_at=oops at=1 open=100 high=110 low=90 close=100"
    elif fault=="trailing_bar":text=text.replace("close=100.0","close=100.0 unexpected=1",1)
    elif fault=="malformed_extra_context":text+="\nHDK_STRATEGY_CONTEXT"
    path.write_text(text)
    with pytest.raises(ValueError,match="S10|marcador"):validate(tmp_path,PARAMS,signals)


def test_no_target_is_an_explicit_profile_choice_and_stop_stays_required(monkeypatch):
    import pandas as pd
    from mt5_extracao.trade_analytics import ledger
    at=1792454700
    frame=pd.DataFrame([dict(direction=1,entry_time=pd.Timestamp(at,unit="s"),exit_time=pd.Timestamp(at+300,unit="s"),
        entry_price=100.,exit_price=99.,stop_price=99.,target_price=0.,volume=.1,pnl=-1.,exit_reason="stop",completeness=ledger.COMPLETE)])
    monkeypatch.setattr(execution.ledger,"load",lambda *a,**k:frame)
    record={"run_id":"s10","metrics":{"trades":1},"trades":[[frame.exit_time.iloc[0].strftime("%Y-%m-%d %H:%M:%S"),-1.,.1]]}
    signals=[dict(direction=1,available_at=at,opened=True,filled_volume=.1)]
    with pytest.raises(ValueError):execution._ledger_rows(None,record,signals)
    assert execution._ledger_rows(None,record,signals,target_required=False)[0]["target_price"]==0
    frame.loc[0,"stop_price"]=0
    with pytest.raises(ValueError):execution._ledger_rows(None,record,signals,target_required=False)


def write_contexts(path,contexts,snapshots):
    lines=[]
    for row,bars in zip(contexts,snapshots):
        lines.append("prefix MT5\tHDK_STRATEGY_CONTEXT "+" ".join(f"{key}={value}" for key,value in row.items()))
        for at,op,high,low,close in bars:
            lines.append(f"prefix MT5\tHDK_STRATEGY_BAR event_at={row['event_at']} at={at} open={op} high={high} low={low} close={close}")
    path.parent.mkdir(parents=True,exist_ok=True);path.write_text("\r\n".join(lines))


def test_incremental_shrink_is_refused_even_when_constant_prices_hide_the_lost_bars(tmp_path):
    bars=[(1792454400+i*300,100.,110.,90.,100.) for i in range(302)]
    first=bars[:301];next_window=bars[2:302]
    rows=[context(first,reset=True),context(next_window,reset=False)]
    path=tmp_path/"Tester/Agent-1/logs/test.log"
    write_contexts(path,rows,[first,[next_window[-1]]])
    with pytest.raises(ValueError,match="S10"):validate(tmp_path,PARAMS,[])


def test_rolling_1500_bar_window_matches_the_producer_and_keeps_mt5_prefixes(tmp_path):
    bars=[(1792454400+i*300,100.,110.,90.,100.) for i in range(1501)]
    bars[1499]=(*bars[1499][:-1],106.);bars[1500]=(*bars[1500][:-1],94.)
    first=bars[:1500];rolling=bars[1:1501]
    rows=[context(first,reset=True),context(rolling,reset=False)]
    path=tmp_path/"Tester/Agent-1/logs/test.log"
    write_contexts(path,rows,[first,[rolling[-1]]])
    proof=validate(tmp_path,PARAMS,[])
    assert proof["contexts"]==2 and [r["direction"] for r in proof["decisions"]]==[1,-1]


@pytest.mark.parametrize("marker",["S09_SIGNAL","HDK_STRATEGY_SIGNAL"])
def test_every_selected_signal_marker_must_be_consumed(tmp_path,marker):
    path=tmp_path/"Tester/Agent-1/logs/test.log";path.parent.mkdir(parents=True)
    path.write_text(f"MT5 prefix {marker} event_at=100 confirmed_at=200 available_at=200 direction=1 opened=1 execution_state=4 volume=1 order=2 deal=2 reason=ok\n"
                    f"MT5 prefix {marker} event_at=200 confirmed_at=300 available_at=300 direction=-1 opened=0 execution_state=6 volume=0 order=0 deal=0\n")
    with pytest.raises(ValueError,match="marcador"):execution.native_signals(tmp_path,marker=marker)
