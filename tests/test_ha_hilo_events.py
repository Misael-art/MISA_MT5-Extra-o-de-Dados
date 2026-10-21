"""Janelas HA/HiLo simuladas com referência Decimal; não exercita reversão broker."""
from decimal import Decimal
import math

import pandas as pd
import pytest

from mt5_extracao.presets.ha_hilo_events import validate
from tests.test_ha_hilo import decimal_reference
from tests.test_stoch_events import write_contexts

PARAMS = dict(hilo_period=10,hilo_offset=0,stop_buffer_points=0)


def context(window, *, reset=True, params=PARAMS, point=.01):
    frame=pd.DataFrame(window,columns=["at","open","high","low","close"])
    ho,hc,line,_=decimal_reference(frame,params["hilo_period"])[-1]
    buy=line is not None and hc>line and hc>ho
    sell=line is not None and hc<line and hc<ho
    levels=dict.fromkeys(("stop_long","stop_short","target_long","target_short","guard_long","guard_short"),0.)
    long=short=False
    if buy or sell:
        buffer=Decimal(params["stop_buffer_points"])*Decimal(str(point))
        sl,ss=line-buffer,line+buffer
        if all(math.isfinite(float(v)) for v in (buffer,sl,ss)):
            levels.update(stop_long=float(sl),stop_short=float(ss))
            close=Decimal(str(window[-1][-1]));long=buy and 0<sl<close;short=sell and ss>close
    at=window[-1][0]
    return dict(event_at=at,confirmed_at=at+300,available_at=at+300,count=len(window),reset=int(reset),point=point,tick=.25,
                long=int(long),short=int(short),**levels)


def bars(direction=1, *, shift=0):
    rows=[]
    for i in range(300):
        close=100+direction*i*.1
        rows.append((1792454400+shift+i*300,close-direction*.05,close+.1,close-.1,close))
    return rows


def trace(directory, *, params=PARAMS):
    snapshots=[bars(),bars(-1,shift=300)]
    rows=[context(w,params=params) for w in snapshots]
    path=directory/"Tester/Agent-1/logs/test.log";write_contexts(path,rows,snapshots)
    signals=[dict(event_at=r["event_at"],confirmed_at=r["confirmed_at"],available_at=r["available_at"],direction=d)
             for r,d in zip(rows,(1,-1)) if r["long"] or r["short"]]
    return path,rows,signals


@pytest.mark.parametrize("buffer",[0,15])
def test_both_sides_keep_zero_target_guard_and_raw_buffer_proposals(tmp_path,buffer):
    params={**PARAMS,"stop_buffer_points":buffer}
    _,rows,signals=trace(tmp_path,params=params);proof=validate(tmp_path,params,signals)
    assert [r["direction"] for r in proof["decisions"]]==[1,-1]
    assert [r["exit_intent_from_reference"] for r in proof["decisions"]]==["short","long"]
    assert all(r["target_long"]==r["target_short"]==r["guard_long"]==r["guard_short"]==0 for r in rows)
    assert not proof["source_qualified"] and not proof["reversal_execution_qualified"]


def test_bad_stop_side_preserves_raw_opposite_exit_intent_but_not_entry(tmp_path):
    params={**PARAMS,"stop_buffer_points":2147483647};window=bars();row=context(window,params=params)
    assert not row["long"] and row["stop_long"]<0
    path=tmp_path/"Tester/Agent-1/logs/test.log";write_contexts(path,[row],[window])
    proof=validate(tmp_path,params,[])
    assert len(proof["decisions"])==1 and not proof["decisions"][0]["entry_allowed_by_reference"]
    assert proof["decisions"][0]["exit_intent_from_reference"]=="short"


def test_buffer_overflow_refuses_wrapper_proposals_and_exit_intent(tmp_path):
    params={**PARAMS,"stop_buffer_points":2147483647};window=bars();row=context(window,params=params,point=1e308)
    path=tmp_path/"Tester/Agent-1/logs/test.log";write_contexts(path,[row],[window])
    assert not validate(tmp_path,params,[])["decisions"]


@pytest.mark.parametrize("fault",["target","guard","stop","clock","wrong_side","orphan","malformed","missing"])
def test_strict_journal_and_proposals_refuse_mismatch(tmp_path,fault):
    path,rows,signals=trace(tmp_path);text=path.read_text()
    if fault=="target":text=text.replace("target_long=0.0","target_long=1",1)
    if fault=="guard":text=text.replace("guard_short=0.0","guard_short=1",1)
    if fault=="stop":text=text.replace(f"stop_long={rows[0]['stop_long']}","stop_long=1",1)
    if fault=="clock":signals[0]["confirmed_at"]+=1
    if fault=="wrong_side":signals[0]["direction"]=-1
    if fault=="orphan":text+="\nHDK_STRATEGY_BAR event_at=1 at=1 open=100 high=101 low=99 close=100"
    if fault=="malformed":text+="\nHDK_STRATEGY_CONTEXT"
    if fault=="missing":text=""
    path.write_text(text)
    with pytest.raises(ValueError,match="HA/HiLo|marcador"):validate(tmp_path,PARAMS,signals)


def test_rolling_1500_can_lose_initial_hilo_state_without_reusing_full_frame_seed(tmp_path):
    all_bars=[(1792454400+i*300,100.,100.1,99.9,100.) for i in range(1501)]
    all_bars[10]=(*all_bars[10][:2],110.1,99.9,110.)
    for i in (1499,1500):all_bars[i]=(*all_bars[i][:-1],100.01)
    first,rolling=all_bars[:1500],all_bars[1:]
    rows=[context(first),context(rolling,reset=False)]
    assert rows[0]["long"] and not rows[1]["long"] and rows[1]["stop_long"]==0
    path=tmp_path/"Tester/Agent-1/logs/test.log";write_contexts(path,rows,[first,[rolling[-1]]])
    proof=validate(tmp_path,PARAMS,[])
    assert len(proof["decisions"])==1
    rows[1]["count"]=1499;write_contexts(path,rows,[first,[rolling[-1]]])
    with pytest.raises(ValueError,match="HA/HiLo"):validate(tmp_path,PARAMS,[])
