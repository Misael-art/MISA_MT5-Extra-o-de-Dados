"""Propostas Madrid com referência Decimal independente e janelas nativas simuladas."""
from decimal import Decimal
import math

import pytest

from mt5_extracao.presets.madrid_events import validate
from tests.test_madrid import crossing, decimal_reference, market
from tests.test_stoch_events import write_contexts

PARAMS = dict(first_period=3, period_step=1, distance_atr=1., distance_points=0, trail_line=10, offset_atr=.25)


def context(window, *, reset=True, params=PARAMS, point=.01):
    import pandas as pd
    frame = pd.DataFrame(window, columns=["at", "open", "high", "low", "close"])
    means, atrs = decimal_reference(frame, params["first_period"], params["period_step"])
    m, previous, atr = means[-1], means[-2], atrs[-1]
    close, old = Decimal(str(window[-1][-1])), Decimal(str(window[-2][-1]))
    distance = (Decimal(params["distance_points"])*Decimal(str(point)) if params["distance_points"] else
                Decimal(str(params["distance_atr"]))*atr)
    offset = Decimal(str(params["offset_atr"]))*atr
    ready = atr > 0 and math.isfinite(float(distance)) and math.isfinite(float(offset))
    levels = dict.fromkeys(("stop_long", "stop_short", "target_long", "target_short", "guard_long", "guard_short"), 0.)
    long = short = False
    if ready:
        sl, ss = m[params["trail_line"]-1]-offset, m[params["trail_line"]-1]+offset
        levels.update(guard_long=float(sl), guard_short=float(ss))
        long = all(a>b for a,b in zip(m,m[1:])) and old<=previous[0] and close>m[0] and close-m[0]<=distance
        short = all(a<b for a,b in zip(m,m[1:])) and old>=previous[0] and close<m[0] and m[0]-close<=distance
        selected = m[0]+distance if long else m[0]-distance
        if (long or short) and selected>0 and math.isfinite(float(selected)):
            levels.update(stop_long=float(sl),stop_short=float(ss),target_long=float(m[0]+distance),target_short=float(m[0]-distance))
            long &= 0<sl<close
            short &= ss>close
        else:
            long = short = False
    event = window[-1][0]
    return dict(event_at=event,confirmed_at=event+300,available_at=event+300,count=len(window),reset=int(reset),point=point,tick=.25,
                long=int(long),short=int(short),**levels)


def bars(direction=1, *, shift=0):
    data = crossing(direction)
    return [(1792454400+shift+i*300,*row) for i,row in enumerate(data[["open","high","low","close"]].to_numpy())]


def trace(directory, *, params=PARAMS):
    snapshots = [bars(1),bars(-1,shift=300)]
    contexts = [context(w,params=params) for w in snapshots]
    path = directory/"Tester/Agent-1/logs/test.log"
    write_contexts(path,contexts,snapshots)
    signals = [dict(event_at=r["event_at"],confirmed_at=r["confirmed_at"],available_at=r["available_at"],direction=d)
               for r,d in zip(contexts,(1,-1)) if r["long"] or r["short"]]
    return path,contexts,signals


@pytest.mark.parametrize("points",[0,1000])
def test_both_sides_derive_brute_targets_from_atr_or_recorded_point(tmp_path, points):
    params = {**PARAMS,"distance_points":points}
    _,rows,signals = trace(tmp_path,params=params)
    proof = validate(tmp_path,params,signals)
    assert [r["direction"] for r in proof["decisions"]]==[1,-1]
    assert rows[0]["target_long"]==(111 if points else 165)
    assert not proof["source_qualified"] and not proof["move_guard_execution_qualified"]


@pytest.mark.parametrize("kind",["no_cross","bad_selected_tp","bad_stop_side","distance_exceeded"])
def test_guards_remain_proposals_when_no_entry_is_allowed(tmp_path,kind):
    window=bars(-1);params={**PARAMS}
    if kind=="no_cross":window[-1]=(*window[-1][:-1],100.)
    if kind=="bad_selected_tp":params["distance_atr"]=100.
    if kind=="bad_stop_side":window=bars(1);params["offset_atr"]=100.
    if kind=="distance_exceeded":params["distance_points"]=1
    row=context(window,params=params)
    path=tmp_path/"Tester/Agent-1/logs/test.log";write_contexts(path,[row],[window])
    assert validate(tmp_path,params,[])["decisions"]==[]
    assert row["guard_short"]>0
    if kind!="bad_stop_side":assert row["target_long"]==row["stop_long"]==0
    else:assert row["target_short"]>0  # O wrapper propõe; o motor recusa o lado do SL.
    changed=path.read_text().replace(f"guard_short={row['guard_short']}","guard_short=0",1)
    path.write_text(changed)
    with pytest.raises(ValueError,match="Madrid"):validate(tmp_path,params,[])


@pytest.mark.parametrize("fault",["tp","stop","clock","wrong_signal","malformed_bar","extra_marker","no_context"])
def test_refuses_mismatched_levels_clocks_or_unconsumed_protocol(tmp_path,fault):
    path,rows,signals=trace(tmp_path);text=path.read_text()
    if fault=="tp":text=text.replace(f"target_long={rows[0]['target_long']}","target_long=1",1)
    if fault=="stop":text=text.replace(f"stop_long={rows[0]['stop_long']}","stop_long=1",1)
    if fault=="clock":signals[0]["confirmed_at"]+=1
    if fault=="wrong_signal":signals[0]["direction"]=-1
    if fault=="malformed_bar":text+="\nHDK_STRATEGY_BAR event_at=oops"
    if fault=="extra_marker":text+="\nHDK_STRATEGY_CONTEXT"
    if fault=="no_context":text=""
    path.write_text(text)
    with pytest.raises(ValueError,match="Madrid|marcador"):validate(tmp_path,PARAMS,signals)


def test_rolling_1500_and_incremental_shrink_with_constant_prices(tmp_path):
    window=[(1792454400+i*300,100.,110.,90.,100.) for i in range(1501)]
    first,second=window[:1500],window[1:]
    rows=[context(first),context(second,reset=False)]
    path=tmp_path/"Tester/Agent-1/logs/test.log";write_contexts(path,rows,[first,[second[-1]]])
    assert validate(tmp_path,PARAMS,[])["contexts"]==2
    rows[1]["count"]=1499
    write_contexts(path,rows,[first,[second[-1]]])
    with pytest.raises(ValueError,match="Madrid"):validate(tmp_path,PARAMS,[])


@pytest.mark.parametrize("kind",["overflow_distance","zero_atr"])
def test_unavailable_distance_or_flat_atr_does_not_publish_guard(tmp_path,kind):
    window=bars();params={**PARAMS};point=.01
    if kind=="overflow_distance":params["distance_points"]=2147483647;point=1e308
    else:window=[(t,100.,100.,100.,100.) for t,*_ in window]
    row=context(window,params=params,point=point)
    assert row["guard_long"]==row["guard_short"]==0
    path=tmp_path/"Tester/Agent-1/logs/test.log";write_contexts(path,[row],[window])
    assert not validate(tmp_path,params,[])["decisions"]


def test_rolling_window_restarts_ema_seed_instead_of_reusing_full_frame(tmp_path):
    data=market(1501)
    window=[(1792454400+i*300,*row) for i,row in enumerate(data[["open","high","low","close"]].to_numpy())]
    params={**PARAMS,"first_period":100,"period_step":100}
    first,rolling=window[:1500],window[1:]
    rows=[context(first,params=params),context(rolling,reset=False,params=params)]
    # M10=1000 mantém efeito observável da semente inicial, mesmo após1500.
    assert rows[0]["guard_long"]!=rows[1]["guard_long"]
    path=tmp_path/"Tester/Agent-1/logs/test.log";write_contexts(path,rows,[first,[rolling[-1]]])
    assert validate(tmp_path,params,[])["contexts"]==2
