"""VWAP: referência Decimal do diário observado; sem terminal/servidor."""
from decimal import Decimal, localcontext
import pytest
from mt5_extracao.presets.vwap_events import validate

PARAMS=dict(entry_band_pct=1.,target_band_pct=0.,stop_offset_points=200,volume_source=0)


def context(window, volumes, *, reset, params=PARAMS, available=True):
    """Calcula independentemente a última VWAP e as propostas, sem usar a estratégia."""
    event=window[-1][0]
    has_real=any(v[1]>0 for v in volumes)
    source=2 if params["volume_source"]==2 or (params["volume_source"]==0 and has_real) else 1
    levels={key:0. for key in ("stop_long","stop_short","target_long","target_short","guard_long","guard_short")}
    long=short=False
    if available:
        with localcontext() as ctx:
            ctx.prec=50
            pv=Decimal(0);total=Decimal(0);day=None;anchored=False
            for i,((at,_,high,low,close),volume) in enumerate(zip(window,volumes)):
                if at//86400!=day:
                    day=at//86400;pv=Decimal(0);total=Decimal(0);anchored=i>0 or at%86400==0
                if anchored:
                    v=Decimal(volume[1 if source==2 else 2])
                    if v:
                        pv+=(Decimal(str(high))+Decimal(str(low))+Decimal(str(close)))/3*v;total+=v
            if anchored and total:
                mean=pv/total;entry=Decimal(str(params["entry_band_pct"]))/100;target=Decimal(str(params["target_band_pct"]))/100
                lower,upper=mean*(1-entry),mean*(1+entry)
                close=Decimal(str(window[-1][-1]));buy=Decimal(str(window[-1][3]))<=lower;sell=Decimal(str(window[-1][2]))>=upper
                if buy!=sell:
                    offset=Decimal(params["stop_offset_points"])*Decimal(".01")
                    levels.update(stop_long=float(lower-offset),stop_short=float(upper+offset),target_long=float(mean*(1-target)),target_short=float(mean*(1+target)))
                    long=buy and 0<lower-offset<close;short=sell and upper+offset>close
    row=dict(event_at=event,confirmed_at=event+300,available_at=event+300,count=len(window),reset=int(reset),point=.01,tick=.25,
             long=int(long),short=int(short),**levels)
    volume_row=dict(event_at=event,available_at=event+300,requested=params["volume_source"],source=source if available else 0,
                    count=len(window),reset=int(reset) if available else 1,available=int(available))
    return row,volume_row


def write(path,contexts,snapshots,volume_contexts,volume_snapshots):
    lines=[]
    for row,bars,vol,volumes in zip(contexts,snapshots,volume_contexts,volume_snapshots):
        lines.append("MT5 prefix\tHDK_VWAP_CONTEXT "+" ".join(f"{k}={v}" for k,v in vol.items()))
        for at,real,ticks in volumes:
            lines.append(f"MT5 prefix\tHDK_VWAP_VOLUME event_at={row['event_at']} at={at} real={real} ticks={ticks}")
        lines.append("MT5 prefix\tHDK_STRATEGY_CONTEXT "+" ".join(f"{k}={v}" for k,v in row.items()))
        for at,op,high,low,close in bars:
            lines.append(f"MT5 prefix\tHDK_STRATEGY_BAR event_at={row['event_at']} at={at} open={op} high={high} low={low} close={close}")
    path.parent.mkdir(parents=True,exist_ok=True);path.write_text("\r\n".join(lines))


def trace(directory, *, params=PARAMS, count=300, available=True):
    day=1792454400//86400*86400
    bars=[(day+i*300,100.,100.1,99.9,100.) for i in range(count+1)]
    bars[count-1]=(bars[count-1][0],100.,100.1,98.8,99.5)
    bars[count]=(bars[count][0],100.,101.5,99.9,100.5)
    volumes=[(bar[0],2,20) for bar in bars]
    first,second=bars[:count],bars[:count+1] if count<1500 else bars[1:count+1]
    vf,vs=volumes[:count],volumes[:count+1] if count<1500 else volumes[1:count+1]
    pairs=[context(first,vf,reset=True,params=params,available=available),context(second,vs,reset=False,params=params,available=available)]
    path=directory/"Tester/Agent-1/logs/test.log"
    write(path,[p[0] for p in pairs],[first,[second[-1]]],[p[1] for p in pairs],[vf,[vs[-1]]] if available else [[],[]])
    signals=[dict(event_at=row["event_at"],confirmed_at=row["confirmed_at"],available_at=row["available_at"],direction=1 if row["long"] else -1)
             for row,_ in pairs if row["long"] or row["short"]]
    return path,[p[0] for p in pairs],signals


@pytest.mark.parametrize("source",[0,1,2])
def test_observed_volume_source_and_decimal_both_sides(source,tmp_path):
    params={**PARAMS,"volume_source":source}
    _,_,signals=trace(tmp_path,params=params)
    proof=validate(tmp_path,params,signals)
    assert proof["contexts"]==2 and [r["direction"] for r in proof["decisions"]]==[1,-1]
    assert all(r["target_long"]>0 and r["target_short"]>0 for r in proof["decisions"])
    assert all(r["volume_source"]==(1 if source==1 else 2) for r in proof["decisions"])
    assert not proof["source_qualified"] and not proof["execution_levels_qualified"] and not proof["external_parity"]


def test_rolling_1500_window_and_explicit_unavailable_are_not_invented_data(tmp_path):
    _,_,signals=trace(tmp_path,count=1500)
    assert validate(tmp_path,PARAMS,signals)["contexts"]==2
    trace(tmp_path,available=False)
    proof=validate(tmp_path,PARAMS,[])
    assert proof["unavailable_contexts"]==2 and proof["decisions"]==[]
    with pytest.raises(ValueError):validate(tmp_path,PARAMS,signals)


def test_auto_tick_fallback_requires_no_positive_real_in_the_complete_window(tmp_path):
    path,_,signals=trace(tmp_path)
    path.write_text(path.read_text().replace("real=2","real=0").replace("source=2 count=","source=1 count="))
    proof=validate(tmp_path,PARAMS,signals)
    assert all(r["volume_source"]==1 for r in proof["decisions"])
    path.write_text(path.read_text().replace("source=1 count=","source=2 count="))
    with pytest.raises(ValueError):validate(tmp_path,PARAMS,signals)


@pytest.mark.parametrize("fault",["truncated_header","truncated_volume","extra_volume","orphan","wrong_time","wrong_count","wrong_source","wrong_request","negative","no_volumes","bad_stop","guard","bad_availability","volume_shrink"])
def test_malformed_incomplete_or_divergent_volume_evidence_is_refused(tmp_path,fault):
    path,rows,signals=trace(tmp_path);text=path.read_text()
    if fault=="truncated_header":text+="\nHDK_VWAP_CONTEXT event_at=1"
    elif fault=="truncated_volume":text+="\nHDK_VWAP_VOLUME event_at=1 at=1 real=0"
    elif fault=="extra_volume":text=text.replace("ticks=20","ticks=20 extra=1",1)
    elif fault=="orphan":text+="\nHDK_VWAP_VOLUME event_at=1 at=1 real=2 ticks=20"
    elif fault=="wrong_time":text=text.replace(f"at={1792454400//86400*86400} real=2","at=1 real=2",1)
    elif fault=="wrong_count":text=text.replace("source=2 count=300","source=2 count=301",1)
    elif fault=="wrong_source":text=text.replace("source=2 count=300","source=1 count=300",1)
    elif fault=="wrong_request":text=text.replace("requested=0","requested=1",1)
    elif fault=="negative":text=text.replace("real=2","real=-1",1)
    elif fault=="no_volumes":text="\n".join(line for line in text.splitlines() if "HDK_VWAP_VOLUME" not in line)
    elif fault=="bad_stop":text=text.replace(f"stop_long={rows[0]['stop_long']}","stop_long=1",1)
    elif fault=="guard":text=text.replace("guard_long=0.0","guard_long=1",1)
    elif fault=="bad_availability":text=text.replace("available=1","available=0",1)
    elif fault=="volume_shrink":text=text.replace("source=2 count=301 reset=0","source=2 count=300 reset=0",1)
    path.write_text(text)
    with pytest.raises(ValueError,match="VWAP|marcador"):validate(tmp_path,PARAMS,signals)


def test_source_switch_requires_full_volume_reset_and_previous_day_real_selects_whole_window(tmp_path):
    day=1792454400//86400*86400
    bars=[(day+i*300,100.,100.1,99.9,100.) for i in range(301)]
    bars[299]=(bars[299][0],100.,100.1,98.8,99.5)
    vf=[(b[0],0,20) for b in bars[:300]]
    vs=[(b[0],2 if i==300 else 0,20) for i,b in enumerate(bars)]
    first,second=context(bars[:300],vf,reset=True),context(bars,vs,reset=True)
    path=tmp_path/"Tester/Agent-1/logs/test.log"
    write(path,[first[0],{**second[0],"reset":0}],[bars[:300],[bars[-1]]],[first[1],second[1]],[vf,vs])
    assert validate(tmp_path,PARAMS,[])["contexts"]==2
    text=path.read_text().replace("source=2 count=301 reset=1","source=2 count=301 reset=0")
    path.write_text(text)
    with pytest.raises(ValueError):validate(tmp_path,PARAMS,[])
    # Real positivo somente no dia anterior: Auto continua real e o dia atual sem peso não tem VWAP.
    vp=[(b[0],2 if i==0 else 0,20) for i,b in enumerate(bars)]
    row,vol=context(bars,vp,reset=True)
    assert row["long"]==row["short"]==0 and vol["source"]==2
    write(path,[row],[bars],[vol],[vp])
    assert validate(tmp_path,PARAMS,[])["decisions"]==[]
