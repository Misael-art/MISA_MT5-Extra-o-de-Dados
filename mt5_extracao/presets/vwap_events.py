"""Concilia janela OHLC, volumes efetivos e decisões VWAP; níveis são propostas."""
import hashlib
import math
import re
import pandas as pd
from ..composition.schema import canonical_json
from ..strategies import get_strategy
from .protocol import strict_events
from .strategy_context import read_journal, windows

_CONTEXT=re.compile(r"HDK_VWAP_CONTEXT event_at=(\d+) available_at=(\d+) requested=(\d+) source=(\d+) count=(\d+) reset=(\d+) available=(\d+)")
_VOLUME=re.compile(r"HDK_VWAP_VOLUME event_at=(\d+) at=(\d+) real=(-?\d+) ticks=(-?\d+)")


def _fail():
    raise ValueError("VWAP: contexto, volumes/fonte ou decisão ausente/divergente; preserve o diário e confira a primeira diferença.")


def validate(directory, params, signals):
    text=read_journal(directory)
    volume_contexts=[]
    for match in strict_events(text,"HDK_VWAP_CONTEXT",_CONTEXT):
        values=tuple(map(int,match.groups()))
        event,available_at,requested,source,count,reset,available=values
        if (not event<available_at or requested!=params["volume_source"] or source not in (0,1,2)
                or not 300<=count<=1500 or reset not in (0,1) or available not in (0,1)
                or (available and source==0) or (not available and (source!=0 or reset!=1))):_fail()
        volume_contexts.append(values)
    if not volume_contexts or [r[0] for r in volume_contexts]!=sorted({r[0] for r in volume_contexts}):_fail()
    volumes={}
    for match in strict_events(text,"HDK_VWAP_VOLUME",_VOLUME):
        event,at,real,ticks=map(int,match.groups())
        if not 0<=real<=2**63-1 or not 0<=ticks<=2**63-1:_fail()
        volumes.setdefault(event,[]).append((at,real,ticks))
    if set(volumes)!={r[0] for r in volume_contexts if r[-1]}:_fail()
    observed=list(windows(text,_fail))
    if [r[0]["event_at"] for r in observed]!=[r[0] for r in volume_contexts]:_fail()
    volume_window=[];previous_source=0;decisions=[];by_event={};digest=hashlib.sha256();unavailable=0
    for (row,window),volume_context in zip(observed,volume_contexts):
        event,available_at,requested,source,count,reset,available=volume_context
        if count!=row["count"] or available_at>row["available_at"]:_fail()
        levels={key:0. for key in ("stop_long","stop_short","target_long","target_short","guard_long","guard_short")}
        long=short=False
        if not available:
            volume_window=[];previous_source=0;unavailable+=1
        else:
            new=volumes[event]
            if reset:
                if len(new)!=count:_fail()
                volume_window=new
            else:
                if (len(new)!=1 or not volume_window or source!=previous_source
                        or count not in (len(volume_window),len(volume_window)+1)):_fail()
                volume_window=(volume_window+new)[-count:]
            if len(volume_window)!=count or [r[0] for r in volume_window]!=[r[0] for r in window]:_fail()
            has_real=any(r[1]>0 for r in volume_window)
            chosen=2 if requested==2 or (requested==0 and has_real) else 1
            if chosen!=source or (source==2 and not has_real):_fail()
            frame=pd.DataFrame(window,columns=["at","open","high","low","close"])
            frame["time"]=pd.to_datetime(frame["at"],unit="s")
            frame["real_volume"]=[r[1] for r in volume_window]
            frame["tick_volume"]=[r[2] for r in volume_window]
            # A escolha efetiva foi registrada para a janela inteira; não inferir fonte por barra futura.
            strategy=get_strategy("vwap_bands",**{**params,"volume_source":source})
            expected=strategy.gerar_sinais(frame).iloc[-1]
            candidate_long,candidate_short=bool(expected.entry_long),bool(expected.entry_short)
            if candidate_long or candidate_short:
                offset=params["stop_offset_points"]*row["point"]
                levels.update(stop_long=float(expected.stop_long)-offset,stop_short=float(expected.stop_short)+offset,
                              target_long=float(expected.target_long),target_short=float(expected.target_short))
                close=window[-1][-1]
                long=candidate_long and 0<levels["stop_long"]<close
                short=candidate_short and levels["stop_short"]>close
            previous_source=source
        tolerance=max(1e-10,abs(window[-1][-1])*1e-12)
        if (row["long"]!=long or row["short"]!=short
                or any(not math.isfinite(value) or abs(row[key]-value)>tolerance for key,value in levels.items())):_fail()
        by_event[event]=row
        digest.update(canonical_json({"context":row,"window":window,"volume_context":volume_context,"volumes":volume_window}).encode("utf-8"))
        if long or short:decisions.append({"event_at":event,"direction":1 if long else -1,"volume_source":source,**levels})
    for signal in signals:
        row=by_event.get(signal["event_at"])
        if (row is None or not row["long" if signal["direction"]==1 else "short"]
                or signal["confirmed_at"]!=row["confirmed_at"] or signal["available_at"]<row["available_at"]):_fail()
    return {"strategy":"vwap_bands","reference":"Python variant on each observed native closed OHLC and volume window",
            "contexts":len(observed),"unavailable_contexts":unavailable,"trace_sha256":digest.hexdigest(),"decisions":decisions,
            "native_window_bars":1500,"external_parity":False,"source_qualified":False,"execution_levels_qualified":False}
