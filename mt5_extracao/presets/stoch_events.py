"""S10: reconstrói cada janela OHLC realmente registrada, sem supor seed fixo no terminal.

Valida a decisão declarada; preenchimentos continuam dependendo do recibo/livro/saldo
conferidos pelo serviço de execução. Não comprova sucesso do Move Guard nem paridade externa.
"""
import hashlib
import math
from pathlib import Path
import re
import pandas as pd
from ..robots import mql
from ..strategies import get_strategy
from ..composition.schema import canonical_json

_CONTEXT=re.compile(r"HDK_STRATEGY_CONTEXT ([^\r\n]+)")
_BAR=re.compile(r"HDK_STRATEGY_BAR event_at=(\d+) at=(\d+) open=([^ ]+) high=([^ ]+) low=([^ ]+) close=([^\s]+)")
_FIELDS={"event_at","confirmed_at","available_at","count","reset","point","tick","long","short",
         "stop_long","stop_short","target_long","target_short","guard_long","guard_short"}
_INTS={"event_at","confirmed_at","available_at","count","reset","long","short"}
_EMPTY=float.fromhex("0x1.fffffffffffffp+1023")


def _fail():
    raise ValueError("S10: contexto/janela/decisão ausente ou divergente da variante Python; preserve o diário e confira a primeira diferença.")


def validate(directory, params, signals):
    text="\n".join(mql.read_text(p) for p in sorted((Path(directory)/"Tester").glob("Agent-*/logs/*.log")))
    contexts=[]
    for raw in _CONTEXT.findall(text):
        pairs=[item.split("=",1) for item in raw.strip().split()]
        if any(len(pair)!=2 for pair in pairs) or len(pairs)!=len(_FIELDS) or {p[0] for p in pairs}!=_FIELDS:_fail()
        try:row={key:(int(value) if key in _INTS else float(value)) for key,value in pairs}
        except (ValueError,OverflowError):_fail()
        if (not all(math.isfinite(v) and v!=_EMPTY for v in row.values()) or
                row["long"] not in (0,1) or row["short"] not in (0,1) or row["reset"] not in (0,1) or
                not row["event_at"]<row["confirmed_at"]<=row["available_at"] or
                not 300<=row["count"]<=1500 or row["point"]<=0 or row["tick"]<=0):_fail()
        contexts.append(row)
    if not contexts or [r["event_at"] for r in contexts]!=sorted({r["event_at"] for r in contexts}):_fail()
    bars={}
    try:
        for event,at,*values in _BAR.findall(text):bars.setdefault(int(event),[]).append((int(at),*map(float,values)))
    except (ValueError,OverflowError):_fail()
    if set(bars)!={r["event_at"] for r in contexts}:_fail()
    window=[];decisions=[];digest=hashlib.sha256();strategy=get_strategy("stoch_ema9",**params)
    by_event={}
    for row in contexts:
        new=bars[row["event_at"]]
        if row["reset"]:
            if len(new)!=row["count"]:_fail()
            window=new
        else:
            if len(new)!=1 or not window:_fail()
            window=(window+new)[-row["count"]:]
        if (len(window)!=row["count"] or window[-1][0]!=row["event_at"] or
                [r[0] for r in window]!=sorted({r[0] for r in window}) or
                any(not math.isfinite(v) or v<=0 or v==_EMPTY for r in window for v in r[1:]) or
                any(r[2]<max(r[1],r[3],r[4]) or r[3]>min(r[1],r[2],r[4]) for r in window)):_fail()
        frame=pd.DataFrame(window,columns=["at","open","high","low","close"])
        frame["time"]=pd.to_datetime(frame["at"],unit="s")
        expected=strategy.gerar_sinais(frame).iloc[-1]
        close=window[-1][-1];point=row["point"]
        ready=len(window)>=strategy.min_bars
        ema=float(expected.stop_long) if ready else 0
        levels={"stop_long":ema-params["stop_offset_points"]*point if ready else 0,
                "stop_short":ema+params["stop_offset_points"]*point if ready else 0,
                "target_long":close+params["target_points"]*point if ready and params["target_points"] else 0,
                "target_short":close-params["target_points"]*point if ready and params["target_points"] else 0}
        levels.update(guard_long=levels["stop_long"],guard_short=levels["stop_short"])
        tolerance=max(1e-10,abs(close)*1e-12)
        if any(not math.isfinite(value) or abs(row[key]-value)>tolerance for key,value in levels.items()):_fail()
        long=bool(expected.entry_long) and 0<levels["stop_long"]<close
        short=bool(expected.entry_short) and levels["stop_short"]>close
        if row["long"]!=long or row["short"]!=short:_fail()
        by_event[row["event_at"]]=row
        digest.update(canonical_json({"context":row,"window":window}).encode("utf-8"))
        if long or short:decisions.append({"event_at":row["event_at"],"direction":1 if long else -1,**levels})
    for signal in signals:
        row=by_event.get(signal["event_at"])
        if (row is None or not row["long" if signal["direction"]==1 else "short"] or
                signal["confirmed_at"]!=row["confirmed_at"] or signal["available_at"]<row["available_at"]):_fail()
    return {"strategy":"stoch_ema9","reference":"Python variant on each observed native closed window",
            "contexts":len(contexts),"trace_sha256":digest.hexdigest(),"decisions":decisions,
            "native_window_bars":1500,"external_parity":False,"move_guard_execution_qualified":False,"source_qualified":False}
