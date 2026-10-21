"""Janelas OHLC observadas do motor compartilhado; leitura estrita sem presumir completude."""
import math
from pathlib import Path
import re
from ..robots import mql
from .protocol import strict_events

_CONTEXT=re.compile(r"HDK_STRATEGY_CONTEXT ([^\r\n]+)")
_BAR=re.compile(r"HDK_STRATEGY_BAR event_at=(\d+) at=(\d+) open=([^ ]+) high=([^ ]+) low=([^ ]+) close=([^\s]+)")
_FIELDS={"event_at","confirmed_at","available_at","count","reset","point","tick","long","short",
         "stop_long","stop_short","target_long","target_short","guard_long","guard_short"}
_INTS={"event_at","confirmed_at","available_at","count","reset","long","short"}
_EMPTY=float.fromhex("0x1.fffffffffffffp+1023")



def read_journal(directory):
    return "\n".join(mql.read_text(p) for p in sorted((Path(directory)/"Tester").glob("Agent-*/logs/*.log")))


def windows(text, fail):
    contexts=[]
    for match in strict_events(text,"HDK_STRATEGY_CONTEXT",_CONTEXT):
        raw=match.group(1)
        pairs=[item.split("=",1) for item in raw.strip().split()]
        if any(len(pair)!=2 for pair in pairs) or len(pairs)!=len(_FIELDS) or {p[0] for p in pairs}!=_FIELDS:fail()
        try:row={key:(int(value) if key in _INTS else float(value)) for key,value in pairs}
        except (ValueError,OverflowError):fail()
        if (not all(math.isfinite(v) and v!=_EMPTY for v in row.values()) or
                row["long"] not in (0,1) or row["short"] not in (0,1) or row["reset"] not in (0,1) or
                not row["event_at"]<row["confirmed_at"]<=row["available_at"] or
                not 300<=row["count"]<=1500 or row["point"]<=0 or row["tick"]<=0):fail()
        contexts.append(row)
    if not contexts or [r["event_at"] for r in contexts]!=sorted({r["event_at"] for r in contexts}):fail()
    bars={}
    try:
        for match in strict_events(text,"HDK_STRATEGY_BAR",_BAR):
            event,at,*values=match.groups()
            bars.setdefault(int(event),[]).append((int(at),*map(float,values)))
    except (ValueError,OverflowError):fail()
    if set(bars)!={r["event_at"] for r in contexts}:fail()
    window=[]
    for row in contexts:
        new=bars[row["event_at"]]
        if row["reset"]:
            if len(new)!=row["count"]:fail()
            window=new
        else:
            if len(new)!=1 or not window or row["count"] not in (len(window),len(window)+1):fail()
            window=(window+new)[-row["count"]:]
        if (len(window)!=row["count"] or window[-1][0]!=row["event_at"] or
                [r[0] for r in window]!=sorted({r[0] for r in window}) or
                any(not math.isfinite(v) or v<=0 or v==_EMPTY for r in window for v in r[1:]) or
                any(r[2]<max(r[1],r[3],r[4]) or r[3]>min(r[1],r[2],r[4]) for r in window)):fail()
        yield row, window
