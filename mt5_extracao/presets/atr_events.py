"""TM07: ATR canônico e referência Decimal, semente/base imutáveis e contexto causal."""
from decimal import Decimal, ROUND_CEILING, localcontext
from pathlib import Path
import re
from ..robots import mql

FIELDS=set("event_at position at_msc initial base_known seed baseline_bar current_open baseline current horizon times highs lows closes".split())
INTS=set("event_at position at_msc initial base_known seed baseline_bar current_open horizon".split())

def reference(highs,lows,closes):
    with localcontext() as ctx:
        ctx.prec=50
        result=highs[0]-lows[0]
        for i in range(1,len(highs)):
            tr=max(highs[i]-lows[i],abs(highs[i]-closes[i-1]),abs(lows[i]-closes[i-1]))
            result=result*Decimal(13)/14+tr/14
        return +result

def horizon(policy,baseline,current):
    with localcontext() as ctx:
        ctx.prec=50
        raw=Decimal(policy.atr_base_seconds)*baseline/current
        return max(policy.atr_min_seconds,min(policy.max_seconds,int((raw-Decimal("1e-9")).to_integral_value(rounding=ROUND_CEILING))))

def observations(directory,by_origin,policy):
    text="\n".join(mql.read_text(p) for p in (Path(directory)/"Tester").glob("Agent-*/logs/*.log"))
    matches=list(re.finditer(r"HDK_ATR (event_at=[^\r\n]+)",text));result=[];first={};last={}
    if text.count("HDK_ATR event_at=")!=len(matches):raise ValueError("Journal ATR truncado/desconhecido; preserve os logs.")
    for match in matches:
        tokens=[part.split("=",1) for part in match.group(1).split()];row=dict(tokens)
        if len(tokens)!=len(row) or set(row)!=FIELDS:raise ValueError("Campos do journal ATR ausentes/desconhecidos.")
        for key in INTS:row[key]=int(row[key])
        for key in ("baseline","current"):row[key]=Decimal(row[key])
        row["times"]=[int(v) for v in row["times"].split(",")]
        vectors={k:[Decimal(v) for v in row[k].split(",")] for k in ("highs","lows","closes")}
        n=len(row["times"]);origin,pid,at=row["event_at"],row["position"],row["at_msc"]
        if origin not in by_origin or by_origin[origin][0]!=pid:raise ValueError("Observação ATR sem entrada/posição próprias.")
        entry=next(f for f in by_origin[origin][1] if f["entry"]==0)
        if (n<250 or any(len(v)!=n for v in vectors.values()) or row["times"][0]!=row["seed"]
            or any(t<=0 or t>=row["current_open"] for t in row["times"]) or any(a>=b for a,b in zip(row["times"],row["times"][1:]))
            or row["current_open"]*1000>at or at<entry["at_msc"] or row["initial"] not in (0,1)
            or not all(v.is_finite() for vs in vectors.values() for v in vs)
            or any(not l<=c<=h for h,l,c in zip(vectors["highs"],vectors["lows"],vectors["closes"]))
            or any(not row[k].is_finite() or row[k]<=0 for k in ("baseline","current"))):
            raise ValueError("ATR contém ausência, aquecimento insuficiente, preço inválido ou informação futura.")
        independent=reference(**vectors);tolerance=Decimal("2e-16")*max(1,abs(independent))
        import pandas as pd
        from ..strategies.indicators import atr
        canonical=Decimal(str(float(atr(pd.DataFrame({k[:-1] if k!="closes" else "close":list(map(float,v)) for k,v in vectors.items()}),14).iloc[-1])))
        if independent<=0 or abs(row["current"]-independent)>tolerance or abs(canonical-independent)>tolerance:
            raise ValueError("ATR nativo diverge da referência independente/cálculo canônico desde a semente declarada.")
        initial=first.get(pid)
        if initial is None:
            if (row["initial"]!=1 or n!=250 or row["baseline_bar"]!=row["times"][-1] or row["base_known"]!=at
                or row["current_open"]*1000>entry["at_msc"] or row["baseline"]!=row["current"]):
                raise ValueError("Base ATR inicial não foi conhecida no primeiro fill; não reconstruir nem inventar informação.")
            first[pid]=dict(row)
        elif (row["initial"] or any(row[k]!=initial[k] for k in ("baseline","baseline_bar","base_known","seed")) or at<last[pid]["at_msc"]):
            raise ValueError("Base/semente/idade ATR reiniciada após parcial, reinício ou correção da fonte.")
        if row["horizon"]!=horizon(policy,row["baseline"],row["current"]):raise ValueError("Horizonte ATR fora da referência/limites declarados.")
        for key,values in vectors.items():row[key]=[str(v) for v in values]
        for key in ("baseline","current"):row[key]=str(row[key])
        result.append(row);last[pid]=row
    return result

def context(suffix,decision,pid,rows):
    tokens=[part.split("=",1) for part in suffix.strip().split()];fields=dict(tokens)
    expected=set("atr_known atr_baseline atr_current atr_horizon atr_context".split())
    if len(tokens)!=len(fields) or set(fields)!=expected:raise ValueError("Contexto ATR de decisão ausente/truncado.")
    values={k:Decimal(v) for k,v in fields.items()}
    if not all(v.is_finite() for v in values.values()):raise ValueError("Contexto ATR não finito.")
    if values["atr_known"]==0:
        if any(values.values()):raise ValueError("ATR desconhecido contém valores presumidos.")
        return dict(atr_baseline=None,atr_current=None,atr_horizon_seconds=None),None,None
    available=[r for r in rows if r["position"]==pid and r["at_msc"]<=decision]
    if not available:raise ValueError("Decisão ATR sem observação causal anterior.")
    last=available[-1];pairs=dict(atr_baseline="baseline",atr_current="current",atr_horizon="horizon",atr_context="at_msc")
    if values["atr_known"]!=1 or any(values[k]!=Decimal(str(last[v])) for k,v in pairs.items()):
        raise ValueError("Decisão ATR diverge do último contexto conhecido; não usar futuro nem contexto antigo.")
    return dict(atr_baseline=last["baseline"],atr_current=last["current"],atr_horizon_seconds=last["horizon"],
        atr_context_known_msc=last["at_msc"],atr_seed=last["seed"],atr_baseline_known_msc=last["base_known"]),float(last["baseline"]),float(last["current"])
