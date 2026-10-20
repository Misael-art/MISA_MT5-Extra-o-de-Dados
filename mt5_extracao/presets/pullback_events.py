"""Confere o sinal Pullback observado contra a implementação Python existente.

O prefixo OHLC começa na mesma âncora fechada do Expert; não certifica a fonte
nem autoriza cache. S09_* é o contrato de eventos v1 compartilhado, preservado.
"""
import re,math
from decimal import Decimal,ROUND_CEILING
from pathlib import Path
import pandas as pd
from ..robots import mql
from ..strategies import get_strategy,indicators
BAR=re.compile(r"PB_BAR at=(\d+) open=([^ ]+) high=([^ ]+) low=([^ ]+) close=([^\s]+)")
ENTRY=re.compile(r"PB_ENTRY event_at=(\d+) direction=(-?\d+) swing=([^ ]+) quote=([^ ]+) stop_points=(\d+) target_points=(\d+) ema=([^ ]+) adx=([^ ]+) rsi_previous=([^ ]+) rsi=([^ ]+) point=([^ ]+) tick=([^\s]+)")

def validate(directory,params,signals,ledger):
    text="\n".join(mql.read_text(p) for p in (Path(directory)/"Tester").glob("Agent-*/logs/*.log"))
    bars=[(int(at),*map(float,values)) for at,*values in BAR.findall(text)]
    entries=ENTRY.findall(text)
    if (not bars or len({r[0] for r in bars})!=len(bars) or bars!=sorted(bars)
        or not all(math.isfinite(v) and v>0 for r in bars for v in r[1:])
        or any(r[2]<max(r[1],r[3],r[4]) or r[3]>min(r[1],r[2],r[4]) for r in bars)):
        raise ValueError("Pullback: barras fechadas de referência ausentes/duplicadas/inválidas; preserve o diário.")
    if len(entries)!=len(signals):raise ValueError("Pullback: sinal sem decisão da estratégia observada.")
    frame=pd.DataFrame(bars,columns=["at","open","high","low","close"]).set_index("at")
    frame["time"]=pd.to_datetime(frame.index,unit="s")
    strategy=get_strategy("pullback",**params)
    expected=strategy.gerar_sinais(frame)
    ema=indicators.ema(frame.close,params["trend"]);adx=indicators.adx(frame,14)["adx"]
    rsi=indicators.rsi(frame.close,params["rsi_period"])
    filled=[s for s in signals if s["opened"]];checked=[]
    for raw,signal in zip(entries,signals):
        if not all(math.isfinite(float(v)) for v in raw):raise ValueError("Pullback: decisão contém número não finito.")
        at,direction=int(raw[0]),int(raw[1]);swing,quote=float(raw[2]),float(raw[3])
        if at!=signal["event_at"] or direction!=signal["direction"] or at not in frame.index:
            raise ValueError("Pullback: origem/direção diverge do sinal nativo.")
        index=frame.index.get_loc(at)
        row=expected.loc[at];side="long" if direction==1 else "short"
        if index<299 or not row["entry_"+side] or abs(swing-row["stop_"+side])>1e-8:
            raise ValueError("Pullback: entrada/swing diverge da referência Python causal.")
        actual=list(map(float,raw[6:10]));reference=[ema.iloc[index],adx.iloc[index],rsi.iloc[index-1],rsi.iloc[index]]
        # Índice ADX 0..100: tolerância absoluta 1e-4 entre runtimes; os demais
        # usam 1e-7. Próximo ao limiar não converter incerteza em aceite.
        tolerances=(1e-7,1e-4,1e-7,1e-7)
        if (abs(reference[1]-params["adx_min"])<=tolerances[1] or
            any(abs(a-b)>limit for a,b,limit in zip(actual,reference,tolerances))):
            raise ValueError("Pullback: EMA/ADX/RSI divergem da referência.")
        point,tick=map(Decimal,raw[10:12]);distance=(Decimal(raw[3])-Decimal(raw[2]))*direction
        if point<=0 or tick<=0 or distance<=0:raise ValueError("Pullback: grade/distância inválida.")
        slticks=(distance/tick-Decimal("1e-8")).to_integral_value(rounding=ROUND_CEILING)
        tpticks=(slticks*Decimal(str(params["target_r"]))-Decimal("1e-8")).to_integral_value(rounding=ROUND_CEILING)
        stop_points,target_points=int(raw[4]),int(raw[5])
        if Decimal(stop_points)*point!=slticks*tick or Decimal(target_points)*point!=tpticks*tick:
            raise ValueError("Pullback: SL/TP diverge do swing/R arredondado para fora na grade.")
        if signal["opened"]:
            position=ledger[filled.index(signal)];sl=quote-direction*stop_points*float(point);tp=quote+direction*target_points*float(point)
            if abs(position["stop_price"]-sl)>float(point)/2 or abs(position["target_price"]-tp)>float(point)/2:
                raise ValueError("Pullback: SL/TP do livro difere do plano estrutural observado.")
        checked.append(dict(event_at=at,direction=direction,swing=swing,quote=quote,stop_points=stop_points,target_points=target_points))
    return dict(strategy="pullback",reference="existing Python EMA/ADX/RSI on observed closed prefix",bars=len(bars),decisions=checked,source_qualified=False)
