import math
from decimal import Decimal,ROUND_CEILING
import pandas as pd
import pytest
from mt5_extracao.presets import pullback_events as events
from mt5_extracao.strategies import get_strategy,indicators

@pytest.fixture
def capture(tmp_path):
    rows=[]
    for i in range(600):
        c=round(100+.015*i+1.2*math.sin(i/7),5)
        rows.append(dict(at=1700000000+i*300,open=c,high=round(c+.1,5),low=round(c-.13,5),close=c))
    frame=pd.DataFrame(rows).set_index("at");frame["time"]=pd.to_datetime(frame.index,unit="s")
    strategy=get_strategy("pullback");signals=strategy.gerar_sinais(frame)
    ema=indicators.ema(frame.close,200);adx=indicators.adx(frame)["adx"];rsi=indicators.rsi(frame.close)
    index=next(i for i in range(300,600) if signals.entry_long.iloc[i])
    at=int(frame.index[index]);swing=signals.stop_long.iloc[index];quote=frame.close.iloc[index]+.01
    stop=int(((Decimal(str(quote))-Decimal(str(swing)))/Decimal(".01")-Decimal("1e-8")).to_integral_value(rounding=ROUND_CEILING))
    entry=f"PB_ENTRY event_at={at} direction=1 swing={swing} quote={quote} stop_points={stop} target_points={2*stop} ema={ema.iloc[index]} adx={adx.iloc[index]} rsi_previous={rsi.iloc[index-1]} rsi={rsi.iloc[index]} point=.01 tick=.01"
    lines=["PB_BAR "+" ".join(f"{k}={v}" for k,v in row.items()) for row in rows]
    directory=tmp_path/"Tester/Agent-local/logs";directory.mkdir(parents=True)
    log=directory/"trial.log";log.write_text("\n".join(lines+[entry]),encoding="utf-8")
    return tmp_path,log,strategy.params,[dict(event_at=at,direction=1,opened=False)],index,lines,entry

def test_reference_prefix_future_append_does_not_change_confirmed_decision(capture):
    root,log,params,signals,index,lines,entry=capture
    future=events.validate(root,params,signals,[])
    log.write_text("\n".join(lines[:index+1]+[entry]),encoding="utf-8")
    prefix=events.validate(root,params,signals,[])
    assert prefix["decisions"]==future["decisions"] and prefix["bars"]<future["bars"]

@pytest.mark.parametrize("fault",["missing_decision","missing_bars","nan","wrong_swing","wrong_grid","ambiguous_adx"])
def test_invalid_or_ambiguous_native_decision_cannot_qualify(capture,fault):
    root,log,params,signals,index,lines,entry=capture
    if fault=="missing_decision":entry=""
    if fault=="missing_bars":lines=[]
    if fault=="nan":entry=entry.replace("point=.01","point=nan")
    if fault=="wrong_swing":entry=entry.replace("swing=","swing=9")
    if fault=="wrong_grid":entry=entry.replace("tick=.01","tick=.02")
    if fault=="ambiguous_adx":
        params=dict(params);params["adx_min"]=float(entry.split(" adx=")[1].split()[0])-.00001
    log.write_text("\n".join(lines+[entry]),encoding="utf-8")
    with pytest.raises(ValueError):events.validate(root,params,signals,[])
