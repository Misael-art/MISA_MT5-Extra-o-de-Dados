from dataclasses import replace
from decimal import Decimal, ROUND_CEILING
import math
import pandas as pd
import pytest
from mt5_extracao.strategies.temporal_atr import AtrTemporalPolicy, observations, decide
from mt5_extracao.strategies import ExitRules
from mt5_extracao.strategies.backtester import run_backtest
from mt5_extracao.trade_analytics.execution import engine
from tests.test_backtester import Manual,bars,SPEC,CFG
from tests.test_trade_resim import _ledger_rows

@pytest.mark.parametrize("current,expected",[(2,10),(4,5),(.1,100),(1e300,2),(0,None),(float("nan"),None),(float("inf"),None)])
def test_horizon_independent_reference_and_boundaries(current,expected):
 p=AtrTemporalPolicy(max_seconds=100,atr_base_seconds=10,atr_min_seconds=2)
 assert p.horizon(2,current)==expected
 if expected is not None:
  reference=max(2,min(100,int((Decimal(10)*Decimal(2)/Decimal(str(current))-Decimal("1e-9")).to_integral_value(rounding=ROUND_CEILING))))
  assert expected==reference
  assert p.decision(expected*1000-1,-100,atr_baseline=2,atr_current=current)=="hold"
  assert p.decision(expected*1000,-100,atr_baseline=2,atr_current=current)==("time_absolute" if expected==100 else "time_atr")
  assert p.decision(expected*1000+1,-100,atr_baseline=2,atr_current=current)==("time_absolute" if expected==100 else "time_atr")
 assert p.decision(100000,None,atr_baseline=None,atr_current=None)=="time_absolute"

@pytest.mark.parametrize("params",[dict(),dict(max_seconds=5,atr_base_seconds=10,atr_min_seconds=1),dict(max_seconds=100,atr_base_seconds=10,atr_min_seconds=11),dict(max_seconds=100,atr_base_seconds=10),dict(max_seconds=100,atr_base_seconds=True,atr_min_seconds=1)])
def test_refuses_missing_or_inconsistent_bounds(params):
 if not params:params=dict(atr_base_seconds=10,atr_min_seconds=1)
 with pytest.raises(ValueError):AtrTemporalPolicy(**params)

@pytest.mark.parametrize("side",[1,-1])
def test_frozen_seed_closed_context_future_prefix_and_two_engines(side):
 data=bars([[100,101,99,100]]*258);data["time"]=pd.date_range("2026-10-14",periods=len(data),freq="min")
 data.loc[251,["high","low"]]=[115,85] # ATR dobrado a partir da mesma semente
 p=AtrTemporalPolicy(max_seconds=600,atr_base_seconds=300,atr_min_seconds=60)
 values=observations(p,data)
 result,meta=decide(p,180000,-1,values,252,direction=side,opened_index=250)
 assert result=="time_atr" and meta["atr_baseline"]==2
 assert meta["atr_current"]==pytest.approx(float(Decimal(4)*Decimal(13)/14+Decimal(2)/14))
 assert meta["atr_horizon_seconds"]==156
 prefix=observations(p,data.iloc[:253]);assert decide(p,180000,-1,prefix,252,direction=side,opened_index=250)==(result,meta)
 strategy=Manual(long=[249] if side>0 else [],short=[249] if side<0 else [],stop={249:50 if side>0 else 150},rules=ExitRules(temporal=p))
 cfg=replace(CFG,risk_per_trade=.005);out=run_backtest(data,strategy,SPEC,cfg,observation_timeframe="M1")
 assert out.trades[0].exit_reason=="horizonte ATR limitado" and out.trades[0].exit_time==data.time.iloc[253]
 sim=engine.simulate(_ledger_rows(out.trades),data,SPEC,cfg,engine.ExitPolicy.from_rules(strategy.rules),observation_timeframe="M1")
 assert sim.trades[0].exit_time==out.trades[0].exit_time and sim.trades[0].pnl==out.trades[0].pnl
 for field in ("high","low","close"):
  missing=data.copy();missing.loc[245,field]=float("nan")
  actual,ctx=decide(p,180000,-1,observations(p,missing),252,direction=side,opened_index=250)
  assert actual=="hold" and ctx["atr_horizon_seconds"] is None
 assert decide(p,180000,0,observations(p,data),252,direction=side,opened_index=249)[1]["atr_horizon_seconds"] is None
