from dataclasses import replace
import pandas as pd
import pytest
from mt5_extracao.strategies import ExitRules
from mt5_extracao.strategies.temporal_donchian import DonchianTemporalPolicy,observations,decide
from mt5_extracao.strategies.backtester import run_backtest
from mt5_extracao.trade_analytics.execution import engine
from tests.test_backtester import Manual,bars,SPEC,CFG
from tests.test_trade_resim import _ledger_rows

@pytest.mark.parametrize('idle,expected',[(59999,'hold'),(60000,'time_donchian'),(60001,'time_donchian')])
def test_idle_boundary_requires_return_and_preserves_old_deadline(idle,expected):
    p=DonchianTemporalPolicy(max_seconds=3600,channel_idle_seconds=60)
    assert p.decision(90000,0,idle_ms=idle,returned=True)==expected
    assert p.decision(90000,0,idle_ms=idle,returned=False)=='hold'
    assert p.decision(-1,0,idle_ms=idle,returned=True)=='unavailable'
    assert DonchianTemporalPolicy(max_seconds=1,channel_idle_seconds=60).decision(90000,None,idle_ms=idle,returned=True)=='time_absolute'

@pytest.mark.parametrize('direction',[1,-1])
def test_closed_breakout_return_and_existing_engines(direction):
    data=bars([[100,101,99,100]]*28);data['time']=pd.date_range('2026-10-14 10:00',periods=len(data),freq='min')
    data.loc[21,['high','close']]=[111,110]
    if direction<0:
        high=data.high.copy();data['open']=200-data.open;data['close']=200-data.close;data['high']=200-data.low;data['low']=200-high
    p=DonchianTemporalPolicy(max_seconds=3600,channel_idle_seconds=60)
    values=observations(p,data)
    assert decide(p,60000,0,values,20,direction=direction,opened_index=20)[0]=='hold'
    assert decide(p,120000,0,values,21,direction=direction,opened_index=20)[0]=='hold'
    decision,context=decide(p,180000,0,values,22,direction=direction,opened_index=20)
    assert decision=='time_donchian' and context['channel_idle_ms']==60000 and context['channel_returned']
    prefix=observations(p,data.iloc[:24]);assert decide(p,180000,0,prefix,22,direction=direction,opened_index=20)==(decision,context)
    strategy=Manual(long=[19] if direction>0 else [],short=[19] if direction<0 else [],stop={19:90 if direction>0 else 110},rules=ExitRules(temporal=p))
    cfg=replace(CFG,risk_per_trade=.001);result=run_backtest(data,strategy,SPEC,cfg,observation_timeframe='M1')
    assert result.trades[0].exit_time==data.time.iloc[23] and result.trades[0].exit_reason=='retorno ao canal sem renovação'
    sim=engine.simulate(_ledger_rows(result.trades),data,SPEC,cfg,engine.ExitPolicy.from_rules(strategy.rules),observation_timeframe='M1')
    assert sim.trades[0].exit_time==result.trades[0].exit_time and sim.trades[0].pnl==result.trades[0].pnl
    missing=data.copy();missing.loc[20,'high']=float('nan');bad=observations(p,missing)
    assert decide(p,180000,0,bad,22,direction=direction,opened_index=20)[0]=='hold'


def test_no_favorable_close_breakout_means_no_return_signal():
    data=bars([[100,101,99,100]]*25);data['time']=pd.date_range('2026-10-14',periods=25,freq='min')
    p=DonchianTemporalPolicy(max_seconds=3600,channel_idle_seconds=1)
    result,context=decide(p,999999,0,observations(p,data),24,direction=1,opened_index=20)
    assert result=='hold' and not context['channel_breakout_seen'] and context['channel_idle_ms'] is None


def test_idle_rule_without_absolute_ceiling_is_refused():
    with pytest.raises(ValueError,match='prazo absoluto'):DonchianTemporalPolicy(channel_idle_seconds=30)
