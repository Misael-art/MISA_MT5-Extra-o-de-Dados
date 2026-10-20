from dataclasses import replace
import pytest
from tests.test_backtester import Manual,bars,SPEC,CFG
from tests.test_trade_resim import _ledger_rows
from mt5_extracao.strategies import ExitRules
from mt5_extracao.strategies.temporal import TemporalPolicy
from mt5_extracao.strategies.backtester import run_backtest
from mt5_extracao.trade_analytics.execution import engine

@pytest.mark.parametrize("direction",[1,-1])
def test_decision_at_close_executes_at_next_open_and_resimulation_agrees(direction):
    rows=[[100,101,99,100],[100,102,99,101],[105,106,104,105],[110,111,109,110]]
    if direction<0:rows=[[200-o,200-l,200-h,200-c] for o,h,l,c in rows]
    rules=ExitRules(temporal=TemporalPolicy(max_seconds=60))
    strategy=Manual(long=[0] if direction>0 else [],short=[0] if direction<0 else [],stop={0:90 if direction>0 else 110},rules=rules)
    data=bars(rows);cfg=replace(CFG,risk_per_trade=.001)
    result=run_backtest(data,strategy,SPEC,cfg,observation_timeframe="M1")
    t=result.trades[0];assert t.exit_time==data.time.iloc[2] and t.exit_price==(105 if direction>0 else 95)
    assert t.exit_reason=="prazo absoluto"
    trace=result.trajectory["temporal_decisions"]
    assert trace[0]["event"]=="decision" and trace[1]["event"]=="execution"
    assert trace[0]["available_at"]<=trace[1]["available_at"]
    policy=engine.ExitPolicy.from_rules(rules)
    sim=engine.simulate(_ledger_rows(result.trades),data,SPEC,cfg,policy,observation_timeframe="M1")
    assert sim.trades[0].pnl==t.pnl and sim.trades[0].exit_reason==t.exit_reason and sim.trades[0].exit_price==t.exit_price

@pytest.mark.parametrize("opening,reason",[(85,"stop"),(125,"alvo")])
def test_server_price_protection_in_gap_precedes_pending_temporal_market(opening,reason):
    data=bars([[100,101,99,100],[100,101,99,100],[opening,opening+1,opening-1,opening]])
    strategy=Manual(long=[0],stop={0:90},rules=ExitRules(target_r=2,temporal=TemporalPolicy(max_seconds=60)))
    out=run_backtest(data,strategy,SPEC,replace(CFG,risk_per_trade=.001),observation_timeframe="M1")
    assert out.trades[0].exit_price==opening and out.trades[0].exit_reason==reason

def test_legacy_max_bars_still_closes_at_same_bar_close():
    data=bars([[100,101,99,100],[100,102,99,101],[105,106,104,105]])
    strategy=Manual(long=[0],stop={0:90},rules=ExitRules(max_bars=1,temporal=TemporalPolicy(max_seconds=60)))
    out=run_backtest(data,strategy,SPEC,replace(CFG,risk_per_trade=.001),observation_timeframe="M1")
    assert out.trades[0].exit_price==101 and out.trades[0].exit_reason=="tempo máximo"
    assert not out.trajectory["temporal_decisions"]

def test_temporal_without_timeframe_or_with_grid_is_explicitly_unavailable():
    data=bars([[100,101,99,100]]*4);strategy=Manual(long=[0],stop={0:90},rules=ExitRules(temporal=TemporalPolicy(max_seconds=60)))
    with pytest.raises(ValueError,match="timeframe"):run_backtest(data,strategy,SPEC,CFG)
    rows=_ledger_rows(run_backtest(data,strategy,SPEC,CFG,observation_timeframe="M1").trades)
    with pytest.raises(ValueError,match="gradiente"):
        engine.simulate(rows,data,SPEC,CFG,engine.ExitPolicy(temporal=TemporalPolicy(max_seconds=60),grid_levels=1),observation_timeframe="M1")


@pytest.mark.parametrize("direction",[1,-1])
def test_partial_cash_entry_cost_and_remaining_volume_enter_progress_pnl(direction):
    rows=[[100,101,99,100],[100,111,99,105],[105,106,101,102],[103,104,101,103]]
    if direction<0:rows=[[200-o,200-l,200-h,200-c] for o,h,l,c in rows]
    data=bars(rows)
    # Fonte M1 explícita, sem converter a grade da fixture por inferência.
    import pandas as pd
    data['time']=pd.date_range('2026-10-14 10:00',periods=len(data),freq='min')
    rules=ExitRules(partial_r=1,partial_fraction=.5,breakeven_after_partial=True,
                    temporal=TemporalPolicy(progress_seconds=60,minimum_profit=10))
    strategy=Manual(long=[0] if direction>0 else [],short=[0] if direction<0 else [],stop={0:90 if direction>0 else 110},rules=rules)
    cfg=replace(CFG,risk_per_trade=.002,commission_per_lot=1)
    result=run_backtest(data,strategy,SPEC,cfg,observation_timeframe='M1')
    decisions=result.trajectory['temporal_decisions'];first=decisions[0]
    assert first['age_ms']==120000 and first['net_observed']==9
    assert result.trades[0].exit_reason=='prazo sem lucro mínimo' and result.trades[0].pnl==9
    again=engine.simulate(_ledger_rows(result.trades),data,SPEC,cfg,engine.ExitPolicy.from_rules(rules),observation_timeframe='M1')
    assert again.trades[0].pnl==9 and again.trajectory['temporal_decisions'][0]['age_ms']==120000


def test_pending_temporal_target_gap_keeps_existing_no_extra_target_slippage():
    data=bars([[100,101,99,100],[100,102,99,101],[125,126,124,125]])
    strategy=Manual(long=[0],stop={0:90},rules=ExitRules(target_r=2,temporal=TemporalPolicy(max_seconds=60)))
    result=run_backtest(data,strategy,SPEC,replace(CFG,risk_per_trade=.002,slippage_points=1),observation_timeframe='M1')
    assert result.trades[0].exit_reason=='alvo' and result.trades[0].exit_price==125


@pytest.mark.parametrize('direction',[1,-1])
def test_observed_swap_is_included_before_time_loss_decision(direction):
    import pandas as pd
    data=bars([[100,101,99,100]]*5)
    data['time']=pd.date_range('2026-10-14 23:58',periods=5,freq='min')
    rules=ExitRules(temporal=TemporalPolicy(gain_seconds=60,loss_seconds=60))
    strategy=Manual(long=[0] if direction>0 else [],short=[0] if direction<0 else [],stop={0:90 if direction>0 else 110},rules=rules)
    spec=replace(SPEC,swap_mode=1,swap_long=-1,swap_short=-1,swap_triple_day=3)
    cfg=replace(CFG,risk_per_trade=.001,swap=True)
    result=run_backtest(data,strategy,spec,cfg,observation_timeframe='M1')
    trace=result.trajectory['temporal_decisions']
    assert trace[0]['net_observed']==-3 and trace[0]['age_ms']==120000
    assert result.trades[0].pnl==-3 and result.trades[0].exit_reason=='prazo de perda'
