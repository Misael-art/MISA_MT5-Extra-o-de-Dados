from dataclasses import replace
from datetime import datetime
import pytest
from mt5_extracao.strategies.cooldown import CooldownPolicy,from_filters
from mt5_extracao.strategies.session_entry import source_seconds
from mt5_extracao.strategies.backtester import run_backtest
from mt5_extracao.strategies import ExitRules
from tests.test_backtester import Manual,bars,SPEC,CFG

@pytest.mark.parametrize("delta,reason",[(0,"cooldown"),(59999,"cooldown"),(60000,"allowed"),(60001,"allowed")])
def test_millisecond_inclusive_boundary(delta,reason):
    assert CooldownPolicy(60).decide(100000+delta,100000)["reason"]==reason

@pytest.mark.parametrize("seconds",[True,0,-1,1.5,2147483648,float("nan")])
def test_seconds_not_coerced(seconds):
    with pytest.raises(ValueError):CooldownPolicy(seconds)

def test_absence_and_no_prior_close():
    policy=CooldownPolicy(60)
    assert policy.decide(100000,0)["allowed"]
    assert policy.decide(100000,None)["reason"]=="history_unavailable"
    assert policy.decide(100000,100001)["reason"]=="history_unavailable"
    with pytest.raises(ValueError):from_filters([{"mode":"cooldown_v1","params":{"seconds":60}}]*2)

def simulate(data, strategy, seconds=120):
    return run_backtest(data,strategy,SPEC,CFG,cooldown=CooldownPolicy(seconds),entry_time_basis="broker",observation_timeframe="M1")

def test_intrabar_close_uses_known_bar_end_and_partial_does_not_start_wait():
    data=bars([[100,101,99,100]]*9,start="2026-01-01 09:00",freq="min")
    data.loc[1,"high"]=111 # parcial, sem fechamento
    data.loc[3,"high"]=121 # alvo total, intrabar: conhecido somente09:04
    strategy=Manual(long=range(8),stop={i:90 for i in range(8)},rules=ExitRules(target_r=2,partial_r=1,partial_fraction=.5,breakeven_after_partial=False))
    result=simulate(data,strategy)
    rows=result.trajectory["cooldown_events"]
    assert len(result.trades)==2
    assert result.trades[0].entry_time==data.time.iloc[1]
    assert result.trades[1].entry_time==data.time.iloc[6] # 09:04 +120s
    assert [r["reason"] for r in rows]==["allowed","cooldown","cooldown","allowed"]
    known=source_seconds(datetime(2026,1,1,9,4))*1000
    assert rows[1]["last_close_msc"]==known
    assert result.trades[0].exit_time==data.time.iloc[3] # campo legado não vira tick exato
    assert any(e["kind"]=="partial" for e in result.trajectory["records"])

def test_signal_close_at_open_can_start_wait_immediately_and_future_bars_do_not_change_decisions():
    data=bars([[100,101,99,100]]*9,start="2026-01-01 09:00",freq="min")
    strategy=Manual(long=range(8),stop={i:90 for i in range(8)},exit_long=[2])
    result=simulate(data,strategy)
    assert result.trades[0].exit_time==data.time.iloc[3]
    assert result.trades[1].entry_time==data.time.iloc[5]
    prefix=simulate(data.iloc[:6],Manual(long=range(5),stop={i:90 for i in range(5)},exit_long=[2]))
    cutoff=source_seconds(data.time.iloc[5])*1000
    assert [r for r in result.trajectory["cooldown_events"] if r["at_msc"]<=cutoff]==prefix.trajectory["cooldown_events"]

def test_missing_timeframe_or_clock_requires_action():
    data=bars([[100,101,99,100]]*3,start="2026-01-01",freq="min")
    with pytest.raises(ValueError,match="timeframe"):
        run_backtest(data,Manual(),SPEC,CFG,cooldown=CooldownPolicy(60),entry_time_basis="broker")
    with pytest.raises(ValueError,match="entry_time_basis"):
        run_backtest(data,Manual(),SPEC,CFG,cooldown=CooldownPolicy(60),observation_timeframe="M1")
