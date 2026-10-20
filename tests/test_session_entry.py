from dataclasses import replace
from datetime import datetime, timezone
import pandas as pd
import pytest
from mt5_extracao.strategies.session_entry import SessionEntryPolicy, source_seconds
from mt5_extracao.strategies.backtester import run_backtest
from mt5_extracao.strategies import ExitRules
from tests.test_backtester import Manual, bars, SPEC, CFG


def decide(policy, clock, cycles=0):
    return policy.decide(source_seconds(datetime.fromisoformat(clock)), cycles)


@pytest.mark.parametrize("clock,reason",[
    ("2026-01-01 08:59:59","outside_window"), ("2026-01-01 09:00:00","allowed"),
    ("2026-01-01 09:29:59","allowed"), ("2026-01-01 09:30:00","excluded_window"),
    ("2026-01-01 09:40:00","allowed"), ("2026-01-01 10:00:00","outside_window")])
def test_half_open_boundaries(clock, reason):
    policy=SessionEntryPolicy(start_minute=540,end_minute=600,exclude_start_minute=570,exclude_end_minute=580)
    assert decide(policy,clock)["reason"]==reason


def test_overnight_and_weekday_belong_to_anchored_session():
    policy=SessionEntryPolicy(start_minute=23*60,end_minute=60,anchor_minute=17*60,weekdays=1<<3,max_cycles=3)
    assert decide(policy,"2026-01-01 23:00")["allowed"]
    assert decide(policy,"2026-01-02 00:59:59",2)["allowed"]
    assert decide(policy,"2026-01-02 00:59:59",3)["reason"]=="quota"
    assert decide(policy,"2026-01-02 01:00")["reason"]=="outside_window"
    assert decide(policy,"2026-01-02 23:00")["reason"]=="weekday"
    assert decide(policy,"2026-01-02 00:00")["session_start"]==source_seconds(datetime(2026,1,1,17))


@pytest.mark.parametrize("value",[-1,True,1440,float("nan")])
def test_invalid_minutes_never_coerce(value):
    with pytest.raises(ValueError):SessionEntryPolicy(start_minute=value)


def test_unknown_count_clock_and_dst_are_not_fabricated():
    assert decide(SessionEntryPolicy(),"2026-01-01",-1)["reason"]=="history_unavailable"
    with pytest.raises(ValueError):source_seconds(datetime(2026,1,1,tzinfo=timezone.utc))
    with pytest.raises(ValueError):SessionEntryPolicy(weekdays=0)


@pytest.mark.parametrize("quota",[1,3,7])
def test_one_three_n_limit_is_inclusive(quota):
    policy=SessionEntryPolicy(max_cycles=quota)
    assert decide(policy,"2026-01-01",quota-1)["allowed"]
    assert decide(policy,"2026-01-01",quota)["reason"]=="quota"


def test_actual_fill_only_consumes_quota_and_exit_management_continues_after_window():
    # Primeiro sinal abre além do stop (sem fill); o segundo preenche e deve poder sair após 09:03.
    data=bars([[100,101,99,100]]*7,start="2026-01-01 09:00",freq="min")
    data.loc[1,"open"]=80
    strategy=Manual(long=[0,1,4],stop={0:90,1:90,4:90},exit_long=[3])
    policy=SessionEntryPolicy(start_minute=540,end_minute=543,max_cycles=1)
    result=run_backtest(data,strategy,SPEC,CFG,entry_policy=policy,entry_time_basis="broker")
    assert result.skipped_gap==1 and len(result.trades)==1
    assert result.trades[0].entry_time==data.time.iloc[2] and result.trades[0].exit_time==data.time.iloc[4]
    fills=[r for r in result.trajectory["session_events"] if r["event"]=="fill"]
    assert len(fills)==1 and fills[0]["cycles"]==1


def test_quota_day_reset_partial_and_causality():
    data=bars([[100,111,100,105]]*8,start="2026-01-01 23:56",freq="min")
    strategy=Manual(long=range(7),stop={i:90 for i in range(7)},rules=ExitRules(max_bars=2,partial_r=1,partial_fraction=.5,breakeven_after_partial=False))
    policy=SessionEntryPolicy(max_cycles=1)
    result=run_backtest(data,strategy,SPEC,CFG,entry_policy=policy,entry_time_basis="broker")
    assert len(result.trades)==2
    assert any(r["reason"]=="quota" for r in result.trajectory["session_events"] if r["event"]=="check")
    fills=[r for r in result.trajectory["session_events"] if r["event"]=="fill"]
    assert [r["cycles"] for r in fills]==[1,1]
    prefix_strategy=Manual(long=range(3),stop={i:90 for i in range(3)},rules=strategy.rules)
    prefix=run_backtest(data.iloc[:4],prefix_strategy,SPEC,CFG,entry_policy=policy,entry_time_basis="broker")
    cutoff=source_seconds(data.time.iloc[3])
    assert [r for r in result.trajectory["session_events"] if r["at"]<=cutoff]==prefix.trajectory["session_events"]


def test_repeated_or_regressing_source_clock_is_refused():
    data=bars([[100,101,99,100]]*3,start="2026-01-01",freq="min")
    data.loc[1,"time"]=data.time.iloc[0]
    with pytest.raises(ValueError,match="Relógio"):
        run_backtest(data,Manual(),SPEC,CFG,entry_policy=SessionEntryPolicy(),entry_time_basis="broker")


@pytest.mark.parametrize("basis",[None,"utc","local"])
def test_backtest_requires_declared_broker_clock(basis):
    data=bars([[100,101,99,100]]*3,start="2026-01-01",freq="min")
    with pytest.raises(ValueError,match="entry_time_basis"):
        run_backtest(data,Manual(),SPEC,CFG,entry_policy=SessionEntryPolicy(),entry_time_basis=basis)


def test_anchor_matches_observed_calendar_without_resetting_quota_on_a_gap():
    from mt5_extracao.analytics.calendar import sessionize_m1
    data=bars([[100,101,99,100]]*3,start="2026-01-01 17:00",freq="min")
    data.loc[2,"time"]=pd.Timestamp("2026-01-01 20:00")
    _,sessions=sessionize_m1(data,"X",session_anchor="17:00")
    assert len(sessions)==2  # segmentos observados separados pelo gap, no mesmo dia lógico
    policy=SessionEntryPolicy(anchor_minute=17*60,max_cycles=1)
    for session in sessions:
        at=source_seconds(session["open_broker_time"])
        assert policy.session_start(at)==source_seconds(session["daily_start_broker_time"])
        assert policy.decide(at,1)["reason"]=="quota"
