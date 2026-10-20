from dataclasses import replace
from decimal import Decimal
import math
import pandas as pd
import pytest
from mt5_extracao.strategies.temporal_er import EfficiencyTemporalPolicy, observation_values
from mt5_extracao.strategies.temporal import TemporalPolicy
from mt5_extracao.strategies import ExitRules
from mt5_extracao.strategies.backtester import run_backtest
from mt5_extracao.trade_analytics.execution import engine
from tests.test_backtester import Manual, bars, SPEC, CFG
from tests.test_trade_resim import _ledger_rows


@pytest.mark.parametrize('age,expected',[(9999,'hold'),(10000,'time_er'),(10001,'time_er')])
def test_minimum_age_uses_first_fill_and_er_can_act_without_pnl(age,expected):
    policy=EfficiencyTemporalPolicy(er_min_seconds=10,maximum_er=.4)
    assert policy.decision(age,0,er=.2)==expected
    if age>=10000:assert policy.decision(age,None,er=.2)==expected


@pytest.mark.parametrize('er',[None,float('nan'),float('inf'),-.1,1.1,.4,.4-5e-13,.4+5e-13])
def test_unknown_invalid_and_equality_do_not_become_inefficient_zero(er):
    p=EfficiencyTemporalPolicy(er_min_seconds=10,maximum_er=.4)
    assert p.decision(10000,0,er=er)=='hold'


@pytest.mark.parametrize('settings',[
    dict(er_min_seconds=-1),dict(er_min_seconds=True),dict(er_min_seconds=1.5),
    dict(maximum_er=True),dict(maximum_er=float('nan')),dict(maximum_er=-.1),dict(maximum_er=1.1)])
def test_typed_policy_rejects_invalid_units(settings):
    with pytest.raises(ValueError):EfficiencyTemporalPolicy(**settings)


def test_deadlines_and_financial_conditions_have_priority_and_cannot_be_extended():
    p=EfficiencyTemporalPolicy(max_seconds=10,er_min_seconds=5,maximum_er=1)
    assert p.decision(10000,None,er=0)=='time_absolute'
    assert p.decision(10000,None,er=None)=='time_absolute'
    p=EfficiencyTemporalPolicy(loss_seconds=10,er_min_seconds=5,maximum_er=1)
    assert p.decision(10000,-1,er=0)=='time_loss'
    assert EfficiencyTemporalPolicy(er_min_seconds=1,maximum_er=0).decision(1000,0,er=0)=='hold'
    assert TemporalPolicy().to_dict()=={k:v for k,v in EfficiencyTemporalPolicy().to_dict().items() if k not in ('er_min_seconds','maximum_er')}


def test_canonical_formula_independent_decimal_warmup_flat_missing_prefix_and_correction():
    from mt5_extracao.analytics import indicator_registry
    p=EfficiencyTemporalPolicy(er_min_seconds=1,maximum_er=1)
    values=[Decimal(100+i%3) for i in range(35)]
    data=pd.DataFrame(dict(close=[float(v) for v in values]))
    out=observation_values(p,data)
    assert out.iloc[:20].isna().all()
    for i in range(20,len(values)):
        window=values[i-20:i+1];path=sum(abs(b-a) for a,b in zip(window,window[1:]));ref=abs(window[-1]-window[0])/path if path else Decimal(0)
        assert math.isclose(out.iloc[i],float(ref),rel_tol=0,abs_tol=1e-12)
    prefix=out.iloc[:25].copy();extended=observation_values(p,pd.concat([data,pd.DataFrame(dict(close=[10000,1]))],ignore_index=True))
    pd.testing.assert_series_equal(prefix,extended.iloc[:25])
    corrected=data.copy();corrected.loc[10,'close']=110
    changed=observation_values(p,corrected);assert changed.iloc[20]!=out.iloc[20]
    pd.testing.assert_series_equal(changed.iloc[31:],out.iloc[31:])
    flat=observation_values(p,pd.DataFrame(dict(close=[100.]*22)));assert flat.iloc[20]==0
    missing=data.copy();missing.loc[10,'close']=float('nan');assert pd.isna(observation_values(p,missing).iloc[20])
    definition=indicator_registry.get_definition('trend.efficiency_ratio_20')
    assert definition['parameters']['period']==20 and definition['warmup_count']==21


@pytest.mark.parametrize('direction',[1,-1])
def test_er_runs_through_both_existing_engines_only_after_closed_bar(direction):
    rows=[[100,100.1,99.9,100]]*26
    data=bars(rows);data['time']=pd.date_range('2026-10-14 10:00',periods=len(data),freq='min')
    p=EfficiencyTemporalPolicy(er_min_seconds=60,maximum_er=.5)
    strategy=Manual(long=[19] if direction>0 else [],short=[19] if direction<0 else [],stop={19:90 if direction>0 else 110},rules=ExitRules(temporal=p))
    cfg=replace(CFG,risk_per_trade=.001)
    result=run_backtest(data,strategy,SPEC,cfg,observation_timeframe='M1')
    assert len(result.trades)==1 and result.trades[0].exit_time==data.time.iloc[21]
    assert result.trades[0].exit_reason=='eficiência baixa após idade mínima'
    trace=result.trajectory['temporal_decisions'][0];assert trace['age_ms']==60000 and trace['er_value']==0
    sim=engine.simulate(_ledger_rows(result.trades),data,SPEC,cfg,engine.ExitPolicy.from_rules(strategy.rules),observation_timeframe='M1')
    assert sim.trades[0].pnl==result.trades[0].pnl and sim.trades[0].exit_reason==result.trades[0].exit_reason
