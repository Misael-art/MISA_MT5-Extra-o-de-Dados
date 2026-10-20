from dataclasses import replace
import pytest
from mt5_extracao.strategies.temporal import TemporalPolicy,elapsed_ms
from mt5_extracao.trade_analytics.trajectories.clocks import Clock

@pytest.mark.parametrize("net,reason",[(1,"time_gain"),(-1,"time_loss"),(0,"time_tie"),(.01,"time_tie"),(-.01,"time_tie")])
def test_deadline_is_inclusive_and_band_covers_equalities(net,reason):
    p=TemporalPolicy(gain_seconds=10,loss_seconds=10,tie_seconds=10,flat_band=.01)
    assert p.decision(9999,net)=="hold"
    assert p.decision(10000,net)==reason and p.decision(10001,net)==reason

def test_absolute_progress_priority_missing_and_minimum_equality():
    p=TemporalPolicy(max_seconds=30,progress_seconds=10,minimum_profit=2,gain_seconds=20,loss_seconds=20,tie_seconds=20)
    assert p.decision(9999,-1)=="hold" and p.decision(10000,1.99)=="time_progress"
    assert p.decision(10000,2)=="hold" and p.decision(20000,2)=="time_gain"
    assert p.decision(30000,None)=="time_absolute" and p.decision(29999,None)=="unavailable"
    assert p.decision(-1,2)=="unavailable"

@pytest.mark.parametrize("kwargs",[dict(max_seconds=-1),dict(gain_seconds=True),dict(money_digits=9),dict(minimum_profit=.001),dict(flat_band=float('nan'))])
def test_invalid_parameters_are_not_silently_coerced(kwargs):
    with pytest.raises(ValueError):TemporalPolicy(**kwargs)

def test_declared_clock_includes_gaps_and_never_resets_after_partial():
    clock=Clock("broker",3)
    assert elapsed_ms("2026-10-14 23:59:59","2026-10-15 00:00:01",clock)==2000
    assert elapsed_ms("2026-10-09 23:59:59","2026-10-12 00:00:01",clock)==172802000
    with pytest.raises(ValueError):elapsed_ms("2026-10-15","2026-10-14",clock)
    p=TemporalPolicy(loss_seconds=60)
    assert p.decision(60000,-1)=="time_loss" # quantidade parcial não é argumento nem reset
