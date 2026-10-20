"""Regressão: uma intenção inviável não é redução confirmada nem autoriza BE."""
from dataclasses import replace
import pytest
from tests.test_backtester import Manual,bars,SPEC,CFG
from mt5_extracao.strategies import ExitRules
from mt5_extracao.strategies.backtester import run_backtest

@pytest.mark.parametrize('direction',[1,-1])
def test_indivisible_half_keeps_initial_protection_and_one_trade(direction):
    rows=[[100,101,99,100],[100,101,99,100],[100,111,99,110],[100,101,89,90]]
    if direction<0:rows=[[200-o,200-l,200-h,200-c] for o,h,l,c in rows]
    strategy=Manual(long=[0] if direction>0 else [],short=[0] if direction<0 else [],
                    stop={0:90 if direction>0 else 110},
                    rules=ExitRules(partial_r=1,partial_fraction=.5,breakeven_after_partial=True))
    result=run_backtest(bars(rows),strategy,SPEC,replace(CFG,risk_per_trade=.001))
    assert len(result.trades)==1
    trade=result.trades[0]
    assert trade.volume==1 and trade.exit_reason=='stop'
    assert trade.exit_price==(90 if direction>0 else 110) and trade.pnl==-10
