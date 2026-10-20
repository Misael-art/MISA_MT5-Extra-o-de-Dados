"""T19.19 inicial: SL no fill antes do lote, sem nível futuro nos sinais."""
from dataclasses import replace
from io import StringIO

import pandas as pd
import pytest

from mt5_extracao.composition import BaseAdapter, Composition, base_composition
from mt5_extracao.strategies.base import ExitRules, Strategy
from mt5_extracao.strategies.backtester import BacktestConfig, SymbolSpec, run_backtest


SPEC = SymbolSpec(symbol="TEST",point=1,tick_size=1,tick_value=1,
                  volume_min=.01,volume_step=.01,volume_max=100)
CFG = BacktestConfig(initial_equity=1000,risk_per_trade=.01,slippage_points=1,swap=False)


class NoPrematureStop(Strategy):
    key = "causal_stop_reference"
    def __init__(self,direction=1,rules=None):
        self.params={};self.direction=direction;self.rules=rules or ExitRules()
    def describe(self):
        return "referência de SL no fill"
    def exit_rules(self):
        return self.rules
    def _signals(self,df,out):
        out.loc[0,"entry_long" if self.direction>0 else "entry_short"]=True


def bars(direction=1):
    values=[(100,101,99,100),(110,120,108,115),(115,124,112,120)] if direction>0 else [
        (100,101,99,100),(90,95,85,88),(88,91,75,80)]
    result=pd.DataFrame(values,columns=["open","high","low","close"])
    result["time"]=pd.date_range("2026-01-01",periods=len(result),freq="min")
    result["spread"]=2
    return result


@pytest.mark.parametrize("direction,entry,stop,target",[(1,113,103,123),(-1,89,99,79)])
def test_fill_anchored_stop_precedes_sizing_without_backdating_level(direction,entry,stop,target):
    strategy=NoPrematureStop(direction,ExitRules(stop_points=10,target_points=10))
    signals=strategy.gerar_sinais(bars(direction))
    assert bool(signals.loc[0,"entry_long" if direction>0 else "entry_short"])
    assert signals[["stop_long","stop_short"]].isna().all().all()
    result=run_backtest(bars(direction),strategy,SPEC,CFG)
    trade,=result.trades
    assert (trade.entry_price,trade.stop_price,trade.target_price)==(entry,stop,target)
    assert trade.volume==1 and abs(entry-stop)*SPEC.money_per_price_unit*trade.volume==10
    assert trade.exit_price==target and trade.exit_reason=="alvo"
    exported=pd.read_csv(StringIO(result.trades_frame().to_csv(index=False)))
    assert exported.loc[0,"stop_price"]==stop and exported.loc[0,"target_price"]==target


def test_missing_stop_remains_inoperable_and_typed_override_enables_only_protected_fill():
    strategy=NoPrematureStop()
    assert not strategy.gerar_sinais(bars()).entry_long.any()
    assert run_backtest(bars(),strategy,SPEC,CFG).trades==[]
    rules=ExitRules(stop_points=10,target_points=10)
    assert run_backtest(bars(),strategy,SPEC,CFG,exit_rules_override=rules).trades[0].stop_price==103
    for operation in (lambda:strategy.gerar_sinais(bars(),exit_rules_override={}),
                      lambda:run_backtest(bars(),strategy,SPEC,CFG,exit_rules_override={})):
        with pytest.raises(ValueError,match="tipadas"):
            operation()


@pytest.mark.parametrize("value",[0,-1,True,float("nan"),float("inf"),"10"])
def test_stop_distance_must_be_positive_finite_typed(value):
    with pytest.raises(ValueError,match="Stop fixo.*positivos e finitos"):
        ExitRules(stop_points=value)


def test_missing_point_and_non_tick_distance_never_produce_unprotected_trade():
    for spec in (replace(SPEC,known=False),replace(SPEC,point=0)):
        with pytest.raises(ValueError,match="especificação|contrato|point|tick"):
            run_backtest(bars(),NoPrematureStop(rules=ExitRules(stop_points=10)),spec,CFG)
    with pytest.raises(ValueError,match="múltiplo"):
        run_backtest(bars(),NoPrematureStop(rules=ExitRules(stop_points=.5)),SPEC,CFG)


def test_future_bars_do_not_rewrite_trade_already_closed():
    strategy=NoPrematureStop(rules=ExitRules(stop_points=10,target_points=10))
    prefix=bars();original=run_backtest(prefix,strategy,SPEC,CFG)
    future=pd.concat([prefix,pd.DataFrame([{"time":pd.Timestamp("2026-01-01 00:03"),
                      "open":120,"high":200,"low":1,"close":150,"spread":2}])],ignore_index=True)
    assert run_backtest(future,strategy,SPEC,CFG).trades==original.trades


def test_existing_trailing_never_relaxes_fixed_initial_protection():
    class WithAtr(NoPrematureStop):
        def _signals(self,df,out):
            super()._signals(df,out)
            out["atr"]=[2,2,10,10]
    frame=bars();frame=pd.concat([frame,pd.DataFrame([{"time":pd.Timestamp("2026-01-01 00:03"),
                      "open":112,"high":118,"low":109,"close":113,"spread":2}])],ignore_index=True)
    trade,=run_backtest(frame,WithAtr(rules=ExitRules(stop_points=10,trailing_atr=2)),SPEC,CFG).trades
    # Entrada113/SL103; trailing115-4=111; ATR10 não relaxa para100; sai111-slip=110.
    assert trade.stop_price==103 and trade.exit_reason=="stop móvel" and trade.exit_price==110


def test_composition_combines_fixed_gain_and_stop_without_a_parallel_engine(monkeypatch):
    data=base_composition("ema_atr",symbol="TEST",timeframe="M1").to_dict()
    data["protection"]={"mode":"fixed_points","params":{"stop_points":10}}
    data["profit_exit"]={"mode":"fixed_points","params":{"target_points":10}}
    monkeypatch.setattr(BaseAdapter,"strategy",lambda self:NoPrematureStop())
    comp=Composition(data)
    trade,=BaseAdapter(comp).run(bars(),SPEC,CFG,timeframe="M1").trades
    assert (trade.entry_price,trade.stop_price,trade.target_price)==(113,103,123)
    blocked=base_composition("pullback",symbol="TEST",timeframe="M1").to_dict()
    blocked["protection"]=data["protection"]
    with pytest.raises(ValueError,match="Pullback"):
        BaseAdapter(Composition(blocked))
