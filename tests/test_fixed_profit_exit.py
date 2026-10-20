"""T19.18 inicial: alvo fixo conhecido no fill, pelo motor/custos/CSV existentes."""
from dataclasses import replace

import pandas as pd
import pytest

from mt5_extracao.composition import BaseAdapter, Composition, base_composition
from mt5_extracao.strategies.base import ExitRules, Strategy
from mt5_extracao.strategies.backtester import BacktestConfig, SymbolSpec, run_backtest


class OneSignal(Strategy):
    key = "reference"
    def __init__(self, direction=1, rules=None):
        self.params = {}; self.direction = direction; self.rules = rules or ExitRules()
    def describe(self):
        return "sinal de referência"
    def exit_rules(self):
        return self.rules
    def _signals(self, df, out):
        name = "long" if self.direction > 0 else "short"
        out.loc[0, "entry_" + name] = True
        out.loc[0, "stop_" + name] = 95 if self.direction > 0 else 105
        out.loc[0, "target_" + name] = 999 if self.direction > 0 else 1


SPEC = SymbolSpec(symbol="TEST", point=1, tick_size=1, tick_value=1,
                  volume_min=.01, volume_step=.01, volume_max=100)
CFG = BacktestConfig(initial_equity=1000, risk_per_trade=.01, slippage_points=1, swap=False)


def bars(direction=1):
    values = [(100,101,99,100), (110,120,108,115), (115,124,112,120)] if direction > 0 else [
        (100,101,99,100), (90,95,85,88), (88,91,75,80)]
    frame = pd.DataFrame(values, columns=["open","high","low","close"])
    frame["time"] = pd.date_range("2026-01-01", periods=len(frame), freq="min")
    frame["spread"] = 2
    return frame


@pytest.mark.parametrize("direction,entry,target", [(1,113,123),(-1,89,79)])
def test_target_uses_actual_next_open_fill_with_spread_slippage_and_gap(direction, entry, target):
    result = run_backtest(bars(direction), OneSignal(direction, ExitRules(target_points=10)), SPEC, CFG)
    trade, = result.trades
    assert trade.entry_price == entry and trade.target_price == target
    assert trade.exit_price == target and trade.exit_reason == "alvo"
    # Fonte/âncora independente: fill +/- 10 pontos; não close do sinal nem alvo999/1 da estratégia.
    assert trade.entry_time == bars(direction).time.iloc[1]
    assert trade.stop_price == (95 if direction > 0 else 105)
    assert trade.pnl == pytest.approx((target-entry)*direction*trade.volume)
    exported = pd.read_csv(__import__("io").StringIO(result.trades_frame().to_csv(index=False)))
    assert exported.loc[0,"target_price"] == target and exported.loc[0,"exit_price"] == target


def test_stop_first_conflict_and_future_bars_preserve_confirmed_trade():
    frame = bars(); frame.loc[1,"high"] = 150; frame.loc[1,"low"] = 90
    trade, = run_backtest(frame, OneSignal(rules=ExitRules(target_points=10)), SPEC, CFG).trades
    assert trade.target_price == 123 and trade.exit_reason == "stop" and trade.exit_price == 94
    prefix = bars()
    initial = run_backtest(prefix, OneSignal(rules=ExitRules(target_points=10)), SPEC, CFG)
    future = pd.concat([prefix, pd.DataFrame([{"time":pd.Timestamp("2026-01-01 00:03"),
                        "open":120,"high":200,"low":1,"close":150,"spread":2}])],ignore_index=True)
    extended = run_backtest(future, OneSignal(rules=ExitRules(target_points=10)), SPEC, CFG)
    assert initial.trades == extended.trades


@pytest.mark.parametrize("points", [0,-1,True,float("nan"),float("inf"),"10"])
def test_invalid_or_ambiguous_rules_are_actionable(points):
    with pytest.raises(ValueError, match="positivos e finitos"):
        ExitRules(target_points=points)
    with pytest.raises(ValueError, match="duas fontes"):
        ExitRules(target_points=10,target_r=2)


@pytest.mark.parametrize("spec", [replace(SPEC,known=False),replace(SPEC,point=0),
                                  replace(SPEC,tick_size=float("nan")),replace(SPEC,symbol="")])
def test_missing_spec_cannot_invent_point_units(spec):
    with pytest.raises(ValueError, match="especificação|contrato|point|tick"):
        run_backtest(bars(),OneSignal(rules=ExitRules(target_points=10)),spec,CFG)


def test_non_tick_distance_is_rejected_and_default_path_preserved():
    with pytest.raises(ValueError, match="múltiplo"):
        run_backtest(bars(),OneSignal(rules=ExitRules(target_points=.5)),SPEC,CFG)
    original = OneSignal()
    plain = run_backtest(bars(),original,SPEC,CFG)
    explicit = run_backtest(bars(),original,SPEC,CFG,exit_rules_override=ExitRules())
    assert plain.trades == explicit.trades
    assert plain.trades[0].target_price == 999


@pytest.mark.parametrize("mode,params,target",[("fixed_points",{"target_points":10},123),
                                               ("fixed_r",{"target_r":2},149)])
def test_composition_reaches_existing_engine_with_explicit_override(monkeypatch, mode, params, target):
    data = base_composition("ema_atr",symbol="TEST",timeframe="M1").to_dict()
    data["profit_exit"] = {"mode":mode,"params":params}
    comp = Composition(data)
    monkeypatch.setattr(BaseAdapter,"strategy",lambda self: OneSignal())
    frame = bars(); frame.loc[2,"high"] = 150
    result = BaseAdapter(comp).run(frame,SPEC,CFG,timeframe="M1")
    assert result.trades[0].target_price == target
    assert result.trades[0].exit_price == target
    assert result.trades[0].stop_price == 95
    assert Composition(comp.to_json()).config_hash == comp.config_hash
