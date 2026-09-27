"""T6.3: backtester com custos, risco e regras de saída."""
import math

import numpy as np
import pandas as pd
import pytest

from mt5_extracao.strategies import ExitRules, Strategy, StrategyInfo, get_strategy
from mt5_extracao.strategies.backtester import BacktestConfig, SymbolSpec, compute_metrics, run_backtest
from tests.market_data import ohlc_from_close, random_walk

INFO = StrategyInfo("Manual", "teste", "-", "-", "baixa", ("D1",), "-", "-")


class Manual(Strategy):
    """Estratégia de teste: sinais informados explicitamente por barra."""
    key = "manual"
    info = INFO

    def __init__(self, long=(), short=(), exit_long=(), stop=None, target=None, rules=None):
        super().__init__()
        self.long, self.short, self.xl = set(long), set(short), set(exit_long)
        self.stop, self.target, self.rules = stop or {}, target or {}, rules or ExitRules()

    def _signals(self, df, out):
        idx = df.index
        out["entry_long"] = idx.isin(self.long)
        out["entry_short"] = idx.isin(self.short)
        out["exit_long"] = idx.isin(self.xl)
        for i, v in self.stop.items():
            out.loc[i, "stop_long" if i in self.long else "stop_short"] = v
        for i, v in self.target.items():
            out.loc[i, "target_long" if i in self.long else "target_short"] = v
        out["atr"] = 1.0

    def exit_rules(self):
        return self.rules


def bars(rows, start="2024-01-01", freq="D", spread=0):
    df = pd.DataFrame(rows, columns=["open", "high", "low", "close"], dtype=float)
    df.insert(0, "time", pd.date_range(start, periods=len(df), freq=freq))
    df["spread"] = spread
    return df


SPEC = SymbolSpec(point=1.0, tick_size=1.0, tick_value=1.0, volume_min=1, volume_step=1)
CFG = BacktestConfig(initial_equity=10_000, risk_per_trade=0.01)  # arrisca 100 por operação


def test_lot_sizing_rounds_down_and_respects_minimum():
    spec = SymbolSpec(point=0.00001, tick_size=0.00001, tick_value=1.0, volume_min=0.01, volume_step=0.01)
    # risco 1000, stop de 0.0050 (500 ticks × 1 = 500 por lote) -> 2 lotes
    assert spec.lots_for_risk(1000, 0.0050) == pytest.approx(2.0)
    assert spec.lots_for_risk(1000, 0.0033) == pytest.approx(3.03)
    assert spec.lots_for_risk(1, 0.0500) == 0.0  # abaixo do lote mínimo


def test_entry_next_open_and_target():
    df = bars([[100, 101, 99, 100], [100, 102, 99, 101], [101, 111, 100, 110], [110, 111, 109, 110]])
    res = run_backtest(df, Manual(long=[0], stop={0: 90}, target={0: 110}), SPEC, CFG)
    t = res.trades[0]
    assert t.entry_time == df["time"][1] and t.entry_price == 100          # abertura da barra seguinte
    assert t.volume == 10                                                   # 100 / (100-90)
    assert t.exit_reason == "alvo" and t.exit_price == 110
    assert t.pnl == pytest.approx(100) and t.r_multiple == pytest.approx(1.0)


def test_stop_wins_when_stop_and_target_same_bar():
    df = bars([[100, 101, 99, 100], [100, 101, 99, 100], [100, 115, 85, 100]])
    t = run_backtest(df, Manual(long=[0], stop={0: 90}, target={0: 110}), SPEC, CFG).trades[0]
    assert t.exit_reason == "stop" and t.exit_price == 90 and t.r_multiple == pytest.approx(-1.0)


def test_gap_through_stop_fills_at_open():
    df = bars([[100, 101, 99, 100], [100, 101, 99, 100], [80, 82, 79, 81]])
    t = run_backtest(df, Manual(long=[0], stop={0: 90}), SPEC, CFG).trades[0]
    assert t.exit_price == 80 and t.r_multiple == pytest.approx(-2.0)


def test_spread_slippage_and_commission_are_charged():
    df = bars([[100, 101, 99, 100], [100, 101, 99, 100], [100, 101, 99, 100]], spread=2)
    cfg = BacktestConfig(initial_equity=10_000, risk_per_trade=0.01, slippage_points=1, commission_per_lot=0.5)
    t = run_backtest(df, Manual(long=[0], stop={0: 80}), SPEC, cfg).trades[0]
    assert t.entry_price == 103          # open 100 + spread 2 + slippage 1
    assert t.exit_reason == "fim dos dados" and t.exit_price == 99   # close 100 - slippage
    vol = t.volume
    assert vol == 4                      # 100 / 23 -> 4 lotes
    assert t.pnl == pytest.approx((99 - 103) * vol - 2 * 0.5 * vol)


def test_short_pays_spread_on_exit():
    df = bars([[100, 101, 99, 100], [100, 101, 99, 100], [95, 96, 90, 91]], spread=1)
    t = run_backtest(df, Manual(short=[0], stop={0: 110}, target={0: 92}), SPEC, CFG).trades[0]
    assert t.direction == -1 and t.entry_price == 100
    assert t.exit_reason == "alvo" and t.exit_price == 92   # ask mínimo 91 <= 92
    assert t.pnl > 0


def test_min_lot_skips_trade():
    df = bars([[100, 101, 99, 100]] * 4)
    res = run_backtest(df, Manual(long=[0], stop={0: 1}), SymbolSpec(point=1, tick_value=10, volume_min=1),
                       BacktestConfig(initial_equity=1000, risk_per_trade=0.01))
    assert res.trades == [] and res.skipped_min_lot == 1


def test_partial_breakeven_and_target_r():
    rows = [[100, 101, 99, 100], [100, 101, 99, 100], [100, 111, 100, 110], [110, 111, 99, 105]]
    rules = ExitRules(target_r=2.0, partial_r=1.0, partial_fraction=0.5)
    t = run_backtest(bars(rows), Manual(long=[0], stop={0: 90}, rules=rules), SPEC, CFG).trades[0]
    # 10 lotes: 5 realizados em 110 (+50); resto volta ao zero a zero (stop em 100)
    assert t.exit_reason == "stop no zero a zero"
    assert t.pnl == pytest.approx(50) and t.r_multiple == pytest.approx(0.5)


def test_trailing_time_and_signal_exits():
    up = [[100 + i, 101 + i, 99.5 + i, 100.5 + i] for i in range(10)] + [[109, 109, 100, 101]]
    t = run_backtest(bars(up), Manual(long=[0], stop={0: 90}, rules=ExitRules(trailing_atr=2)), SPEC, CFG).trades[0]
    assert t.exit_reason == "stop móvel" and t.exit_price == pytest.approx(109.5 - 2)  # último fechamento - 2×ATR
    flat = bars([[100, 101, 99, 100]] * 10)
    t = run_backtest(flat, Manual(long=[0], stop={0: 90}, rules=ExitRules(max_bars=3)), SPEC, CFG).trades[0]
    assert t.exit_reason == "tempo máximo" and t.bars == 3
    t = run_backtest(flat, Manual(long=[0], exit_long=[4], stop={0: 90}), SPEC, CFG).trades[0]
    assert t.exit_reason == "sinal de saída" and t.exit_time == flat["time"][5]


def test_eod_exit_and_no_overnight_entry():
    df = bars([[100, 101, 99, 100]] * 6, start="2024-01-02 17:00", freq="30min")  # 17:00..19:30 mesmo dia
    df.loc[3:, "time"] = pd.date_range("2024-01-03 09:00", periods=3, freq="30min")
    rules = ExitRules(eod_exit=True)
    res = run_backtest(df, Manual(long=[0, 2], stop={0: 90, 2: 90}, rules=rules), SPEC, CFG)
    assert len(res.trades) == 1   # sinal na última barra do dia (2) não vira entrada no dia seguinte
    assert res.trades[0].exit_reason == "fim do dia" and res.trades[0].exit_time == df["time"][2]


def test_start_index_only_counts_later_entries():
    df = bars([[100, 101, 99, 100]] * 10)
    res = run_backtest(df, Manual(long=[1, 6], stop={1: 90, 6: 90}, rules=ExitRules(max_bars=1)), SPEC, CFG,
                       start_index=5)
    assert [t.entry_time for t in res.trades] == [df["time"][7]]


def test_metrics_and_equity_curve():
    df = random_walk(1200, seed=11)
    res = run_backtest(df, get_strategy("donchian"), SymbolSpec(point=0.01, tick_value=0.01), BacktestConfig())
    m = res.metrics
    assert m["trades"] == len(res.trades) > 5
    assert 0 <= m["win_rate"] <= 1 and 0 <= m["max_drawdown_pct"] < 1
    assert res.equity.iloc[-1] == pytest.approx(res.config.initial_equity + m["net_profit"])
    assert sum(m["exit_reasons"].values()) == m["trades"]
    assert list(res.trades_frame().columns)[:3] == ["direction", "entry_time", "exit_time"]


def test_compute_metrics_edge_cases():
    assert compute_metrics([], 1000)["profit_factor"] == 0.0
    df = bars([[100, 101, 99, 100], [100, 101, 99, 100], [100, 111, 100, 110]])
    res = run_backtest(df, Manual(long=[0], stop={0: 90}, target={0: 110}), SPEC, CFG)
    assert math.isinf(res.metrics["profit_factor"])
