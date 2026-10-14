"""T16.7: modos de saída (alvo em R, break-even, trailing por ATR e por barras) sobre as mesmas entradas."""
import numpy as np
import pandas as pd
import pytest

from mt5_extracao.strategies.backtester import BacktestConfig, SymbolSpec
from mt5_extracao.trade_analytics.execution import engine, modes

SPEC, ZERO = SymbolSpec(symbol="X", known=False), BacktestConfig(swap=False)


def _bars(path):
    """Barras de amplitude 0,4 em torno do caminho de fechamentos."""
    close = np.asarray(path, dtype=float)
    open_ = np.concatenate([[close[0]], close[:-1]])
    times = pd.date_range("2026-01-05 10:00", periods=len(close), freq="15min")
    return pd.DataFrame({"time": times, "open": open_, "high": np.maximum(open_, close) + 0.2,
                         "low": np.minimum(open_, close) - 0.2, "close": close, "tick_volume": 1, "spread": 0,
                         "real_volume": 0})


def _entry(bars, stop, target=None):
    return pd.DataFrame([{"direction": 1, "entry_time": bars["time"].iloc[0], "entry_price": bars["open"].iloc[0],
                          "stop_price": stop, "target_price": target, "volume": 1.0,
                          "exit_time": bars["time"].iloc[-1], "exit_reason": "fim dos dados"}])


def _run(mode, bars, row, **params):
    policy = modes.MODES[mode].policy(engine.ExitPolicy(), params)
    atr = pd.Series(np.full(len(bars), 1.0))
    return engine.simulate(row, bars, SPEC, ZERO, policy, atr).trades[0]


def test_registry_has_the_exit_modes():
    assert {"ponto_fixo", "alvo_r", "break_even", "trailing_atr", "trailing_barras"} <= set(modes.exit_modes())
    assert all(m.help for m in modes.MODES.values())


def test_target_in_r():
    bars = _bars([100, 101, 102, 103, 104, 105])
    t = _run("alvo_r", bars, _entry(bars, 99), r=2)          # risco 1 → alvo 102
    assert t.exit_reason == "alvo" and t.exit_price == 102


def test_break_even_only_after_the_trigger():
    up_then_down = _bars([100, 100.5, 101.6, 100.8, 99.9, 98, 97])
    t = _run("break_even", up_then_down, _entry(up_then_down, 98), r=1)   # risco 2 → gatilho 102 nunca chega
    assert t.exit_reason == "stop" and t.exit_price == 98
    hit = _bars([100, 101, 102.5, 101, 99.5, 98, 97])
    t = _run("break_even", hit, _entry(hit, 98), r=1)                      # tocou 102: stop vai para 100
    assert t.exit_reason == "stop no zero a zero" and t.exit_price == 100


def test_trailing_atr_only_moves_in_favour():
    bars = _bars([100, 101, 102, 103, 102.5, 104, 105, 103, 101, 99])
    t = _run("trailing_atr", bars, _entry(bars, 95), multiplo=2)            # stop = fechamento − 2
    assert t.exit_reason == "stop móvel"
    assert t.exit_price == pytest.approx(103)                                # 105 − 2, o maior alcançado
    assert t.pnl > 0


def test_trailing_bars_follows_the_lows():
    bars = _bars([100, 101, 102, 103, 104, 103.5, 102, 101])
    t = _run("trailing_barras", bars, _entry(bars, 95), barras=3)
    assert t.exit_reason == "stop móvel" and t.exit_price > 100


def test_compare_modes_on_a_real_run(tmp_path):
    from mt5_extracao import robots
    from mt5_extracao.database_manager import DatabaseManager
    from mt5_extracao.robots import backtest
    from mt5_extracao.trade_analytics.execution import runner
    from tests.market_data import random_walk
    db = DatabaseManager(db_path=str(tmp_path / "b.db"))
    db.save_ohlcv_data("BBB", "15 minutos", random_walk(3000, seed=4, vol=0.6, freq="15min", spread=10))
    robot = next(r for r in robots.load_catalog(None, db) if r.key == "python:donchian")
    run = backtest.run_python(db, None, robot, "BBB", "M15")
    table = runner.compare(db, None, run["run_id"])
    assert set(table) == set(modes.exit_modes())
    base = table["ponto_fixo"]["metrics"]
    assert base["net"] == pytest.approx(run["metrics"]["net_profit"], rel=1e-9)
    assert all(v["metrics"]["n"] == base["n"] for v in table.values())          # mesmas entradas em todos
    assert len({round(v["metrics"]["net"], 6) for v in table.values()}) > 1      # modos diferentes, resultados diferentes
    db.close()
