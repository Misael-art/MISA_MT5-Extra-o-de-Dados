"""T16.8 (parte 1): martingale e anti-martingale só para robô que os suporta, com as travas do robô."""
import types

import pandas as pd
import pytest

from mt5_extracao.robots import mql, risk
from mt5_extracao.trade_analytics.execution import sizing


def _robot(inputs, presets=None, facts=None):
    items = [mql.MqlInput(name=n, type="double", default=str(v), comment=c) for n, v, c in inputs]
    rating = risk.RiskRating(risk.MODERATE, [], facts or {})
    return types.SimpleNamespace(inputs=items, presets=presets or {}, preset_risks={}, risk=rating)


def _seq(pnls, r_per_money=0.01, start="2026-01-05 10:00", per_day=1):
    rows = []
    for k, p in enumerate(pnls):
        t = pd.Timestamp(start) + pd.Timedelta(days=k // per_day, minutes=10 * (k % per_day))
        rows.append({"entry_time": t, "exit_time": t + pd.Timedelta(minutes=5), "volume": 1.0, "pnl": float(p),
                     "r_multiple": p * r_per_money})
    return pd.DataFrame(rows)


def test_support_needs_the_parameter_and_no_block():
    assert sizing.martingale_support(_robot([("InpRisk", 1, "risco")])) is None
    robot = _robot([("InpLotMultiplier", 1.5, "multiplicador de lote")])
    assert sizing.martingale_support(robot) == {"param": "InpLotMultiplier", "multiplier": 1.5}
    blocked = _robot([("InpLotMultiplier", 1.5, ""), ("InpBlockAveraging", "true", "proibir preço médio")])
    assert sizing.martingale_support(blocked) is None


def test_martingale_doubles_after_losses_and_resets_after_a_win():
    out = sizing.apply(_seq([-10, -10, -10, 20, -10]), 100_000, 2.0, sizing.Safety(risk_pct=None, max_levels=5))
    assert out["trades"]["volume"].tolist() == [1, 2, 4, 8, 1]
    assert out["trades"]["pnl"].tolist() == [-10, -20, -40, 160, -10]
    assert out["max_streak"] == 3


def test_anti_martingale_grows_after_wins():
    out = sizing.apply(_seq([10, 10, -10, 10]), 100_000, 2.0, sizing.Safety(risk_pct=None), anti=True)
    assert out["trades"]["volume"].tolist() == [1, 2, 4, 1]


def test_robot_limits_fire_like_the_robot():
    # risco por operação: 1% de 100 mil = 1.000; cada lote arrisca 100 (pnl/r) → no máximo 10 lotes
    out = sizing.apply(_seq([-10] * 6), 100_000, 3.0, sizing.Safety(risk_pct=1.0, max_levels=10))
    assert out["trades"]["volume"].max() <= 10 + 1e-9 and out["triggers"]["risco por operação"] >= 1
    out = sizing.apply(_seq([-10] * 6), 100_000, 2.0, sizing.Safety(volume_max=3, max_levels=10))
    assert out["trades"]["volume"].max() == 3 and out["triggers"]["volume máximo"] >= 1
    out = sizing.apply(_seq([-100] * 3), 1_000, 2.0, sizing.Safety(max_drawdown_pct=20, max_levels=10))
    assert out["halted"] and out["trades"]["skipped"].tolist() == [False, False, True]
    day = sizing.apply(_seq([-30, -30, -30, 5], per_day=4), 1_000, 1.0, sizing.Safety(daily_loss_pct=5))
    assert day["trades"]["skipped"].tolist() == [False, False, True, True] and day["triggers"]["perda diária"] == 2


def test_cap_of_levels_resets_the_progression():
    out = sizing.apply(_seq([-1] * 5), 100_000, 2.0, sizing.Safety(max_levels=2))
    assert out["trades"]["volume"].tolist() == [1, 2, 4, 1, 2] and out["triggers"]["teto de multiplicações"] == 1


def test_safety_from_robot_and_global_fallback():
    import configparser
    cfg = configparser.ConfigParser()
    cfg["ROBOTS"] = {"max_real_risk_pct": "0.5"}
    robot = _robot([("InpLotMultiplier", 2, "")], facts={"daily_loss_pct": 3.0})
    s = sizing.robot_safety(robot, None, cfg, types.SimpleNamespace(volume_max=50))
    assert s.risk_pct == 0.5 and s.daily_loss_pct == 3.0 and s.volume_max == 50
    assert any("teto global" in n for n in s.notes) and any("drawdown" in n for n in s.notes)


# --- T16.8 (parte 2): gradiente na mesma operação ------------------------------------------

from mt5_extracao.strategies.backtester import BacktestConfig, SymbolSpec        # noqa: E402
from mt5_extracao.trade_analytics.execution import engine                         # noqa: E402

SPEC, ZERO = SymbolSpec(symbol="X", known=False), BacktestConfig(swap=False)
PPU = SPEC.money_per_price_unit       # resultado em dinheiro = preço × valor do ponto


def _bars(path):
    import numpy as np
    close = np.asarray(path, dtype=float)
    open_ = np.concatenate([[close[0]], close[:-1]])
    times = pd.date_range("2026-01-05 10:00", periods=len(close), freq="15min")
    return pd.DataFrame({"time": times, "open": open_, "high": np.maximum(open_, close),
                         "low": np.minimum(open_, close), "close": close, "tick_volume": 1, "spread": 0,
                         "real_volume": 0})


def _entry(bars, stop, target=None):
    return pd.DataFrame([{"direction": 1, "entry_time": bars["time"].iloc[0], "entry_price": bars["open"].iloc[0],
                          "stop_price": stop, "target_price": target, "volume": 1.0,
                          "exit_time": bars["time"].iloc[-1], "exit_reason": "fim dos dados"}])


def _point():
    return SPEC.point


def test_grid_against_averages_down_and_takes_the_worst_case_with_the_stop():
    pts = 1 / _point()                                  # 1,0 de preço em pontos
    bars = _bars([100, 99, 98, 101, 103])
    policy = engine.ExitPolicy(grid_levels=2, grid_direction="contra", grid_spacing_points=pts)
    sim = engine.simulate(_entry(bars, 95, 102), bars, SPEC, ZERO, policy)
    t = sim.trades[0]
    assert sim.adds == 2 and t.volume == 3                # entrou em 100, 99 e 98
    assert t.exit_reason == "alvo" and t.pnl == pytest.approx(((102 - 100) + (102 - 99) + (102 - 98)) * PPU)
    crash = _bars([100, 99, 98, 94])                       # toca os dois níveis e o stop
    t = engine.simulate(_entry(crash, 95), crash, SPEC, ZERO, policy).trades[0]
    assert t.exit_reason == "stop" and t.pnl == pytest.approx(((95 - 100) + (95 - 99) + (95 - 98)) * PPU)


def test_grid_exposure_cap():
    pts = 1 / _point()
    bars = _bars([100, 99, 98, 97, 96, 101])
    policy = engine.ExitPolicy(grid_levels=4, grid_spacing_points=pts, grid_max_volume=2)
    sim = engine.simulate(_entry(bars, 90), bars, SPEC, ZERO, policy)
    assert sim.adds == 1 and sim.capped == 3 and sim.max_volume == 2


def test_dynamic_grid_uses_the_atr_known_before_the_entry():
    bars = _bars([100, 100, 98, 101, 103])
    atr = pd.Series([2.0, 50.0, 50.0, 50.0, 50.0])         # o ATR depois da entrada não pode valer
    rows = _entry(bars, 90, 102).assign(entry_time=bars["time"].iloc[1], entry_price=100)
    policy = engine.ExitPolicy(grid_levels=1, grid_spacing_atr=1.0)
    sim = engine.simulate(rows, bars, SPEC, ZERO, policy, atr)
    assert sim.adds == 1                                   # espaçamento 2 (ATR da barra anterior): nível 98


def test_directional_grid_pyramids_and_raises_the_stop():
    pts = 1 / _point()
    bars = _bars([100, 101, 102, 103, 101.5, 100])
    policy = engine.ExitPolicy(grid_levels=2, grid_direction="favor", grid_spacing_points=pts, grid_trail_stop=True)
    t = engine.simulate(_entry(bars, 95), bars, SPEC, ZERO, policy).trades[0]
    assert t.volume == 3 and t.exit_reason == "stop móvel"
    assert t.exit_price == 101                              # stop subiu para a entrada anterior (101)
    assert t.pnl == pytest.approx(((101 - 100) + (101 - 101) + (101 - 102)) * PPU)


def test_martingale_only_for_robots_that_support_it(tmp_path):
    from mt5_extracao import robots
    from mt5_extracao.database_manager import DatabaseManager
    from mt5_extracao.robots import backtest
    from mt5_extracao.trade_analytics.execution import modes, runner
    from tests.market_data import random_walk
    db = DatabaseManager(db_path=str(tmp_path / "b.db"))
    db.save_ohlcv_data("BBB", "15 minutos", random_walk(3000, seed=4, vol=0.6, freq="15min", spread=10))
    donchian = next(r for r in robots.load_catalog(None, db) if r.key == "python:donchian")
    run = backtest.run_python(db, None, donchian, "BBB", "M15")
    assert "martingale" not in modes.available(donchian)                         # Python: sem parâmetro
    with pytest.raises(ValueError):
        runner.compare(db, None, run["run_id"], ["martingale"], robot=donchian)
    grids = runner.compare(db, None, run["run_id"], ["ponto_fixo", "gradiente_fixo", "gradiente_dinamico",
                                                     "gradiente_direcional"])
    assert all(v["metrics"]["n"] == grids["ponto_fixo"]["metrics"]["n"] for v in grids.values())
    assert grids["gradiente_fixo"]["adds"] >= 0 and grids["gradiente_fixo"]["max_volume"] >= 1
    mart = _robot([("InpLotMultiplier", 2, "multiplicador de lote")], facts={"risk_pct": 1.0})
    table = runner.compare(db, None, run["run_id"], ["ponto_fixo", "martingale", "anti_martingale"], robot=mart)
    assert table["martingale"]["study_only"] and "triggers" in table["martingale"]["sequence"]
    assert table["martingale"]["floating_equity"] is None
    assert table["martingale"]["valuation"]["status"] == "unavailable"
    assert table["ponto_fixo"]["valuation"]["resolution"] == "bar_close"
    assert table["ponto_fixo"]["floating_equity"] is not None
    assert table["martingale"]["metrics"]["n"] <= table["ponto_fixo"]["metrics"]["n"]
    db.close()
