"""T16.4: agregações e estatística das duas lentes do Laboratório."""
import numpy as np
import pandas as pd
import pytest

from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.strategies.backtester import Trade, compute_metrics
from mt5_extracao.trade_analytics import aggregate, ledger, stats


@pytest.fixture
def db(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "base.db"))
    yield db
    db.close()


def _planted(n_days=200, seed=1):
    """Perde às sextas; ganha das 10h às 11h; ruído no resto."""
    rng = np.random.default_rng(seed)
    trades = []
    for d in pd.bdate_range("2025-01-06", periods=n_days):
        for hour in (9, 10, 14):
            entry = d + pd.Timedelta(hours=hour, minutes=5)
            pnl = rng.normal(0, 10)
            if d.dayofweek == 4:
                pnl -= 15
            if hour == 10:
                pnl += 12
            trades.append(Trade(direction=1, entry_time=entry, exit_time=entry + pd.Timedelta(minutes=40),
                                entry_price=100, exit_price=100, volume=1, pnl=float(pnl), r_multiple=pnl / 10,
                                bars=8, exit_reason="alvo", equity_before=0))
    return trades


def test_metrics_match_the_backtester():
    trades = _planted(60)
    mine = stats.metrics([t.pnl for t in trades], 100_000)
    ref = compute_metrics(trades, 100_000)
    assert mine["n"] == ref["trades"] and mine["win_rate"] == pytest.approx(ref["win_rate"])
    assert mine["profit_factor"] == pytest.approx(min(ref["profit_factor"], stats.PF_CAP))
    assert mine["payoff"] == pytest.approx(ref["payoff"]) and mine["net"] == pytest.approx(ref["net_profit"])
    assert mine["max_drawdown_pct"] == pytest.approx(ref["max_drawdown_pct"])


def test_planted_effects_show_up(db):
    ledger.save_trades(db, "r", _planted(), robot_key="python:x", symbol="WIN$N", timeframe="M5")
    lab = aggregate.frame(db, None, robot_key="python:x")
    days = aggregate.by_dimension(lab, ["day_of_week"]).set_index("day_of_week")
    assert days.loc[4, "expectancy"] < -10 and days.loc[4, "ci_high"] < 0          # sexta: negativa, com intervalo
    assert days.drop(4)["expectancy"].min() > -5
    hours = aggregate.by_dimension(lab, ["time_bucket"]).set_index("time_bucket")
    assert hours.loc["10:00", "expectancy"] > hours.loc["09:00", "expectancy"] + 8
    heat = aggregate.crosstab(lab, "time_bucket", "day_of_week")
    assert heat["value"].loc["10:00", 4] < heat["value"].loc["10:00", 1]
    assert heat["n"].loc["10:00", 1] == 40 and not heat["small"].loc["10:00", 1]


def test_small_cells_are_marked(db):
    ledger.save_trades(db, "r", _planted(5), robot_key="python:x", symbol="WIN$N", timeframe="M5")
    table = aggregate.by_dimension(aggregate.frame(db, None), ["day_of_week"])
    assert table["small"].all() and (table["n"] < 30).all()


def test_two_lenses_curves_and_correlation(db):
    t = _planted(80)
    ledger.save_trades(db, "a1", t, robot_key="python:a", symbol="WIN$N", timeframe="M5")
    ledger.save_trades(db, "a2", t[:100], robot_key="python:a", symbol="WDO$N", timeframe="M5")
    ledger.save_trades(db, "b1", t, robot_key="python:b", symbol="WIN$N", timeframe="M5")
    by_robot = aggregate.robot_view(aggregate.frame(db, None, robot_key="python:a"))
    assert set(by_robot["symbol"]) == {"WIN$N", "WDO$N"}
    by_asset = aggregate.asset_view(aggregate.frame(db, None, symbol="WIN$N"))
    assert set(by_asset["robot_key"]) == {"python:a", "python:b"}
    lab = aggregate.frame(db, None, symbol="WIN$N")
    curves = aggregate.equity_curves(lab, "robot_key")
    assert curves["python:a"].iloc[-1] == pytest.approx(sum(x.pnl for x in t))
    corr = aggregate.daily_correlation(lab)
    assert corr.loc["python:a", "python:b"] == pytest.approx(1.0)       # mesmas operações: perdas que se somam


def test_bootstrap_ci_contains_the_mean_and_needs_two_days():
    days = pd.Series(pd.bdate_range("2025-01-06", periods=100).repeat(3))
    values = pd.Series(np.random.default_rng(3).normal(5, 10, len(days)))
    lo, hi = stats.bootstrap_ci(values, days)
    assert lo < values.mean() < hi
    assert stats.bootstrap_ci(pd.Series([1.0, 2.0]), pd.Series(pd.to_datetime(["2025-01-06"] * 2))) == (None, None)


def test_empty_book():
    assert aggregate.by_dimension(pd.DataFrame(), ["day_of_week"]).empty
