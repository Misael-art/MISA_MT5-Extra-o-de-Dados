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
    assert mine["profit_factor"] == pytest.approx(ref["profit_factor"])
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


@pytest.mark.parametrize("values,expected", [
    ([1, 2], (2, 0, 0, 0)), ([-1, -2], (0, 2, 0, 0)),
    ([0, -0.0], (0, 0, 2, 0)), ([2, -1, 0, 0], (1, 1, 2, 0)),
    ([None, np.nan, np.inf, -np.inf, "inválido", True, False], (0, 0, 0, 7)),
    (["1.5", "-2", "0", None], (1, 1, 1, 1)),
])
def test_outcome_counts_distinguish_zero_and_unknown(values, expected):
    lab = pd.DataFrame({"time_bucket": ["09:00"] * len(values), "pnl": values})
    original = lab.copy(deep=True)
    result = aggregate.outcome_counts(lab, "time_bucket")
    assert result.iloc[0][["wins", "losses", "flat", "unknown"]].tolist() == list(expected)
    assert result.iloc[0]["n"] == len(values) == sum(expected)
    pd.testing.assert_frame_equal(lab, original)


def test_outcome_counts_keep_missing_dimension_and_financial_metrics():
    lab = pd.DataFrame({"time_bucket": pd.Categorical(["09:00", "09:00", None, None],
                                                     categories=["09:00", "10:00"]),
                        "pnl": [4.0, -2.0, 0.0, 0.0]})
    original = lab.copy(deep=True)
    before = stats.metrics(lab["pnl"])
    result = aggregate.outcome_counts(lab, "time_bucket")
    assert len(result) == 2  # não cria amostra na categoria sem operações
    assert result["n"].sum() == len(lab)
    assert (result[["wins", "losses", "flat", "unknown"]].sum(axis=1) == result["n"]).all()
    assert result.loc[result["time_bucket"].isna(), "flat"].iloc[0] == 2
    assert stats.metrics(lab["pnl"]) == before
    pd.testing.assert_frame_equal(lab, original)


def test_outcome_counts_empty_book_has_stable_columns():
    result = aggregate.outcome_counts(pd.DataFrame(), "time_bucket")
    assert result.empty
    assert result.columns.tolist() == ["time_bucket", "n", "wins", "losses", "flat", "unknown"]


@pytest.mark.parametrize("configured,minutes,slots", [
    (None, 30, 48), ("60", 60, 24), ("17", 17, 85),
    ("inválido", 30, 48), ("0", 1, 1440), ("-3", 1, 1440), ("2000", 2000, 1),
])
def test_hourly_grid_uses_the_enrichment_configuration(configured, minutes, slots):
    import configparser
    from mt5_extracao.trade_analytics import enrich
    config = None
    if configured is not None:
        config = configparser.ConfigParser()
        config["TRADE_ANALYTICS"] = {"time_bucket_min": configured}
    times = pd.Series(pd.to_datetime(["2025-01-01 00:05", "2025-01-01 01:31"]))
    lab = enrich.calendar_columns(times, minutes).assign(pnl=[0.0, 5.0])
    original = lab.copy(deep=True)
    result = aggregate.hourly_summary(lab, config)
    table = result["table"]
    assert result["bucket_min"] == minutes and len(table) == slots
    assert result["timed_n"] == result["measured_n"] == 2
    assert result["untimed_n"] == result["missing_result_n"] == 0
    assert table["n"].sum() == 2 and table["time_bucket"].iloc[0] == "00:00"
    observed = aggregate.by_dimension(lab, ["time_bucket"], config, ci=False).set_index("time_bucket")
    compared = table.set_index("time_bucket").loc[observed.index]
    for column in ("n", "expectancy", "win_rate", "small"):
        pd.testing.assert_series_equal(compared[column], observed[column], check_dtype=False)
    missing = table["n"].eq(0)
    assert table.loc[missing, ["expectancy", "win_rate"]].isna().all().all()
    assert not table.loc[missing, "small"].any()
    pd.testing.assert_frame_equal(lab, original)


def test_hourly_grid_reports_unplaced_and_invalid_results_without_fake_zero():
    lab = pd.DataFrame({"time_bucket": ["00:00", "01:00", "00:15", None, "02:00", "03:00"],
                        "pnl": [0.0, 4.0, -5.0, 9.0, np.nan, np.inf]})
    original = lab.copy(deep=True)
    result = aggregate.hourly_summary(lab)
    table = result["table"].set_index("time_bucket")
    assert result["timed_n"] == 4 and result["measured_n"] == 2
    assert result["untimed_n"] == 2 and result["missing_result_n"] == 2
    assert table["n"].sum() == 2
    assert table.loc["00:00", "expectancy"] == 0 and table.loc["00:00", "n"] == 1
    assert table.loc["00:30", "n"] == 0 and np.isnan(table.loc["00:30", "expectancy"])
    assert np.isnan(table.loc["02:00", "expectancy"]) and not table.loc["02:00", "small"]
    pd.testing.assert_frame_equal(lab, original)


def test_hourly_grid_empty_book_has_no_measurements():
    result = aggregate.hourly_summary(pd.DataFrame())
    assert len(result["table"]) == 48
    assert result["table"]["n"].sum() == result["timed_n"] == result["measured_n"] == 0
    assert result["table"][["expectancy", "win_rate"]].isna().all().all()
    assert not result["table"]["small"].any()
