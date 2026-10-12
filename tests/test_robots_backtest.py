"""T8.3: backtest de estratégias Python pelo card, gravado em _robot_runs com a curva de capital."""
import types

import pytest

from mt5_extracao import robots
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.robots import backtest, store
from tests.market_data import trend


@pytest.fixture
def db(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "db.sqlite"))
    db.save_ohlcv_data("BBB", "1 dia", trend(1300, step=0.2, noise=1.0, freq="D", spread=5, volume=3000))
    return db


def test_equity_curve_and_downsample_keep_the_extremes():
    trades = [types.SimpleNamespace(pnl=p, exit_time=f"2024-01-{i + 1:02d}") for i, p in enumerate([10, -30, 5])]
    assert backtest.equity_curve(trades, 100.0, "2024-01-01") == [
        ["2024-01-01", 100.0], ["2024-01-01", 110.0], ["2024-01-02", 80.0], ["2024-01-03", 85.0]]
    points = [["t", float(v)] for v in [100] + [100 + ((i * 37) % 50) - 25 for i in range(2000)] + [90]]
    points[777][1] = -500.0                                   # o pior ponto não pode sumir na redução
    small = backtest.downsample(points)
    assert len(small) <= backtest.MAX_CURVE_POINTS and small[0] == points[0] and small[-1] == points[-1]
    assert min(p[1] for p in small) == -500.0 and max(p[1] for p in small) == max(p[1] for p in points)
    assert backtest.downsample(points[:50]) == points[:50]


def test_run_python_saves_run_and_card_shows_it(db):
    items = robots.load_catalog(None, db)
    donchian = robots.get_robot(items, "donchian")
    assert store.load_runs(db) == [] and donchian.headline is None            # base antiga: sem a tabela
    run = backtest.run_python(db, None, donchian, "BBB", "1 dia")
    metrics = run["metrics"]
    assert run["timeframe"] == "D1" and run["bars"] == 1300 and metrics["trades"] > 0
    assert run["equity"][0][1] == 100_000.0 and len(run["equity"]) == metrics["trades"] + 1
    assert run["equity"][-1][1] == pytest.approx(100_000.0 + metrics["net_profit"])
    assert run["params"] == donchian_params()

    saved = store.load_runs(db, "python:donchian")
    assert len(saved) == 1 and saved[0]["metrics"]["trades"] == metrics["trades"] and saved[0]["equity"] == run["equity"]
    again = robots.get_robot(robots.load_catalog(None, db), "donchian")
    assert again.stage == "testado" and len(again.runs) == 1
    head = again.headline
    assert (head["kind"], head["approved"], head["extra_label"]) == ("backtest", None, "acerto")
    assert head["trades"] == metrics["trades"] and head["max_drawdown"] == metrics["max_drawdown_pct"]
    assert robots.get_robot(robots.load_catalog(None, db), "squeeze").runs == []


def donchian_params():
    from mt5_extracao.strategies import get_strategy
    return dict(get_strategy("donchian").params)


def test_newest_run_first_and_period_start(db):
    donchian = robots.get_robot(robots.load_catalog(None, db), "donchian")
    first = backtest.run_python(db, None, donchian, "BBB", "D1")
    second = backtest.run_python(db, None, donchian, "BBB", "D1", start="2021-01-01")
    assert second["bars"] < first["bars"] and second["data_start"] >= "2021-01-01"
    runs = store.load_runs(db, "python:donchian")
    assert len(runs) == 2 and {r["run_id"] for r in runs} == {first["run_id"], second["run_id"]}
    assert runs[0]["created_at"] >= runs[1]["created_at"]


def test_validation_wins_over_backtest_on_the_card(db):
    from mt5_extracao.strategies import validation
    donchian = robots.get_robot(robots.load_catalog(None, db), "donchian")
    backtest.run_python(db, None, donchian, "BBB", "D1")
    validation.save_validation(db, "BBB", "D1", {
        "strategy": "donchian", "approved": False, "status": "x", "params": {},
        "oos_metrics": {"profit_factor": 1.1, "trades": 90},
        "checks": [{"name": "Janelas walk-forward positivas", "value": 0.4}],
        "robustness": {"robust": False}, "monte_carlo": {"dd_percentile": 0.3},
        "data_start": "2020-01-01", "data_end": "2024-01-01"})
    head = robots.get_robot(robots.load_catalog(None, db), "donchian").headline
    assert (head["kind"], head["approved"], head["trades"]) == ("validação", False, 90)


def test_run_python_explains_what_is_missing(db):
    items = robots.load_catalog(None, db)
    donchian = robots.get_robot(items, "donchian")
    with pytest.raises(ValueError, match="ZZZ"):
        backtest.run_python(db, None, donchian, "ZZZ", "D1")
    with pytest.raises(ValueError, match="Poucas barras"):
        backtest.run_python(db, None, donchian, "BBB", "D1", start=str(db.load_ohlcv("BBB", "1 dia")["time"].iloc[-50]))
    with pytest.raises(ValueError, match="Timeframe inválido"):
        backtest.run_python(db, None, donchian, "BBB", "X9")
    mql5 = robots.Robot(key="mql5:x", kind="mql5", name="X")
    with pytest.raises(ValueError, match="Strategy Tester"):
        backtest.run_python(db, None, mql5, "BBB", "D1")
    assert store.load_runs(db) == []                                          # nada gravado em caso de erro
