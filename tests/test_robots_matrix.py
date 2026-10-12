"""T11.6: matriz ativo x timeframe x robô e as três regras para ela não enganar."""
import uuid

import pytest

from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.robots import catalog, matrix, scaffold, store
from mt5_extracao.strategies import validation
from mt5_extracao.timeframes import Timeframe
from tests.market_data import trend


@pytest.fixture
def env(tmp_path):
    db = DatabaseManager(db_type="sqlite", db_path=str(tmp_path / "db.sqlite"))
    for symbol, tf in (("EURUSD", Timeframe.M15), ("EURUSD", Timeframe.H1), ("GBPUSD", Timeframe.H1)):
        db.save_ohlcv_data(symbol, tf.label, trend(60, freq="h"))
    robots = [catalog.robot_from_file(scaffold.create("Matriz", tmp_path / "robos", magic=710001)[0])] + \
        [next(r for r in catalog.python_robots() if r.key == key) for key in ("python:donchian", "python:ema_atr")]
    return db, robots


def run(db, robot, symbol, tf, net, trades=60, pf=1.5, source="strategy tester", when="2026-01-01 10:00:00", **metrics):
    record = {"run_id": uuid.uuid4().hex, "robot_key": robot.key, "source": source, "symbol": symbol, "timeframe": tf,
              "data_start": "2025-01-01 00:00:00", "data_end": "2025-12-31 00:00:00", "bars": 0, "params": {},
              "metrics": {"trades": trades, "net_profit": net, "profit_factor": pf, "return_pct": net / 10000,
                          "win_rate": 0.4, "max_drawdown_pct": 0.05, "tester_model": "cada tick com ticks reais",
                          **metrics}, "equity": [], "trades": [], "created_at": when}
    store.save_run(db, record)
    return record


def check(db, robot, symbol, tf, approved, when="2026-01-02 10:00:00", fingerprint=None):
    store.save_check(db, {"check_id": uuid.uuid4().hex, "robot_key": robot.key, "preset": "", "approved": approved,
                          "allows_real": approved, "static_status": "", "statistical_status": "", "run_id": "",
                          "items": [], "created_at": when, "symbol": symbol, "timeframe": tf,
                          "fingerprint": catalog.fingerprint(robot) if fingerprint is None else fingerprint})


def cell(m, robot, symbol, tf):
    return next(c for c in m["cells"] if (c["robot"], c["symbol"], c["timeframe"]) == (robot.key, symbol, tf))


def test_inventory_timeframes_and_what_is_missing(env):
    db, robots = env
    m = matrix.build(db, None, robots)
    assert m["symbols"] == ["EURUSD", "GBPUSD"] and m["timeframes"] == ["M15", "H1"]          # do menor para o maior
    assert m["inventory"]["EURUSD"]["H1"]["bars"] == 60 and m["cells"] == []
    assert [r["key"] for r in m["robots"]] == [r.key for r in robots] and m["robots"][0]["tested"] == 0
    assert matrix.cell_status(m, robots[0].key, "EURUSD", "H1") == matrix.UNTESTED
    assert matrix.cell_status(m, robots[0].key, "GBPUSD", "M15") == matrix.NO_DATA
    assert set(m["statuses"]) == {matrix.APPROVED, matrix.REJECTED, matrix.PROMISING, matrix.FEW_TRADES,
                                  matrix.NEGATIVE, matrix.UNTESTED, matrix.NO_DATA}


def test_rule_1_profit_is_not_approval(env):
    db, robots = env
    mql = robots[0]
    run(db, mql, "EURUSD", "H1", net=900)
    run(db, mql, "GBPUSD", "H1", net=900)
    run(db, mql, "EURUSD", "M15", net=-50)
    m = matrix.build(db, None, robots)
    assert cell(m, mql, "EURUSD", "H1")["status"] == matrix.PROMISING            # lucro sem bateria: só promissor
    assert cell(m, mql, "EURUSD", "M15")["status"] == matrix.NEGATIVE
    check(db, mql, "EURUSD", "H1", True)
    check(db, mql, "GBPUSD", "H1", False)
    m = matrix.build(db, None, robots)
    approved = cell(m, mql, "EURUSD", "H1")
    assert approved["status"] == matrix.APPROVED and approved["verdict"].startswith("bateria de segurança em 2026-01-02")
    assert cell(m, mql, "GBPUSD", "H1")["status"] == matrix.REJECTED             # lucrou, mas reprovou
    check(db, mql, "EURUSD", "H1", True, when="2026-01-03 10:00:00", fingerprint="codigo-antigo")
    stale = cell(matrix.build(db, None, robots), mql, "EURUSD", "H1")
    assert stale["status"] == matrix.REJECTED and "o código mudou" in stale["verdict"]


def test_rule_2_few_trades_never_count(env):
    db, robots = env
    mql = robots[0]
    run(db, mql, "EURUSD", "H1", net=5000, trades=12, pf=9.0)
    check(db, mql, "EURUSD", "H1", True)
    m = matrix.build(db, None, robots)
    assert cell(m, mql, "EURUSD", "H1")["status"] == matrix.FEW_TRADES and m["min_trades"] == 30
    assert matrix.positives(m, mql.key) == []


def test_rule_3_every_cell_says_where_the_number_came_from(env):
    db, robots = env
    mql, donchian = robots[0], robots[1]
    run(db, mql, "EURUSD", "H1", net=100)
    run(db, mql, "EURUSD", "M15", net=100, tester_model="OHLC de 1 minuto")
    run(db, donchian, "EURUSD", "H1", net=100, source="backtest")
    m = matrix.build(db, None, robots)
    assert cell(m, mql, "EURUSD", "H1")["origin"] == "Strategy Tester, cada tick com ticks reais"
    assert cell(m, mql, "EURUSD", "M15")["origin"] == "Strategy Tester, OHLC de 1 minuto"
    assert "otimista em custo" in cell(m, donchian, "EURUSD", "H1")["origin"]


def test_most_recent_run_wins_and_auxiliary_runs_are_ignored(env):
    db, robots = env
    mql = robots[0]
    run(db, mql, "EURUSD", "H1", net=2000, pf=3.0, when="2026-01-01 10:00:00")          # o melhor
    run(db, mql, "EURUSD", "H1", net=-300, pf=0.8, when="2026-02-01 10:00:00")          # o mais recente
    run(db, mql, "EURUSD", "H1", net=9999, source="strategy tester (bateria)", when="2026-03-01 10:00:00")
    run(db, mql, "EURUSD", "H1", net=9999, source="backtest python (paridade)", when="2026-03-02 10:00:00")
    m = matrix.build(db, None, robots)
    assert len(m["cells"]) == 1 and m["cells"][0]["net_profit"] == -300 and m["cells"][0]["status"] == matrix.NEGATIVE


def test_python_strategy_uses_the_saved_validation(env):
    db, robots = env
    donchian = robots[1]
    run(db, donchian, "EURUSD", "H1", net=500, source="backtest")
    assert cell(matrix.build(db, None, robots), donchian, "EURUSD", "H1")["status"] == matrix.PROMISING
    validation.save_validation(db, "EURUSD", "H1", {
        "strategy": "donchian", "approved": True, "status": "ok", "params": {},
        "oos_metrics": {"profit_factor": 1.6, "trades": 120},
        "checks": [{"name": "Janelas walk-forward positivas", "value": 0.8}],
        "robustness": {"robust": True}, "monte_carlo": {"dd_percentile": 0.1},
        "data_start": "2020-01-01", "data_end": "2024-01-01"})
    approved = cell(matrix.build(db, None, robots), donchian, "EURUSD", "H1")
    assert approved["status"] == matrix.APPROVED and approved["verdict"].startswith("validação fora da amostra")


def test_positives_is_the_robot_view(env):
    db, robots = env
    mql = robots[0]
    run(db, mql, "EURUSD", "H1", net=300, pf=1.2)
    run(db, mql, "GBPUSD", "H1", net=900, pf=2.5)
    run(db, mql, "EURUSD", "M15", net=-100)
    check(db, mql, "EURUSD", "H1", True)
    rows = matrix.positives(matrix.build(db, None, robots), mql.key)
    assert [(r["symbol"], r["timeframe"], r["status"]) for r in rows] == [
        ("EURUSD", "H1", matrix.APPROVED), ("GBPUSD", "H1", matrix.PROMISING)]     # aprovado antes, mesmo com PF menor
    assert matrix.positives(matrix.build(db, None, robots), robots[2].key) == []
