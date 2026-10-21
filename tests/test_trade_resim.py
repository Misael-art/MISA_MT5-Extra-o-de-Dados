"""T16.6: motor de re-simulação — o modo base reproduz o backtester (paridade) e as regras conservadoras valem."""
from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from mt5_extracao.strategies import REGISTRY, get_strategy
from mt5_extracao.strategies.backtester import BacktestConfig, SymbolSpec, Trade, run_backtest
from mt5_extracao.trade_analytics.execution import engine
from tests.market_data import random_walk

CFG = BacktestConfig(commission_per_lot=2.0, slippage_points=1.0)
SPEC = SymbolSpec(symbol="X", known=False)


def _ledger_rows(trades):
    return pd.DataFrame([{"direction": t.direction, "entry_time": t.entry_time, "exit_time": t.exit_time,
                          "entry_price": t.entry_price, "stop_price": t.stop_price, "target_price": t.target_price,
                          "volume": t.volume, "exit_reason": t.exit_reason} for t in trades])


@pytest.mark.parametrize("key", sorted(REGISTRY))
def test_base_mode_reproduces_backtester_or_refuses_missing_signal_management_context(key):
    strategy = get_strategy(key)
    bars = random_walk(4000, seed=11, vol=0.6, freq="15min", spread=10)
    # Distâncias em pontos exigem especificação conhecida. Os seis casos anteriores
    # conservam a fixture desconhecida e sua cobertura original.
    rules = strategy.exit_rules()
    units_required = (rules.target_points is not None or rules.stop_points is not None
                      or rules.signal_target_points is not None or rules.signal_stop_offset_points>0
                      or rules.move_guard_from_signal_stop or getattr(rules,"absolute_signal_target",False))
    spec = replace(SPEC, known=True, tick_size=.01) if units_required else SPEC
    result = run_backtest(bars, strategy, spec, CFG)
    assert result.trades, f"{key}: o cenário precisa gerar operações"
    if rules.move_guard_from_signal_stop:
        # Não projetar silenciosamente uma política que perde a gestão EMA do sinal.
        with pytest.raises(ValueError, match="Move Guard exige os níveis"):
            engine.ExitPolicy.from_rules(rules)
        from mt5_extracao.trade_analytics.execution.runner import base_policy
        with pytest.raises(ValueError, match="Move Guard exige os níveis"):
            base_policy("python:"+key)
        return
    atr = strategy.gerar_sinais(bars.reset_index(drop=True))["atr"]
    sim = engine.simulate(_ledger_rows(result.trades), bars, spec, CFG,
                          engine.ExitPolicy.from_rules(strategy.exit_rules()), atr)
    assert sim.skipped == 0 and sim.approximate == 0 and len(sim.trades) == len(result.trades)
    for real, again in zip(result.trades, sim.trades):
        assert again.exit_time == real.exit_time, (key, real, again)
        assert again.exit_reason == real.exit_reason
        assert again.exit_price == pytest.approx(real.exit_price, abs=1e-9)
        assert again.pnl == pytest.approx(real.pnl, rel=1e-9, abs=1e-6)


def _bars(rows):
    times = pd.date_range("2026-01-05 10:00", periods=len(rows), freq="15min")
    return pd.DataFrame({"time": times, "open": [r[0] for r in rows], "high": [r[1] for r in rows],
                         "low": [r[2] for r in rows], "close": [r[3] for r in rows], "tick_volume": 1, "spread": 0,
                         "real_volume": 0})


def _one(bars, stop, target=None, exit_i=None, reason="fim dos dados"):
    return pd.DataFrame([{"direction": 1, "entry_time": bars["time"].iloc[0], "entry_price": bars["open"].iloc[0],
                          "stop_price": stop, "target_price": target, "volume": 1.0,
                          "exit_time": bars["time"].iloc[exit_i if exit_i is not None else len(bars) - 1],
                          "exit_reason": reason}])


ZERO = BacktestConfig(swap=False)


def test_stop_first_when_stop_and_target_share_a_bar():
    bars = _bars([(100, 100.5, 99.5, 100), (100, 103, 97, 100)])          # a 2ª barra toca stop (98) e alvo (102)
    sim = engine.simulate(_one(bars, 98, 102), bars, SPEC, ZERO, engine.ExitPolicy())
    assert sim.trades[0].exit_reason == "stop" and sim.trades[0].exit_price == 98


def test_gap_fills_at_the_open():
    bars = _bars([(100, 100.5, 99.5, 100), (95, 96, 94, 95)])              # abre abaixo do stop
    sim = engine.simulate(_one(bars, 98), bars, SPEC, ZERO, engine.ExitPolicy())
    assert sim.trades[0].exit_price == 95


def test_original_signal_exit_is_kept_and_can_be_turned_off():
    bars = _bars([(100, 100.5, 99.5, 100), (101, 101.5, 100.5, 101), (102, 102.5, 101.5, 102), (103, 104, 102, 103)])
    row = _one(bars, 95, exit_i=2, reason="sinal de saída")
    kept = engine.simulate(row, bars, SPEC, ZERO, engine.ExitPolicy())
    assert kept.trades[0].exit_time == bars["time"].iloc[2] and kept.trades[0].exit_price == 102
    free = engine.simulate(row, bars, SPEC, ZERO, engine.ExitPolicy(keep_original_exits=False))
    assert free.trades[0].exit_reason == "fim dos dados"


def test_rows_without_stop_or_entry_are_skipped():
    bars = _bars([(100, 101, 99, 100)] * 3)
    rows = pd.concat([_one(bars, None), _one(bars, 98).assign(entry_time=pd.NaT)])
    sim = engine.simulate(rows, bars, SPEC, ZERO, engine.ExitPolicy())
    assert sim.skipped == 2 and sim.trades == []


def test_resimulate_a_run_from_the_book(tmp_path):
    from mt5_extracao import robots
    from mt5_extracao.database_manager import DatabaseManager
    from mt5_extracao.robots import backtest
    from mt5_extracao.trade_analytics import ledger
    from mt5_extracao.trade_analytics.execution import runner
    db = DatabaseManager(db_path=str(tmp_path / "b.db"))
    db.save_ohlcv_data("BBB", "15 minutos", random_walk(3000, seed=4, vol=0.6, freq="15min", spread=10))
    robot = next(r for r in robots.load_catalog(None, db) if r.key == "python:donchian")
    run = backtest.run_python(db, None, robot, "BBB", "M15")
    original = ledger.load(db, run_id=run["run_id"])
    assert original["stop_price"].notna().all()                  # o backtester agora grava o stop inicial
    sim = runner.resimulate(db, None, run["run_id"], save=True)
    again = ledger.load(db, run_id=run["run_id"] + "#ponto_fixo")
    assert len(again) == len(original) and set(again["source"]) == {"resim:ponto_fixo"}
    assert again["pnl"].sum() == pytest.approx(original["pnl"].sum(), rel=1e-9)
    db.close()


def test_resim_rows_do_not_double_count_in_the_lenses(tmp_path):
    from mt5_extracao.database_manager import DatabaseManager
    from mt5_extracao.trade_analytics import ledger
    db = DatabaseManager(db_path=str(tmp_path / "b.db"))
    t = [{"direction": 1, "entry_time": pd.Timestamp("2026-01-05 10:00"), "exit_time": "2026-01-05 11:00", "pnl": 1.0}]
    ledger.save_trades(db, "r", t, robot_key="a", symbol="X")
    ledger.save_trades(db, "r#trailing", t, robot_key="a", symbol="X", source="resim:trailing")
    assert len(ledger.load(db, robot_key="a")) == 1 and len(ledger.runs(db)) == 1
    assert len(ledger.load(db, robot_key="a", include_resim=True)) == 2
    assert len(ledger.load(db, source="resim:trailing")) == 1 and len(ledger.runs(db, include_resim=True)) == 2
    db.close()
