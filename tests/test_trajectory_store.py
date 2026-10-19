"""Persistência aditiva de trajetórias e integração com a publicação normal."""
from copy import deepcopy

import pandas as pd
import pytest
from sqlalchemy import inspect, text

from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.strategies.backtester import BacktestConfig, SymbolSpec, run_backtest
from mt5_extracao.trade_analytics import ledger
from mt5_extracao.trade_analytics.trajectories import store
from mt5_extracao.trade_analytics.trajectories.clocks import Clock
from tests.test_backtester import Manual, bars


@pytest.fixture
def prepared(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "trajectory.sqlite"))
    data = bars([[100, 101, 99, 100], [100, 102, 99, 101], [101, 103, 100, 102]],
                start="2026-01-05 10:00", freq="min")
    result = run_backtest(data, Manual(long=[0], stop={0: 90}),
                          SymbolSpec(point=1, tick_size=1, tick_value=1), BacktestConfig(),
                          observation_timeframe="M1")
    ledger.save_trades(db, "r", result.trades, symbol="X", timeframe="M1", source="python")
    yield db, result.trajectory
    db.close()


def test_sidecar_round_trip_is_idempotent_and_leaves_raw_ledger_untouched(prepared):
    db, payload = prepared
    before = ledger.load(db, run_id="r")
    first = store.save(db, "r", payload, source="python", clock=Clock("utc"))
    assert store.save(db, "r", payload, source="python", clock=Clock("utc")) == first
    loaded = store.load(db, "r")
    assert loaded["trajectory_id"] == first and loaded["observations"] == payload
    pd.testing.assert_frame_equal(before, ledger.load(db, run_id="r"))
    with db.engine.connect() as conn:
        assert conn.execute(text(f"SELECT COUNT(*) FROM {store.TABLE}")).scalar() == 1


def test_changed_ledger_does_not_reuse_or_delete_an_old_trajectory(prepared):
    db, payload = prepared
    store.save(db, "r", payload, source="python", clock=Clock("utc"))
    with db.engine.begin() as conn:
        conn.execute(text("UPDATE _trade_ledger SET pnl=pnl+1 WHERE run_id='r'"))
    with pytest.raises(ValueError, match="mudou"):
        store.load(db, "r")
    with pytest.raises(ValueError, match="conciliam"):
        store.save(db, "r", payload, source="python", clock=Clock("utc"))
    with db.engine.connect() as conn:
        assert conn.execute(text(f"SELECT COUNT(*) FROM {store.TABLE}")).scalar() == 1


def test_corrupted_payload_is_not_accepted_as_a_result(prepared):
    db, payload = prepared
    store.save(db, "r", payload, source="python", clock=Clock("utc"))
    with db.engine.begin() as conn:
        conn.execute(text(f"UPDATE {store.TABLE} SET payload_json=payload_json || 'corrupt'"))
    with pytest.raises(ValueError, match="integridade"):
        store.load(db, "r")


@pytest.mark.parametrize("field,value", [("direction", -1), ("fill_price", 1234), ("bar_time", "2026-01-05 10:00")])
def test_foreign_or_inconsistent_observations_cannot_bind_to_the_ledger(prepared, field, value):
    db, payload = prepared
    payload = deepcopy(payload)
    payload["records"][-1][field] = value
    with pytest.raises(ValueError, match="conciliam"):
        store.save(db, "r", payload, source="python", clock=Clock("utc"))


def test_old_database_read_does_not_create_a_sidecar(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "old.sqlite"))
    try:
        before = inspect(db.engine).get_table_names()
        assert store.load(db, "old") is None
        assert inspect(db.engine).get_table_names() == before
    finally:
        db.close()


def test_mixed_second_and_fractional_precision_does_not_erase_a_real_entry(tmp_path):
    from mt5_extracao.trade_analytics.trajectories import service
    db = DatabaseManager(db_path=str(tmp_path / "fractional.sqlite"))
    try:
        rows = [dict(direction=1, entry_price=100, pnl=1,
                     entry_time="2026-01-05 10:00:00", exit_time="2026-01-05 10:01:00"),
                dict(direction=1, entry_price=100, pnl=2,
                     entry_time="2026-01-05 10:02:00.123456", exit_time="2026-01-05 10:03:00.654321")]
        ledger.save_trades(db, "fractional", rows, symbol="X", timeframe="M1", source="demo")
        with db.engine.connect() as conn:
            before = conn.execute(text("SELECT entry_time,exit_time FROM _trade_ledger ORDER BY seq")).all()
        loaded = ledger.load(db, run_id="fractional")
        assert loaded.entry_time.notna().all() and loaded.exit_time.notna().all()
        assert loaded.iloc[1].entry_time == pd.Timestamp("2026-01-05 10:02:00.123456")
        report = service.query(db, "fractional", unit="price")
        assert report["units"][1]["age_min"] == pytest.approx(60.530865 / 60)
        with db.engine.connect() as conn:
            assert conn.execute(text("SELECT entry_time,exit_time FROM _trade_ledger ORDER BY seq")).all() == before
    finally:
        db.close()


def test_normal_python_and_saved_comparison_publish_observations_bound_to_snapshot(tmp_path):
    from mt5_extracao import robots
    from mt5_extracao.robots import backtest
    from mt5_extracao.trade_analytics.execution import runner
    from tests.market_data import random_walk
    db = DatabaseManager(db_path=str(tmp_path / "normal.sqlite"))
    try:
        db.save_ohlcv_data("X", "15 minutos", random_walk(3000, seed=4, vol=0.6, freq="15min", spread=10))
        robot = next(r for r in robots.load_catalog(None, db) if r.key == "python:donchian")
        run = backtest.run_python(db, None, robot, "X", "M15")
        current = store.load(db, run["run_id"])
        assert current is not None and current["manifest_id"] == run["data_manifest_id"]
        assert current["clock_origin"] == "data_manifest"
        assert current["producer_identity"] == run["metrics"]["execution_identity"]
        compared = runner.compare(db, None, run["run_id"], ["ponto_fixo", "trailing_atr"], save=True)
        for mode in compared:
            saved = store.load(db, run["run_id"] + "#" + mode)
            assert saved is not None and saved["source"] == "resim:" + mode
            assert saved["manifest_id"] == current["manifest_id"]
            assert saved["observations"]["records"]
    finally:
        db.close()


def test_saving_legacy_resimulation_freezes_current_inputs_without_rebinding_original(tmp_path):
    from mt5_extracao.analytics import datasets
    from mt5_extracao.trade_analytics.execution import runner
    from mt5_extracao.trade_analytics.execution.engine import ExitPolicy
    db = DatabaseManager(db_path=str(tmp_path / "legacy-resim.sqlite"))
    try:
        data = bars([[100, 101, 99, 100], [100, 102, 99, 101], [101, 103, 100, 102]],
                    start="2026-01-05 10:00", freq="min")
        data["tick_volume"], data["real_volume"] = 1, 0
        db.save_ohlcv_data("X", "1 minuto", data)
        original = dict(direction=1, entry_time=data.time.iloc[0], exit_time=data.time.iloc[-1],
                        entry_price=100, exit_price=102, stop_price=90, target_price=None,
                        volume=1, pnl=2, exit_reason="fim dos dados")
        ledger.save_trades(db, "old", [original], symbol="X", timeframe="M1", source="mql5_tester")
        before = ledger.load(db, run_id="old")
        assert datasets.for_run(db, "old") is None
        runner.resimulate(db, None, "old", policy=ExitPolicy(), save=True)
        saved = store.load(db, "old#ponto_fixo")
        assert saved["manifest_id"] and saved["producer_identity"]["data_provenance_status"] == "current_data_captured_for_resimulation"
        assert datasets.for_run(db, "old") is None
        pd.testing.assert_frame_equal(before, ledger.load(db, run_id="old"))
    finally:
        db.close()
