"""Consulta real/CLI e indisponibilidade explícita do legado."""
import json

import numpy as np
import pytest
from sqlalchemy import text

from mt5_extracao import cli
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.trade_analytics import ledger
from mt5_extracao.trade_analytics.trajectories import service
from tests.test_trade_cli import setup, _call
from tests.test_trajectory_store import prepared


def test_cli_consults_saved_cycle_without_changing_ledger(setup, capsys):
    db, run, config = setup
    before = ledger.load(db, run_id=run["run_id"])
    code, out = _call(config, capsys, "trajectory", "--run", run["run_id"], "--seq", "0", "--threshold", "1")
    assert code == cli.EXIT_OK
    result = out["data"]["trajectory"]
    assert result["trajectory_id"] and result["data_manifest_id"] == run["data_manifest_id"]
    assert result["status"] == "observed" and len(result["units"]) == 1
    assert result["units"][0]["final_net_pnl"] == before.iloc[0].pnl
    assert result["units"][0]["identity"]["source"] == "python"
    assert ledger.load(db, run_id=run["run_id"]).equals(before)
    json.dumps(result, allow_nan=False)


def test_price_query_uses_frozen_data_and_rejects_unqualified_conversion(setup, monkeypatch):
    from mt5_extracao.trade_analytics.trajectories import paths
    db, run, _ = setup
    original = paths.bar_intervals
    preparations = []
    def counted(*args, **kwargs):
        preparations.append(len(args[0]))
        return original(*args, **kwargs)
    monkeypatch.setattr(paths, "bar_intervals", counted)
    first = service.query(db, run["run_id"], unit="price")
    assert len(first["units"]) > 1
    assert preparations == [3000]  # uma preparação da fonte por consulta, não por trade
    with db.engine.begin() as conn:
        table = db.find_table_for_symbol("BBB", "15 minutos")
        conn.execute(text(f'UPDATE "{table}" SET high=high+100000'))
    assert service.query(db, run["run_id"], unit="price") == first
    assert preparations == [3000, 3000]  # não há cache global entre consultas
    with pytest.raises(ValueError, match="historicamente qualificada"):
        service.query(db, run["run_id"], unit="points")


def test_legacy_missing_observations_or_entry_is_not_a_zero_cash_curve(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "old.sqlite"))
    try:
        ledger.save_trades(db, "old", [{"exit_time": "2026-01-05 11:00", "pnl": 10}],
                           symbol="X", timeframe="M1", source="mql5_tester")
        money = service.query(db, "old")
        assert money["reason"] == "missing_producer_observations" and money["units"] == []
        price = service.query(db, "old", unit="price")
        assert price["status"] == "unavailable" and price["units"][0]["age_min"] is None
        assert price["units"][0]["limitations"] == ["missing_entry_time"]
    finally:
        db.close()


@pytest.mark.parametrize("seq", [True, -1, 0.5])
def test_invalid_sequence_is_rejected(setup, seq):
    db, run, _ = setup
    with pytest.raises(ValueError, match="Seq"):
        service.query(db, run["run_id"], seq=seq)


def test_selected_row_keeps_full_run_duration_population(setup, capsys):
    db, run, config = setup
    code, out = _call(config, capsys, "trajectory", "--run", run["run_id"], "--seq", "0",
                      "--age", "0", "--age", "15", "--age", "60")
    assert code == cli.EXIT_OK
    report = out["data"]["trajectory"]
    assert len(report["units"]) == 1 and report["duration"]["population"] == "full_run"
    assert report["duration"]["n_input"] == report["n_ledger"] == run["metrics"]["trades"]
    assert sum(g["n"] for g in report["duration"]["groups"]) == report["n_ledger"]
    assert report["duration"]["probability_of_winning"] is None


def test_sdk_numpy_selection_and_grid_are_strict_json(setup):
    db, run, _ = setup
    result = service.query(db, run["run_id"], seq=np.int64(0), ages=[np.int64(0), np.int64(15)],
                           thresholds=[np.int64(1)])
    parsed = json.loads(json.dumps(result, allow_nan=False))
    assert parsed["request"]["seq"] == 0 and parsed["request"]["ages_min"] == [0, 15]


def test_captured_observed_ticks_are_not_promoted_to_complete_path(tmp_path):
    from mt5_extracao.analytics import datasets
    from tests.test_data_eligibility import prices, save_ticks
    db = DatabaseManager(db_path=str(tmp_path / "captured.sqlite"), broker_utc_offset=0)
    try:
        db.save_ohlcv_data("X", "1 hora", prices())
        save_ticks(db)
        snapshot = datasets.capture(db, "X", "H1", families=["ticks"])
        row = {"entry_time": "2025-01-06 00:00", "exit_time": "2025-01-06 02:00", "direction": 1,
               "entry_price": 100, "exit_price": 100, "volume": 1, "pnl": 0}
        ledger.save_trades(db, "ticks", [row], symbol="X", timeframe="H1", source="mql5_tester")
        datasets.bind(db, "ticks", snapshot.manifest_id)
        result = service.query(db, "ticks", unit="price", thresholds=[1])
        assert result["units"][0]["resolution_reason"] == "captured_quotes"
        assert result["units"][0]["path_gate"]["eligible"]
        assert not result["absolute_crossing"]["data_gate"]["eligible"]
        assert all(not t["first_absolute_crossing_known"] for t in result["units"][0]["thresholds"])
        with db.engine.begin() as conn:
            conn.execute(text('UPDATE "x_ticks" SET flags=1.5'))
        revised = datasets.capture(db, "X", "H1", families=["ticks"])
        ledger.save_trades(db, "invalid", [row], symbol="X", timeframe="H1", source="mql5_tester")
        datasets.bind(db, "invalid", revised.manifest_id)
        bad = service.query(db, "invalid", unit="price")
        assert bad["units"][0]["resolution_reason"] == "qualified_ohlc_fallback"
        assert not bad["units"][0]["quote_gate"]["eligible"]
        bases = {p["basis"] for p in bad["units"][0]["points"]}
        assert "ohlc_interval" in bases
        assert bases <= {"bar_open", "ohlc_interval", "ledger_execution_endpoint"}
        assert service.query(db, "ticks", unit="price", thresholds=[1]) == result
    finally:
        db.close()


def test_price_query_uses_producer_close_and_never_future_exit(prepared):
    from mt5_extracao.trade_analytics.trajectories import store
    from mt5_extracao.trade_analytics.trajectories.clocks import Clock
    db, payload = prepared
    store.save(db, "r", payload, source="python", clock=Clock("utc"))
    closed = service.query(db, "r", unit="price")["units"][0]
    money = service.query(db, "r")["units"][0]
    assert closed["age_min"] == money["age_min"]
    assert closed["points"][-1]["basis"] == "producer_execution_fill"
    cut = service.query(db, "r", unit="price", as_of="2026-01-05 10:02:30")["units"][0]
    assert cut["right_censored"] and cut["age_min"] == 1
    assert "as_of_between_observations" in cut["limitations"]
    assert all(p["basis"] != "producer_execution_fill" for p in cut["points"])
