"""T13.9: contrato comum, ausência explícita e compatibilidade sem mutação."""
import copy
import json
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import text

from mt5_extracao import cli, profit_factor as pf
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.robots import backtest, battery, store
from mt5_extracao.strategies import report, validation
from mt5_extracao.strategies.backtester import Trade, compute_metrics
from mt5_extracao.trade_analytics import stats
from tests.market_data import random_walk


def trades(values):
    stamp = pd.Timestamp("2025-01-01")
    return [Trade(1, stamp, stamp, 1, 1, 1, value, value, 1, "alvo", 100000) for value in values]


@pytest.mark.parametrize("values,expected,state,losses", [
    ([2000, -1, 0], 2000.0, "defined", 1),
    ([100, -1], 100.0, "defined", 1),
    ([99, -1], 99.0, "defined", 1),
    ([999, -1], 999.0, "defined", 1),
    ([-1, -2, 0], 0.0, "defined", 2),
    ([5, 2], None, "no_losses", 0),
    ([0, -0.0], None, "no_losses", 0),
    ([], None, "no_trades", 0),
])
def test_producers_share_uncapped_pf_and_keep_other_metrics(values, expected, state, losses):
    original = list(values)
    card = backtest.metrics_from_results([["t", x, 1] for x in values], 100000)
    core = compute_metrics(trades(values), 100000)
    lab = stats.metrics(values, 100000)
    for result in (card, core, lab):
        assert result["profit_factor"] == expected
        assert result["profit_factor_status"] == state
        assert result["losses"] == losses
        assert result["gross_profit"] == sum(x for x in values if x > 0)
        assert result["gross_loss"] == -sum(x for x in values if x < 0)
        assert result["profit_factor_version"] == 1
        json.dumps(result, allow_nan=False)
    assert battery._profit_factor(values) == expected
    assert values == original
    assert core["net_profit"] == card["net_profit"] == lab["net"] == sum(values)
    assert core["max_drawdown_pct"] == card["max_drawdown_pct"] == lab["max_drawdown_pct"]
    assert core["trades"] == card["trades"] == lab["n"] == len(values)


@pytest.mark.parametrize("values", [[float("nan")], [float("inf")], [-float("inf")],
                                   [1, None, -1], [True, -1], ["inválido"]])
def test_invalid_pnl_does_not_become_a_usable_pf(values):
    result = pf.fields(values)
    assert result["profit_factor"] is None and result["profit_factor_status"] == "invalid_pnl"
    assert not pf.passes(result["profit_factor"], 1.3)
    json.dumps(result, allow_nan=False)


def test_ordering_and_text_do_not_reward_absence_or_cap_finite_values():
    values = [None, 0, float("nan"), 2000, 100, float("inf")]
    ordered = sorted(values, key=pf.sort_key)
    assert ordered[:3] == [2000, 100, 0]
    assert all(pf.finite(x) is None for x in ordered[3:])
    assert pf.text(2000) == "2000,00" and pf.text(None) == "indefinido"
    assert not pf.passes(None, 1.3) and not pf.passes(float("inf"), 1.3)
    assert pf.passes(2000, 1.3)
    assert cli._fmt_num(None) == "indefinido"


def test_no_loss_oos_cannot_be_approved_even_when_other_checks_pass(monkeypatch):
    sample = trades([1] * 300)
    metrics = compute_metrics(sample, 100000)
    monkeypatch.setattr(validation, "walk_forward", lambda *a: {
        "oos_trades": sample, "oos_metrics": metrics, "positive_windows": 1.0, "windows": []})
    monkeypatch.setattr(validation, "run_backtest", lambda *a: SimpleNamespace(metrics=metrics, skipped_min_lot=0))
    monkeypatch.setattr(validation, "robustness", lambda *a: {"robust": True, "share_ok": 1.0})
    monkeypatch.setattr(validation, "monte_carlo", lambda *a: {"runs": 100, "dd_percentile": 0.0})
    result = validation.validate(random_walk(100), "donchian")
    assert all(c["passed"] for c in result["checks"][1:])
    assert not result["approved"] and not result["checks"][0]["passed"]
    assert "Sem perdas" in result["checks"][0]["text"]
    json.dumps(result, allow_nan=False)


@pytest.mark.parametrize("recorded,values,expected", [(99.0, [2000, -1], 2000.0),
                                                     (99.0, [5, 1], None), (0.0, [], None)])
def test_legacy_run_projection_reconciles_pf_without_rewriting_database(tmp_path, recorded, values, expected):
    db = DatabaseManager(db_path=str(tmp_path / "old.db"))
    run = {"run_id": "old", "robot_key": "python:donchian", "source": "backtest", "symbol": "X",
           "timeframe": "H1", "data_start": "2025-01-01", "data_end": "2025-12-31", "bars": 1,
           "metrics": {"trades": len(values), "profit_factor": recorded, "net_profit": sum(values)},
           "equity": [["t", 100]], "trades": [["t", x, 1] for x in values], "created_at": "2026-01-01"}
    store.save_run(db, run)
    with db.engine.connect() as conn:
        before = conn.execute(text("SELECT metrics_json, trades_json, equity_json FROM _robot_runs")).one()
    loaded = store.load_runs(db)[0]
    assert loaded["metrics"]["profit_factor"] == expected
    assert loaded["metrics"]["profit_factor_recorded"] == recorded
    assert loaded["trades"] == run["trades"] and loaded["equity"] == run["equity"]
    with db.engine.connect() as conn:
        assert conn.execute(text("SELECT metrics_json, trades_json, equity_json FROM _robot_runs")).one() == before
    db.close()


def test_legacy_validation_sentinel_loses_projected_approval_but_stored_result_is_preserved(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "old.db"))
    with db.engine.begin() as conn:
        conn.execute(text(validation._CREATE))
        conn.execute(text("INSERT INTO _strategy_validation (symbol,timeframe,strategy,approved,status,"
                          "profit_factor_oos,trades_oos,positive_windows,robust,mc_dd,result_json,created_at) "
                          "VALUES ('X','H1','donchian',1,'antiga',999,300,1,1,0,'{}','2026-01-01')"))
    rows = validation.load_validations(db)
    assert not bool(rows.iloc[0]["approved"]) and bool(rows.iloc[0]["approved_recorded"])
    assert pd.isna(rows.iloc[0]["profit_factor_oos"])
    assert rows.iloc[0]["profit_factor_recorded"] == 999
    assert rows.iloc[0]["profit_factor_status"] == "legacy_unverified"
    html = report._matrix([], rows)
    assert "indefinido" in html and "∞" not in html and "✅ aprovada" not in html
    with db.engine.connect() as conn:
        assert conn.execute(text("SELECT approved,profit_factor_oos,status FROM _strategy_validation")).one() == (1, 999, "antiga")
    db.close()


def test_new_contract_reloads_finite_ratio_above_old_validation_cap():
    saved = pf.fields([2000, -1])
    assert pf.read_fields(saved)["profit_factor"] == 2000
    assert pf.read_fields({"profit_factor": 99})["profit_factor"] is None
    assert pf.read_fields({"profit_factor": 999})["profit_factor"] is None
    assert pf.read_fields({"profit_factor": 1.7})["profit_factor_status"] == "legacy_reported"
    original = copy.deepcopy(saved)
    pf.read_fields(saved)
    assert saved == original


@pytest.mark.parametrize("values", [[2000, -1], [5, 1], []])
def test_new_validation_round_trip_keeps_ratio_or_null_with_reason(tmp_path, values):
    db = DatabaseManager(db_path=str(tmp_path / "new.db"))
    metrics = {**pf.fields(values), "trades": len(values)}
    verdict = {"strategy": "donchian", "params": {}, "approved": False, "status": "reprovada",
               "oos_metrics": metrics, "checks": [{"name": "Janelas", "value": 0.0}],
               "robustness": {"robust": False}, "monte_carlo": {"dd_percentile": 0.0},
               "data_start": "2025-01-01", "data_end": "2025-12-31"}
    validation.save_validation(db, "X", "H1", verdict)
    loaded = validation.load_validations(db).iloc[0]
    assert loaded["profit_factor_oos"] == metrics["profit_factor"]
    assert loaded["profit_factor_status"] == metrics["profit_factor_status"]
    with db.engine.connect() as conn:
        scalar, raw = conn.execute(text("SELECT profit_factor_oos,result_json FROM _strategy_validation")).one()
    assert scalar == metrics["profit_factor"]
    assert json.loads(raw)["oos_metrics"] == metrics
    assert "Infinity" not in raw and "NaN" not in raw
    db.close()


def test_pre_trades_column_database_opens_and_marks_legacy_cap_unverified(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "pre-trades.db"))
    with db.engine.begin() as conn:
        conn.execute(text(store._CREATE_RUNS))
        conn.execute(text("INSERT INTO _robot_runs (run_id,robot_key,source,metrics_json,created_at) "
                          "VALUES ('old','python:donchian','backtest',:metrics,'2025-01-01')"),
                     {"metrics": json.dumps({"trades": 300, "profit_factor": 99})})
    loaded = store.load_runs(db)[0]
    assert loaded["trades"] == [] and loaded["metrics"]["profit_factor"] is None
    assert loaded["metrics"]["profit_factor_recorded"] == 99
    assert loaded["metrics"]["profit_factor_status"] == "legacy_unverified"
    with db.engine.connect() as conn:
        assert not store._has_trades_column(conn)
    db.close()


def test_trade_cli_exports_json_null_and_explains_undefined_in_text(tmp_path, capsys):
    from mt5_extracao.trade_analytics import ledger
    db = DatabaseManager(db_path=str(tmp_path / "controlled.db"))
    ledger.save_trades(db, "controlled", trades([1, 5]), robot_key="python:donchian", symbol="X", timeframe="H1")
    config = tmp_path / "config.ini"
    config.write_text(f"[DATABASE]\ntype=sqlite\npath={db.db_path}\n", encoding="utf-8")
    db.close()
    args = ["--config", str(config)]
    assert cli.main(args + ["--json", "trades", "report", "--robot", "python:donchian"]) == cli.EXIT_OK
    data = json.loads(capsys.readouterr().out.strip().splitlines()[-1])["data"]
    assert data["total"]["profit_factor"] is None
    assert data["total"]["profit_factor_status"] == "no_losses"
    assert cli.main(args + ["trades", "report", "--robot", "python:donchian"]) == cli.EXIT_OK
    assert "indefinido" in capsys.readouterr().out
