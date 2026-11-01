"""DASH-07: validação walk-forward a partir dos recibos canônicos, sem recalcular nem inferir."""
from datetime import datetime, timedelta, timezone

import pytest

from mt5_extracao.analytics import datasets
from mt5_extracao.dashboard import document, generator
from mt5_extracao.strategies import validation


@pytest.fixture
def db(tmp_path):
    from mt5_extracao.database_manager import DatabaseManager
    manager = DatabaseManager(db_path=str(tmp_path / "qa.sqlite"))
    datasets.identity(manager)
    yield manager
    manager.close()


def verdict(strategy="qa_wf", approved=True, pf=1.4, windows=3):
    folds = [{"window": i + 1, "oos_start": f"2026-0{i + 2}-01 00:00:00", "oos_end": f"2026-0{i + 2}-28 23:55:00",
              "params": {"p": i}, "is_profit_factor": 1.6, "is_trades": 80, "is_profit_factor_status": "OK",
              "oos_profit_factor": None if i == 1 else pf, "oos_trades": 20 + i,
              "oos_profit_factor_status": "NO_LOSSES" if i == 1 else "OK", "oos_net_profit": -5.0 if i == 2 else 12.5}
             for i in range(windows)]
    return {"strategy": strategy, "approved": approved, "status": "QA", "params": {"p": 1},
            "oos_metrics": {"profit_factor": pf, "trades": 63, "profit_factor_status": "OK"},
            "checks": [{"name": "Janelas walk-forward positivas", "value": 0.66, "threshold": 0.5, "passed": True,
                        "text": "66% das janelas com lucro"}],
            "robustness": {"robust": True}, "monte_carlo": {"dd_percentile": 0.12}, "windows": folds,
            "criteria": {"n_windows": windows}, "data_start": "2026-01-01 00:00:00", "data_end": "2026-06-30 23:55:00"}


def sections_of(db, cut):
    snapshot = generator.generate(db, symbol="QA", timeframe="M5", as_of=cut.isoformat())
    generator.validate(snapshot)
    return snapshot, [s for s in snapshot["sections"] if s["kind"] == "validation"]


def test_receipt_becomes_fold_timeline_without_inventing_train_dates_or_costs(db):
    validation.save_validation(db, "QA", "M5", verdict())
    snapshot, found = sections_of(db, datetime.now(timezone.utc) + timedelta(days=1))
    section = next(s for s in found if s["id"] == "validation:qa_wf")
    assert section["default_visible"] and section["source_status"] == "FILTER_C_APPROVED"
    assert [f["window"] for f in section["visual"]["folds"]] == [1, 2, 3]
    assert section["visual"]["folds"][1]["oos_profit_factor"] is None          # sem PF não vira zero
    assert section["evidence"]["costs"]["state"] == "NOT_RECORDED"
    assert section["evidence"]["train_dates"]["state"] == "NOT_RECORDED"
    assert section["evidence"]["approved_is_hypothesis"] is False
    assert snapshot["capabilities"]["validation"]["state"] == "AVAILABLE"


def test_rejected_strategy_stays_auditable_but_hidden(db):
    validation.save_validation(db, "QA", "M5", verdict("qa_bad", approved=False, pf=0.8))
    _, found = sections_of(db, datetime.now(timezone.utc) + timedelta(days=1))
    section = found[0]
    assert not section["default_visible"] and section["hidden_reason_code"] == "FILTER_C_REJECTED"


def test_naive_clock_possibly_after_cut_is_omitted_and_counted(db):
    validation.save_validation(db, "QA", "M5", verdict())
    snapshot, found = sections_of(db, datetime.now(timezone.utc))
    assert [s["id"] for s in found] == ["validation:late"]
    assert found[0]["hidden_reason_code"] == "CLOCK_AFTER_CUT" and found[0]["status_namespace"] == "dashboard"
    assert snapshot["capabilities"]["validation"]["state"] == "NO_DATA"


def test_other_symbol_or_timeframe_is_not_mixed(db):
    validation.save_validation(db, "OUTRO", "M5", verdict())
    validation.save_validation(db, "QA", "H1", verdict())
    _, found = sections_of(db, datetime.now(timezone.utc) + timedelta(days=1))
    assert found == []


def test_receipt_without_windows_is_insufficient_not_approved_timeline(db):
    validation.save_validation(db, "QA", "M5", verdict(windows=0))
    _, found = sections_of(db, datetime.now(timezone.utc) + timedelta(days=1))
    assert found[0]["display_state"] == "INSUFFICIENT" and found[0]["visual"]["folds"] == []


def test_reader_does_not_write_or_create_tables(db):
    from sqlalchemy import inspect
    sections_of(db, datetime.now(timezone.utc))
    assert not inspect(db.engine).has_table("_strategy_validation")


def test_document_embeds_validation_renderer():
    assert "js/charts/validation.js" in document.RESOURCES
