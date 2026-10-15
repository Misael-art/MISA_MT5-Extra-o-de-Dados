import json

import pandas as pd
import pytest

from mt5_extracao.analytics import presentation, validation


STAMP = "2026-01-01T00:00:00Z"


def _ledger(*, with_costs=False):
    rows = [
        {"run_id": "r1", "seq": 3, "source": "fixture", "symbol": "EURUSD", "volume": 1.0,
         "entry_time": "2026-01-03T00:00:00Z", "exit_time": "2026-01-03T01:00:00Z", "pnl": 80.0},
        {"run_id": "r1", "seq": 2, "source": "fixture", "symbol": "EURUSD", "volume": 1.0,
         "entry_time": "2026-01-02T00:00:00Z", "exit_time": "2026-01-02T01:00:00Z", "pnl": -150.0},
        {"run_id": "r1", "seq": 1, "source": "fixture", "symbol": "EURUSD", "volume": 1.0,
         "entry_time": "2026-01-01T00:00:00Z", "exit_time": "2026-01-01T01:00:00Z", "pnl": 100.0},
    ]
    if with_costs:
        for row, cost in zip(rows, (3.0, 2.0, 1.0)):
            for component in presentation.COST_COMPONENTS:
                row[f"cost_{component}"] = cost / len(presentation.COST_COMPONENTS)
    return pd.DataFrame(rows)


def _evaluation():
    return {"weak": False, "removed": 1, "rules": [{"field": "spread", "op": "<", "value": 2}],
            "folds": [{"before": {"expectancy": 1.0}, "after": {"expectancy": 2.0}},
                      {"before": {"expectancy": 2.0}, "after": {"expectancy": 1.0}}],
            "folds_improved": 1}


def test_filter_payload_labels_fixed_ledger_and_keeps_unknown_net_and_signals_null():
    original = _ledger()
    snapshot = original.copy(deep=True)
    payload = presentation.build_filter_comparison_payload(
        original, original.iloc[1:].copy(), _evaluation(), minimum_operations=2,
        generated_at_utc=STAMP, as_of_utc=STAMP)

    assert payload["comparison"]["kind"] == "FIXED_LEDGER_COUNTERFACTUAL"
    assert payload["comparison"]["state_changes_replayed"] is False
    assert payload["original"]["recorded_pnl_total"]["value"] == 30.0
    assert payload["original"]["net_pnl_total"]["value"] is None
    assert payload["original"]["net_pnl_total"]["state"] == "INCOMPLETE_COSTS"
    assert payload["signals"] == {"accepted": None, "blocked": None, "source": "NOT_RECORDED"}
    assert payload["stability"] == {
        "state": "EXPLORATORY", "method": "walk_forward_fold_improvement_fraction", "folds": 2,
        "folds_improved": 1, "estimate": 0.5, "interval": None,
        "note": "Fração descritiva de provas que melhoraram; não é probabilidade.",
    }
    json.dumps(payload, allow_nan=False)
    pd.testing.assert_frame_equal(original, snapshot)


def test_complete_costs_and_time_order_produce_net_and_drawdown_metrics():
    ledger = _ledger(with_costs=True)
    payload = presentation.build_filter_comparison_payload(
        ledger, ledger.copy(), _evaluation(), pnl_basis="gross", pnl_unit="account_currency",
        initial_equity=1000, minimum_operations=2, generated_at_utc=STAMP)

    metrics = payload["original"]
    assert metrics["costs"]["complete"] is True
    assert metrics["costs"]["total"] == pytest.approx(6.0)
    assert set(metrics["costs"]["coverage_by_component"].values()) == {1.0}
    assert metrics["net_pnl_total"]["value"] == pytest.approx(24.0)
    assert metrics["max_drawdown_amount"]["value"] == pytest.approx(152.0)
    assert metrics["max_drawdown_pct"]["value"] == pytest.approx(152 / 1099 * 100)
    assert metrics["peak_exposure"]["value"] == pytest.approx(1.0)
    assert payload["delta"]["max_drawdown_amount"]["value"] == pytest.approx(0.0)

    partial = ledger.copy()
    partial.loc[partial.index[0], "cost_spread"] = None
    partial_payload = presentation.build_filter_comparison_payload(
        partial, partial, _evaluation(), pnl_basis="gross", minimum_operations=2, generated_at_utc=STAMP)
    partial_costs = partial_payload["original"]["costs"]
    assert partial_costs["coverage_by_component"]["spread"] == pytest.approx(2 / 3)
    assert partial_payload["original"]["net_pnl_total"]["state"] == "INCOMPLETE_COSTS"


def test_freshness_requires_expiry_and_uses_timezone_aware_instants():
    ledger = _ledger()
    unavailable = presentation.build_filter_comparison_payload(ledger, ledger, {}, generated_at_utc=STAMP)
    stale = presentation.build_filter_comparison_payload(
        ledger, ledger, {}, generated_at_utc=STAMP, as_of_utc="2026-01-02T00:00:00Z",
        valid_until_utc="2026-01-01T12:00:00-03:00")
    expires_now = presentation.build_filter_comparison_payload(
        ledger, ledger, {}, generated_at_utc=STAMP, as_of_utc=STAMP, valid_until_utc=STAMP)
    assert unavailable["evidence"]["freshness"] == "UNAVAILABLE"
    assert stale["evidence"]["freshness"] == "STALE"
    assert expires_now["evidence"]["freshness"] == "STALE"
    with pytest.raises(ValueError, match="fuso horário"):
        presentation.build_filter_comparison_payload(ledger, ledger, {}, as_of_utc="2026-01-01T00:00:00")


def test_exposure_is_unavailable_for_mixed_symbols_or_mixed_timezones():
    ledger = _ledger()
    mixed_symbol = ledger.copy()
    mixed_symbol.loc[mixed_symbol.index[0], "symbol"] = "GBPUSD"
    result = presentation.build_filter_comparison_payload(ledger, mixed_symbol, {}, generated_at_utc=STAMP)
    assert result["original"]["peak_exposure"]["state"] == "AVAILABLE"
    assert result["candidate"]["peak_exposure"]["state"] == "MIXED_SYMBOLS"

    mixed_time = ledger.copy()
    mixed_time.loc[mixed_time.index[0], "entry_time"] = "2026-01-03T00:00:00"
    mixed_time.loc[mixed_time.index[0], "exit_time"] = "2026-01-03T01:00:00"
    result = presentation.build_filter_comparison_payload(ledger, mixed_time, {}, generated_at_utc=STAMP)
    assert result["candidate"]["peak_exposure"]["state"] == "MIXED_TIMEZONES"
    assert result["provenance"]["time_basis"] == "MIXED_TIMEZONES"

    incomplete = ledger.copy()
    incomplete.loc[incomplete.index[0], "exit_time"] = None
    result = presentation.build_filter_comparison_payload(ledger, incomplete, {}, generated_at_utc=STAMP)
    assert result["provenance"]["time_basis"] == "INCOMPLETE_TIME_DATA"
    assert result["candidate"]["recorded_pnl_max_drawdown_amount"]["state"] == "UNAVAILABLE_TIME_ORDER"


def test_hypothesis_catalog_preserves_attempts_and_separates_lifecycle_from_evidence(monkeypatch):
    patterns = [{"pattern_id": "p1", "version": "1", "title": "Pattern", "status": "APPROVED",
                 "definition": {"condition": {"field": "spread"}}}]
    evaluations = [
        {"evaluation_id": "e1", "pattern_id": "p1", "version": "1", "attempt_no": 1,
         "status": "APPROVED", "holdout_reused": 0, "result": {"eligible_for_approval": True,
             "weak": False, "test": {"expectancy_net": 2}}, "validated_at_utc": STAMP},
        {"evaluation_id": "e2", "pattern_id": "p1", "version": "1", "attempt_no": 2,
         "status": "FAILED", "holdout_reused": 0, "result": {"weak": True, "test": {"available": False}},
         "validated_at_utc": STAMP},
        {"evaluation_id": "e3", "pattern_id": "p1", "version": "1", "attempt_no": 3,
         "status": "EXPLORATORY", "holdout_reused": 1, "result": {"weak": False, "test": {"reused": True}},
         "validated_at_utc": STAMP},
    ]
    monkeypatch.setattr(validation, "list_patterns", lambda _db: patterns)
    monkeypatch.setattr(validation, "list_evaluations", lambda *_args, **_kwargs: evaluations)

    payload = presentation.build_hypothesis_catalog(object(), pattern_id="p1", generated_at_utc=STAMP)
    item = payload["items"][0]
    assert item["lifecycle_status"] == "APPROVED"
    assert [attempt["evaluation_id"] for attempt in item["evaluations"]] == ["e1", "e2", "e3"]
    assert [attempt["evidence_state"] for attempt in item["evaluations"]] == [
        "VALIDATED", "INSUFFICIENT", "INSUFFICIENT"]
    assert item["evaluations"][0]["freshness"] == "UNAVAILABLE"
    assert item["evaluations"][0]["provenance"]["source_revision"] is None
    json.dumps(payload, allow_nan=False)
