"""P06: snapshots congelados preservam namespace e relógio no replay controlado."""

from dataclasses import asdict

import pytest

from mt5_extracao import experiments
from mt5_extracao.analytics import datasets, feature_store
from mt5_extracao.analytics.pattern_families import compile_family
from mt5_extracao.analytics.pattern_flow import evaluate_registered_attempt, snapshot_observations
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.experiments import store
from mt5_extracao.presets.research import validate_research_preset
from tests.test_backtester import bars
from tests.test_pattern_campaign import campaign, source
from tests.test_pattern_simulation import COSTS, POLICY, SPEC


COMMON = dict(symbol="SYN", timeframe="M1", clock_version="UTC_fixed_v1",
              calendar_version="synthetic_utc_day", cost_model_id="qa_cost", horizon_bars=1)


def family(name, parameters):
    return compile_family(name, parameters, **COMMON)


def test_snapshot_to_observations_keeps_source_and_explicit_clock_assumption(source):
    db, _ = source
    snap = datasets.capture(db, "SYN", "M1")
    with pytest.raises(ValueError, match="calendário"):
        snapshot_observations(snap)
    prepared = snapshot_observations(snap, session_mode="synthetic_utc_day")
    assert prepared["manifest_id"] == snap.manifest_id
    assert prepared["source_namespace"] == snap.manifest["source"]["namespace"]
    assert prepared["clock_basis"] == "historical_bar_close_assumption"
    assert len(prepared["observations"]) == 15
    assert prepared["observations"][0]["bar_start_utc"] == "2025-01-06T09:00:00+00:00"
    assert prepared["observations"][0]["session_minute"] == 0
    assert prepared["backtest_bars"].source_namespace.nunique() == 1


def test_four_family_campaign_can_finish_with_zero_approved(source):
    db, _ = source
    body, _, _ = campaign(db, max_attempts=4)
    definitions = [family("reversal", {"drop_fraction": "0.013"}),
                   family("continuation_time", {"rise_fraction": "0.01",
                                                "session_start_minute": 0, "session_end_minute": 10}),
                   family("gap", {"gap_fraction": "0.01"}),
                   family("breakout_failure", {"breakout_fraction": "0.02", "lookback_bars": 2})]
    body["grid"] = [item.definition_hash for item in definitions]
    body["economic_plan"] = {"schema_version": 1, "spec_source": "synthetic_override",
                             "symbol_spec": asdict(SPEC), "policy": asdict(POLICY),
                             "costs": asdict(COSTS)}
    protocol = experiments.create_pattern_campaign(db, body)["protocol_id"]
    manifest_id = body["roster"][0]["data_manifest_id"]
    for item in definitions:
        attempt = experiments.register_pattern_attempt(db, protocol, item.body["pattern_id"], item,
                                                       origin="automatic")
        result = evaluate_registered_attempt(db, attempt["request_id"], manifest_id,
                                             session_mode="synthetic_utc_day")
        assert result["receipt"]["event_count"] == 0
        assert result["receipt"]["negative_count"] == 5
        assert result["receipt"]["economic"]["metrics"]["trades"] == 0
        assert result["receipt"]["statistical_approval"] is False
        assert result["receipt"]["economic_approval"] is False
    receipts = store.pattern_evaluations(db, protocol)
    assert len(receipts) == 4 and {row["family"] for row in receipts} == {
        "reversal", "continuation_time", "gap", "breakout_failure"}


def test_signal_precedes_outcome_and_persisted_receipt_survives_restart(tmp_path):
    path = tmp_path / "flow.sqlite"
    db = DatabaseManager(db_path=str(path), broker_utc_offset=0)
    frame = bars([[100, 101, 99, 100], [98, 99, 97, 98],
                  *([[99, 100, 98, 99]] * 13)], start="2025-01-06 09:00", freq="min")
    frame["tick_volume"], frame["real_volume"] = 10, 0
    db.save_ohlcv_data("SYN", "1 minuto", frame)
    try:
        body, _, _ = campaign(db, max_attempts=1)
        reversal = family("reversal", {"drop_fraction": "0.013"})
        body["grid"] = [reversal.definition_hash]
        body["economic_plan"] = {"schema_version": 1, "spec_source": "synthetic_override",
                                 "symbol_spec": asdict(SPEC), "policy": asdict(POLICY),
                                 "costs": asdict(COSTS)}
        protocol = experiments.create_pattern_campaign(db, body)["protocol_id"]
        attempt = experiments.register_pattern_attempt(db, protocol, "reversal", reversal)
        manifest_id = body["roster"][0]["data_manifest_id"]
        first = evaluate_registered_attempt(db, attempt["request_id"], manifest_id,
                                            session_mode="synthetic_utc_day")
        second = evaluate_registered_attempt(db, attempt["request_id"], manifest_id,
                                             session_mode="synthetic_utc_day")
        assert first["receipt"] == second["receipt"]
        assert first["receipt"]["event_count"] == first["receipt"]["complete_count"] == 1
        assert first["receipt"]["economic"]["status"] == "simulated_hypothetical"
        assert len(first["receipt"]["economic"]["trades"]) == 1
        preset = first["receipt"]["research_preset"]
        assert validate_research_preset(preset)["data_manifest_id"] == manifest_id
        assert not preset["native_set_equivalent"] and not preset["activation_authorized"]
        signal = feature_store.query_as_of(db, "2025-01-06T09:02:00Z",
                                           feature_id=first["signal_feature_id"])
        early_outcome = feature_store.query_as_of(db, "2025-01-06T09:02:00Z",
                                                  feature_id=first["outcome_feature_id"])
        final_outcome = feature_store.query_as_of(db, "2025-01-06T09:03:00Z",
                                                  feature_id=first["outcome_feature_id"])
        assert len(signal) == 1 and early_outcome.empty and len(final_outcome) == 1
        reopened = DatabaseManager(db_path=str(path), broker_utc_offset=0)
        try:
            assert store.pattern_evaluations(reopened, protocol) == [first["receipt"]]
            assert len(feature_store.query_as_of(reopened, "2025-01-06T09:03:00Z",
                                                 feature_id=first["outcome_feature_id"])) == 1
        finally:
            reopened.close()
    finally:
        db.close()


def test_validation_uses_train_support_and_purges_boundary_outcome(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "validation.sqlite"), broker_utc_offset=0)
    closes = [100, 98, 99, 100, 100, 100, 98, 99, 100, 100, 100, 100, 100, 100, 100]
    frame = bars([[value, value + 1, value - 1, value] for value in closes],
                 start="2025-01-06 09:00", freq="min")
    frame["tick_volume"], frame["real_volume"] = 10, 0
    db.save_ohlcv_data("SYN", "1 minuto", frame)
    try:
        body, _, _ = campaign(db, max_attempts=1)
        hypothesis = family("reversal", {"drop_fraction": "0.013"})
        body["grid"] = [hypothesis.definition_hash]
        body["validation_plan"] = {"schema_version": 1, "embargo_seconds": 0,
                                   "bootstrap_replicates": 99, "block_length_sessions": 1,
                                   "minimum_sessions": 1}
        protocol = experiments.create_pattern_campaign(db, body)["protocol_id"]
        attempt = experiments.register_pattern_attempt(db, protocol, "validation", hypothesis,
                                                       role="validation")
        result = evaluate_registered_attempt(db, attempt["request_id"],
                                             body["roster"][0]["data_manifest_id"],
                                             session_mode="synthetic_utc_day")
        validation = result["receipt"]["validation"]
        assert validation["status"] == "OK", validation
        assert validation["fold"]["purged"]
        assert validation["estimate"]["tests_counted"] == 1
        assert not result["receipt"]["statistical_approval"]
    finally:
        db.close()


def test_same_symbol_in_two_bases_keeps_persisted_evaluations_separate(source, tmp_path):
    db, path = source
    other = DatabaseManager(db_path=str(tmp_path / "other.sqlite"), broker_utc_offset=0)
    frame = bars([[200, 201, 199, 200]] * 15, start="2025-01-06 09:00", freq="min")
    frame["tick_volume"], frame["real_volume"] = 10, 0
    other.save_ohlcv_data("SYN", "1 minuto", frame)
    try:
        external = datasets.capture(other, "SYN", "M1")
        bundle = tmp_path / "external.json"
        datasets.export_snapshot(other, external.manifest_id, bundle)
        imported = datasets.import_snapshot(db, bundle)
        body, _, _ = campaign(db, max_attempts=1)
        body["roster"].append({"data_manifest_id": imported.manifest_id,
                               "source_namespace": imported.manifest["source"]["namespace"],
                               "symbol": "SYN", "timeframe": "M1"})
        hypothesis = family("reversal", {"drop_fraction": "0.013"})
        body["grid"] = [hypothesis.definition_hash]
        protocol = experiments.create_pattern_campaign(db, body)["protocol_id"]
        attempt = experiments.register_pattern_attempt(db, protocol, "both_sources", hypothesis)
        first, second = [evaluate_registered_attempt(db, attempt["request_id"], manifest_id,
                                                     session_mode="synthetic_utc_day") for manifest_id in
                         (body["roster"][0]["data_manifest_id"], imported.manifest_id)]
        assert first["receipt"]["source_namespace"] != second["receipt"]["source_namespace"]
        assert first["signal_feature_id"] != second["signal_feature_id"]
        reopened = DatabaseManager(db_path=str(path), broker_utc_offset=0)
        try:
            assert len(store.pattern_evaluations(reopened, protocol)) == 2
        finally:
            reopened.close()
    finally:
        other.close()


def test_result_fact_and_ledger_receipt_roll_back_together(tmp_path, monkeypatch):
    db = DatabaseManager(db_path=str(tmp_path / "rollback.sqlite"), broker_utc_offset=0)
    frame = bars([[100, 101, 99, 100], [98, 99, 97, 98],
                  *([[99, 100, 98, 99]] * 13)], start="2025-01-06 09:00", freq="min")
    frame["tick_volume"], frame["real_volume"] = 10, 0
    db.save_ohlcv_data("SYN", "1 minuto", frame)
    try:
        body, _, _ = campaign(db, max_attempts=1)
        hypothesis = family("reversal", {"drop_fraction": "0.013"})
        body["grid"] = [hypothesis.definition_hash]
        protocol = experiments.create_pattern_campaign(db, body)["protocol_id"]
        attempt = experiments.register_pattern_attempt(db, protocol, "rollback", hypothesis)
        manifest_id = body["roster"][0]["data_manifest_id"]
        original = store.event

        def fail_receipt(conn, event_id, protocol_id, request_id, kind, payload):
            if kind == "pattern_evaluated":
                raise RuntimeError("falha sintética do recibo")
            return original(conn, event_id, protocol_id, request_id, kind, payload)

        monkeypatch.setattr(store, "event", fail_receipt)
        with pytest.raises(RuntimeError, match="recibo"):
            evaluate_registered_attempt(db, attempt["request_id"], manifest_id,
                                        session_mode="synthetic_utc_day")
        monkeypatch.setattr(store, "event", original)
        signal_id = "pattern.signal." + store.digest([attempt["request_id"], manifest_id])
        assert feature_store.query_as_of(db, "2025-01-06T09:15:00Z", feature_id=signal_id).empty
        assert store.pattern_evaluations(db, protocol) == []
    finally:
        db.close()
