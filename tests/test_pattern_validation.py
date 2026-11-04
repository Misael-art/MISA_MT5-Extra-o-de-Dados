"""P04: cortes globais, tempo causal e reserva sem labels futuros."""

import pandas as pd
import numpy as np
import pytest

from mt5_extracao.analytics.associations import fit_pattern_baseline, analyze_pattern_baseline
from mt5_extracao.strategies.validation import plan_pattern_folds
from mt5_extracao import experiments
from tests.test_pattern_campaign import campaign, source
from tests.test_pattern_engine import definition


ROLES = {
    "train": {"start_utc": "2025-01-01T00:00:00Z", "end_utc": "2025-01-01T01:00:00Z"},
    "validation": {"start_utc": "2025-01-01T01:00:00Z", "end_utc": "2025-01-01T02:00:00Z"},
    "holdout": {"start_utc": "2025-01-01T02:00:00Z", "end_utc": "2025-01-01T03:00:00Z"},
}


def frame():
    rows = []
    for i, minute in enumerate((5, 55, 65, 115, 125)):
        decision = pd.Timestamp("2025-01-01T00:00:00Z") + pd.Timedelta(minutes=minute)
        rows.append({"source_namespace": "base_1" if i % 2 else "base_2", "symbol": "SYN",
                     "timeframe": "M1", "session_id": "s1", "decision_at_utc": decision,
                     "feature_available_at_utc": decision,
                     "label_end_utc": decision + pd.Timedelta(minutes=10),
                     "label_available_at_utc": decision + pd.Timedelta(minutes=10)})
    return pd.DataFrame(rows, index=[10, 11, 12, 13, 14])


def test_global_folds_purge_crossing_outcomes_and_preserve_censored():
    samples = frame()
    samples.loc[14, "label_end_utc"] = pd.NaT
    samples.loc[14, "label_available_at_utc"] = pd.NaT
    plan = plan_pattern_folds(samples, ROLES, embargo_seconds=0)
    assert plan["roles"]["train"]["scored"] == [10]
    assert plan["roles"]["train"]["purged"] == [11]
    assert plan["roles"]["validation"]["scored"] == [12]
    assert plan["roles"]["validation"]["purged"] == [13]
    assert plan["roles"]["holdout"]["censored"] == [14]


def test_elapsed_time_embargo_applies_across_sources_without_row_gap():
    samples = frame()
    plan = plan_pattern_folds(samples, ROLES, embargo_seconds=20 * 60)
    assert plan["roles"]["train"]["embargoed"] == [11]
    assert plan["roles"]["validation"]["embargoed"] == [13]
    assert plan["roles"]["train"]["scored"] == [10]


def test_future_feature_and_naive_clock_are_rejected_before_any_fold():
    samples = frame()
    samples.loc[10, "feature_available_at_utc"] += pd.Timedelta(seconds=1)
    with pytest.raises(ValueError, match="Vazamento futuro"):
        plan_pattern_folds(samples, ROLES, embargo_seconds=0)
    samples = frame()
    samples["decision_at_utc"] = samples.decision_at_utc.dt.tz_localize(None)
    with pytest.raises(ValueError, match="fuso horário"):
        plan_pattern_folds(samples, ROLES, embargo_seconds=0)


def paired_samples(*, start, sessions=12, effect=0.2, alternating=False):
    rows = []
    for i in range(sessions):
        decision = pd.Timestamp(start) + pd.Timedelta(days=i)
        difference = effect if not alternating else (effect if i % 2 else -effect)
        for is_pattern, value in ((False, 0.1), (True, 0.1 + difference)):
            rows.append({"source_namespace": "frozen_source", "symbol": "SYN", "timeframe": "M1",
                         "horizon_bars": 1, "session_id": f"session_{i:02d}", "stratum_id": "same_hour",
                         "decision_at_utc": decision, "feature_available_at_utc": decision,
                         "is_pattern": is_pattern, "outcome_value": value,
                         "outcome_status": "complete", "label_end_utc": decision + pd.Timedelta(minutes=1),
                         "label_available_at_utc": decision + pd.Timedelta(minutes=1)})
    return pd.DataFrame(rows)


def baseline_kwargs(frozen):
    return dict(frozen=frozen, as_of="2025-02-01T00:00:00Z", alpha=0.05, confidence=0.95,
                tests_counted=2, bootstrap_replicates=499, seed=11, minimum_sessions=10)


def test_train_metadata_freezes_baseline_and_known_effect_is_recovered():
    train = paired_samples(start="2025-01-01T00:00:00Z")
    frozen = fit_pattern_baseline(train, as_of="2025-01-13T00:00:00Z")
    validation = paired_samples(start="2025-01-14T00:00:00Z")
    result = analyze_pattern_baseline(validation, **baseline_kwargs(frozen))
    assert result["status"] == "OK"
    assert result["effect"] == pytest.approx(0.2)
    assert result["p_raw"] > 0 and result["p_adjusted"] == min(1, result["p_raw"] * 2)
    assert result["ci_low"] == pytest.approx(0.2)
    assert result["ci_high"] == pytest.approx(0.2)
    assert result["n_sessions"] == 12
    changed = dict(frozen, strata=["other"])
    with pytest.raises(ValueError, match="alterado"):
        analyze_pattern_baseline(validation, **baseline_kwargs(changed))


def test_null_and_inference_resolution_keep_unqualified_results_explicit():
    train = paired_samples(start="2025-01-01T00:00:00Z")
    frozen = fit_pattern_baseline(train, as_of="2025-01-13T00:00:00Z")
    null = paired_samples(start="2025-01-14T00:00:00Z", alternating=True)
    result = analyze_pattern_baseline(null, **baseline_kwargs(frozen))
    assert result["effect"] == pytest.approx(0)
    assert result["p_raw"] > 0.5
    coarse = baseline_kwargs(frozen)
    coarse["tests_counted"] = 100
    coarse["bootstrap_replicates"] = 99
    result = analyze_pattern_baseline(null, **coarse)
    assert result["status"] == "INSUFFICIENT_INFERENCE_RESOLUTION"
    assert result["p_raw"] > 0


def test_no_common_support_and_future_labels_cannot_be_scored():
    train = paired_samples(start="2025-01-01T00:00:00Z")
    frozen = fit_pattern_baseline(train, as_of="2025-01-13T00:00:00Z")
    validation = paired_samples(start="2025-01-14T00:00:00Z")
    validation.loc[validation.is_pattern, "stratum_id"] = "unseen"
    result = analyze_pattern_baseline(validation, **baseline_kwargs(frozen))
    assert result["status"] == "INSUFFICIENT" and result["reason_codes"] == ["NO_COMMON_SUPPORT"]
    validation = paired_samples(start="2025-01-14T00:00:00Z")
    validation["label_available_at_utc"] = pd.Timestamp("2025-03-01T00:00:00Z")
    result = analyze_pattern_baseline(validation, **baseline_kwargs(frozen))
    assert result["n_censored"] == len(validation)
    assert result["effect"] is None


def test_campaign_counts_planned_discarded_and_unplanned_definitions(source):
    db, _ = source
    body, long, _ = campaign(db, max_attempts=3)
    body["endpoints"].append({"id": "mfe", "kind": "secondary"})
    body["validation_plan"] = {"schema_version": 1, "embargo_seconds": 60,
                               "bootstrap_replicates": 999, "block_length_sessions": 2,
                               "minimum_sessions": 10}
    protocol = experiments.create_pattern_campaign(db, body)["protocol_id"]
    family = experiments.pattern_validation_context(db, protocol)
    assert family["tests_counted"] == 4 and family["confirmatory_family_frozen"]
    experiments.register_pattern_attempt(db, protocol, "discarded", long, discard_reason="quality")
    assert experiments.pattern_validation_context(db, protocol)["tests_counted"] == 4
    experiments.register_pattern_attempt(db, protocol, "extra", definition(outcome_definition={"horizon_bars": 2}))
    family = experiments.pattern_validation_context(db, protocol)
    assert family["tests_counted"] == 6 and not family["confirmatory_family_frozen"]


def test_prespecified_autocorrelated_null_is_not_systematically_approved():
    """32 DGPs/seed fixos: tolerância 5 rejeições de 32 sob nulo a 5%."""
    frozen = fit_pattern_baseline(paired_samples(start="2025-01-01T00:00:00Z"),
                                  as_of="2025-01-13T00:00:00Z")
    rejected = 0
    for seed in range(32):
        rng = np.random.default_rng(seed)
        innovations = rng.normal(0, 0.1, size=24)
        noise = np.zeros(24)
        for index, innovation in enumerate(innovations):
            noise[index] = innovation + (0.5 * noise[index - 1] if index else 0)
        samples = paired_samples(start="2025-01-14T00:00:00Z", sessions=24)
        samples.loc[samples.is_pattern, "outcome_value"] = 0.1 + noise
        result = analyze_pattern_baseline(samples, frozen=frozen, as_of="2025-03-01T00:00:00Z",
                                          alpha=0.05, confidence=0.95, tests_counted=1,
                                          bootstrap_replicates=399, seed=seed,
                                          minimum_sessions=10, block_length_sessions=3)
        assert result["status"] == "OK"
        rejected += result["p_adjusted"] <= 0.05
    assert rejected <= 5
