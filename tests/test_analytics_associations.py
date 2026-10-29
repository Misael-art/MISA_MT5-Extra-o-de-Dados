import numpy as np
import pandas as pd
import pytest

from mt5_extracao.analytics import associations


UTC = "UTC"


def _samples(x, y, *, sessions=20, bars_per_session=5):
    rows = []
    start = pd.Timestamp("2026-09-01 12:00", tz=UTC)
    for index, (x_value, y_value) in enumerate(zip(x, y)):
        session = index // bars_per_session
        decision = start + pd.Timedelta(days=session, minutes=(index % bars_per_session + 1) * 5)
        rows.append({
            "symbol": "TEST", "timeframe": "M5", "session_id": f"s{session:03}",
            "decision_time": decision,
            "x_available_at_utc": decision, "y_available_at_utc": decision,
            "x_value": x_value, "y_value": y_value,
        })
    return pd.DataFrame(rows)


def _bars(closes):
    start = pd.Timestamp("2026-10-01 12:00", tz=UTC)
    rows = []
    for index, close in enumerate(closes):
        bar_start = start + pd.Timedelta(minutes=index * 5)
        bar_end = bar_start + pd.Timedelta(minutes=5)
        rows.append({
            "symbol": "TEST", "tf": "M5", "session_id": "session-1",
            "bar_start_utc": bar_start, "bar_end_utc": bar_end,
            "computed_at_utc": bar_end + pd.Timedelta(seconds=1),
            "revision": 1, "close": close,
        })
    return pd.DataFrame(rows)


def test_forward_targets_are_mature_only_after_the_future_close_is_received():
    bars = _bars([100, 110, 121, 100])
    as_of = bars["computed_at_utc"].max() + pd.Timedelta(seconds=1)
    targets = associations.build_forward_close_targets(bars, horizons=(1, 2), as_of=as_of)

    one_bar = targets[targets.target_id == "return.close_forward_1b"].reset_index(drop=True)
    two_bars = targets[targets.target_id == "return.close_forward_2b"].reset_index(drop=True)
    assert one_bar.loc[0, "value"] == pytest.approx(0.1)
    assert one_bar.loc[0, "state"] == "MATURE"
    assert two_bars.loc[0, "value"] == pytest.approx(0.21)
    assert two_bars.loc[0, "label_end_utc"] == bars.loc[2, "bar_end_utc"]
    assert pd.isna(one_bar.loc[3, "value"])
    assert one_bar.loc[3, "reason"] == "HORIZON_BARS_NOT_AVAILABLE"
    assert two_bars.loc[2, "state"] == "NOT_MATURE"

    delayed = bars.copy()
    delayed.loc[2, "computed_at_utc"] = as_of + pd.Timedelta(hours=1)
    delayed_targets = associations.build_forward_close_targets(delayed, horizons=(1,), as_of=as_of)
    delayed_label = delayed_targets.iloc[1]
    assert pd.isna(delayed_label["value"])
    assert delayed_label["state"] == "NOT_AVAILABLE_AS_OF"
    assert delayed_label["reason"] == "SOURCE_REVISION_RECEIVED_AFTER_AS_OF"


def test_incomplete_source_bars_do_not_become_labels():
    bars = _bars([100, 101, 102])
    bars["quality_status"] = "COMPLETE"
    bars["coverage_ratio"] = 1.0
    bars["is_partial"] = 0
    bars.loc[1, "quality_status"] = "PARTIAL"
    as_of = bars["computed_at_utc"].max() + pd.Timedelta(minutes=1)

    targets = associations.build_forward_close_targets(bars, horizons=(1,), as_of=as_of)

    assert targets.loc[0, "state"] == "UNAVAILABLE"
    assert targets.loc[0, "reason"] == "SOURCE_BAR_NOT_ACCEPTABLE"


def test_pearson_and_spearman_capture_strong_monotonic_association_with_block_inference():
    rng = np.random.default_rng(19)
    x = rng.normal(size=120)
    y = 2.5 * x + rng.normal(scale=0.025, size=120)
    samples = _samples(x, y)
    as_of = samples.decision_time.max() + pd.Timedelta(minutes=1)

    pearson = associations.analyze_association(samples, method="pearson", as_of=as_of,
                                               bootstrap_replicates=99, seed=91)
    spearman = associations.analyze_association(samples, method="spearman", as_of=as_of,
                                                bootstrap_replicates=99, seed=91)

    assert pearson["status"] == "OK"
    assert pearson["effect"] > 0.99
    assert pearson["ci_low"] > 0.98
    assert spearman["status"] == "OK"
    assert spearman["effect"] > 0.99
    assert pearson["inference"]["method"].startswith("circular moving-session-block")
    assert pearson["inference"]["valid_replicates"] == 99


def test_pearson_and_spearman_do_not_mislabel_a_symmetric_nonlinear_pattern():
    x = np.linspace(-1.0, 1.0, 100)
    y = x ** 2
    samples = _samples(x, y)
    result = associations.analyze_association(samples, method="spearman",
                                              as_of=samples.decision_time.max() + pd.Timedelta(minutes=1),
                                              bootstrap_replicates=49, seed=4)

    assert result["status"] == "OK"
    assert abs(result["effect"]) < 0.02


def test_constants_and_missing_values_are_explicit():
    samples = _samples(np.ones(100), np.arange(100, dtype=float))
    samples.loc[0, "x_value"] = np.nan
    result = associations.analyze_association(samples, as_of=samples.decision_time.max() + pd.Timedelta(minutes=1),
                                              bootstrap_replicates=49)

    assert result["status"] == "CONSTANT_INPUT"
    assert result["reason_codes"] == ["X_CONSTANT_OR_NEAR_CONSTANT"]
    assert result["n_missing_x"] == 1
    assert result["effect"] is None
    assert result["p_raw"] is None


def test_point_in_time_violations_are_excluded_and_counted():
    samples = _samples(np.arange(100, dtype=float), np.arange(100, dtype=float))
    samples.loc[0, "x_available_at_utc"] += pd.Timedelta(seconds=1)
    cutoff = samples.decision_time.max() - pd.Timedelta(seconds=1)
    result = associations.analyze_association(samples, as_of=cutoff, minimum_pairs=1, minimum_sessions=1,
                                              bootstrap_replicates=29)

    assert result["n_temporal_rejected"] == 1
    assert result["n_after_as_of"] == 1
    assert result["n_pairs"] == 98


def test_future_target_associations_require_mature_and_received_labels():
    samples = _samples(np.arange(100, dtype=float), np.arange(100, dtype=float))
    samples["y_state"] = "MATURE"
    samples["label_end_utc"] = samples["decision_time"] + pd.Timedelta(minutes=5)
    samples["y_available_at_utc"] = samples["label_end_utc"]
    cutoff = samples.decision_time.max() + pd.Timedelta(minutes=6)
    samples.loc[0, "y_state"] = "NOT_MATURE"
    samples.loc[1, "y_available_at_utc"] = cutoff + pd.Timedelta(minutes=1)

    result = associations.analyze_association(samples, kind=associations.FEATURE_TARGET, as_of=cutoff,
                                              minimum_pairs=1, minimum_sessions=1, bootstrap_replicates=29)

    assert result["n_immature"] == 2
    assert result["n_pairs"] == 98
    assert result["status"] == "OK"


def test_family_correction_counts_invalid_comparisons_too():
    source = [{"p_raw": 0.01}, {"p_raw": None}, {"p_raw": 0.2}]
    adjusted = associations.adjust_bonferroni(source, tests_counted=20)

    assert [item["p_adjusted"] for item in adjusted] == [0.2, None, 1.0]
    assert all(item["multiple_testing"]["tests_counted"] == 20 for item in adjusted)
    assert source[0].get("p_adjusted") is None
    with pytest.raises(ValueError, match="menor"):
        associations.adjust_bonferroni(source, tests_counted=2)


def test_mixed_symbols_or_timeframes_are_rejected():
    samples = _samples(np.arange(100, dtype=float), np.arange(100, dtype=float))
    samples.loc[0, "symbol"] = "OTHER"

    with pytest.raises(ValueError, match="exatamente um valor de symbol"):
        associations.analyze_association(samples, as_of=samples.decision_time.max() + pd.Timedelta(minutes=1))


def test_bootstrap_is_deterministic_for_a_fixed_seed():
    rng = np.random.default_rng(41)
    x = rng.normal(size=100)
    samples = _samples(x, x + rng.normal(scale=0.3, size=100))
    as_of = samples.decision_time.max() + pd.Timedelta(minutes=1)
    first = associations.analyze_association(samples, as_of=as_of, bootstrap_replicates=49, seed=123)
    second = associations.analyze_association(samples, as_of=as_of, bootstrap_replicates=49, seed=123)

    assert first == second
