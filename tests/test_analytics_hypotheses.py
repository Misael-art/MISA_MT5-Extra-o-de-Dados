"""T13.4: hipóteses declarativas, amostras causais e validação temporal."""
from copy import deepcopy
from datetime import datetime, timedelta
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine, inspect

from mt5_extracao.analytics.hypotheses import get_definition, list_definitions, validate_definition
from mt5_extracao.analytics.pattern_engine import _breakout_outcome, _volume, build_observations, condition_mask
from mt5_extracao.analytics.validation import (
    DEFINITIONS_TABLE,
    EVALUATIONS_TABLE,
    RESERVATIONS_TABLE,
    _condition_for,
    evaluate_and_store,
    evaluate_samples,
    list_evaluations,
    transition_status,
)


def _bars_for_sessions(sessions, minutes=8):
    rows = []
    for day, opening, mode in sessions:
        previous = opening
        for minute in range(minutes):
            if mode == "up":
                close = opening + 0.2 * (minute + 1)
            elif mode == "down":
                close = opening - 0.05 * (minute + 1)
            else:
                close = opening
            open_ = opening if minute == 0 else previous
            rows.append({"time": pd.Timestamp(day) + pd.Timedelta(hours=9, minutes=minute),
                         "open": open_, "high": max(open_, close) + 0.01, "low": min(open_, close) - 0.01,
                         "close": close, "tick_volume": 3, "real_volume": 0, "spread": 2})
            previous = close
    return pd.DataFrame(rows)


def _evaluation_samples(n=200, seed=1, noise=False):
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2020-01-01", periods=n, freq="D")
    feature = (np.arange(n) % 2 == 0)
    if noise:
        target = rng.random(n) < 0.5
        outcome = rng.normal(0, 0.01, n)
    else:
        target = feature.copy()
        outcome = np.where(feature, 0.01, -0.005)
    frame = pd.DataFrame({
        "observation_time": dates,
        "entry_time": dates + pd.Timedelta(minutes=1),
        "horizon_end": dates + pd.Timedelta(minutes=5),
        "previous_session_return": np.where(feature, 0.03, 0.0),
        "gap_return": np.where(target, -0.01, 0.0),
        "target_success": target,
        "outcome_return": outcome,
        "gross_return": outcome,
        "cost_bps": 0.0,
        "cost_observed": True,
        "trade_executed": True,
    })
    if noise:
        frame["feature"] = rng.integers(0, 2, n)
        frame["success"] = target
        frame["target_success"] = target
    return frame


def _simple_definition(status="DRAFT"):
    definition = deepcopy(get_definition("H01"))
    definition.update(pattern_id="TEST-T13.4", version="test", status=status)
    definition.pop("parameter_grid", None)
    definition["condition"] = {"field": "feature", "operator": "eq", "value": 1}
    definition["target"] = {"field": "success", "operator": "eq", "value": True}
    definition["validation"]["min_samples"] = 10
    return definition


def test_catalog_definitions_are_json_data_with_declared_parameter_references():
    definitions = list_definitions()
    assert {item["pattern_id"] for item in definitions} >= {"H01", "H02", "H03", "H04", "H05"}
    for definition in definitions:
        validate_definition(definition)
        assert isinstance(definition["condition"], dict)
    assert get_definition("H01")["status"] == "EXPLORATORY"
    drafts = [item for item in definitions if item["status"] == "DRAFT"]
    assert {item["pattern_id"] for item in drafts} >= {f"H{i:02d}" for i in range(6, 17)}
    for draft in drafts:
        with pytest.raises(ValueError, match="Construtor de amostras indisponível"):
            build_observations(pd.DataFrame(), "EURUSD", draft)

    invalid = deepcopy(get_definition("H01"))
    invalid["condition"]["conditions"][0]["parameter"] = "nao_declarado"
    with pytest.raises(ValueError, match="parâmetros não declarados"):
        validate_definition(invalid)


def test_condition_tree_and_missing_target_are_preserved_as_unknown():
    frame = pd.DataFrame({"weekday": [0, 1, 0], "ratio": [1.5, 0.7, np.nan], "success": [True, pd.NA, False]})
    condition = {"operator": "or", "conditions": [
        {"operator": "and", "conditions": [
            {"field": "weekday", "operator": "eq", "value": 0},
            {"field": "ratio", "operator": "gt", "value": 1.0}]},
        {"field": "weekday", "operator": "eq", "value": 1},
    ]}
    assert condition_mask(frame, condition).tolist() == [True, True, False]
    definition = deepcopy(get_definition("H05"))
    definition["condition"] = {"field": "weekday", "operator": "eq", "value": 0}
    definition["target"] = {"field": "success", "operator": "eq", "value": True}
    _, target = _condition_for(frame, definition)
    assert target.iloc[0] == True
    assert pd.isna(target.iloc[1])
    assert target.iloc[2] == False


def test_h01_uses_the_executable_five_minute_horizon_and_known_zero_volume():
    bars = _bars_for_sessions([
        ("2026-01-05", 100.0, "flat"),
        ("2026-01-06", 100.0, "up"),
        ("2026-01-07", 101.0, "down"),
        ("2026-01-08", 100.5, "flat"),
    ])
    result = build_observations(bars, "EURUSD", get_definition("H01"), point=0.00001)
    row = result.loc[result["observation_time"].dt.date == datetime(2026, 1, 7).date()].iloc[0]
    assert row["previous_session_return"] >= 0.015
    assert row["gap_return"] <= -0.005
    assert row["volume_source"] == "real_volume"
    assert row["entry_time"] == row["observation_time"] + pd.Timedelta(minutes=1)
    assert row["horizon_end"] == row["observation_time"] + pd.Timedelta(minutes=5)


def test_h02_requires_the_trigger_bar_to_close_before_entry():
    rows = []
    for day in pd.bdate_range("2026-01-05", periods=4):
        previous = 100.0
        for minute in range(96):
            if minute <= 84:
                close = 100.0 + 1.2 * minute / 84
            else:
                close = 101.2 + 0.08 * (minute - 84)
            open_ = 100.0 if minute == 0 else previous
            rows.append({"time": day + pd.Timedelta(hours=16, minutes=minute), "open": open_,
                         "high": max(open_, close) + 0.01, "low": min(open_, close) - 0.01,
                         "close": close, "tick_volume": 1, "real_volume": 1, "spread": 2})
            previous = close
    result = build_observations(pd.DataFrame(rows), "EURUSD", get_definition("H02"), point=0.00001)
    row = result.loc[result["observation_time"].dt.date == datetime(2026, 1, 7).date()].iloc[0]
    assert row["observation_time"] == pd.Timestamp("2026-01-07 17:25")
    assert row["entry_time"] == pd.Timestamp("2026-01-07 17:25")
    assert row["return_to_trigger"] >= 0.0115
    assert row["target_success"]
    assert row["horizon_end"] == pd.Timestamp("2026-01-07 17:36")


def test_h03_uses_prior_sessions_for_volume_decline_not_the_current_session():
    days = pd.bdate_range("2026-01-05", periods=70)
    rows = []
    for day in days:
        for minute in range(31):
            rows.append({"time": day + pd.Timedelta(hours=9, minutes=minute), "open": 100, "high": 100.1,
                         "low": 99.9, "close": 100, "real_volume": 10, "tick_volume": 100,
                         "spread": 2})
    bars = pd.DataFrame(rows)
    definition = get_definition("H03")
    first = build_observations(bars, "EURUSD", definition, point=0.00001)
    target_day = days[60]
    base = first.loc[first["observation_time"].dt.date == target_day.date()].iloc[0]
    changed = bars.copy()
    changed.loc[changed["time"].dt.date == target_day.date(), "real_volume"] = 1_000_000
    second = build_observations(changed, "EURUSD", definition, point=0.00001)
    altered = second.loc[second["observation_time"].dt.date == target_day.date()].iloc[0]
    assert base["volume_decline_ratio"] == pytest.approx(1.0)
    assert altered["volume_decline_ratio"] == base["volume_decline_ratio"]
    assert not bool(base["trade_executed"])


def test_h03_returns_no_samples_during_volume_history_warmup():
    bars = _bars_for_sessions([(day, 100, "flat") for day in pd.bdate_range("2026-01-05", periods=8)], minutes=31)
    result = build_observations(bars, "EURUSD", get_definition("H03"), point=0.00001)
    assert result.empty


def test_h05_exposes_previous_session_regime_as_a_declared_feature():
    bars = []
    for day in pd.bdate_range("2026-01-05", periods=5):
        bars.extend(_bars_for_sessions([(day, 100.0, "flat")], minutes=70).to_dict("records"))
    result = build_observations(pd.DataFrame(bars), "EURUSD", get_definition("H05"), point=0.00001)
    assert len(result) >= 2
    assert result["prior_regime"].notna().all()
    assert set(result["prior_regime"]) <= {"TREND", "RANGE", "TRANSITION", "UNCERTAIN"}


def test_h04_condition_uses_only_the_closed_opening_range_volume():
    weekdays = pd.bdate_range("2026-01-05", periods=30)
    rows = []
    target_day = pd.Timestamp("2026-01-19")
    for day in weekdays:
        for minute in range(5):
            volume = 15 if day == target_day and minute < 2 else 10
            rows.append({"time": day + pd.Timedelta(hours=9, minutes=minute), "open": 100, "high": 100.1,
                         "low": 99.9, "close": 100, "real_volume": volume, "tick_volume": 1000,
                         "spread": 2})
    bars = pd.DataFrame(rows)
    definition = deepcopy(get_definition("H04"))
    definition["parameters"]["opening_range_minutes"] = 2
    definition["parameters"]["weekday_baseline_occurrences"] = 2
    base = build_observations(bars, "EURUSD", definition, point=0.00001)
    before = base.loc[base["observation_time"].dt.date == target_day.date()].iloc[0]

    late = bars["time"].dt.date == target_day.date()
    late &= bars["time"].dt.minute >= 2
    bars.loc[late, "real_volume"] = 50_000
    after = build_observations(bars, "EURUSD", definition, point=0.00001)
    after = after.loc[after["observation_time"].dt.date == target_day.date()].iloc[0]
    assert before["opening_range_weekday_volume_ratio"] == pytest.approx(1.5)
    assert after["opening_range_weekday_volume_ratio"] == before["opening_range_weekday_volume_ratio"]
    mask, _ = _condition_for(pd.DataFrame([after]), definition)
    assert bool(mask.iloc[0])
    assert after["observation_time"] == target_day + pd.Timedelta(hours=9, minutes=2)


def test_volume_source_is_series_wide_and_breakout_requires_unambiguous_closed_signal():
    volumes = pd.DataFrame({"real_volume": [0, np.nan, 0], "tick_volume": [5, 6, 7]})
    selected, source = _volume(volumes)
    assert source == "tick_volume"
    assert selected.tolist() == [5, 6, 7]

    group = pd.DataFrame({
        "time": pd.date_range("2026-01-05 09:00", periods=5, freq="min"),
        "_bar_end": pd.date_range("2026-01-05 09:01", periods=5, freq="min"),
        "open": [100, 100, 100, 102.2, 103], "high": [101, 100.5, 102, 103, 104],
        "low": [99, 99.5, 99.8, 102, 102.8], "close": [100, 100, 102, 103, 103.5],
        "spread": [2] * 5,
    })
    definition = deepcopy(get_definition("H05"))
    definition["parameters"]["opening_range_minutes"] = 2
    clear = _breakout_outcome(group, definition, 0.00001, 2)
    assert clear["trade_executed"]
    assert clear["entry_time"] == group["time"].iloc[3]

    ambiguous_group = group.copy()
    ambiguous_group.loc[2, "low"] = 98.0
    ambiguous = _breakout_outcome(ambiguous_group, definition, 0.00001, 2)
    assert ambiguous["breakout_ambiguous"]
    assert pd.isna(ambiguous["breakout_success"])
    incomplete = _breakout_outcome(group.iloc[:1], definition, 0.00001, 2)
    assert not incomplete["breakout_ambiguous"]
    assert not incomplete["breakout_available"]
    assert incomplete["breakout_reason"] == "opening_range_incomplete"


def test_parameter_choice_uses_training_only_and_walk_forward_purges_label_horizon():
    definition = deepcopy(get_definition("H01"))
    definition["validation"]["min_samples"] = 10
    samples = _evaluation_samples(200)
    first = evaluate_samples(samples, definition, n_boot=100, tests_counted=9)
    altered = samples.copy()
    altered.loc[altered.index[-40:], "outcome_return"] = -100.0
    second = evaluate_samples(altered, definition, n_boot=100, tests_counted=9)
    assert first["parameter_selection"]["selected_parameters"] == second["parameter_selection"]["selected_parameters"]
    assert first["multiple_testing"]["tests_counted"] == 9
    assert first["folds"]
    assert all(fold["purged"] for fold in first["folds"])

    first_fold = first["folds"][0]
    check_only = samples.copy()
    check_mask = check_only["entry_time"].between(first_fold["check_first_entry"],
                                                   first_fold["check_last_entry"])
    check_only.loc[check_mask, "gap_return"] = -0.02
    check_only.loc[check_mask, "outcome_return"] = 100.0
    changed_fold = evaluate_samples(check_only, definition, n_boot=100, tests_counted=9)["folds"][0]
    assert changed_fold["selected_parameters"] == first_fold["selected_parameters"]


def test_seeded_noise_does_not_pass_the_validation_gate():
    definition = _simple_definition(status="EXPLORATORY")
    samples = _evaluation_samples(400, seed=20261015, noise=True)
    result = evaluate_samples(samples, definition, n_boot=200)
    assert not result["eligible_for_approval"]
    assert result["approved"] is False
    assert result["multiple_testing"]["method"] == "Bonferroni family-wise"


def test_strong_synthetic_effect_is_only_validating_not_automatically_approved():
    definition = deepcopy(get_definition("H01"))
    definition.pop("parameter_grid", None)
    definition["validation"]["min_samples"] = 15
    result = evaluate_samples(_evaluation_samples(200), definition, n_boot=200)
    assert result["eligible_for_approval"]
    assert result["status"] == "VALIDATING"
    assert result["approved"] is False


def test_persistent_attempts_count_all_candidates_and_never_reuse_final_holdout(tmp_path):
    definition = _simple_definition()
    definition["parameter_grid"] = {"prior_return_threshold": [0.01, 0.02]}
    samples = _evaluation_samples(120)
    samples["feature"] = (np.arange(len(samples)) % 2 == 0).astype(int)
    samples["success"] = samples["feature"] == 1
    engine = create_engine("sqlite:///" + str(tmp_path / "hypotheses.sqlite"))
    db = SimpleNamespace(engine=engine)
    try:
        first = evaluate_and_store(db, definition, samples, "EURUSD", "M1", n_boot=100)
        second = evaluate_and_store(db, definition, samples, "EURUSD", "M1", n_boot=100)
        assert first["holdout_reused"] is False
        assert second["holdout_reused"] is True
        assert second["test"]["reused"] is True
        assert first["multiple_testing"]["tests_counted"] == 2
        assert second["multiple_testing"]["tests_counted"] == 4
        rows = list_evaluations(db, pattern_id=definition["pattern_id"])
        assert [row["attempt_no"] for row in rows] == [1, 2]
        assert [row["tests_counted"] for row in rows] == [2, 2]
        assert rows[1]["holdout_reused"] == 1
        assert {DEFINITIONS_TABLE, EVALUATIONS_TABLE, RESERVATIONS_TABLE} <= set(inspect(engine).get_table_names())
        assert all(name.startswith("_") for name in inspect(engine).get_table_names())

        assert transition_status(db, definition["pattern_id"], definition["version"], "EXPLORATORY") == "EXPLORATORY"
        assert transition_status(db, definition["pattern_id"], definition["version"], "VALIDATING") == "VALIDATING"
        with pytest.raises(ValueError, match="não passou pelos critérios OOS"):
            transition_status(db, definition["pattern_id"], definition["version"], "APPROVED")
    finally:
        engine.dispose()


def test_failed_attempt_is_persisted_and_final_holdout_stays_reserved(tmp_path):
    definition = _simple_definition()
    samples = _evaluation_samples(60)
    samples["feature"] = (np.arange(len(samples)) % 2 == 0).astype(int)
    samples["success"] = samples["feature"] == 1
    failed_samples = samples.drop(columns=["outcome_return"])
    engine = create_engine("sqlite:///" + str(tmp_path / "failed-attempt.sqlite"))
    db = SimpleNamespace(engine=engine)
    try:
        first = evaluate_and_store(db, definition, failed_samples, "EURUSD", "M1", n_boot=100)
        assert first["status"] == "FAILED"
        assert first["error"]["type"] == "ValueError"
        assert first["holdout_reused"] is False
        assert first["test"]["available"] is False

        second = evaluate_and_store(db, definition, samples, "EURUSD", "M1", n_boot=100)
        assert second["holdout_reused"] is True
        assert second["test"]["reused"] is True
        rows = list_evaluations(db, pattern_id=definition["pattern_id"])
        assert [row["status"] for row in rows] == ["FAILED", "EXPLORATORY"]
        assert rows[0]["result"]["error"]["message"] == "As observações precisam conter outcome_return."
        assert rows[1]["holdout_reused"] == 1
    finally:
        engine.dispose()


def test_write_guard_blocks_catalog_writes(tmp_path):
    definition = _simple_definition()
    samples = _evaluation_samples(60)
    samples["feature"] = (np.arange(len(samples)) % 2 == 0).astype(int)
    samples["success"] = samples["feature"] == 1
    engine = create_engine("sqlite:///" + str(tmp_path / "guard.sqlite"))
    db = SimpleNamespace(engine=engine)
    try:
        with pytest.raises(RuntimeError, match="bloqueada pelo guarda"):
            evaluate_and_store(db, definition, samples, "EURUSD", "M1", write_guard=lambda: "limite de disco")
        assert inspect(engine).get_table_names() == []
    finally:
        engine.dispose()
