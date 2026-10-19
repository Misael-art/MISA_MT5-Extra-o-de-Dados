import json

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import inspect, text

from mt5_extracao.analytics import feature_store, features, indicator_registry, producers, relation_dto, target_registry
from mt5_extracao.database_manager import DatabaseManager


@pytest.fixture
def db(tmp_path):
    manager = DatabaseManager(db_type="sqlite", db_path=str(tmp_path / "relations.sqlite"))
    yield manager
    manager.close()


def _bars(count=45):
    close = pd.Series(np.linspace(1.0, 1.2, count), dtype=float)
    return pd.DataFrame({
        "open": close,
        "high": close + 0.01,
        "low": close - 0.01,
        "close": close,
        "real_volume": np.zeros(count, dtype=float),
        "tick_volume": np.arange(count, dtype=float) + 1,
    })


def test_core_registry_is_typed_and_returns_defensive_copies():
    definitions = indicator_registry.definitions()
    assert {"momentum.rsi_14", "momentum.macd_signal_12_26_9", "volume.real", "volume.ticks"} <= {
        item["feature_id"] for item in definitions
    }
    assert all(item["formula"] and item["unit"] and item["availability_rule"] for item in definitions)
    assert all(item["timeframe_policy"] == "same_as_source" for item in definitions)
    definitions[0]["formula"] = "changed by caller"
    assert indicator_registry.get_definition("momentum.rsi_14")["formula"] != "changed by caller"
    with pytest.raises(ValueError, match="desconhecido"):
        indicator_registry.get_definition("unknown")


def test_macd_is_pure_deterministic_and_has_explicit_warmup():
    close = pd.Series(np.linspace(1, 2, 50), dtype=float)
    first = indicator_registry.macd(close)
    second = indicator_registry.macd(close)
    pd.testing.assert_frame_equal(first, second)
    assert first["line"].first_valid_index() == 25
    assert first["signal"].first_valid_index() == 33
    assert first["histogram"].first_valid_index() == 33
    assert np.isfinite(first.loc[33:, ["line", "signal", "histogram"]].to_numpy()).all()
    with pytest.raises(ValueError, match="menor"):
        indicator_registry.macd(close, fast=26, slow=26)


def test_core_features_preserve_warmup_missing_volume_and_known_zero():
    bars = _bars()
    bars.loc[20, "real_volume"] = np.nan
    rows = indicator_registry.compute_core(bars, feature_ids=[
        "momentum.macd_line_12_26_9", "momentum.macd_signal_12_26_9", "volume.real", "volume.ticks",
    ])
    indexed = {(row["feature_id"], row["bar_index"]): row for row in rows}
    assert indexed[("momentum.macd_line_12_26_9", 24)]["state"] == "WARMUP"
    assert indexed[("momentum.macd_line_12_26_9", 25)]["state"] == "AVAILABLE"
    assert indexed[("momentum.macd_signal_12_26_9", 32)]["reason"] == "REQUIRES_34_BARS"
    assert indexed[("momentum.macd_signal_12_26_9", 33)]["state"] == "AVAILABLE"
    assert indexed[("volume.real", 0)]["value"] == 0.0
    assert indexed[("volume.real", 20)]["state"] == "SOURCE_VALUE_MISSING"
    assert indexed[("volume.real", 20)]["value"] is None
    assert indexed[("volume.ticks", 20)]["value"] == 21.0
    assert all(row["value"] is None or np.isfinite(row["value"]) for row in rows)


def test_core_feature_reports_missing_source_and_zero_denominator():
    close = pd.Series(np.linspace(1, 2, 30), dtype=float)
    only_close = pd.DataFrame({"close": close})
    atr_rows = indicator_registry.compute_core(only_close, feature_ids=["volatility.atr_14"])
    assert all(row["state"] == "SOURCE_UNAVAILABLE" for row in atr_rows)
    assert "high" in atr_rows[0]["reason"] and "low" in atr_rows[0]["reason"]

    bars = _bars(30)
    bars.loc[20, "close"] = 0.0
    return_rows = indicator_registry.compute_core(bars, feature_ids=["price.return_1"])
    row = next(row for row in return_rows if row["bar_index"] == 21)
    assert row["state"] == "INVALID_DENOMINATOR"
    assert row["value"] is None


def test_feature_definition_migration_only_adds_typed_columns(db):
    with db.engine.begin() as conn:
        conn.execute(text("CREATE TABLE user_prices (id INTEGER PRIMARY KEY, close REAL)"))
        conn.execute(text("INSERT INTO user_prices (id, close) VALUES (1, 1.25)"))
        conn.execute(text("CREATE TABLE _feature_definitions (feature_id TEXT NOT NULL, algorithm_version TEXT NOT NULL, "
                          "name TEXT NOT NULL, parameters_json TEXT NOT NULL, dependencies_json TEXT NOT NULL, "
                          "registered_at_utc TEXT NOT NULL, PRIMARY KEY (feature_id, algorithm_version))"))
        conn.execute(text("INSERT INTO _feature_definitions VALUES "
                          "('legacy', '1.0', 'legacy feature', '{}', '[]', '2026-10-15T10:00:00Z')"))

    feature_store.ensure_schema(db)
    names = set(inspect(db.engine).get_table_names())
    assert {"user_prices", "_feature_definitions", "_feature_values"} <= names
    columns = {column["name"] for column in inspect(db.engine).get_columns("_feature_definitions")}
    assert {"formula", "unit", "warmup_count", "warmup_unit", "source_id", "timeframe",
            "availability_rule", "value_type", "family"} <= columns
    legacy = feature_store.list_definitions(db)[0]
    assert legacy["feature_id"] == "legacy"
    assert legacy["formula"] is None and legacy["warmup_count"] is None
    with db.engine.connect() as conn:
        assert conn.execute(text("SELECT close FROM user_prices WHERE id = 1")).scalar() == 1.25


def test_feature_definitions_are_shared_across_value_timeframes(db):
    assert indicator_registry.register_core(db, "M1")["registered"] == len(indicator_registry.definitions())
    assert indicator_registry.register_core(db, "M5")["registered"] == 0
    stored = features.get_feature(db, "momentum.rsi_14")
    assert stored["timeframe"] == "same_as_source"
    assert stored["source_id"] == "bars_derived:{timeframe}:close"
    assert stored["dependencies"] == ["bars_derived:{timeframe}"]
    assert [item["feature_id"] for item in features.affected_by(db, ["bars_derived:M5"])] == sorted(
        item["feature_id"] for item in indicator_registry.definitions())


def test_registry_persistence_is_immutable_and_t13_3_reuses_contract(db):
    result = indicator_registry.register_core(db, "M1")
    assert result == {"registered": len(indicator_registry.definitions()), "total": len(indicator_registry.definitions()),
                      "timeframe": "M1"}
    assert indicator_registry.register_core(db, "M1")["registered"] == 0
    stored = features.get_feature(db, "momentum.macd_signal_12_26_9")
    assert stored["value_type"] == "NUMBER"
    assert stored["warmup_count"] == 34 and stored["warmup_unit"] == "bars"
    assert stored["timeframe"] == "same_as_source" and "closed_bar_received" in stored["availability_rule"]

    producers.register_definitions(db, "rvol", {"rvol.real", "rvol.reference_sessions"},
                                   {"lookback": 20, "min_sessions": 5}, "M5", "real")
    rvol = features.get_feature(db, "rvol.real")
    assert rvol["source_id"] == "bars_derived:{timeframe}:real_volume"
    assert rvol["warmup_count"] == 5 and rvol["warmup_unit"] == "sessions"
    assert rvol["availability_rule"] == "closed_bar_received_after_prior_session_reference_is_mature"
    producers.register_definitions(db, "rvol", {"rvol.real", "rvol.reference_sessions"},
                                   {"lookback": 20, "min_sessions": 5}, "M15", "real")
    rvol = features.get_feature(db, "rvol.real")
    assert rvol["algorithm_version"].startswith(producers.ALGORITHM_VERSION + "+")
    assert rvol["timeframe"] == "same_as_source"
    assert rvol["source_id"] == "bars_derived:{timeframe}:real_volume"
    assert rvol["dependencies"] == ["bars_derived:M15", "market_calendar"]

    producers.register_definitions(db, "regime", {
        "regime.adx", "regime.confidence", "regime.duration", "regime.efficiency", "regime.r2",
        "regime.state",
    }, producers.DEFAULTS["regime"], "M5")
    assert features.get_feature(db, "regime.adx")["unit"] == "points_0_100"
    assert features.get_feature(db, "regime.confidence")["unit"] == "fraction"
    structure = indicator_registry.producer_contract("level.dist_prev_high_atr", "structure",
                                                     {"atr_n": 14}, "M15")
    assert structure["warmup_count"] == 15 and structure["warmup_unit"] == "bars"
    assert structure["unit"] == "atr_units"
    volume = indicator_registry.producer_contract("volume.session_per_minute.real", "volume",
                                                  {"min_sessions": 5}, "M15", "real")
    assert volume["unit"] == "volume_units:instrument/minute"
    state = indicator_registry.producer_contract("regime.state", "regime", producers.DEFAULTS["regime"], "M15")
    raw = indicator_registry.producer_contract("regime.raw", "regime", producers.DEFAULTS["regime"], "M15")
    assert state["warmup_count"] == raw["warmup_count"] + 2

    changed = indicator_registry.get_definition("momentum.rsi_14")
    changed["formula"] = "different formula"
    with pytest.raises(ValueError, match="nova versão do algoritmo"):
        features.register(db, changed["feature_id"], changed["name"], changed["algorithm_version"],
                          parameters=changed["parameters"], dependencies=["bars_derived:M1"],
                          family=changed["family"], formula=changed["formula"], unit=changed["unit"],
                          warmup_count=changed["warmup_count"], warmup_unit=changed["warmup_unit"],
                          source_id="bars_derived:M1:close", timeframe="M1",
                          availability_rule=changed["availability_rule"], value_type=changed["value_type"])


def test_feature_store_enforces_declared_types_before_batch_write(db):
    features.register(db, "flag", "Flag", "1.0", value_type="BOOLEAN")
    features.register(db, "label", "Label", "1.0", value_type="CATEGORY")
    features.register(db, "score", "Score", "1.0", value_type="NUMBER")
    stamp = "2026-10-15T12:00:00Z"
    feature_store.save_value(db, "TEST", "M1", "flag", "1.0", stamp, True,
                             observed_at=stamp, available_at=stamp)
    feature_store.save_value(db, "TEST", "M1", "label", "1.0", stamp, "TREND",
                             observed_at=stamp, available_at=stamp)
    with pytest.raises(ValueError, match="value_type CATEGORY"):
        feature_store.save_value(db, "TEST", "M1", "label", "1.0", stamp, 3.5)
    with pytest.raises(ValueError, match="value_type NUMBER"):
        feature_store.save_values(db, [
            {"symbol": "TEST", "timeframe": "M1", "feature_id": "score", "algorithm_version": "1.0",
             "observation_time": stamp, "value": 0.25},
            {"symbol": "TEST", "timeframe": "M1", "feature_id": "score", "algorithm_version": "1.0",
             "observation_time": "2026-10-15T12:01:00Z", "value": True},
        ])
    assert feature_store.query_as_of(db, "2026-10-15T12:05:00Z", symbol="TEST").shape[0] == 2


def test_target_registry_tracks_horizon_maturity_and_write_guard(db):
    blocked = lambda: "limite de disco"
    with pytest.raises(RuntimeError, match="limite de recursos"):
        target_registry.register_standard(db, "M1", horizons=(3,), write_guard=blocked)
    assert not inspect(db.engine).has_table(target_registry.TABLE)

    first = target_registry.register_standard(db, "M1", horizons=(3, 5, 3))
    assert first == {"inserted": 2, "unchanged": 0, "timeframe": "M1"}
    assert target_registry.register_standard(db, "M1", horizons=(3, 5))["unchanged"] == 2
    targets = target_registry.list_targets(db, timeframe="M1")
    assert [item["horizon_bars"] for item in targets] == [3, 5]
    assert targets[0]["label_start_rule"] == "decision_bar_close"
    assert targets[0]["label_end_rule"] == "close_at_horizon_bars_after_decision"
    assert "never available before label_end" in targets[0]["availability_rule"]
    assert targets[0]["target_kind"] == "CONTINUOUS"
    with pytest.raises(ValueError, match="outra algorithm_version"):
        target_registry.register(db, {**target_registry.standard_targets("M1", (3,))[0],
                                      "formula": "different target"})


def test_relation_dto_fixture_is_versioned_and_rejects_nonstandard_nan():
    dto = relation_dto.synthetic_fixture()
    decoded = json.loads(relation_dto.dumps(dto))
    assert decoded["relation_schema_version"] == 1
    assert decoded["synthetic_fixture"] is True
    assert decoded["indicator"]["value"] is None
    assert decoded["relation"]["effect"] is None
    assert decoded["relation"]["state"] == "NOT_COMPUTED"
    with pytest.raises(ValueError, match="número finito"):
        relation_dto.build_relation_dto(
            indicator={**dto["indicator"], "value": float("nan"), "state": "AVAILABLE", "reason": None},
            target=dto["target"], relation=dto["relation"], scope=dto["scope"],
            provenance=dto["provenance"], generated_at_utc=dto["generated_at_utc"], synthetic_fixture=True,
        )
    with pytest.raises(ValueError, match="entre zero e um"):
        relation_dto.build_relation_dto(
            indicator=dto["indicator"], target=dto["target"],
            relation={**dto["relation"], "p_raw": 1.1}, scope=dto["scope"], provenance=dto["provenance"],
            generated_at_utc=dto["generated_at_utc"], synthetic_fixture=True,
        )
    with pytest.raises(ValueError, match="data e hora UTC válida"):
        relation_dto.build_relation_dto(
            indicator=dto["indicator"], target=dto["target"], relation=dto["relation"], scope=dto["scope"],
            provenance=dto["provenance"], generated_at_utc="not-a-timeZ", synthetic_fixture=True,
        )


@pytest.mark.parametrize("bulk", [False, True])
@pytest.mark.parametrize("value_type,value", [("NUMBER", 0.25), ("CATEGORY", "TREND"), ("BOOLEAN", True)])
def test_typed_invalidation_preserves_asof_history(db, bulk, value_type, value):
    features.register(db, "typed", "Typed", "1.0", value_type=value_type)
    initial = "2026-10-15T12:00:00Z"
    correction = "2026-10-15T13:00:00Z"
    feature_store.save_value(db, "TEST", "M1", "typed", "1.0", initial, value,
                             observed_at=initial, available_at=initial)
    record = dict(symbol="TEST", timeframe="M1", feature_id="typed", algorithm_version="1.0",
                  observation_time=initial, value=None, quality="INVALIDATED", source_revision="2",
                  observed_at=correction, available_at=correction)
    if bulk:
        assert feature_store.save_values(db, [record])["revised"] == 1
    else:
        assert feature_store.save_value(db, **record) == 2
    history = feature_store.query_as_of(db, "2026-10-15T12:30:00Z", symbol="TEST")
    assert len(history) == 1 and history.iloc[0]["quality"] == "VALID"
    current = feature_store.query_as_of(db, correction, symbol="TEST")
    assert len(current) == 1 and current.iloc[0]["quality"] == "INVALIDATED"
    assert pd.isna(current.iloc[0]["numeric_value"]) and pd.isna(current.iloc[0]["categorical_value"])
    record["quality"] = "VALID"
    with pytest.raises(ValueError, match="INVALIDATED"):
        feature_store.save_value(db, **record)


@pytest.mark.parametrize("source", ["tick", "ticks"])
def test_producer_tick_aliases_describe_same_tick_source(source):
    contract = indicator_registry.producer_contract("rvol." + source, "rvol", {"min_sessions": 5}, "M5", source)
    assert contract["source_id"] == "bars_derived:{timeframe}:tick_volume"
    assert contract["unit"] == "ratio"
