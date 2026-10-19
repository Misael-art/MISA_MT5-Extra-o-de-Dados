import json

import pandas as pd
import pytest
from sqlalchemy import inspect, text

from mt5_extracao import cli
from mt5_extracao.analytics import feature_store, features
from mt5_extracao.database_manager import DatabaseManager


@pytest.fixture
def db(tmp_path):
    manager = DatabaseManager(db_type="sqlite", db_path=str(tmp_path / "features.sqlite"))
    yield manager
    manager.close()


def register_close(db):
    features.register(db, "close_m1", "M1 close", "1.0", dependencies=["bars:M1"])


def save(db, value, *, available_at="2026-10-14T10:00:00Z", observed_at="2026-10-14T10:00:00Z",
         source_revision="1"):
    return feature_store.save_value(
        db, "EURUSD", "M1", "close_m1", "1.0", "2026-10-14T09:59:00Z", value,
        available_at=available_at, observed_at=observed_at, source_revision=source_revision,
    )


def test_schema_is_additive_and_internal(db):
    feature_store.ensure_schema(db)

    names = set(inspect(db.engine).get_table_names())
    assert {"_feature_definitions", "_feature_values"} <= names
    assert not {"feature_definitions", "feature_values"} & names
    assert feature_store.list_definitions(db) == []


def test_definitions_are_versioned_immutable_and_validate_dependencies(db):
    assert features.register(db, "close_m1", "M1 close", "1.0", dependencies=["bars:M1"])
    assert features.register(db, "return_m1", "M1 return", "1.0", dependencies=["feature:close_m1"])
    assert not features.register(db, "close_m1", "M1 close", "1.0", dependencies=["bars:M1"])
    assert features.register(db, "close_m1", "M1 close", "2.0", dependencies=["bars:M1"],
                             parameters={"method": "adjusted"})
    assert len([item for item in features.list_features(db, include_versions=True)
                if item["feature_id"] == "close_m1"]) == 2

    with pytest.raises(ValueError, match="nova algorithm_version"):
        features.register(db, "close_m1", "M1 close", "1.0", dependencies=["bars:M5"])
    with pytest.raises(ValueError, match="Dependência não registrada"):
        features.register(db, "unknown_dependent", "Unknown", "1.0", dependencies=["feature:missing"])
    with pytest.raises(ValueError, match="ciclos"):
        features.register(db, "close_m1", "M1 close", "3.0", dependencies=["feature:return_m1"])


def test_late_correction_adds_revision_and_as_of_keeps_old_knowledge(db):
    register_close(db)
    assert save(db, 1.1) == 1
    # A repetição da mesma origem e resultado não cria revisão nem muda a disponibilidade histórica.
    assert save(db, 1.1, available_at="2026-10-14T10:30:00Z", observed_at="2026-10-14T10:30:00Z") == 1
    assert save(db, 1.2, available_at="2026-10-14T11:00:00Z", observed_at="2026-10-14T11:00:00Z",
                source_revision="2") == 2
    with pytest.raises(ValueError, match="não pode retroceder"):
        save(db, 1.3, available_at="2026-10-14T10:30:00Z", observed_at="2026-10-14T10:30:00Z",
             source_revision="3")

    before = feature_store.query_as_of(db, "2026-10-14T10:59:59Z", symbol="EURUSD", timeframe="M1",
                                       feature_id="close_m1")
    after = feature_store.query_as_of(db, "2026-10-14T11:00:00Z", symbol="EURUSD", timeframe="M1",
                                      feature_id="close_m1")
    repeated = feature_store.query_as_of(db, "2026-10-14T10:59:59Z", symbol="EURUSD", timeframe="M1",
                                         feature_id="close_m1")
    assert before[["numeric_value", "revision"]].iloc[0].tolist() == [1.1, 1]
    assert after[["numeric_value", "revision", "source_revision"]].iloc[0].tolist() == [1.2, 2, "2"]
    pd.testing.assert_frame_equal(before, repeated)
    with db.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM _feature_values")).scalar() == 2


def test_invalidated_value_hides_old_value_only_after_invalidation_time(db):
    features.register(db, "context.test", "Contexto de teste", "1.0", dependencies=["bars:M1"])
    observation = "2026-10-14T09:00:00Z"
    available = "2026-10-14T09:05:00Z"
    invalidated = "2026-10-14T10:00:00Z"
    first = {"symbol": "TEST", "timeframe": "M1", "feature_id": "context.test",
             "algorithm_version": "1.0", "observation_time": observation, "value": 1.25,
             "available_at": available, "observed_at": available, "source_revision": "before"}

    assert feature_store.save_values(db, [first]) == {"inserted": 1, "revised": 0, "unchanged": 0}
    tombstone = {"symbol": "TEST", "timeframe": "M1", "feature_id": "context.test",
                 "algorithm_version": "1.0", "observation_time": observation, "value": None,
                 "available_at": invalidated, "observed_at": invalidated, "source_revision": "after",
                 "quality": "INVALIDATED", "metadata": {"reason": "entrada corrigida"}}
    assert feature_store.save_values(db, [tombstone]) == {"inserted": 0, "revised": 1, "unchanged": 0}

    before = feature_store.query_as_of(db, "2026-10-14T09:59:59.999999Z", symbol="TEST", timeframe="M1",
                                      feature_id="context.test")
    assert before[["numeric_value", "revision"]].iloc[0].tolist() == [1.25, 1]
    after = feature_store.query_as_of(db, "2026-10-14T10:00:00.000001Z", symbol="TEST", timeframe="M1",
                                     feature_id="context.test")
    assert len(after) == 1
    assert pd.isna(after.iloc[0]["numeric_value"])
    assert pd.isna(after.iloc[0]["categorical_value"])
    assert after.iloc[0]["quality"] == "INVALIDATED"
    assert feature_store.save_values(db, [tombstone]) == {"inserted": 0, "revised": 0, "unchanged": 1}
    joined = feature_store.asof_join(
        pd.DataFrame({"decision_time": [pd.Timestamp("2026-10-14T10:00:00.000001Z")]}),
        feature_store.query_history(db, symbol="TEST", timeframe="M1", feature_id="context.test"),
    )
    assert joined.iloc[0]["quality"] == "INVALIDATED"
    assert pd.isna(joined.iloc[0]["numeric_value"])


def test_point_in_time_does_not_expose_close_before_it_is_available(db):
    register_close(db)
    feature_store.save_value(
        db, "EURUSD", "M1", "close_m1", "1.0", "2026-10-14T17:25:00Z", 1.25,
        observed_at="2026-10-14T17:25:00Z", available_at="2026-10-14T18:00:00Z",
        source_revision="bar-18:00",
    )

    at_observation = feature_store.query_as_of(db, "2026-10-14T17:25:00Z", symbol="EURUSD", timeframe="M1")
    at_availability = feature_store.query_as_of(db, "2026-10-14T18:00:00Z", symbol="EURUSD", timeframe="M1")
    assert at_observation.empty
    assert at_availability["numeric_value"].tolist() == [1.25]


def test_expired_values_are_not_returned_and_timezones_are_required(db):
    register_close(db)
    feature_store.save_value(
        db, "EURUSD", "M1", "close_m1", "1.0", "2026-10-14T09:59:00Z", "CLOSED",
        observed_at="2026-10-14T10:00:00Z", available_at="2026-10-14T10:00:00Z",
        valid_until="2026-10-14T10:05:00Z",
    )
    assert len(feature_store.query_as_of(db, "2026-10-14T10:04:59Z")) == 1
    assert feature_store.query_as_of(db, "2026-10-14T10:05:00Z").empty
    with pytest.raises(ValueError, match="fuso horário"):
        feature_store.query_as_of(db, "2026-10-14 10:00:00")
    with pytest.raises(ValueError, match="posterior"):
        feature_store.save_value(
            db, "EURUSD", "M1", "close_m1", "1.0", "2026-10-14T09:59:00Z", 1.1,
            observed_at="2026-10-14T10:00:00Z", available_at="2026-10-14T10:00:00Z",
            valid_until="2026-10-14T10:00:00Z",
        )


def test_only_transitive_dependents_are_selected_for_recalculation(db):
    features.register(db, "returns", "Returns", "1.0", dependencies=["bars:M1"])
    features.register(db, "rvol", "Relative volume", "1.0", dependencies=["bars:M1"])
    features.register(db, "regime", "Regime", "1.0", dependencies=["feature:returns", "feature:rvol"])
    features.register(db, "news_context", "News context", "1.0", dependencies=["economic_calendar"])

    affected = features.affected_by(db, ["bars:M1"])
    assert [item["feature_id"] for item in affected] == ["returns", "rvol", "regime"]
    assert [item["feature_id"] for item in features.affected_by(db, ["feature:returns"])] == ["regime"]
    assert features.affected_by(db, ["unrelated_source"]) == []

    calls = []
    handlers = {name: (lambda _db, definition, name=name: calls.append(name) or definition["algorithm_version"])
                for name in ("returns", "rvol", "regime", "news_context")}
    result = features.recalculate_affected(db, ["bars:M1"], handlers)
    assert calls == ["returns", "rvol", "regime"]
    assert [item["feature_id"] for item in result] == calls

    called = []
    with pytest.raises(ValueError, match="Faltam recálculos"):
        features.recalculate_affected(db, ["bars:M1"], {"returns": lambda *_: called.append("returns")})
    assert called == []


def test_analytics_features_cli_lists_queries_and_reports_dependents(db, tmp_path, capsys):
    register_close(db)
    save(db, 1.5)
    config = tmp_path / "config.ini"
    config.write_text(f"[DATABASE]\ntype = sqlite\npath = {db.db_path}\n", encoding="utf-8")

    assert cli.main(["--config", str(config), "analytics", "features", "list"]) == 0
    assert "close_m1" in capsys.readouterr().out
    assert cli.main(["--config", str(config), "analytics", "features", "values", "--symbol", "EURUSD",
                     "--tf", "M1", "--feature", "close_m1", "--as-of", "2026-10-14T10:00:00Z"]) == 0
    assert "1.5" in capsys.readouterr().out
    assert cli.main(["--config", str(config), "analytics", "features", "affected", "--changed", "bars:M1"]) == 0
    assert "close_m1" in capsys.readouterr().out

    assert cli.main(["--config", str(config), "--json", "analytics", "features", "values", "--symbol",
                     "EURUSD", "--tf", "M1", "--feature", "close_m1", "--as-of",
                     "2026-10-14T10:00:00Z"]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["data"]["values"][0]["numeric_value"] == 1.5


def test_shared_source_recalculates_parent_before_child_and_keeps_as_of_history(db):
    features.register(db, "z_parent", "Parent", "1", dependencies=["bars:M1"])
    features.register(db, "a_child", "Child", "1",
                      dependencies=["bars:M1", "feature:z_parent"])
    features.register(db, "unrelated", "Unrelated", "1", dependencies=["calendar"])
    observation = "2026-10-14T09:59:00Z"
    first = "2026-10-14T10:00:00Z"
    revised = "2026-10-14T11:00:00Z"

    def write(name, value, available, revision):
        return feature_store.save_value(
            db, "QA", "M1", name, "1", observation, value,
            available_at=available, observed_at=available, source_revision=revision,
        )

    write("z_parent", 3, first, "1")
    write("a_child", 6, first, "1")
    calls = []

    def parent(manager, definition):
        calls.append(definition["feature_id"])
        return write("z_parent", 7, revised, "2")

    def child(manager, definition):
        calls.append(definition["feature_id"])
        current = feature_store.query_as_of(manager, revised, feature_id="z_parent")
        return write("a_child", 2 * current.iloc[0]["numeric_value"], revised, "2")

    results = features.recalculate_affected(
        db, ["bars:M1"], {"z_parent": parent, "a_child": child},
    )
    assert calls == ["z_parent", "a_child"]
    assert [item["feature_id"] for item in results] == calls
    assert feature_store.query_as_of(db, revised, feature_id="a_child")["numeric_value"].tolist() == [14]
    assert feature_store.query_as_of(db, first, feature_id="a_child")["numeric_value"].tolist() == [6]


def test_scoped_definitions_keep_topological_order_without_changing_default_selection(db):
    for tf, version in [("M15", "1"), ("H1", "2")]:
        source = f"bars_derived:{tf}"
        features.register(db, "z_parent", "Pai", version, dependencies=[source])
        features.register(db, "a_child", "Filho", version, dependencies=[source, "feature:z_parent"])
    assert features.affected_by(db, ["bars_derived:M15"]) == []
    selected = lambda item: "bars_derived:M15" in item["dependencies"]
    result = features.affected_by(db, ["bars_derived:M15"], definition_filter=selected)
    assert [(item["feature_id"], item["algorithm_version"]) for item in result] == [("z_parent", "1"), ("a_child", "1")]
    calls = []
    handlers = {name: (lambda db_, definition: calls.append(definition["feature_id"]))
                for name in ["z_parent", "a_child"]}
    features.recalculate_affected(db, ["bars_derived:M15"], handlers, definition_filter=selected)
    assert calls == ["z_parent", "a_child"]
    assert features.affected_by(db, ["unrelated"], definition_filter=selected) == []
