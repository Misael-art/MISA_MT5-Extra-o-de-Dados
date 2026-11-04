import json

import pandas as pd
import pytest
from sqlalchemy import event, inspect, text

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
    assert "ix_feature_values_key_revision" in {index["name"]
                                                 for index in inspect(db.engine).get_indexes("_feature_values")}


def test_definitions_are_versioned_immutable_and_validate_dependencies(db):
    assert features.register(db, "close_m1", "M1 close", "1.0", dependencies=["bars:M1"])
    assert features.register(db, "return_m1", "M1 return", "1.0", dependencies=["feature:close_m1"])
    assert not features.register(db, "close_m1", "M1 close", "1.0", dependencies=["bars:M1"])
    assert features.register(db, "close_m1", "M1 close", "2.0", dependencies=["bars:M1"],
                             parameters={"method": "adjusted"})
    assert len([item for item in features.list_features(db, include_versions=True)
                if item["feature_id"] == "close_m1"]) == 2

    with pytest.raises(ValueError, match="nova versão do algoritmo"):
        features.register(db, "close_m1", "M1 close", "1.0", dependencies=["bars:M5"])
    with pytest.raises(ValueError, match="Dependência não registrada"):
        features.register(db, "unknown_dependent", "Unknown", "1.0", dependencies=["feature:missing"])
    with pytest.raises(ValueError, match="ciclos"):
        features.register(db, "close_m1", "M1 close", "3.0", dependencies=["feature:return_m1"])


def test_definition_name_survives_metadata_validation_and_is_immutable(db):
    definition = dict(feature_id="qa.named", name="  Nome da série QA  ", algorithm_version="1",
                      parameters={"period": 7}, dependencies=["bars:M5"], family=" qa ",
                      unit=" points ", value_type=" NUMBER ")
    assert feature_store.register_definition(db, **definition)
    stored = feature_store.list_definitions(db)[0]
    assert (stored["name"], stored["family"], stored["unit"], stored["value_type"]) == (
        "Nome da série QA", "qa", "points", "NUMBER")
    registered_at = stored["registered_at_utc"]
    assert not feature_store.register_definition(db, **definition)
    with pytest.raises(ValueError, match="nova versão"):
        feature_store.register_definition(db, **{**definition, "name": "Outro nome"})
    assert feature_store.list_definitions(db)[0]["registered_at_utc"] == registered_at
    assert feature_store.list_definitions(db)[0]["name"] == "Nome da série QA"
    assert feature_store.register_definition(db, **{**definition, "algorithm_version": "2", "name": "Outro nome"})
    assert [(d["algorithm_version"], d["name"]) for d in feature_store.list_definitions(db)] == [
        ("1", "Nome da série QA"), ("2", "Outro nome")]


def test_named_definition_reaches_cli_and_inventory_without_changing_values(db, tmp_path, capsys):
    from mt5_extracao.laboratory import inventory
    features.register(db, "qa.named", "Série QA legível", "1", dependencies=["bars:M1"], value_type="NUMBER")
    feature_store.save_value(db, "QA", "M1", "qa.named", "1", "2025-01-01T00:00Z", 12.,
                             available_at="2025-01-01T00:01Z", observed_at="2025-01-01T00:01Z")
    before = feature_store.query_history(db, feature_id="qa.named")
    config = tmp_path / "qa.ini"
    config.write_text(f"[DATABASE]\ntype=sqlite\npath={db.db_path}\n", encoding="utf-8")
    assert cli.main(["--config", str(config), "--json", "analytics", "features", "list"]) == 0
    result = json.loads(capsys.readouterr().out)["data"]["features"]
    assert result[0]["name"] == "Série QA legível"
    assert inventory.search(db, query="qa.named")["items"][0]["name"] == "Série QA legível"
    pd.testing.assert_frame_equal(before, feature_store.query_history(db, feature_id="qa.named"))


def test_legacy_definition_name_is_not_silently_rewritten_or_breaks_repeated_producer(db, caplog):
    feature_store.register_definition(db, "qa.legacy", "Nome original", "1", {}, [])
    # Reprodução explícita de metadado escrito pela implementação anterior; nunca usa banco pessoal.
    with db.engine.begin() as conn:
        conn.execute(text("UPDATE _feature_definitions SET name='value_type', definition_schema_version=1 "
                          "WHERE feature_id='qa.legacy'"))
    assert not feature_store.register_definition(db, "qa.legacy", "Nome original", "1", {}, [])
    row = feature_store.list_definitions(db)[0]
    assert (row["name"], row["definition_schema_version"], row["name_status"]) == (
        "value_type", 1, "UNVERIFIED_LEGACY_NAME")
    assert "nome legado não verificável" in caplog.text
    with pytest.raises(ValueError, match="nova versão"):
        feature_store.register_definition(db, "qa.legacy", "Nome original", "1", {"period": 9}, [])
    from mt5_extracao.laboratory import inventory
    assert "Nome legado não verificável" in inventory.search(db, query="qa.legacy")["items"][0]["reason"]


def test_new_explicit_value_type_name_cannot_be_treated_as_a_legacy_placeholder(db):
    assert feature_store.register_definition(db, "qa.valid", "value_type", "1", {}, [])
    row = feature_store.list_definitions(db)[0]
    assert row["definition_schema_version"] == 2 and row["name_status"] == "DECLARED"
    with pytest.raises(ValueError, match="nova versão"):
        feature_store.register_definition(db, "qa.valid", "Outro nome", "1", {}, [])


def test_old_definition_schema_gets_additive_marker_without_rewriting_rows(db):
    with db.engine.begin() as conn:
        conn.execute(text("CREATE TABLE _feature_definitions (feature_id TEXT, algorithm_version TEXT, "
                          "name TEXT, parameters_json TEXT, dependencies_json TEXT, registered_at_utc TEXT, "
                          "PRIMARY KEY (feature_id, algorithm_version))"))
        conn.execute(text("INSERT INTO _feature_definitions VALUES ('qa.old','1','value_type','{}','[]','2025-01-01')"))
    # A leitura reconhece o legado sem migrar nem criar tabelas.
    assert feature_store.list_definitions(db)[0]["name_status"] == "UNVERIFIED_LEGACY_NAME"
    assert "definition_schema_version" not in {c["name"] for c in inspect(db.engine).get_columns("_feature_definitions")}
    feature_store.ensure_schema(db)
    row = feature_store.list_definitions(db)[0]
    assert row["name"] == "value_type" and row["registered_at_utc"] == "2025-01-01"
    assert row["definition_schema_version"] == 1
    assert not feature_store.register_definition(db, "qa.old", "Nome do produtor", "1", {}, [])


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

def test_save_values_only_reads_existing_rows_for_observations_in_the_batch(db):
    register_close(db)
    start = pd.Timestamp("2026-10-01T00:00:00Z")
    history = [{"symbol": "EURUSD", "timeframe": "M1", "feature_id": "close_m1",
                "algorithm_version": "1.0", "observation_time": stamp, "value": float(index),
                "available_at": stamp, "observed_at": stamp, "source_revision": "seed"}
               for index, stamp in enumerate(pd.date_range(start, periods=1000, freq="min"))]
    assert feature_store.save_values(db, history)["inserted"] == 1000
    statements = []

    def capture(_conn, _cursor, statement, _parameters, _context, _executemany):
        if "SELECT observation_time, revision, value_hash, available_at_utc" in statement:
            statements.append(statement)

    event.listen(db.engine, "before_cursor_execute", capture)
    next_observation = start + pd.Timedelta(minutes=1000)
    result = feature_store.save_values(db, [{"symbol": "EURUSD", "timeframe": "M1", "feature_id": "close_m1",
                                             "algorithm_version": "1.0", "observation_time": next_observation,
                                             "value": 1000.0, "available_at": next_observation,
                                             "observed_at": next_observation, "source_revision": "append"}])
    event.remove(db.engine, "before_cursor_execute", capture)
    assert result["inserted"] == 1
    assert len(statements) == 1 and "observation_time IN (" in statements[0]


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
