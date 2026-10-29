"""Catálogo/consulta canônicos: versões persistidas sem fórmula local, tipos e paginação sem mistura."""
import base64
import json

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import inspect, text

from mt5_extracao import cli, sdk
from mt5_extracao.analytics import datasets, feature_store, indicator_registry, query_service
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.laboratory import inventory

CUT = "2025-01-01T00:10:00Z"


@pytest.fixture
def db(tmp_path):
    manager = DatabaseManager(db_path=str(tmp_path / "catalog.sqlite"), broker_utc_offset=0)
    datasets.declare_source(manager, {"provider":"qa", "server":"Synthetic"})
    yield manager
    manager.close()


def register(db, ident="qa.metric", version="1", unit="fraction", kind="NUMBER", params=None):
    feature_store.register_definition(db, ident, "Série QA " + version, version,
        {"period":int(version)} if params is None else params, ["bars:M1"], family="qa", unit=unit,
        value_type=kind, warmup_count=0, warmup_unit="bars", availability_rule="declared available_at")


def fill(db, ident="qa.metric", version="1", values=(0., .1, .2, .3)):
    records = []
    for i, value in enumerate(values):
        observation = pd.Timestamp("2025-01-01T00:00Z") + pd.Timedelta(minutes=i)
        records.append(dict(symbol="QA", timeframe="M1", feature_id=ident, algorithm_version=version,
            observation_time=observation, value=value, available_at=observation + pd.Timedelta(minutes=1),
            observed_at=observation + pd.Timedelta(minutes=1)))
    feature_store.save_values(db, records)


def series(db, **changes):
    return query_service.series(db, **{**dict(symbol="QA", timeframe="M1", feature_ids=["qa.metric"], as_of=CUT),
                                      **changes})


def test_catalog_sdk_and_inventory_discover_persisted_versions_without_local_formula(db):
    register(db, version="1")
    register(db, version="2", unit="percent")
    before_tables = inspect(db.engine).get_table_names()
    before = feature_store.list_definitions(db)
    all_defs = query_service.catalog(db)
    assert all_defs == sdk.query_catalog(db)
    found = [d for d in all_defs["definitions"] if d["feature_id"] == "qa.metric"]
    assert [(d["algorithm_version"], d["unit"], d["computable"]) for d in found] == [
        ("1", "fraction", False), ("2", "percent", False)]
    assert all(d["reference"] is None and d["alignment"] is None for d in found)
    assert all_defs["base"]["provider"] == "qa"
    inv = inventory.search(db, query="qa.metric")["items"]
    assert {(d["feature_id"], d["algorithm_version"], d["unit"]) for d in found} == {
        (d["feature_id"], d["algorithm_version"], d["unit"]) for d in inv}
    assert feature_store.list_definitions(db) == before
    assert inspect(db.engine).get_table_names() == before_tables
    assert len(sdk.query_catalog()["definitions"]) == len(indicator_registry.definitions())


def test_variants_keep_real_units_and_parameters_without_implicit_conversion_or_latest_choice(db):
    register(db, version="1")
    register(db, version="2", unit="percent")
    fill(db)
    fill(db, version="2", values=(0., 10., 20., 30.))
    result = series(db)
    assert result["status"] == "ok" and len(result["values"]) == 8
    definition = result["definitions"]["qa.metric"]
    assert definition["unit"] is None and definition["metadata_state"] == "VERSION_DEPENDENT"
    assert definition["algorithm_versions"] == ["1", "2"]
    assert definition["variants"]["1"]["unit"] == "fraction"
    assert definition["variants"]["2"]["parameters"] == {"period":2}
    assert [v["value"] for v in result["values"] if v["algorithm_version"] == "2"] == [0., 10., 20., 30.]
    one = series(db, algorithm_version="1")
    assert len(one["values"]) == 4 and one["definitions"]["qa.metric"]["unit"] == "fraction"
    assert series(db, algorithm_version="absent")["values"] == []
    before = series(db, as_of="2025-01-01T00:00:59Z")
    assert before["values"] == [] and before["reasons"]
    assert result["values"][0]["value"] == 0. and result["values"][0]["quality"] == "VALID"
    json.dumps(result, allow_nan=False)


@pytest.mark.parametrize("kind,value,category", [("CATEGORY", "compra", "compra"), ("BOOLEAN", False, "false")])
def test_non_numeric_values_keep_declared_type_and_verbatim_category(db, kind, value, category):
    register(db, kind=kind, unit="code")
    fill(db, values=[value])
    result = series(db)
    assert result["status"] == "ok"
    assert result["values"][0]["value"] is None and result["values"][0]["category"] == category
    assert result["definitions"]["qa.metric"]["output_kind"] == kind.lower()


def test_stored_contract_conflicting_with_registry_is_not_reconstructed_as_local_formula(db):
    spec = indicator_registry.get_definition("price.return_1")
    register(db, ident=spec["feature_id"], version=spec["algorithm_version"], params={"period":99})
    fill(db, ident=spec["feature_id"], version=spec["algorithm_version"])
    entry = next(d for d in query_service.catalog(db)["definitions"] if d["feature_id"] == spec["feature_id"])
    assert entry["metadata_state"] == "REGISTRY_CONFLICT" and not entry["computable"]
    assert "parameters" in entry["metadata_conflicts"]
    result = series(db, feature_ids=[spec["feature_id"]])
    assert result["definitions"][spec["feature_id"]]["variants"][spec["algorithm_version"]]["parameters"] == {"period":99}
    assert inventory.search(db, query=spec["feature_id"])["items"][0]["state"] == "STORED_ONLY"


def test_compatible_registry_keeps_semantic_categories_stored_as_numbers(db):
    from mt5_extracao.analytics import indicator_contracts
    spec = indicator_contracts.contract("trend.alligator_alignment")
    feature_store.register_definition(db, spec["feature_id"], spec["name"], spec["algorithm_version"],
        spec["parameters"], ["bars:M1"], **{key:spec[key] for key in (
            "family", "formula", "unit", "warmup_count", "warmup_unit", "availability_rule", "value_type")})
    fill(db, ident=spec["feature_id"], version=spec["algorithm_version"], values=[-1., 0., 1.])
    result = series(db, feature_ids=[spec["feature_id"]])
    definition = result["definitions"][spec["feature_id"]]
    assert definition["output_kind"] == "category"
    variant = definition["variants"][spec["algorithm_version"]]
    assert variant["value_type"] == "NUMBER" and variant["buffers"] == spec["buffers"]
    assert [r["value"] for r in result["values"]] == [-1., 0., 1.]


def test_old_direction_version_keeps_declared_categorical_unit_without_borrowing_new_states(db):
    ident = "trend.alligator_alignment"
    register(db, ident=ident, version="9", unit="direction_code_-1_0_1")
    fill(db, ident=ident, version="9", values=[-1., 0., 1.])
    result = series(db, feature_ids=[ident], algorithm_version="9")
    definition = result["definitions"][ident]
    assert definition["output_kind"] == "category"
    variant = definition["variants"]["9"]
    assert variant["origin"] == "data_store" and not variant["computable"]
    assert "categories" not in variant["buffers"][0]  # significados de outra versão não são herdados


def test_pagination_repeated_ids_and_page_size_change_match_one_shot(db):
    register(db)
    fill(db)
    full = series(db)
    first = series(db, feature_ids=["qa.metric", "qa.metric"], page_size=1)
    assert first["selection"]["feature_ids"] == ["qa.metric"]
    second = series(db, page_size=3, cursor=first["page"]["next_cursor"])
    assert first["values"] + second["values"] == full["values"]
    assert second["page"]["next_cursor"] is None


def test_cursor_rejects_changed_revision_and_different_base(db, tmp_path):
    register(db)
    fill(db)
    first = series(db, page_size=1)
    cursor = first["page"]["next_cursor"]
    other = DatabaseManager(db_path=str(tmp_path / "other.sqlite"), broker_utc_offset=0)
    try:
        register(other)
        fill(other)
        result = series(other, cursor=cursor)
        assert result["status"] == "error" and result["errors"][0]["code"] == "STALE_CURSOR"
    finally:
        other.close()
    feature_store.save_value(db, "QA", "M1", "qa.metric", "1", "2025-01-01T00:00Z", 999.,
        available_at="2025-01-01T00:05Z", observed_at="2025-01-01T00:05Z", source_revision="2")
    stale = series(db, cursor=cursor)
    assert stale["values"] == [] and stale["errors"][0]["code"] == "STALE_CURSOR"
    assert series(db)["values"][0]["value"] == 999.


def test_future_revision_does_not_invalidate_fixed_historical_cut(db):
    register(db)
    fill(db)
    first = series(db, page_size=1)
    feature_store.save_value(db, "QA", "M1", "qa.metric", "1", "2025-01-01T00:00Z", 999.,
        available_at="2025-01-01T00:20Z", observed_at="2025-01-01T00:20Z", source_revision="2")
    following = series(db, cursor=first["page"]["next_cursor"])
    assert following["status"] == "ok" and len(following["values"]) == 3


@pytest.mark.parametrize("payload", [[], {}, {"k":[1, 2, 3]}, {"v":2, "s":"x", "g":"0"*64, "k":[]}])
def test_malformed_cursor_payloads_are_structured_not_python_exceptions(db, payload):
    register(db)
    cursor = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode()
    result = series(db, cursor=cursor)
    assert result["status"] == "error" and result["errors"][0]["code"] == "BAD_CURSOR"


@pytest.mark.parametrize("change,code", [({"feature_ids":"qa.metric"}, "BAD_FEATURES"),
    ({"feature_ids":[False]}, "BAD_FEATURES"), ({"feature_ids":np.array(["qa.metric"])}, "BAD_FEATURES"),
    ({"feature_ids":[]}, "MISSING_SELECTION"), ({"as_of":"invalid-date"}, "BAD_TIME"),
    ({"as_of":None}, "BAD_TIME"), ({"algorithm_version":True}, "BAD_VERSION"),
    ({"start":"2025-01-03T00:00Z", "end":"2025-01-01T00:00Z"}, "BAD_RANGE")])
def test_invalid_selection_is_json_safe_and_does_not_modify_definitions(db, change, code):
    register(db)
    before = feature_store.list_definitions(db)
    result = series(db, **change)
    assert result["status"] == "error" and result["errors"][0]["code"] == code
    json.dumps(result, allow_nan=False)
    assert feature_store.list_definitions(db) == before


def test_missing_stored_definition_does_not_borrow_current_registry_contract(db):
    ident = "price.return_1"
    register(db, ident=ident, version="9", unit="points")
    fill(db, ident=ident, version="9")
    with db.engine.begin() as conn:
        conn.execute(text("DELETE FROM _feature_definitions WHERE feature_id=:id"), {"id":ident})
    result = series(db, feature_ids=[ident])
    assert result["status"] == "error" and result["errors"][0]["code"] == "MISSING_DEFINITION"
    row = next(r for r in inventory.search(db, query=ident)["items"] if r["algorithm_version"] == "9")
    assert row["state"] == "UNAVAILABLE" and "sem definição" in row["reason"]


def test_inventory_symbol_and_timeframe_must_match_the_same_series(db):
    register(db)
    for symbol, tf in [("EURUSD", "H1"), ("AAPL", "M5")]:
        feature_store.save_value(db, symbol, tf, "qa.metric", "1", "2025-01-01T00:00Z", 1.,
            available_at="2025-01-01T01:00Z", observed_at="2025-01-01T01:00Z")
    assert inventory.search(db, symbol="EURUSD", timeframe="M5")["matched"] == 0
    assert inventory.search(db, symbol="EURUSD", timeframe="H1")["matched"] == 1


def test_inventory_cursor_rejects_changed_catalog(db):
    first = inventory.search(db, page_size=1)
    register(db)
    with pytest.raises(ValueError, match="inventário mudou"):
        inventory.search(db, page_size=1, cursor=first["next_cursor"])


def test_cli_catalog_and_explicit_version_equal_sdk(db, tmp_path, capsys):
    register(db)
    register(db, version="2", unit="percent")
    fill(db)
    fill(db, version="2", values=[10., 20.])
    config = tmp_path / "qa.ini"
    config.write_text(f"[DATABASE]\ntype=sqlite\npath={db.db_path}\n[APP]\nbroker_utc_offset=0\n", encoding="utf-8")
    prefix = ["--config", str(config), "--json", "analytics", "features"]
    assert cli.main([*prefix, "catalog"]) == 0
    catalog = json.loads(capsys.readouterr().out)["data"]["envelope"]
    assert catalog == json.loads(json.dumps(sdk.query_catalog(db)))
    assert cli.main([*prefix, "series", "--symbol", "QA", "--tf", "M1", "--feature", "qa.metric",
                     "--as-of", CUT, "--algorithm-version", "2"]) == 0
    got = json.loads(capsys.readouterr().out)["data"]["envelope"]
    assert got == sdk.query_series(db, symbol="QA", timeframe="M1", feature_ids=["qa.metric"],
                                    as_of=CUT, algorithm_version="2")


def test_unavailable_database_is_an_explicit_error(db):
    db.close()
    db.engine = None  # dispose() sozinho permite reconexão pelo SQLAlchemy
    assert query_service.catalog(db)["errors"][0]["code"] == "BASE_UNAVAILABLE"
    assert series(db)["errors"][0]["code"] == "BASE_UNAVAILABLE"


def test_pure_catalog_does_not_stamp_namespace_or_create_tables(tmp_path):
    manager = DatabaseManager(db_path=str(tmp_path / "unstamped.sqlite"), broker_utc_offset=0)
    try:
        before = inspect(manager.engine).get_table_names()
        query_service.definition_catalog(manager)
        assert inspect(manager.engine).get_table_names() == before
        assert "_feature_definitions" not in before
    finally:
        manager.close()
