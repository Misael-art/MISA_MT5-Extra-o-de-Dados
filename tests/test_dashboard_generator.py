"""Fotografia usa store real em QA; relógios, versões, limite e leitura sem DDL."""
from datetime import datetime, timedelta, timezone
import hashlib
import json

import pandas as pd
import pytest
from sqlalchemy import event, text

from mt5_extracao.analytics import datasets, feature_store
from mt5_extracao.dashboard import generator
from mt5_extracao.database_manager import DatabaseManager


@pytest.fixture
def db(tmp_path):
    manager = DatabaseManager(db_path=str(tmp_path / "qa.sqlite"))
    datasets.identity(manager)
    feature_store.register_definition(manager, "qa.value", "Valor QA", "v1", {}, [],
        family="volume", unit="x", timeframe="M5", value_type="NUMBER")
    yield manager
    manager.close()


def clock():
    return pd.Timestamp.now(tz="UTC") + pd.Timedelta(seconds=1)


def save(db, *, value=0, version="v1", available=None, observation=None, until=None, quality="VALID"):
    stamp = clock() - pd.Timedelta(hours=2)
    return feature_store.save_value(db, "QA", "M5", "qa.value", version, observation or stamp, value,
        available_at=available or stamp + pd.Timedelta(minutes=5), observed_at=available or stamp + pd.Timedelta(minutes=5),
        valid_until=until, quality=quality, source_revision="qa-source", metadata={"reconstructed": True})


def photo(db, **kw):
    return generator.generate(db, symbol="QA", timeframe="M5", as_of=clock(), **kw)


def test_zero_provenance_and_no_write(db):
    save(db)
    statements = []
    def record(conn, cursor, statement, params, context, executemany):
        statements.append(statement)
    event.listen(db.engine, "before_cursor_execute", record)
    try:
        result = photo(db)
    finally:
        event.remove(db.engine, "before_cursor_execute", record)
    assert result["errors"] == []
    assert result["facts"][0]["value"] == 0
    assert result["facts"][0]["display_state"] == "AVAILABLE"
    assert result["provenance"]["base"]["namespace"] == datasets.identity(db)["namespace"]
    assert all(not any(word in s.upper() for word in ("INSERT ", "UPDATE ", "CREATE ", "DELETE ", "ALTER "))
               for s in statements)
    assert generator.validate(result) is result
    assert all(key in result["capabilities"] for key in generator.CAPABILITY_SOURCES)
    assert result["sections"][0]["visual"]["value"] is None
    assert result["capabilities"]["surprise"]["reason_code"] == "SURPRISE_PRODUCER_MISSING"


def test_late_revision_future_fact_and_contract_do_not_leak(db):
    cut = clock()
    observation = cut - pd.Timedelta(hours=2)
    save(db, value=12, observation=observation, available=cut - pd.Timedelta(hours=1))
    save(db, value=99, observation=observation, available=cut + pd.Timedelta(hours=1))
    save(db, value=500, observation=cut + pd.Timedelta(minutes=1), available=cut + pd.Timedelta(minutes=6))
    result = generator.generate(db, symbol="QA", timeframe="M5", as_of=cut)
    assert [r["value"] for r in result["facts"]] == [12]
    assert "99.0" not in json.dumps(result)
    with db.engine.begin() as connection:
        connection.execute(text("UPDATE _feature_definitions SET registered_at_utc=:future"),
                           {"future": (cut + pd.Timedelta(days=1)).isoformat()})
    result = generator.generate(db, symbol="QA", timeframe="M5", as_of=cut)
    assert result["facts"] == result["contracts"] == []


def test_expiry_boundary_is_stale_not_zero(db):
    cut = clock()
    save(db, value=42, until=cut)
    result = generator.generate(db, symbol="QA", timeframe="M5", as_of=cut)
    assert result["facts"][0]["value"] == 42
    assert result["facts"][0]["display_state"] == "STALE"
    assert result["facts"][0]["valid_until_utc"] == cut.isoformat()


def test_invalidated_revision_remains_auditable(db):
    observation = clock() - pd.Timedelta(hours=3)
    save(db, value=12, observation=observation)
    save(db, value=None, observation=observation, quality="INVALIDATED", available=clock() - pd.Timedelta(minutes=1))
    result = photo(db)
    assert result["facts"][0]["quality"] == "INVALIDATED"
    assert result["facts"][0]["value"] is None


def test_versions_are_explicit_and_not_combined(db):
    feature_store.register_definition(db, "qa.value", "Valor QA2", "v2", {}, [],
        family="volume", unit="points", timeframe="M5", value_type="NUMBER")
    save(db, value=10)
    save(db, value=20, version="v2")
    assert photo(db)["errors"][0]["code"] == "AMBIGUOUS_VERSION"
    result = photo(db, versions={"qa.value": "v1"})
    assert [r["value"] for r in result["facts"]] == [10]
    assert result["contracts"][0]["unit"] == "x"
    assert photo(db, versions={"qa.value": "v3"})["errors"][0]["code"] == "UNKNOWN_VERSION"


def test_missing_identity_returns_error_without_schema_creation(tmp_path):
    manager = DatabaseManager(db_path=str(tmp_path / "empty.sqlite"))
    try:
        assert photo(manager)["errors"][0]["code"] == "BASE_IDENTITY_MISSING"
        from sqlalchemy import inspect
        assert "_dataset_origin" not in inspect(manager.engine).get_table_names()
    finally:
        manager.close()
    assert photo(None)["errors"][0]["code"] == "BASE_UNAVAILABLE"


def test_no_data_does_not_invent_values(db):
    result = photo(db)
    assert result["facts"] == [] and result["data_cutoff_utc"] is None
    assert result["capabilities"]["quality"]["state"] == "NO_DATA"


def test_row_limit_refuses_partial_photograph(db, monkeypatch):
    for minute in range(3):
        save(db, value=minute, observation=clock() - pd.Timedelta(hours=3) + pd.Timedelta(minutes=minute))
    monkeypatch.setattr(generator, "MAX_FACTS", 2)
    result = photo(db)
    assert result["errors"] and not result["facts"]


@pytest.mark.parametrize("changes", [{"schema_version": 999}, {"facts": []}])
def test_schema_and_digest_refuse_tampering(db, changes):
    save(db)
    result = photo(db)
    result.update(changes)
    with pytest.raises(ValueError):
        generator.validate(result)


@pytest.mark.parametrize("changes", [{"timeframe": "M7"}, {"as_of": "2026-01-01"},
                                      {"start": "2020-01-01T00:00:00Z"}, {"symbol": ""}])
def test_selection_limits(changes):
    args = dict(symbol="QA", timeframe="M5", as_of=clock())
    args.update(changes)
    with pytest.raises(ValueError):
        generator.generate(None, **args)


def test_invalid_number_and_unknown_definition_schema_refuse(db):
    save(db)
    with db.engine.begin() as conn:
        conn.execute(text("UPDATE _feature_values SET numeric_value=:value"), {"value": float("inf")})
    assert photo(db)["errors"][0]["code"] == "NON_FINITE_FACT"
    with db.engine.begin() as conn:
        conn.execute(text("UPDATE _feature_definitions SET definition_schema_version=99"))
    assert photo(db)["errors"][0]["code"] == "DEFINITION_SCHEMA_UNKNOWN"


def test_hostile_json_is_text_and_snapshot_frozen_after_server_creation(db):
    from mt5_extracao.dashboard.server import DashboardServer
    import re
    hostile = '</script><script>alert("QA")</script>&'
    snapshot = generator.generate(db, symbol=hostile, timeframe="M5", as_of=clock(), fixture_label="QA sintética")
    instance = DashboardServer(port=5055, snapshot=snapshot)
    snapshot["subject"]["symbol"] = "alterado"
    with instance.app.test_client() as client:
        response = client.get(instance.url)
    body = response.get_data(as_text=True)
    assert response.status_code == 200
    assert hostile not in body and "&lt;/script&gt;" in body and "FIXTURE" in body
    payload = json.loads(re.search(r'<script id="dashboard-data" type="application/json">(.*?)</script>', body).group(1))
    assert payload["subject"]["symbol"] == hostile
    generator.validate(payload)


def test_same_symbol_different_base_has_distinct_identity(db, tmp_path):
    other = DatabaseManager(db_path=str(tmp_path / "other.sqlite"))
    try:
        datasets.identity(other)
        first, second = photo(db), photo(other)
        assert first["subject"]["base_namespace"] != second["subject"]["base_namespace"]
    finally:
        other.close()


def test_payload_budget_refuses_instead_of_truncating(db, monkeypatch):
    monkeypatch.setattr(generator, "MAX_BYTES", 100)
    with pytest.raises(ValueError, match="PAYLOAD_TOO_LARGE"):
        photo(db)
