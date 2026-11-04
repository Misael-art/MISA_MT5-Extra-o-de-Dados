"""Página SQL, geração em lotes e imagem atômica; dados sintéticos no store existente."""
import base64
import json
import threading

import pytest
from sqlalchemy import event, inspect, text

from mt5_extracao.analytics import datasets, feature_store, query_service
from tests.test_bd18_stored_catalog import CUT, db, fill, register, series


def check_lexical_pages(manager):
    """Também exercitado pelo PostgreSQL real no job dedicado, com collation explícita."""
    ids = ["qa.z", "qa.Z", "qa.á", "qa._", "qa.a"]
    for ident in ids:
        for version in ["2", "10"]:
            register(manager, ident=ident, version=version)
            fill(manager, ident=ident, version=version, values=[1., 2.])
    query = dict(symbol="QA", timeframe="M1", feature_ids=ids, as_of=CUT)
    full = query_service.series(manager, **query)
    assert full["status"] == "ok", full
    keys = [(r["feature_id"], r["observation_time"], r["algorithm_version"]) for r in full["values"]]
    assert keys == sorted(keys) and len(keys) == 20
    rows, cursor = [], None
    while True:
        page = query_service.series(manager, **query, page_size=3, cursor=cursor)
        assert page["status"] == "ok", page
        assert page["definitions"] == full["definitions"]
        rows += page["values"]
        cursor = page["page"]["next_cursor"]
        if cursor is None:
            break
    assert rows == full["values"]


def test_sql_page_has_limit_keyset_and_never_builds_the_full_dataframe(db, monkeypatch):
    register(db);fill(db)
    statements = []
    def record(_conn, _cursor, statement, parameters, _context, _many):
        if "ROW_NUMBER()" in statement:
            statements.append((statement, parameters))
    event.listen(db.engine, "before_cursor_execute", record)
    monkeypatch.setattr(feature_store.pd, "read_sql", lambda *a, **k:pytest.fail("não carregar seleção inteira"))
    try:
        first = series(db, page_size=1)
        second = series(db, page_size=2, cursor=first["page"]["next_cursor"])
    finally:
        event.remove(db.engine, "before_cursor_execute", record)
    assert len(first["values"]) == 1 and len(second["values"]) == 2
    pages = [(sql, params) for sql, params in statements if "LIMIT" in sql]
    assert len(pages) == 2 and pages[0][1][-1] == 2 and pages[1][1][-1] == 3
    assert "OFFSET" not in pages[1][0] and "feature_id COLLATE BINARY >" in pages[1][0]
    assert len(statements) == 4   # uma verificação em lotes + uma página limitada por chamada


def test_lexical_order_is_independent_of_locale_and_page_size(db):
    check_lexical_pages(db)


def test_unseen_versions_and_parameters_are_in_metadata_from_the_first_page(db):
    register(db);register(db, version="2", unit="percent")
    fill(db);fill(db, version="2", values=[10.,20.])
    first = series(db, page_size=1)
    spec = first["definitions"]["qa.metric"]
    assert first["values"][0]["algorithm_version"] == "1"
    assert spec["algorithm_versions"] == ["1","2"] and spec["unit"] is None
    assert spec["variants"]["2"]["unit"] == "percent"
    assert spec["variants"]["2"]["parameters"] == {"period":2}
    cursor = first["page"]["next_cursor"]
    while cursor:
        page = series(db, page_size=2, cursor=cursor)
        assert page["definitions"] == first["definitions"]
        cursor = page["page"]["next_cursor"]


def test_change_outside_the_returned_page_invalidates_cursor_but_future_revision_does_not(db):
    register(db);fill(db)
    first = series(db, page_size=1)
    cursor = first["page"]["next_cursor"]
    feature_store.save_value(db,"QA","M1","qa.metric","1","2025-01-01T00:03Z",999.,
        available_at="2025-01-01T00:20Z",observed_at="2025-01-01T00:20Z")
    assert series(db, cursor=cursor)["status"] == "ok"
    # Corrupção fora da primeira página, sem atualizar value_hash: a geração verifica o conteúdo.
    with db.engine.begin() as conn:
        conn.execute(text("UPDATE _feature_values SET numeric_value=55 WHERE observation_time=:t AND revision=1"),
                     {"t":feature_store._utc_text("2025-01-01T00:03Z","t")})
    result = series(db, cursor=cursor)
    assert result["errors"][0]["code"] == "STALE_CURSOR" and result["values"] == []


def test_page_and_metadata_share_the_scan_snapshot_during_concurrent_write(db, monkeypatch):
    register(db);fill(db)
    with db.engine.connect() as conn:
        conn.exec_driver_sql("PRAGMA journal_mode=WAL");conn.commit()
    original = query_service._scan_selection
    errors = []
    def writer():
        try:
            register(db, version="2", unit="percent")
            fill(db, version="2", values=[10.])
            feature_store.save_value(db,"QA","M1","qa.metric","1","2025-01-01T00:00Z",99.,
                available_at="2025-01-01T00:06Z",observed_at="2025-01-01T00:06Z")
        except Exception as exc:
            errors.append(exc)
    def scan(*args):
        result = original(*args)
        thread = threading.Thread(target=writer)
        thread.start();thread.join(10)
        assert not thread.is_alive() and not errors
        return result
    monkeypatch.setattr(query_service,"_scan_selection",scan)
    first = series(db, page_size=1)
    assert first["values"][0]["value"] == 0.
    assert first["definitions"]["qa.metric"]["algorithm_versions"] == ["1"]
    monkeypatch.setattr(query_service,"_scan_selection",original)
    assert series(db, cursor=first["page"]["next_cursor"])["errors"][0]["code"] == "STALE_CURSOR"
    current = series(db)
    assert current["values"][0]["value"] == 99.
    assert current["definitions"]["qa.metric"]["algorithm_versions"] == ["1","2"]


def test_selection_budget_rejects_before_fetching_or_publishing_a_partial_page(db, monkeypatch):
    register(db);fill(db)
    monkeypatch.setattr(query_service,"MAX_SELECTION_ROWS",2)
    calls = []
    def record(_conn,_cursor,statement,_params,_context,_many):
        if "ROW_NUMBER()" in statement:
            calls.append(statement)
    event.listen(db.engine,"before_cursor_execute",record)
    try:
        result = series(db,page_size=1)
    finally:
        event.remove(db.engine,"before_cursor_execute",record)
    assert result["errors"][0]["code"] == "SELECTION_TOO_LARGE" and not result["values"]
    assert len(calls) == 1 and "LIMIT" not in calls[0]


def test_cursor_format_upgrade_requests_restart_and_new_cursor_is_valid(db):
    register(db);fill(db)
    first = series(db,page_size=1)
    cursor = first["page"]["next_cursor"]
    payload = json.loads(base64.urlsafe_b64decode(cursor))
    assert payload["v"] == 3
    payload["v"] = 2
    legacy = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode()
    result = series(db,cursor=legacy)
    assert result["errors"][0]["code"] == "BAD_CURSOR" and "recomece" in result["errors"][0]["message"]


def test_expired_latest_revision_never_falls_back_and_null_quality_is_preserved(db):
    register(db);fill(db,values=[1.,2.,3.])
    feature_store.save_value(db,"QA","M1","qa.metric","1","2025-01-01T00:00Z",9.,
        available_at="2025-01-01T00:06Z",observed_at="2025-01-01T00:06Z",valid_until="2025-01-01T00:07Z")
    feature_store.save_value(db,"QA","M1","qa.metric","1","2025-01-01T00:01Z",None,
        available_at="2025-01-01T00:08Z",observed_at="2025-01-01T00:08Z",quality="UNAVAILABLE")
    result = series(db,page_size=1)
    assert result["values"][0]["observation_time"].endswith("00:01:00.000000Z")
    assert result["values"][0]["value"] is None and result["values"][0]["quality"] == "UNAVAILABLE"
    next_page = series(db,cursor=result["page"]["next_cursor"])
    assert [r["value"] for r in next_page["values"]] == [3.]


def test_empty_store_does_not_create_feature_tables_or_call_the_fact_scanner(db,monkeypatch):
    before = inspect(db.engine).get_table_names()
    monkeypatch.setattr(query_service,"_scan_selection",lambda *a:pytest.fail("tabela ausente"))
    result = series(db,feature_ids=["price.return_1"])
    assert result["status"] == "ok" and result["values"] == [] and result["reasons"]
    assert "_feature_values" not in inspect(db.engine).get_table_names()
    assert "_feature_values" not in before


def test_identity_can_read_the_existing_transaction_without_schema_writes(db,monkeypatch):
    expected = datasets.identity(db)
    monkeypatch.setattr(datasets,"_schema",lambda *a:pytest.fail("somente leitura nesta conexão"))
    with db.consistent_read() as conn:
        assert datasets.identity(db,connection=conn) == expected


def test_postgresql_order_uses_c_collation_and_bound_cursor_values():
    statement,params = feature_store._asof_statement(CUT,feature_ids=["qa.x"],ordered_for="postgresql",
        after=("qa.x","2025-01-01T00:00:00.000000Z","1"),limit=2)
    assert 'feature_id COLLATE "C"' in str(statement) and 'algorithm_version COLLATE "C"' in str(statement)
    assert params["after_time"] == "2025-01-01T00:00:00.000000+00:00" and params["page_limit"] == 2


def test_cursor_keeps_microsecond_boundaries_without_duplicates_or_omissions(db):
    register(db)
    for microsecond in [1,2,3]:
        feature_store.save_value(db,"QA","M1","qa.metric","1",f"2025-01-01T00:00:00.00000{microsecond}Z",
            float(microsecond),available_at="2025-01-01T00:01Z",observed_at="2025-01-01T00:01Z")
    first = series(db,page_size=1)
    second = series(db,page_size=2,cursor=first["page"]["next_cursor"])
    assert [r["value"] for r in first["values"]+second["values"]] == [1.,2.,3.]
    assert second["page"]["next_cursor"] is None


def test_non_finite_corruption_is_json_safe_and_still_changes_the_cursor_fingerprint(db):
    register(db);fill(db)
    with db.engine.begin() as conn:
        conn.execute(text("UPDATE _feature_values SET numeric_value=:v WHERE observation_time=:t"),
            dict(v=float("inf"),t=feature_store._utc_text("2025-01-01T00:03Z","t")))
    first = series(db,page_size=1)
    assert first["status"] == "ok"
    full = series(db)
    assert full["values"][-1]["value"] is None
    json.dumps(full,allow_nan=False)
    with db.engine.begin() as conn:
        conn.execute(text("UPDATE _feature_values SET numeric_value=:v WHERE observation_time=:t"),
            dict(v=-float("inf"),t=feature_store._utc_text("2025-01-01T00:03Z","t")))
    assert series(db,cursor=first["page"]["next_cursor"])["errors"][0]["code"] == "STALE_CURSOR"
