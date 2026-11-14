"""Planejamento público sintético: schema completo, identidade, ausência e read-only."""
import copy
import json
import sqlite3

import pytest
from sqlalchemy import event, inspect, text

from mt5_extracao.analytics import datasets
from mt5_extracao import cli
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.resources import package_plan


def dump(db):
    with sqlite3.connect(db.db_path) as connection:
        return list(connection.iterdump())


@pytest.fixture
def db(tmp_path):
    db=DatabaseManager(db_path=str(tmp_path/"synthetic.sqlite"))
    datasets.declare_source(db,dict(provider="synthetic_qa",server="test_feed"))
    with db.engine.begin() as con:
        con.execute(text('CREATE TABLE "EURUSD_1_hora" (time TEXT PRIMARY KEY, open REAL, high REAL, low REAL, close REAL, legacy_rsi REAL, legacy_note TEXT DEFAULT \'public\')'))
        con.execute(text('INSERT INTO "EURUSD_1_hora" (time,open,high,low,close,legacy_rsi) VALUES (\'2026-01-01\',100,101,99,100,42)'))
        con.execute(text("INSERT INTO _symbol_tables (symbol,timeframe,table_name) VALUES ('USD_JPY','1_hora','EURUSD_1_hora')"))
        con.execute(text('CREATE TABLE "EURUSD_ticks" (time_msc BIGINT, bid REAL, ask REAL, flags INTEGER)'))
        con.execute(text('CREATE TABLE "_synthetic_dependency" (bar_time TEXT REFERENCES "EURUSD_1_hora"(time), revision INTEGER, received_at_utc TEXT)'))
        con.execute(text('CREATE VIEW "synthetic_view" AS SELECT time,legacy_rsi FROM "EURUSD_1_hora"'))
    yield db
    db.engine.dispose()


def test_inventory_keeps_all_tables_columns_views_and_registered_identity_without_scanning_rows(db):
    before=dump(db);statements=[]
    def observed(conn,cursor,statement,parameters,context,executemany):statements.append(statement)
    event.listen(db.engine,"before_cursor_execute",observed)
    try:result=package_plan.plan(db,["USD_JPY"])
    finally:event.remove(db.engine,"before_cursor_execute",observed)
    assert dump(db)==before
    assert result["base"]["namespace"]==datasets.identity(db)["namespace"]
    assert result["destination"]=="collection_separate_origins" and result["state"]=="planning_only"
    assert not result["export_ready"] and not result["reunion_ready"] and not result["source_written"]
    tables={row["table"]:row for row in result["inventory"]["tables"]}
    assert set(tables)==set(inspect(db.engine).get_table_names())
    assert {c["name"] for c in tables["EURUSD_1_hora"]["columns"]}>={"legacy_rsi","legacy_note","time","open","high","low","close"}
    assert tables["EURUSD_1_hora"]["registration"]["symbol"]=="USD_JPY" and tables["EURUSD_1_hora"]["selection"]=="selected"
    assert tables["EURUSD_ticks"]["selection"]=="requires_owner_scope"  # nome não vira posse
    assert tables["_synthetic_dependency"]["declared_dependencies"]==["EURUSD_1_hora"]
    assert result["inventory"]["views"][0]["view"]=="synthetic_view"
    assert all(row["row_count"] is None and row["data_sha256"] is None for row in tables.values())
    assert all(s.lstrip().upper().startswith(("SELECT","PRAGMA","BEGIN")) for s in statements)
    assert not any("COUNT(" in s.upper() for s in statements)
    assert package_plan.validate_plan(result)==result


def test_schema_change_is_detected_but_a_schema_plan_never_claims_a_content_hash(db):
    first=package_plan.plan(db,["USD_JPY"])
    with db.engine.begin() as con:con.execute(text('UPDATE "EURUSD_1_hora" SET legacy_rsi=43'))
    assert package_plan.plan(db,["USD_JPY"])["plan_sha256"]==first["plan_sha256"]
    with db.engine.begin() as con:con.execute(text('ALTER TABLE "EURUSD_1_hora" ADD COLUMN legacy_extra TEXT'))
    second=package_plan.plan(db,["USD_JPY"])
    assert first["inventory_sha256"]!=second["inventory_sha256"] and first["plan_sha256"]!=second["plan_sha256"]
    assert package_plan.validate_plan(first)==first


def test_missing_namespace_and_orphan_registration_are_not_invented_or_repaired(tmp_path):
    db=DatabaseManager(db_path=str(tmp_path/"legacy.sqlite"))
    with db.engine.begin() as con:
        con.execute(text('CREATE TABLE "EURUSD_1_hora" (time TEXT, close REAL)'))
        con.execute(text("INSERT INTO _symbol_tables VALUES ('LOST','1_hora','missing_table')"))
    before=dump(db);result=package_plan.plan(db,["EURUSD","LOST"])
    assert result["base"] is None and result["missing_registrations"]==["EURUSD","LOST"]
    assert result["orphan_registrations"][0]["table_name"]=="missing_table"
    assert "_dataset_origin" not in inspect(db.engine).get_table_names() and dump(db)==before
    db.engine.dispose()


def test_interrupt_limits_or_partial_metadata_do_not_return_an_approved_prefix(db,monkeypatch):
    before=dump(db);calls=0
    def cancel():
        nonlocal calls
        calls+=1
        if calls==3:raise TimeoutError("synthetic cancellation")
    with pytest.raises(TimeoutError):package_plan.plan(db,["USD_JPY"],check=cancel)
    assert dump(db)==before
    monkeypatch.setattr(package_plan,"MAX_TOTAL_COLUMNS",1)
    with pytest.raises(ValueError,match="colunas"):package_plan.plan(db,["USD_JPY"])
    assert dump(db)==before


@pytest.mark.parametrize("symbols", [[],None,"USD_JPY",[""],["a\nsecret"],["X"]*257])
def test_invalid_selection_is_rejected_before_reading(db,symbols):
    with pytest.raises(ValueError):package_plan.plan(db,symbols)


@pytest.mark.parametrize("fault", ["hash","column","schema","unknown","ready"])
def test_modified_or_unknown_plan_cannot_become_transport_permission(db,fault):
    result=copy.deepcopy(package_plan.plan(db,["USD_JPY"]))
    if fault=="hash":result["plan_sha256"]="0"*64
    elif fault=="column":result["inventory"]["tables"][0]["columns"].pop()
    elif fault=="schema":result["schema_version"]=2
    elif fault=="unknown":result["unknown"]=1
    else:result["export_ready"]=True
    with pytest.raises(ValueError):package_plan.validate_plan(result)


def test_cli_inventories_legacy_database_without_migrating_or_creating_origin(tmp_path, capsys):
    source = tmp_path / "base de QA # café.sqlite"
    config = tmp_path / "qa.ini"
    config.write_text("[DATABASE]\ntype = sqlite\n", encoding="utf-8")
    with sqlite3.connect(source) as connection:
        connection.execute("CREATE TABLE legacy (time TEXT, extra BLOB)")
        connection.execute("INSERT INTO legacy VALUES ('t', X'00FF')")
    before = source.read_bytes()
    argv = ["--config", str(config), "--json", "resources", "package-plan", "--symbols", "TEST"]

    assert cli.main(["--db", str(source), *argv]) == 0
    payload = json.loads(capsys.readouterr().out)["data"]
    assert payload["base"] is None
    assert [table["table"] for table in payload["inventory"]["tables"]] == ["legacy"]
    assert source.read_bytes() == before

    missing = tmp_path / "missing" / "source.sqlite"
    assert cli.main(["--db", str(missing), *argv]) != 0
    result = json.loads(capsys.readouterr().out)
    assert not result["ok"] and result["data"] == {}
    assert not missing.parent.exists()


def test_incomplete_namespace_has_a_clear_error(db):
    with db.engine.begin() as connection:
        connection.execute(text("DELETE FROM _dataset_origin WHERE slot = 'default'"))
    with pytest.raises(ValueError, match="Identidade BD03 incompleta"):
        package_plan.plan(db, ["USD_JPY"])
