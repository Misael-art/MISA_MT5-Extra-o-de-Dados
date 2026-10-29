"""Contexto causal e CSV devem conservar fatos persistidos sem exigir fórmula local."""
import json

import pandas as pd
import pytest
from sqlalchemy import text

from mt5_extracao import cli
from mt5_extracao.analytics import feature_store, indicator_context, indicator_materializer
from mt5_extracao.data_exporter import DataExporter
from mt5_extracao.database_manager import DatabaseManager


@pytest.fixture
def db(tmp_path):
    manager = DatabaseManager(db_path=str(tmp_path / "context.sqlite"), broker_utc_offset=0)
    yield manager
    manager.close()


def register(db, ident="qa.persisted", version="v1", kind="NUMBER", unit="percent"):
    feature_store.register_definition(db, ident, "Série persistida QA", version, {"period":5}, [],
        family="qa", unit=unit, value_type=kind)


def save(db, value, *, ident="qa.persisted", version="v1", observation="2025-01-01T00:00Z",
         available="2025-01-01T00:05Z", valid_until=None):
    feature_store.save_value(db, "QA", "M5", ident, version, observation, value,
        available_at=available, observed_at=available, valid_until=valid_until)


def context(db, decisions, **changes):
    return indicator_context.context(db, pd.DataFrame(dict(decision_time=decisions)),
        **{**dict(symbol="QA", timeframe="M5", feature_ids=["qa.persisted"], algorithm_version="v1"), **changes})


def projection(db, **changes):
    return indicator_context.projection(db, **{**dict(symbol="QA", timeframe="M5", feature_ids=["qa.persisted"],
        algorithm_version="v1", as_of="2025-01-01T00:20Z"), **changes})


def test_registered_only_context_is_causal_preserves_order_and_ignores_late_correction_of_older_observation(db):
    register(db)
    save(db, 1.)
    save(db, 2., observation="2025-01-01T00:05Z", available="2025-01-01T00:06Z")
    save(db, 9., available="2025-01-01T00:10Z")
    save(db, 20., observation="2025-01-01T00:05Z", available="2025-01-01T00:12Z")
    decisions=["2025-01-01T00:12Z", "2025-01-01T00:04:59.999999999Z", "2025-01-01T00:05Z",
               "2025-01-01T00:06Z", "2025-01-01T00:10Z", "2025-01-01T00:05Z"]
    before = feature_store.query_history(db)
    frame=context(db, decisions)
    rows=indicator_context.records(frame)
    assert [r["qa.persisted::value"] for r in rows] == [20., None, 1., 2., 2., 1.]
    assert [r["qa.persisted::state"] for r in rows] == ["AVAILABLE", "UNAVAILABLE", *["AVAILABLE"]*4]
    assert all(r["qa.persisted::unit"] == "percent" for r in rows)
    assert all(json.loads(r["qa.persisted::parameters"]) == {"period":5} for r in rows)
    for row in rows:
        if row["qa.persisted::available_at_utc"] is not None:
            assert pd.Timestamp(row["qa.persisted::available_at_utc"]) <= pd.Timestamp(row["decision_time"])
    pd.testing.assert_frame_equal(before, feature_store.query_history(db))
    assert "999999999" in json.dumps(rows, allow_nan=False)


@pytest.mark.parametrize("kind,value,want", [("NUMBER", 0., 0.), ("CATEGORY", "compra", "compra"),
                                           ("BOOLEAN", False, "false")])
def test_context_projection_and_csv_preserve_typed_values_and_zero(db, tmp_path, kind, value, want):
    register(db, kind=kind)
    save(db, value)
    ctx=context(db, ["2025-01-01T00:05Z"])
    projected=projection(db)
    assert ctx.iloc[0]["qa.persisted::value"] == projected.iloc[0]["qa.persisted::value"] == want
    assert projected.iloc[0]["qa.persisted::state"] == "AVAILABLE"
    path=tmp_path/"saída com espaço.csv"
    DataExporter(db).export_indicator_values(path, symbol="QA", timeframe="M5", feature_ids=["qa.persisted"],
        algorithm_version="v1", as_of="2025-01-01T00:20Z")
    csv=pd.read_csv(path, dtype=str, keep_default_na=False)
    assert csv.iloc[0]["qa.persisted::value"] == str(want)
    assert csv.iloc[0]["qa.persisted::algorithm_version"] == "v1"
    assert csv.iloc[0]["qa.persisted::unit"] == "percent"


def test_category_on_existing_registry_id_is_not_dropped_from_old_version_projection(db):
    register(db, ident="price.return_1", kind="CATEGORY", unit="state")
    save(db, "buy", ident="price.return_1")
    projected=projection(db, feature_ids=["price.return_1"])
    assert projected.iloc[0]["price.return_1::value"] == "buy"
    assert projected.iloc[0]["price.return_1::unit"] == "state"


@pytest.mark.parametrize("timeframe", ["M1", "W1", "MN1"])
def test_explicit_persisted_read_does_not_require_local_timeframe_materialization(db, timeframe):
    register(db)
    feature_store.save_value(db,"QA",timeframe,"qa.persisted","v1","2025-01-01T00:00Z",10.,
        available_at="2025-01-01T00:05Z",observed_at="2025-01-01T00:05Z")
    assert context(db,["2025-01-01T00:05Z"],timeframe=timeframe).iloc[0]["qa.persisted::value"] == 10.
    assert projection(db,timeframe=timeframe).iloc[0]["qa.persisted::value"] == 10.


@pytest.mark.parametrize("operation", ["context", "projection"])
def test_stored_only_requires_explicit_version_without_choosing_latest(db, operation):
    register(db)
    register(db, version="v2", unit="fraction")
    with pytest.raises(ValueError, match="algorithm_version explicitamente"):
        if operation == "context":
            context(db, ["2025-01-01T00:05Z"], algorithm_version=None)
        else:
            projection(db, algorithm_version=None)


def test_versions_never_mix_and_old_parameters_are_not_replaced(db):
    register(db)
    register(db, version="v2", unit="fraction")
    save(db, 12.)
    save(db, .12, version="v2")
    assert context(db, ["2025-01-01T00:05Z"]).iloc[0]["qa.persisted::value"] == 12.
    assert projection(db, algorithm_version="v2").iloc[0]["qa.persisted::value"] == .12
    assert projection(db).iloc[0]["qa.persisted::unit"] == "percent"
    with pytest.raises(ValueError, match="Confira a versão"):
        projection(db, algorithm_version="absent")


def test_expired_fact_does_not_become_available_or_get_filled_with_zero(db):
    register(db)
    save(db, 0., valid_until="2025-01-01T00:06Z")
    rows=indicator_context.records(context(db, ["2025-01-01T00:05Z", "2025-01-01T00:06Z"]))
    assert [r["qa.persisted::value"] for r in rows] == [0., None]
    assert rows[1]["qa.persisted::state"] == "UNAVAILABLE"
    assert projection(db).empty


def test_orphaned_current_version_never_borrows_local_contract(db):
    ident="price.return_1"
    version=indicator_materializer.algorithm_version(ident)
    register(db, ident=ident, version=version)
    save(db, 10., ident=ident, version=version)
    with db.engine.begin() as conn:
        conn.execute(text("DELETE FROM _feature_definitions"))
    with pytest.raises(ValueError, match="sem definição registrada"):
        context(db, ["2025-01-01T00:05Z"], feature_ids=[ident], algorithm_version=None)
    with pytest.raises(ValueError, match="sem definição registrada"):
        projection(db, feature_ids=[ident], algorithm_version=None)


@pytest.mark.parametrize("ids", [[], "qa.persisted", [False], ["missing.id"]])
def test_bad_ids_are_actionable_and_duplicate_ids_do_not_duplicate_columns(db, ids):
    register(db)
    with pytest.raises(ValueError, match="catálogo"):
        projection(db, feature_ids=ids)
    save(db, 0.)
    result=projection(db, feature_ids=["qa.persisted", "qa.persisted"])
    assert len(set(result.columns)) == len(result.columns)


def test_cli_context_and_export_equal_direct_reader_and_failed_selection_preserves_destination(db, tmp_path, capsys):
    register(db, kind="CATEGORY", unit="state")
    save(db, "compra")
    config=tmp_path/"qa.ini"
    config.write_text(f"[DATABASE]\ntype=sqlite\npath={db.db_path}\n[APP]\nbroker_utc_offset=0\n", encoding="utf-8")
    common=["--symbol", "QA", "--tf", "M5", "--features", "qa.persisted", "--algorithm-version", "v1",
            "--as-of", "2025-01-01T00:05Z"]
    prefix=["--config",str(config),"--json","analytics","features"]
    assert cli.main([*prefix,"context",*common]) == 0
    assert json.loads(capsys.readouterr().out)["data"]["context"] == indicator_context.records(
        context(db,["2025-01-01T00:05Z"]))
    path=tmp_path/"saída.csv"
    assert cli.main([*prefix,"export",*common,"--out",str(path)]) == 0
    capsys.readouterr()
    assert pd.read_csv(path,keep_default_na=False).iloc[0]["qa.persisted::value"] == "compra"
    before=path.read_bytes()
    with pytest.raises(ValueError,match="Confira a versão"):
        DataExporter(db).export_indicator_values(path,symbol="QA",timeframe="M5",feature_ids=["qa.persisted"],
            algorithm_version="absent",as_of="2025-01-01T00:05Z")
    assert path.read_bytes() == before
