"""Versões/fatos congelados no store de snapshots existente; sem dependência da base atual."""
import json
import threading

import pandas as pd
import pytest
from sqlalchemy import inspect, text

from mt5_extracao import cli, timeframes
from mt5_extracao.analytics import datasets, feature_store, query_service
from mt5_extracao.database_manager import DatabaseManager


@pytest.fixture
def db(tmp_path):
    manager = DatabaseManager(db_path=str(tmp_path / "source.sqlite"), broker_utc_offset=0)
    manager.save_ohlcv_data("QA", timeframes.parse("M5").label, pd.DataFrame(dict(
        time=pd.date_range("2025-01-01", periods=4, freq="5min"),
        open=100., high=101., low=99., close=100., tick_volume=10., real_volume=0., spread=1.)))
    feature_store.register_definition(manager,"qa.metric","Medida QA","v1",{"period":5},[],
        unit="percent",value_type="NUMBER")
    save(manager, 1.)
    yield manager
    manager.close()


def save(db, value, *, ident="qa.metric", version="v1", observation="2025-01-01T00:00Z",
         available="2025-01-01T00:05Z"):
    feature_store.save_value(db,"QA","M5",ident,version,observation,value,
        available_at=available,observed_at=available)


def freeze(db, **changes):
    return datasets.capture(db,"QA","M5",**{**dict(stored_features=[("qa.metric","v1")]),**changes})


def test_optional_capture_preserves_default_format_and_rejects_live_fallback(db):
    legacy = datasets.capture(db,"QA","M5")
    assert legacy.manifest["schema_version"] == datasets.VERSION == 3
    with pytest.raises(ValueError,match="sem fatos de indicadores"):
        _ = legacy.feature_history
    with pytest.raises(ValueError,match="sem contratos"):
        _ = legacy.feature_definitions
    first=freeze(db)
    assert first.manifest["schema_version"] == 4
    assert first.manifest_id == freeze(db,stored_features=[("qa.metric","v1"),("qa.metric","v1")]).manifest_id
    assert first.feature_definitions[0]["parameters"] == {"period":5}
    assert first.feature_history.numeric_value.tolist() == [1.]


def test_old_manifest_is_immutable_after_revision_and_returned_frames_are_copies(db):
    first=freeze(db)
    before=first.feature_history
    save(db,9.,available="2025-01-01T00:12Z")
    second=freeze(db)
    assert second.manifest_id != first.manifest_id
    assert second.feature_history.numeric_value.tolist() == [1.,9.]
    pd.testing.assert_frame_equal(datasets.load(db,first.manifest_id).feature_history,before)
    changed=first.feature_history
    changed.loc[:,"numeric_value"]=999.
    definition=first.feature_definitions
    definition[0]["parameters"]["period"]=999
    assert first.feature_history.numeric_value.tolist() == [1.]
    assert first.feature_definitions[0]["parameters"] == {"period":5}


def test_full_revisions_remain_available_for_causal_decisions_after_export_and_import(db,tmp_path):
    save(db,9.,available="2025-01-01T00:12Z")
    snapshot=freeze(db)
    path=tmp_path/"snapshot.json"
    datasets.export_snapshot(db,snapshot.manifest_id,path)
    other=DatabaseManager(db_path=str(tmp_path/"other.sqlite"),broker_utc_offset=0)
    try:
        imported=datasets.import_snapshot(other,path)
        assert imported.manifest_id == snapshot.manifest_id
        assert imported.manifest["source"] == snapshot.manifest["source"]
        assert imported.feature_definitions == snapshot.feature_definitions
        history=imported.feature_history
    finally:
        other.close()
    rows=feature_store.asof_join(pd.DataFrame(dict(decision_time=["2025-01-01T00:05Z","2025-01-01T00:12Z"])),history)
    assert rows.numeric_value.tolist() == [1.,9.]
    again=tmp_path/"again.json"
    datasets.export_snapshot(db,snapshot.manifest_id,again)
    assert path.read_bytes() == again.read_bytes()


@pytest.mark.parametrize("kind,value,category",[("CATEGORY","compra","compra"),("BOOLEAN",False,"false")])
def test_types_and_distinct_versions_are_frozen_without_conversion(db,kind,value,category):
    feature_store.register_definition(db,"qa.metric","Estado QA","v2",{},[],unit="state",value_type=kind)
    save(db,value,version="v2")
    snapshot=freeze(db,stored_features=[("qa.metric","v2"),("qa.metric","v1")])
    assert [(d["algorithm_version"],d["unit"]) for d in snapshot.feature_definitions] == [("v1","percent"),("v2","state")]
    rows=snapshot.feature_history
    assert rows.loc[rows.algorithm_version=="v2","categorical_value"].iloc[0] == category
    assert rows.loc[rows.algorithm_version=="v1","numeric_value"].iloc[0] == 1.


def test_window_is_by_observation_and_keeps_future_availability_as_a_fact_not_a_current_value(db):
    save(db,99.,available="2025-01-02T00:00Z")
    save(db,88.,observation="2024-12-31T23:55Z",available="2025-01-01T00:05Z")
    snapshot=freeze(db,start="2025-01-01T00:00",end="2025-01-01T00:05")
    assert snapshot.feature_history.numeric_value.tolist() == [1.,99.]
    rows=feature_store.asof_join(pd.DataFrame(dict(decision_time=["2025-01-01T00:05Z"])),snapshot.feature_history)
    assert rows.numeric_value.tolist() == [1.]


def test_missing_facts_stay_empty_and_legacy_name_stays_unverified(db):
    with db.engine.begin() as conn:
        conn.execute(text("DELETE FROM _feature_values"))
        conn.execute(text("UPDATE _feature_definitions SET name='value_type',definition_schema_version=1"))
    snapshot=freeze(db)
    assert snapshot.feature_history.empty
    assert snapshot.feature_definitions[0]["name_status"] == "UNVERIFIED_LEGACY_NAME"
    assert feature_store.list_definitions(db)[0]["name"] == "value_type"


def test_frozen_zero_and_expiry_are_preserved_after_serialization(db,tmp_path):
    feature_store.save_value(db,"QA","M5","qa.metric","v1","2025-01-01T00:00Z",0.,
        available_at="2025-01-01T00:05Z",observed_at="2025-01-01T00:05Z",valid_until="2025-01-01T00:06Z")
    snapshot=freeze(db)
    path=tmp_path/"expiry.json";datasets.export_snapshot(db,snapshot.manifest_id,path)
    restored=datasets.import_snapshot(db,path)
    rows=feature_store.asof_join(pd.DataFrame(dict(decision_time=["2025-01-01T00:05Z","2025-01-01T00:06Z"])),
        restored.feature_history)
    assert rows.numeric_value.iloc[0] == 0. and pd.isna(rows.numeric_value.iloc[1])


@pytest.mark.parametrize("selection",[None,"qa.metric",["qa.metric"],[("qa.metric","")],[(False,"v1")],[("missing","v1")]])
def test_bad_selection_never_publishes_a_manifest(db,selection):
    with pytest.raises(ValueError):
        freeze(db,stored_features=selection)
    if inspect(db.engine).has_table("_dataset_manifests"):
        with db.engine.connect() as conn:
            assert conn.execute(text("SELECT COUNT(*) FROM _dataset_manifests")).scalar() == 0


def test_unknown_clock_is_rejected_without_stamping_snapshot_tables(db):
    db.offset_declared=False
    with pytest.raises(ValueError,match="offset presumido"):
        freeze(db)
    assert not inspect(db.engine).has_table("_dataset_manifests")


def test_budget_is_enforced_before_any_partial_manifest_is_published(db,monkeypatch):
    save(db,2.,observation="2025-01-01T00:05Z",available="2025-01-01T00:10Z")
    prior=datasets.capture(db,"QA","M5")
    monkeypatch.setattr(query_service,"MAX_SELECTION_ROWS",1)
    with pytest.raises(ValueError,match="excede 1"):
        freeze(db)
    with db.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM _dataset_manifests")).scalar() == 1
    assert datasets.load(db,prior.manifest_id).manifest_id == prior.manifest_id
    with pytest.raises(ValueError,match="excede 0"):
        feature_store.query_history(db,max_rows=0)


def test_bars_contracts_and_feature_revisions_use_one_database_snapshot(db,monkeypatch):
    with db.engine.connect() as conn:
        conn.exec_driver_sql("PRAGMA journal_mode=WAL");conn.commit()
    table=db.find_table_for_symbol("QA",timeframes.parse("M5").label)
    original=db.get_symbol_spec
    errors=[]
    def writer():
        try:
            feature_store.save_values(db,[dict(symbol="QA",timeframe="M5",feature_id="qa.metric",algorithm_version="v1",
                observation_time="2025-01-01T00:00Z",value=9.,available_at="2025-01-01T00:12Z",observed_at="2025-01-01T00:12Z")],
                transaction_hook=lambda conn,counts:conn.execute(text(f'UPDATE "{table}" SET close=100.5')))
        except Exception as exc:
            errors.append(exc)
    def read_spec(symbol,*,connection=None):
        thread=threading.Thread(target=writer,daemon=True);thread.start();thread.join(10)
        assert not thread.is_alive()
        return original(symbol,connection=connection)
    monkeypatch.setattr(db,"get_symbol_spec",read_spec)
    snapshot=freeze(db)
    assert not errors
    assert snapshot.bars.close.tolist() == [100.]*4
    assert snapshot.feature_history.numeric_value.tolist() == [1.]
    assert feature_store.query_history(db).numeric_value.tolist() == [1.,9.]


@pytest.mark.parametrize("corruption",["symbol","revision","clock","selection","contract"])
def test_rehashed_but_inconsistent_package_is_rejected(db,tmp_path,corruption):
    snapshot=freeze(db)
    bundle=datasets._bundle(db,snapshot.manifest_id)
    if corruption=="selection":
        bundle["manifest"]["selection"]["stored_features"]=[["another","v1"]]
    else:
        role="feature_definitions" if corruption=="contract" else "feature_history"
        digest=bundle["manifest"]["frames"][role][0]
        frame=datasets._decode_frame(json.loads(bundle["blocks"].pop(digest)))
        if corruption=="contract":
            entry=json.loads(frame.loc[0,"definition_json"]);entry["unit"]="invented"
            frame.loc[0,"definition_json"]=datasets._json(entry)
        elif corruption=="clock":
            frame["available_at_utc"]=frame.available_at_utc.dt.tz_localize(None)
        else:
            frame.loc[0,corruption]="OTHER" if corruption=="symbol" else 0
        payload=datasets._json(datasets._frame_payload(frame));new=datasets._hash(payload)
        bundle["blocks"][new]=payload;bundle["manifest"]["frames"][role]=[new]
    bundle["manifest_id"]=datasets._hash(datasets._json(bundle["manifest"]))
    path=tmp_path/"bad.json";path.write_text(datasets._json(bundle),encoding="utf-8")
    with pytest.raises(ValueError,match="sem integridade"):
        datasets.import_snapshot(db,path)


def test_official_cli_freeze_records_explicit_feature_versions(db,tmp_path,capsys):
    config=tmp_path/"qa.ini"
    config.write_text(f"[DATABASE]\ntype=sqlite\npath={db.db_path}\n[APP]\nbroker_utc_offset=0\n",encoding="utf-8")
    assert cli.main(["--config",str(config),"--json","laboratory","freeze","--symbol","QA","--tf","M5",
        "--stored-feature","qa.metric","v1"]) == 0
    manifest=json.loads(capsys.readouterr().out)["data"]["manifests"][0]
    assert manifest["stored_features"] == [["qa.metric","v1"]]
    assert datasets.load(db,manifest["id"]).feature_history.numeric_value.tolist() == [1.]
