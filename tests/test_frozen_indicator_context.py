"""Contexto por decisão não consulta fatos ou contratos que chegaram depois do snapshot."""
import json

import pandas as pd
import pytest

from mt5_extracao import cli
from mt5_extracao.analytics import datasets, feature_store, indicator_context, indicator_registry, query_service
from mt5_extracao.database_manager import DatabaseManager
from tests.test_feature_snapshots import db, freeze, save  # noqa: F401


def decisions(*times):
    return pd.DataFrame(dict(decision_time=times))


def test_frozen_context_preserves_old_revision_and_does_not_read_live_source_or_registry(db,monkeypatch):
    old=freeze(db)
    save(db,9.,available="2025-01-01T00:12Z")
    new=freeze(db)
    def forbidden(*args,**kwargs):
        raise AssertionError("fonte atual consultada")
    monkeypatch.setattr(feature_store,"query_history",forbidden)
    monkeypatch.setattr(query_service,"definition_catalog",forbidden)
    monkeypatch.setattr(indicator_registry,"get_definition",forbidden)
    when=decisions("2025-01-01T00:12Z","2025-01-01T00:04:59.999999999Z","2025-01-01T00:05Z")
    before=when.copy(deep=True)
    first=indicator_context.from_snapshot(old,when)
    second=indicator_context.from_snapshot(new,when)
    assert [r["qa.metric::value"] for r in indicator_context.records(first)] == [1.,None,1.]
    assert [r["qa.metric::value"] for r in indicator_context.records(second)] == [9.,None,1.]
    assert set(first["snapshot_manifest_id"]) == {old.manifest_id}
    assert set(first["qa.metric::algorithm_version"]) == {"v1"}
    assert set(first["qa.metric::unit"]) == {"percent"}
    assert json.loads(first.iloc[0]["qa.metric::parameters"]) == {"period":5}
    pd.testing.assert_frame_equal(when,before)


def test_imported_context_uses_original_namespace_and_clock_without_destination_database_reads(db,tmp_path,monkeypatch):
    datasets.declare_source(db,{"provider":"qa","server":"Original"})
    snapshot=freeze(db)
    path=tmp_path/"snapshot.json";datasets.export_snapshot(db,snapshot.manifest_id,path)
    other=DatabaseManager(db_path=str(tmp_path/"other.sqlite"),broker_utc_offset=3)
    try:
        datasets.declare_source(other,{"provider":"other","server":"Destination"})
        imported=datasets.import_snapshot(other,path)
        destination=datasets.identity(other)["namespace"]
        def forbidden(*args,**kwargs):
            raise AssertionError("banco consultado")
        monkeypatch.setattr(other.engine,"connect",forbidden)
        frame=indicator_context.from_snapshot(imported,decisions("2024-12-31T21:05:00-03:00"))
        assert frame.iloc[0]["qa.metric::value"] == 1.
        assert frame.iloc[0]["base_namespace"] == snapshot.manifest["source"]["namespace"] != destination
        assert frame.iloc[0]["base_provider"] == "qa" and frame.iloc[0]["base_server"] == "Original"
        assert frame.iloc[0]["decision_time"] == pd.Timestamp("2025-01-01T00:05Z")
    finally:
        other.close()


def test_ambiguous_version_requires_choice_and_never_selects_latest(db):
    feature_store.register_definition(db,"qa.metric","Outra versão","v2",{},[],unit="fraction",value_type="NUMBER")
    save(db,.01,version="v2")
    snapshot=freeze(db,stored_features=[("qa.metric","v1"),("qa.metric","v2")])
    when=decisions("2025-01-01T00:05Z")
    with pytest.raises(ValueError,match="Mais de uma versão"):
        indicator_context.from_snapshot(snapshot,when)
    first=indicator_context.from_snapshot(snapshot,when,algorithm_version="v1")
    second=indicator_context.from_snapshot(snapshot,when,algorithm_version="v2")
    assert first.iloc[0]["qa.metric::value"] == 1. and second.iloc[0]["qa.metric::value"] == .01
    assert first.iloc[0]["qa.metric::unit"] == "percent" and second.iloc[0]["qa.metric::unit"] == "fraction"


@pytest.mark.parametrize("ident,version",[("price.return_1",None),("qa.metric","later")])
def test_missing_frozen_contract_is_not_completed_from_current_catalog(db,ident,version):
    snapshot=freeze(db)
    with pytest.raises(ValueError,match="não congelado"):
        indicator_context.from_snapshot(snapshot,decisions("2025-01-01T00:05Z"),feature_ids=[ident],algorithm_version=version)
    legacy=datasets.capture(db,"QA","M5")
    with pytest.raises(ValueError,match="sem contratos"):
        indicator_context.from_snapshot(legacy,decisions("2025-01-01T00:05Z"))


@pytest.mark.parametrize("kind,value,want",[("NUMBER",0.,0.),("CATEGORY","compra","compra"),("BOOLEAN",False,"false")])
def test_typed_values_and_expiry_are_used_per_decision_without_numeric_encoding(db,kind,value,want):
    feature_store.register_definition(db,"qa.typed","Tipo QA","v1",{},[],unit="code",value_type=kind)
    feature_store.save_value(db,"QA","M5","qa.typed","v1","2025-01-01T00:00Z",value,
        available_at="2025-01-01T00:05Z",observed_at="2025-01-01T00:05Z",valid_until="2025-01-01T00:06Z")
    snapshot=freeze(db,stored_features=[("qa.typed","v1")])
    frame=indicator_context.from_snapshot(snapshot,decisions("2025-01-01T00:05Z","2025-01-01T00:06Z"))
    assert [r["qa.typed::value"] for r in indicator_context.records(frame)] == [want,None]
    assert list(frame["qa.typed::state"]) == ["AVAILABLE","UNAVAILABLE"]
    json.dumps(indicator_context.records(frame),allow_nan=False)


@pytest.mark.parametrize("when",["2024-12-31T23:59:59.999999999Z","2025-01-01T00:20:00.000000001Z"])
def test_decisions_outside_snapshot_coverage_are_rejected(db,when):
    snapshot=freeze(db)
    assert snapshot.feature_window_utc == (pd.Timestamp("2025-01-01T00:00Z"),pd.Timestamp("2025-01-01T00:20Z"))
    with pytest.raises(ValueError,match="fora da janela congelada"):
        indicator_context.from_snapshot(snapshot,decisions(when))


@pytest.mark.parametrize("column,value",[("symbol","OTHER"),("tf","H1"),("base_namespace","other"),
    ("base_provider","other"),("base_server","other"),("snapshot_manifest_id","other")])
def test_conflicting_decision_scope_is_rejected(db,column,value):
    snapshot=freeze(db);when=decisions("2025-01-01T00:05Z");when[column]=value
    with pytest.raises(ValueError,match="outra origem/seleção"):
        indicator_context.from_snapshot(snapshot,when)


@pytest.mark.parametrize("tf,broker,start,end",[
    ("M1","2025-01-01T00:00","2024-12-31T21:00Z","2024-12-31T21:01Z"),
    ("W1","2025-01-06T00:00","2025-01-05T21:00Z","2025-01-12T21:00Z"),
    ("MN1","2025-01-01T00:00","2024-12-31T21:00Z","2025-01-31T21:00Z")])
def test_native_timeframe_bounds_use_captured_fixed_offset_and_actual_month_end(tmp_path,tf,broker,start,end):
    from mt5_extracao import timeframes
    source=DatabaseManager(db_path=str(tmp_path/"clock.sqlite"),broker_utc_offset=3)
    try:
        source.save_ohlcv_data("QA",timeframes.parse(tf).label,pd.DataFrame(dict(time=[pd.Timestamp(broker)],
            open=[100.],high=[101.],low=[99.],close=[100.],tick_volume=[10.],real_volume=[0.],spread=[1.])))
        feature_store.register_definition(source,"qa.clock","Relógio QA","v1",{},[],unit="percent",value_type="NUMBER")
        feature_store.save_value(source,"QA",tf,"qa.clock","v1",start,2.,available_at=end,observed_at=end)
        snapshot=datasets.capture(source,"QA",tf,stored_features=[("qa.clock","v1")])
        assert snapshot.feature_window_utc == (pd.Timestamp(start),pd.Timestamp(end))
        frame=indicator_context.from_snapshot(snapshot,decisions(start,end))
        assert [r["qa.clock::value"] for r in indicator_context.records(frame)] == [None,2.]
    finally:
        source.close()


def test_limit_and_invalid_decisions_fail_before_join_and_empty_query_preserves_columns(db,monkeypatch):
    snapshot=freeze(db)
    empty=indicator_context.from_snapshot(snapshot,decisions())
    assert empty.empty and "qa.metric::value" in empty and "snapshot_manifest_id" in empty
    monkeypatch.setattr(query_service,"MAX_SELECTION_ROWS",1)
    def forbidden(*args,**kwargs):
        raise AssertionError("junção executada")
    monkeypatch.setattr(feature_store,"asof_join",forbidden)
    with pytest.raises(ValueError,match="excede 1 pares"):
        indicator_context.from_snapshot(snapshot,decisions("2025-01-01T00:05Z","2025-01-01T00:06Z"))
    with pytest.raises(ValueError,match="tabela de decisões"):
        indicator_context.from_snapshot(snapshot,[])
    with pytest.raises(ValueError,match="decision_time"):
        indicator_context.from_snapshot(snapshot,pd.DataFrame(dict(other=[1])))


def test_cli_manifest_context_equals_frozen_reader_and_leaves_live_mode_available(db,tmp_path,capsys):
    snapshot=freeze(db);save(db,9.,available="2025-01-01T00:12Z")
    config=tmp_path/"qa.ini"
    config.write_text(f"[DATABASE]\ntype=sqlite\npath={db.db_path}\n[APP]\nbroker_utc_offset=0\n",encoding="utf-8")
    prefix=["--config",str(config),"--json","analytics","features","context","--symbol","QA","--tf","M5",
            "--features","qa.metric","--algorithm-version","v1","--as-of","2025-01-01T00:12Z"]
    assert cli.main([*prefix,"--manifest",snapshot.manifest_id]) == 0
    frozen=json.loads(capsys.readouterr().out)["data"]["context"]
    assert frozen == indicator_context.records(indicator_context.from_snapshot(snapshot,decisions("2025-01-01T00:12Z")))
    assert cli.main(["--config",str(config),"--json","analytics","features","context","--symbol","QA","--tf","M5",
        "--as-of","2025-01-01T00:12Z","--manifest",snapshot.manifest_id]) == 0
    assert json.loads(capsys.readouterr().out)["data"]["context"] == frozen
    assert cli.main(prefix) == 0
    assert json.loads(capsys.readouterr().out)["data"]["context"][0]["qa.metric::value"] == 9.
    assert cli.main([*prefix,"--manifest",snapshot.manifest_id,"--symbol","OTHER"]) == 2
    assert "divergem do snapshot" in json.loads(capsys.readouterr().out)["errors"][0]
