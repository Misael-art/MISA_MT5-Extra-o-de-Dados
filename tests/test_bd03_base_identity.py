"""BD03: identidade de base no contrato existente (namespace por banco, T20.4).

Prova o que o mecanismo atual garante com duas bases de mesmo símbolo, sem schema novo.
As lacunas de propagação do namespace aos consumidores ficam em docs/dados/BD03_IDENTIDADE.md.
"""
import shutil
from datetime import datetime, timezone

import pandas as pd
import pytest

from mt5_extracao.analytics import datasets, feature_store, relation_dto
from mt5_extracao.chart_study import store as notes
from mt5_extracao.database_manager import DatabaseManager
from tests.market_data import sine, trend
from tests.test_chart_study import capture as _note_capture

SYMBOL = "EURUSD"


def _base(path, bars):
    db = DatabaseManager(db_path=str(path))
    db.save_ohlcv_data(SYMBOL, "1 hora", bars)
    return db


def _bases(tmp_path):
    a = _base(tmp_path / "base_a.sqlite", trend(120, freq="h", start="2026-01-05"))
    b = _base(tmp_path / "base_b.sqlite", sine(120, freq="h", start="2026-01-05"))
    return a, b


def _value(db, value):
    feature_store.register_definition(db, "bd03.probe", "probe", "1", {}, [], value_type="NUMBER")
    feature_store.save_value(db, SYMBOL, "H1", "bd03.probe", "1", pd.Timestamp("2026-01-05T10:00Z"), value,
                             observed_at=pd.Timestamp("2026-01-05T11:00Z"), available_at=pd.Timestamp("2026-01-05T11:00Z"))


def test_same_symbol_in_two_bases_has_distinct_namespace_data_and_values(tmp_path):
    a, b = _bases(tmp_path)
    snap_a, snap_b = datasets.capture(a, SYMBOL, "H1"), datasets.capture(b, SYMBOL, "H1")
    ns_a, ns_b = snap_a.manifest["source"]["namespace"], snap_b.manifest["source"]["namespace"]
    assert ns_a != ns_b
    assert snap_a.manifest_id != snap_b.manifest_id
    assert not snap_a.bars["close"].equals(snap_b.bars["close"])
    _value(a, 1.0)
    _value(b, 2.0)
    assert feature_store.query_history(a, symbol=SYMBOL, feature_id="bd03.probe")["numeric_value"].tolist() == [1.0]
    assert feature_store.query_history(b, symbol=SYMBOL, feature_id="bd03.probe")["numeric_value"].tolist() == [2.0]


def test_namespace_is_stable_across_reopen(tmp_path):
    a, _ = _bases(tmp_path)
    first = datasets.capture(a, SYMBOL, "H1").manifest["source"]["namespace"]
    reopened = DatabaseManager(db_path=a.db_path)
    assert datasets.capture(reopened, SYMBOL, "H1").manifest["source"]["namespace"] == first


def test_imported_snapshot_keeps_its_origin_and_does_not_mix_with_local_base(tmp_path):
    a, b = _bases(tmp_path)
    local = datasets.capture(a, SYMBOL, "H1")
    foreign = datasets.capture(b, SYMBOL, "H1", source={"provider": "feed-b"})
    package = datasets.export_snapshot(b, foreign.manifest_id, tmp_path / "b.json")
    imported = datasets.import_snapshot(a, package)
    assert imported.manifest["source"]["namespace"] == foreign.manifest["source"]["namespace"]
    assert imported.manifest["source"]["provider"] == "feed-b"
    assert datasets.load(a, imported.manifest_id).bars["close"].equals(foreign.bars["close"])
    assert datasets.capture(a, SYMBOL, "H1").manifest_id == local.manifest_id  # base local intacta


def _history(db):
    return feature_store.with_base_identity(db, feature_store.query_history(db, symbol=SYMBOL, feature_id="bd03.probe"))


def test_backup_and_move_keep_identity_and_fork_derives_a_new_one(tmp_path):
    a, _ = _bases(tmp_path)
    original = datasets.identity(a)
    a.engine.dispose()
    moved = tmp_path / "moved" / "base.sqlite"
    moved.parent.mkdir()
    shutil.move(a.db_path, moved)
    backup = tmp_path / "backup.sqlite"
    shutil.copy(moved, backup)
    assert datasets.identity(DatabaseManager(db_path=str(moved)))["namespace"] == original["namespace"]
    restored = DatabaseManager(db_path=str(backup))
    assert datasets.identity(restored)["namespace"] == original["namespace"]
    assert datasets.identity(restored)["namespace"] == original["namespace"]  # reabrir não gera UUID
    derived = datasets.fork(restored)
    assert derived["namespace"] != original["namespace"]
    assert (derived["parent_namespace"], derived["lineage"]) == (original["namespace"], "fork")
    assert datasets.identity(DatabaseManager(db_path=str(backup)))["namespace"] == derived["namespace"]


def test_divergent_copy_is_distinguished_by_fork_or_by_content_revision(tmp_path):
    a, _ = _bases(tmp_path)
    before = datasets.capture(a, SYMBOL, "H1")
    a.engine.dispose()
    shutil.copy(a.db_path, tmp_path / "copy.sqlite")
    copy = DatabaseManager(db_path=str(tmp_path / "copy.sqlite"))
    changed = trend(120, freq="h", start="2026-01-05")
    changed.loc[5, "close"] += 1.0
    copy.save_ohlcv_data(SYMBOL, "1 hora", changed)
    unforked = datasets.capture(copy, SYMBOL, "H1")
    assert unforked.manifest["source"]["namespace"] == before.manifest["source"]["namespace"]
    assert unforked.manifest_id != before.manifest_id  # mesmo namespace, conteúdo distinto: chave de cache distinta
    datasets.fork(copy)
    forked = datasets.capture(copy, SYMBOL, "H1")
    assert forked.manifest["source"]["namespace"] != before.manifest["source"]["namespace"]


@pytest.mark.parametrize("same_data", [False, True])
def test_two_bases_queried_together_keep_origin_values_versions_and_cache_keys(tmp_path, same_data):
    a = _base(tmp_path / "a.sqlite", trend(120, freq="h", start="2026-01-05"))
    b = _base(tmp_path / "b.sqlite", (trend if same_data else sine)(120, freq="h", start="2026-01-05"))
    _value(a, 1.0)
    _value(b, 1.0 if same_data else 2.0)
    joined = pd.concat([_history(a), _history(b)], ignore_index=True)
    assert joined["base_namespace"].nunique() == 2
    assert joined.groupby("base_namespace")["algorithm_version"].nunique().tolist() == [1, 1]
    assert len(joined.drop_duplicates(["base_namespace", "observation_time", "feature_id", "revision"])) == 2
    snap_a, snap_b = datasets.capture(a, SYMBOL, "H1"), datasets.capture(b, SYMBOL, "H1")
    assert snap_a.manifest_id != snap_b.manifest_id  # namespace entra no hash: dados iguais, chaves distintas
    if same_data:
        assert snap_a.bars["close"].equals(snap_b.bars["close"])


def test_incompatible_feed_is_refused_without_writing(tmp_path):
    a, _ = _bases(tmp_path)
    datasets.capture(a, SYMBOL, "H1", source={"provider": "feed-a", "server": "srv-a"})
    with a.engine.connect() as conn:
        before = conn.execute(datasets.text("SELECT COUNT(*) FROM _dataset_manifests")).scalar_one()
    for wrong in ({"provider": "feed-b"}, {"provider": "feed-a", "server": "srv-b"}):
        with pytest.raises(ValueError, match="recusada"):
            datasets.capture(a, SYMBOL, "H1", source=wrong)
    with a.engine.connect() as conn:
        assert conn.execute(datasets.text("SELECT COUNT(*) FROM _dataset_manifests")).scalar_one() == before
    implicit = datasets.capture(a, SYMBOL, "H1")  # sem rótulo: herda a fonte declarada, não inventa outra
    assert (implicit.manifest["source"]["provider"], implicit.manifest["source"]["server"]) == ("feed-a", "srv-a")


def test_third_adapter_uses_the_same_contract_without_consumer_changes(tmp_path):
    """E04: uma terceira fonte entra por declare_source/capture e chega carimbada aos consumidores."""
    third = _base(tmp_path / "csv.sqlite", sine(120, freq="h", start="2026-02-02"))
    assert datasets.declare_source(third, {"provider": "csv-import"}) == {"provider": "csv-import", "server": None}
    snap = datasets.capture(third, SYMBOL, "H1")
    assert snap.manifest["source"]["provider"] == "csv-import"
    _value(third, 3.0)
    assert _history(third)["base_provider"].tolist() == ["csv-import"]


def test_notes_freeze_their_base_and_old_notes_keep_their_bytes(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "notes.sqlite"))
    capture = _note_capture.__wrapped__()
    first = notes.freeze(db, "note_1", capture, datetime.now(timezone.utc))
    assert first["base"]["namespace"] == datasets.identity(db)["namespace"]
    datasets.fork(db)
    assert notes.freeze(db, "note_1", capture, datetime.now(timezone.utc)) == first  # fotografia congelada
    second = notes.freeze(db, "note_2", capture, datetime.now(timezone.utc))
    assert second["base"]["namespace"] != first["base"]["namespace"]


def test_relation_dto_requires_base_identity_outside_fixtures():
    fixture = relation_dto.synthetic_fixture()
    assert fixture["provenance"]["base_namespace"] is None
    real = {key: fixture[key] for key in ("indicator", "target", "relation", "scope", "provenance")}
    with pytest.raises(ValueError, match="base_namespace"):
        relation_dto.build_relation_dto(**real, generated_at_utc=fixture["generated_at_utc"])
    real["provenance"] = {**real["provenance"], "base_namespace": "abc"}
    assert relation_dto.build_relation_dto(**real, generated_at_utc=fixture["generated_at_utc"])["provenance"][
        "base_namespace"] == "abc"
