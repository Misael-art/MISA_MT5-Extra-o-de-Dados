"""BD03: identidade de base no contrato existente (namespace por banco, T20.4).

Prova o que o mecanismo atual garante com duas bases de mesmo símbolo, sem schema novo.
As lacunas de propagação do namespace aos consumidores ficam em docs/dados/BD03_IDENTIDADE.md.
"""
import shutil

import pandas as pd

from mt5_extracao.analytics import datasets, feature_store
from mt5_extracao.database_manager import DatabaseManager
from tests.market_data import sine, trend

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


def test_file_copy_shares_namespace_measured_risk(tmp_path):
    """Cópia de arquivo herda o namespace: duas cópias divergentes não se distinguem por ele."""
    a, _ = _bases(tmp_path)
    original = datasets.capture(a, SYMBOL, "H1").manifest["source"]["namespace"]
    a.engine.dispose()
    shutil.copy(a.db_path, tmp_path / "copy.sqlite")
    copy = DatabaseManager(db_path=str(tmp_path / "copy.sqlite"))
    assert datasets.capture(copy, SYMBOL, "H1").manifest["source"]["namespace"] == original
