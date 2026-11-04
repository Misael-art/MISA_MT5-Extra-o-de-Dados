"""BD04/R04: o extrator real grava origem e recebimento pelo contrato da base (provedor falso)."""
import datetime as dt

import pandas as pd
from sqlalchemy import text

from mt5_extracao.analytics import datasets
from tests.test_csv_external_source import CsvExternalSource, _minutes, write_mt5_export
from tests.test_extraction_resume import _epoch, _extractor, _run, no_sleep  # noqa: F401  (fixture)


def _receipts(db):
    with db.engine.connect() as conn:
        return pd.read_sql(text("SELECT time, received_at_utc, origin FROM _ohlcv_receipts ORDER BY time"), conn)


def test_extraction_declares_source_and_records_receipts(connector, fake_mt5, tmp_path):
    ext, db = _extractor(connector, tmp_path / "t.db")
    before = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%S")
    assert _run(ext)
    assert (datasets.identity(db)["provider"], datasets.identity(db)["server"]) == ("mt5", "Fake-Demo")
    receipts = _receipts(db)
    assert len(receipts) == len(db.load_ohlcv("WIN$N", "1 minuto")) > 0
    assert set(receipts["origin"]) == {"write:mt5@Fake-Demo"}
    assert (receipts["received_at_utc"] >= before).all()


def test_incompatible_base_refuses_extraction_without_writing(connector, fake_mt5, tmp_path):
    ext, db = _extractor(connector, tmp_path / "t.db")
    datasets.declare_source(db, {"provider": "outro-feed"})
    assert _run(ext) is False
    assert db.load_ohlcv("WIN$N", "1 minuto").empty
    with db.engine.connect() as conn:
        has_receipts = conn.execute(text("SELECT name FROM sqlite_master WHERE name='_ohlcv_receipts'")).first()
        statuses = [row[0] for row in conn.execute(text("SELECT status FROM _extraction_log"))]
    assert has_receipts is None and set(statuses) == {"failed"}


def test_reopen_same_source_updates_and_resume_keeps_original_receipts(connector, fake_mt5, tmp_path):
    fake_mt5.fail_ranges = [(_epoch(dt.datetime(2024, 1, 3, 9)), _epoch(dt.datetime(2024, 1, 3, 18)))]
    ext, db = _extractor(connector, tmp_path / "t.db")
    assert _run(ext) is False
    first = _receipts(db)
    fake_mt5.fail_ranges = []
    assert _run(ext) is True  # retomada: só o bloco que faltou
    after = _receipts(db)
    kept = after[after.time.isin(first.time)]
    pd.testing.assert_frame_equal(kept.reset_index(drop=True), first.reset_index(drop=True))  # sem nova antiguidade
    assert len(after) > len(first)
    ext2, _ = _extractor(connector, tmp_path / "t.db")
    assert ext2._process_symbol("WIN$N", 1, "1 minuto", dt.datetime(2024, 1, 2), dt.datetime(2024, 1, 5),
                                include_indicators=False, overwrite=True)  # mesma fonte reabre e atualiza
    assert len(_receipts(db)) == 2 * len(after)  # nova observação = novo recebimento, mesmo conteúdo


def test_label_given_after_writing_does_not_qualify_old_receipts(connector, fake_mt5, tmp_path):
    ext, db = _extractor(connector, tmp_path / "t.db")
    bars = db.load_ohlcv("WIN$N", "1 minuto")
    frame = pd.DataFrame({"time": pd.date_range("2023-12-01", periods=3, freq="min"), "open": 1.0, "high": 1.0,
                          "low": 1.0, "close": 1.0, "tick_volume": 1, "spread": 1, "real_volume": 0})
    assert bars.empty and db.save_ohlcv_data("WIN$N", "1 minuto", frame)
    datasets.declare_source(db, {"provider": "mt5", "server": "Fake-Demo"})
    assert set(_receipts(db)["origin"]) == {"write:unknown"}


def test_configured_csv_fallback_keeps_base_source_and_records_real_origin(connector, fake_mt5, tmp_path):
    fake_mt5.fail_ranges = [(_epoch(dt.datetime(2024, 1, 3, 9)), _epoch(dt.datetime(2024, 1, 3, 18)))]
    csv_dir = tmp_path / "csv"
    csv_dir.mkdir()
    write_mt5_export(csv_dir / "WIN$N.csv", _minutes("2024-01-03", 30))
    ext, db = _extractor(connector, tmp_path / "t.db")
    ext.external_source = CsvExternalSource(str(csv_dir))
    assert _run(ext)
    origins = _receipts(db)["origin"].value_counts().to_dict()
    assert origins["fallback:CsvExternalSource"] == 30 and "write:mt5@Fake-Demo" in origins
    assert datasets.identity(db)["provider"] == "mt5"
