import datetime as dt
from pathlib import Path

import pandas as pd
import numpy as np
import pytest
from sqlalchemy import text

from mt5_extracao import data_quality
from mt5_extracao.database_manager import DatabaseManager


def _bars(times, **overrides):
    n = len(times)
    df = pd.DataFrame({"time": pd.to_datetime(times), "open": [10.0] * n, "high": [11.0] * n,
                       "low": [9.0] * n, "close": [10.5] * n, "tick_volume": [5] * n})
    for col, values in overrides.items():
        df[col] = values
    return df


def test_clean_series_has_no_problems():
    times = pd.date_range("2024-01-02 09:00", periods=10, freq="min")
    r = data_quality.check_quality(_bars(times))
    assert not data_quality.has_problems(r)
    assert r["rows"] == 10 and r["bars_per_day"] == {"days": 1, "min": 10, "max": 10, "mean": 10.0}


def test_invalid_ohlc_zero_volume_duplicates():
    times = ["2024-01-02 09:00", "2024-01-02 09:01", "2024-01-02 09:01", "2024-01-02 09:02"]
    df = _bars(times, high=[11.0, 8.0, 11.0, 11.0], close=[10.5, 10.5, 10.5, 12.0], tick_volume=[5, 5, 0, 5])
    r = data_quality.check_quality(df)
    assert r["invalid_ohlc"]["count"] == 2      # high < low (09:01) e close > high (09:02)
    assert r["zero_volume"]["count"] == 1
    assert r["duplicate_time"]["count"] == 1
    assert data_quality.has_problems(r)


def test_gaps_only_inside_session_same_weekday():
    times = ["2024-01-02 09:00", "2024-01-02 09:01", "2024-01-02 09:05",   # lacuna no pregão
             "2024-01-02 18:40", "2024-01-02 18:50",                         # fora do pregão
             "2024-01-03 09:00",                                             # virada de dia
             "2024-01-06 10:00", "2024-01-06 10:30"]                         # sábado
    r = data_quality.check_quality(_bars(times), bar_minutes=1, session_start="09:00", session_end="18:30")
    assert r["gaps"]["count"] == 1
    assert r["gaps"]["examples"][0].startswith("2024-01-02 09:01:00 -> 2024-01-02 09:05:00")


def test_daily_timeframe_has_no_gap_check():
    r = data_quality.check_quality(_bars(["2024-01-02", "2024-01-05"]), bar_minutes=1440)
    assert r["gaps"]["count"] == 0


def test_empty():
    assert data_quality.check_quality(pd.DataFrame())["rows"] == 0


@pytest.mark.parametrize("column", ["open", "high", "low", "close"])
@pytest.mark.parametrize("bad", [None, np.nan, np.inf, -np.inf, "inválido"])
def test_missing_or_nonfinite_price_never_passes_as_clean(column, bad):
    frame = _bars(pd.date_range("2024-01-02 09:00", periods=3, freq="min"))
    frame[column] = frame[column].astype(object)
    frame.loc[1, column] = bad
    before = frame.copy(deep=True)
    report = data_quality.check_quality(frame)
    assert report["nonfinite_ohlc"]["count"] == 1
    assert data_quality.blocks_price_analysis(report) and data_quality.has_problems(report)
    pd.testing.assert_frame_equal(frame, before)


def test_invalid_times_and_original_order_are_diagnosed_without_discarding_rows():
    frame = _bars(["2024-01-02 09:02", "2024-01-02 09:00", "2024-01-02 09:01"])
    frame.time = frame.time.astype(object)
    frame.loc[2, "time"] = "sem horário"
    report = data_quality.check_quality(frame)
    assert report["rows"] == 3
    assert report["invalid_time"]["count"] == report["unordered_time"]["count"] == 1
    assert data_quality.blocks_price_analysis(report)
    frame.time = None
    report = data_quality.check_quality(frame)
    assert report["invalid_time"]["count"] == 3 and report["bars_per_day"]["days"] == 0


def test_missing_price_column_is_not_a_clean_dataset():
    report = data_quality.check_quality(_bars(["2024-01-02"]).drop(columns="high"))
    assert report["nonfinite_ohlc"]["count"] == 1


def test_nullable_prices_and_volumes_are_not_silently_skipped():
    frame = _bars(pd.date_range("2024-01-02", periods=2, freq="min"))
    frame["high"] = pd.Series([11, pd.NA], dtype="Float64")
    frame["tick_volume"] = pd.Series([5, pd.NA], dtype="Int64")
    report = data_quality.check_quality(frame)
    assert report["nonfinite_ohlc"]["count"] == report["invalid_volume"]["count"] == 1


def test_missing_negative_and_nonfinite_volume_are_distinct_from_zero():
    frame = _bars(pd.date_range("2024-01-02", periods=4, freq="min"), tick_volume=[0, None, -1, np.inf])
    report = data_quality.check_quality(frame)
    assert report["zero_volume"]["count"] == 1
    assert report["invalid_volume"]["count"] == 3
    assert not data_quality.blocks_price_analysis(report)
    assert data_quality.has_problems(report)


def test_quality_json_column_added_to_old_databases(tmp_path):
    path = tmp_path / "old.db"
    db = DatabaseManager(db_path=str(path))
    with db.engine.begin() as c:  # simula base criada antes da coluna existir
        c.execute(text("DROP TABLE _extraction_log"))
        c.execute(text("CREATE TABLE _extraction_log (table_name TEXT, block_start TIMESTAMP, block_end TIMESTAMP,"
                       " rows INTEGER, status TEXT, source TEXT, updated_at TIMESTAMP,"
                       " PRIMARY KEY (table_name, block_start, block_end))"))
    db2 = DatabaseManager(db_path=str(path))
    db2.record_block("t", dt.datetime(2024, 1, 2), dt.datetime(2024, 1, 3), 1, "ok", "mt5", quality={"rows": 1})
    assert db2.quality_reports("t")[0][1] == {"rows": 1}


def test_frozen_v3_quality_still_imports_and_exports_byte_for_byte(tmp_path):
    from mt5_extracao.analytics import datasets
    fixture = Path(__file__).parent / "fixtures/data_quality/dataset_v3_legacy_quality.json"
    db = DatabaseManager(db_path=str(tmp_path / "receiver.sqlite"), broker_utc_offset=0)
    try:
        snapshot = datasets.import_snapshot(db, fixture)
        restored = datasets.load(db, snapshot.manifest_id)
        assert restored.quality["ohlcv_diagnostic"]["zero_volume"]["count"] == 2
        exported = tmp_path / "snapshot.json"
        datasets.export_snapshot(db, restored.manifest_id, exported)
        assert exported.read_bytes() == fixture.read_bytes()
        current = data_quality.check_quality(restored.bars)
        assert current["invalid_volume"]["count"] == current["zero_volume"]["count"] == 1
    finally:
        db.close()
