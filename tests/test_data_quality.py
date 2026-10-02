import datetime as dt

import pandas as pd
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
