import json
from datetime import datetime
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import text

from mt5_extracao.analytics import calendar, resampling
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.timeframes import Timeframe
from mt5_extracao import resample as legacy_resample


def m1_bars(times, real_volume=None, tick_volume=None):
    times = pd.to_datetime(times)
    count = len(times)
    close = [100.0 + index for index in range(count)]
    if real_volume is None:
        real_volume = [10 + index for index in range(count)]
    if tick_volume is None:
        tick_volume = [2 + index for index in range(count)]
    return pd.DataFrame({
        "time": times, "open": [value - 0.5 for value in close], "high": [value + 1 for value in close],
        "low": [value - 1 for value in close], "close": close, "tick_volume": tick_volume,
        "spread": [5 - index % 3 for index in range(count)], "real_volume": real_volume,
    })


@pytest.fixture
def db(tmp_path):
    return DatabaseManager(db_type="sqlite", db_path=str(tmp_path / "analytics.sqlite"), broker_utc_offset=-3)


def test_m3_ohlcv_is_exact_and_reruns_are_idempotent(db):
    bars = m1_bars(pd.date_range("2026-09-14 09:00", periods=3, freq="min"), real_volume=[0, 0, 0],
                   tick_volume=[3, 4, 5])
    db.save_ohlcv_data("EURUSD", "1 minuto", bars)
    info = SimpleNamespace(name="EURUSD", description="Euro/Dólar", path="Forex\\EURUSD", digits=5,
                           currency_profit="USD", spread=99, bid=1.1, ask=1.2)

    first = resampling.resample_symbol(db, "EURUSD", ["M3"], symbol_info=info)
    second = resampling.resample_symbol(db, "EURUSD", ["M3"], symbol_info=info)

    assert (first[0]["inserted"], first[0]["corrected"], first[0]["unchanged"]) == (1, 0, 0)
    assert (second[0]["inserted"], second[0]["corrected"], second[0]["unchanged"]) == (0, 0, 1)
    with db.engine.connect() as conn:
        row = conn.execute(text("SELECT * FROM bars_derived WHERE symbol='EURUSD' AND tf='M3'"))\
            .mappings().one()
        calendar_row = conn.execute(text("SELECT * FROM market_calendar WHERE symbol='EURUSD'"))\
            .mappings().one()
        assert conn.execute(text("SELECT COUNT(*) FROM bars_derived")).scalar() == 1
    assert (row["open"], row["high"], row["low"], row["close"]) == (99.5, 103.0, 99.0, 102.0)
    assert row["tick_volume"] == 12 and row["spread_min"] == 3
    assert row["real_volume"] == 0 and row["real_volume_known"] == 1
    assert row["bar_count"] == 3 and row["coverage_ratio"] == 1.0 and row["quality_status"] == "COMPLETE"
    assert row["bar_start_utc"] == "2026-09-14T12:00:00.000000+00:00"
    metadata = json.loads(calendar_row["source_metadata_json"])
    assert metadata["symbol_info"]["name"] == "EURUSD"
    assert "bid" not in metadata["symbol_info"] and "spread" not in metadata["symbol_info"]
    assert metadata["calendar_inference"]["source_timeframe"] == "M1"
    assert calendar_row["source"] == "m1_observed"


def test_real_volume_null_is_not_zero(db):
    bars = m1_bars(pd.date_range("2026-09-14 09:00", periods=3, freq="min"), real_volume=[None, 0, 0])
    db.save_ohlcv_data("EURUSD", "1 minuto", bars)

    resampling.resample_symbol(db, "EURUSD", ["M3"], symbol_info=SimpleNamespace(name="EURUSD"))

    with db.engine.connect() as conn:
        row = conn.execute(text("SELECT real_volume, real_volume_observed, real_volume_known, "
                                "real_volume_known_count FROM bars_derived")).one()
    assert row == (None, 0, 0, 2)


def test_zero_tick_volume_is_marked_no_trades(db):
    bars = m1_bars(pd.date_range("2026-09-14 09:00", periods=3, freq="min"), tick_volume=[0, 0, 0])
    db.save_ohlcv_data("EURUSD", "1 minuto", bars)

    resampling.resample_symbol(db, "EURUSD", ["M3"], symbol_info=SimpleNamespace(name="EURUSD"))

    with db.engine.connect() as conn:
        row = conn.execute(text("SELECT quality_status, tick_volume FROM bars_derived")).one()
    assert row == ("NO_TRADES", 0)


def test_session_crossing_midnight_stays_distinct_from_next_session():
    times = list(pd.date_range("2026-09-14 23:58", periods=4, freq="min"))
    times += list(pd.date_range("2026-09-15 00:03", periods=2, freq="min"))
    data, sessions = calendar.sessionize_m1(m1_bars(times), "EURUSD", broker_utc_offset=0,
                                            session_gap_minutes=1, session_anchor="23:00")

    built = resampling.build_bars(data, sessions, "M5", broker_utc_offset=0, symbol="EURUSD")

    assert len(sessions) == 2
    assert sessions[0]["open_broker"] == "2026-09-14 23:58:00.000000"
    assert sessions[0]["close_broker"] == "2026-09-15 00:02:00.000000"
    assert len(built) == 3
    assert [bar["bar_start_broker"] for bar in built] == [
        "2026-09-14 23:55:00.000000", "2026-09-15 00:00:00.000000", "2026-09-15 00:00:00.000000"]
    assert len({bar["session_id"] for bar in built}) == 2
    assert max(bar["bar_count"] for bar in built) == 2


def test_daily_session_bars_do_not_create_rows_for_closed_days():
    times = ["2026-09-11 09:00", "2026-09-11 09:01", "2026-09-14 09:00", "2026-09-14 09:01"]
    data, sessions = calendar.sessionize_m1(m1_bars(times), "WIN$N", broker_utc_offset=-3)

    built = resampling.build_bars(data, sessions, "1D", broker_utc_offset=-3, symbol="WIN$N")

    assert len(built) == 2
    assert [bar["bar_start_broker"] for bar in built] == [
        "2026-09-11 00:00:00.000000", "2026-09-14 00:00:00.000000"]
    assert resampling.parse_target("1D").mode == "session"
    assert resampling.parse_target("1440min").mode == "fixed_24h"
    assert resampling.parse_target("M7").minutes == 7


def test_late_m1_changes_only_its_derived_bar_and_adds_a_revision(db):
    times = list(pd.date_range("2026-09-14 09:00", periods=10, freq="min"))
    initial = m1_bars(times).drop(index=2).reset_index(drop=True)
    full = m1_bars(times)
    db.save_ohlcv_data("EURUSD", "1 minuto", initial)
    info = SimpleNamespace(name="EURUSD")

    resampling.resample_symbol(db, "EURUSD", ["M5"], symbol_info=info)
    db.save_ohlcv_data("EURUSD", "1 minuto", full)
    changed = resampling.resample_symbol(db, "EURUSD", ["M5"], symbol_info=info)

    assert changed[0]["corrected"] == 1 and changed[0]["unchanged"] == 1
    with db.engine.connect() as conn:
        rows = conn.execute(text("SELECT bar_start_broker, revision, quality_status FROM bars_derived "
                                 "WHERE symbol='EURUSD' AND tf='M5' ORDER BY bar_start_broker, revision")).fetchall()
    assert rows == [
        ("2026-09-14 09:00:00.000000", 1, "PARTIAL"),
        ("2026-09-14 09:00:00.000000", 2, "CORRECTED"),
        ("2026-09-14 09:05:00.000000", 1, "COMPLETE"),
    ]


def test_late_m1_at_session_open_keeps_the_session_and_bar_keys_stable(db):
    full = m1_bars(pd.date_range("2026-09-14 09:00", periods=10, freq="min"))
    db.save_ohlcv_data("EURUSD", "1 minuto", full.iloc[1:].reset_index(drop=True))
    info = SimpleNamespace(name="EURUSD")

    resampling.resample_symbol(db, "EURUSD", ["M5"], symbol_info=info)
    db.save_ohlcv_data("EURUSD", "1 minuto", full)
    changed = resampling.resample_symbol(db, "EURUSD", ["M5"], symbol_info=info)

    assert changed[0]["corrected"] == 1 and changed[0]["unchanged"] == 1
    with db.engine.connect() as conn:
        rows = conn.execute(text("SELECT session_id, bar_start_broker, revision FROM bars_derived "
                                 "WHERE symbol='EURUSD' AND tf='M5' ORDER BY bar_start_broker, revision")).fetchall()
    assert len(rows) == 3
    assert rows[0][0] == rows[1][0]
    assert rows[0][1:] == ("2026-09-14 09:00:00.000000", 1)
    assert rows[1][1:] == ("2026-09-14 09:00:00.000000", 2)


def test_missing_extraction_block_marks_partial_bar_as_missing(db):
    bars = m1_bars(["2026-09-14 09:00", "2026-09-14 09:04"])
    db.save_ohlcv_data("EURUSD", "1 minuto", bars)
    table = db.find_table_for_symbol("EURUSD", "1 minuto")
    start, end = datetime(2026, 9, 14, 9, 1), datetime(2026, 9, 14, 9, 4)
    db.record_block(table, start, end, 0, "failed", "mt5")

    resampling.resample_symbol(db, "EURUSD", ["M5"], symbol_info=SimpleNamespace(name="EURUSD"))
    db.record_block(table, start, end, 0, "ok", "mt5")
    resampling.resample_symbol(db, "EURUSD", ["M5"], symbol_info=SimpleNamespace(name="EURUSD"))

    with db.engine.connect() as conn:
        rows = conn.execute(text("SELECT coverage_ratio, quality_status FROM bars_derived "
                                 "ORDER BY revision")).fetchall()
    assert rows == [(pytest.approx(0.4), "MISSING"), (pytest.approx(0.4), "CORRECTED")]


def test_one_day_session_is_distinct_from_fixed_1440_minute_window():
    times = pd.date_range("2026-09-14 23:59", periods=3, freq="min")
    data, sessions = calendar.sessionize_m1(m1_bars(times), "EURUSD", broker_utc_offset=0,
                                            session_anchor="12:00")

    session_bars = resampling.build_bars(data, sessions, "1D", broker_utc_offset=0, symbol="EURUSD")
    fixed_bars = resampling.build_bars(data, sessions, "1440min", broker_utc_offset=0, symbol="EURUSD")

    assert len(session_bars) == 1
    assert len(fixed_bars) == 2
    assert session_bars[0]["bar_start_broker"] == "2026-09-14 12:00:00.000000"
    assert [bar["bar_start_utc"] for bar in fixed_bars] == [
        "2026-09-14T00:00:00.000000+00:00", "2026-09-15T00:00:00.000000+00:00"]


def test_eurusd_m5_ohlcv_matches_legacy_resampler():
    bars = m1_bars(pd.date_range("2026-09-14 09:00", periods=120, freq="min"))
    data, sessions = calendar.sessionize_m1(bars, "EURUSD", broker_utc_offset=0)

    built = resampling.build_bars(data, sessions, "M5", broker_utc_offset=0, symbol="EURUSD")
    legacy = legacy_resample.resample_bars(bars, Timeframe.M1, Timeframe.M5)

    assert len(built) == len(legacy)
    for actual, expected in zip(built, legacy.itertuples(index=False)):
        assert actual["bar_start_broker"] == expected.time.strftime("%Y-%m-%d %H:%M:%S.%f")
        assert (actual["open"], actual["high"], actual["low"], actual["close"]) == (
            expected.open, expected.high, expected.low, expected.close)
        assert actual["tick_volume"] == expected.tick_volume
        assert actual["real_volume"] == expected.real_volume
        assert actual["spread_min"] == expected.spread


def test_cli_session_aware_requires_m1_and_records_mt5_symbol_info(tmp_path, monkeypatch):
    from mt5_extracao import cli

    db_path = tmp_path / "cli.sqlite"
    DatabaseManager(db_path=str(db_path)).save_ohlcv_data(
        "EURUSD", "1 minuto", m1_bars(pd.date_range("2026-09-14 09:00", periods=3, freq="min")))
    config = tmp_path / "config.ini"
    config.write_text(f"[DATABASE]\ntype = sqlite\npath = {db_path}\n[APP]\ntime_basis = broker\n",
                      encoding="utf-8")
    info = SimpleNamespace(name="EURUSD", path="Forex\\EURUSD", digits=5, currency_profit="USD")
    fake_connector = SimpleNamespace(get_symbol_info=lambda symbol: info)
    monkeypatch.setattr(cli.Context, "connector", lambda self: fake_connector)

    result = cli.main(["--config", str(config), "resample", "--symbols", "EURUSD", "--base", "M1",
                       "--tf", "M3", "--session-aware"])

    assert result == cli.EXIT_OK
    with DatabaseManager(db_path=str(db_path)).engine.connect() as conn:
        row = conn.execute(text("SELECT source_metadata_json FROM market_calendar WHERE symbol='EURUSD'"))\
            .scalar_one()
    assert json.loads(row)["symbol_info"]["name"] == "EURUSD"
