import json
from datetime import datetime
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import text

from mt5_extracao.analytics import calendar, resampling
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.timeframes import Timeframe, parse as parse_timeframe
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
        row = conn.execute(text("SELECT * FROM _bars_derived WHERE symbol='EURUSD' AND tf='M3'"))\
            .mappings().one()
        calendar_row = conn.execute(text("SELECT * FROM _market_calendar WHERE symbol='EURUSD'"))\
            .mappings().one()
        assert conn.execute(text("SELECT COUNT(*) FROM _bars_derived")).scalar() == 1
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
                                "real_volume_known_count FROM _bars_derived")).one()
    assert row == (None, 0, 0, 2)


def test_zero_tick_volume_is_marked_no_trades(db):
    bars = m1_bars(pd.date_range("2026-09-14 09:00", periods=3, freq="min"), tick_volume=[0, 0, 0])
    db.save_ohlcv_data("EURUSD", "1 minuto", bars)

    resampling.resample_symbol(db, "EURUSD", ["M3"], symbol_info=SimpleNamespace(name="EURUSD"))

    with db.engine.connect() as conn:
        row = conn.execute(text("SELECT quality_status, tick_volume FROM _bars_derived")).one()
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
    m7 = resampling.parse_target("M7")
    assert (m7.minutes, m7.label, m7.mode) == (7, "M7", "exact_day")
    assert resampling.parse_target("7min") == m7
    assert resampling.parse_target("M9") == resampling.TargetTimeframe("M9", 9, "exact_day")
    assert resampling.parse_target("M5").mode == "intraday"
    assert resampling.parse_target("H13") == resampling.TargetTimeframe("780min", 780, "intraday")
    assert parse_timeframe("M7") is None and parse_timeframe("M9") is None


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
        rows = conn.execute(text("SELECT bar_start_broker, revision, quality_status FROM _bars_derived "
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
        rows = conn.execute(text("SELECT session_id, bar_start_broker, revision FROM _bars_derived "
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
        rows = conn.execute(text("SELECT coverage_ratio, quality_status FROM _bars_derived "
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
        row = conn.execute(text("SELECT source_metadata_json FROM _market_calendar WHERE symbol='EURUSD'"))\
            .scalar_one()
    assert json.loads(row)["symbol_info"]["name"] == "EURUSD"


def _day(symbol="EURUSD", periods=1440, start="2026-09-14 00:00", anchor="00:00"):
    data, sessions = calendar.sessionize_m1(
        m1_bars(pd.date_range(start, periods=periods, freq="min")), symbol,
        broker_utc_offset=0, session_anchor=anchor)
    return data, sessions


def test_m7_keeps_complete_groups_and_drops_the_last_five_minutes():
    data, sessions = _day()
    included = resampling.exact_day_report(data, sessions, "M7", broker_utc_offset=0, symbol="EURUSD")
    dropped = resampling.exact_day_report(
        data, sessions, "M7", broker_utc_offset=0, symbol="EURUSD", partial_last_bar="drop")
    bars = included["bars"]
    rejected = included["rejections"]

    assert len(bars) == 205
    assert [bar["bar_start_broker"] for bar in bars] == [
        bar["bar_start_broker"] for bar in dropped["bars"]]
    assert bars[0]["bar_start_broker"] == "2026-09-14 00:00:00.000000"
    assert bars[-1]["bar_start_broker"] == "2026-09-14 23:48:00.000000"
    assert all(bar["bar_count"] == 7 and bar["is_partial"] == 0 and bar["coverage_ratio"] == 1.0
               and bar["quality_status"] == "COMPLETE" and bar["tf"] == "M7" for bar in bars)
    assert (bars[0]["open"], bars[0]["high"], bars[0]["low"], bars[0]["close"]) == (99.5, 107.0, 99.0, 106.0)
    assert bars[0]["tick_volume"] == 35 and bars[0]["spread_min"] == 3 and bars[0]["real_volume"] == 91
    assert rejected["rejected_groups"] == 1 and rejected["excluded_source_rows"] == 5
    assert rejected["rejected"][0]["start_broker"] == "2026-09-14 23:55:00.000000"
    assert rejected["rejected"][0]["crosses_midnight"] is True
    assert resampling.build_bars(data, sessions, "M7", broker_utc_offset=0, symbol="EURUSD") == bars


def test_m9_fills_a_complete_broker_day_when_every_minute_exists():
    data, sessions = _day()
    report = resampling.exact_day_report(data, sessions, "M9", broker_utc_offset=0, symbol="EURUSD")

    assert len(report["bars"]) == 160
    assert report["rejections"]["rejected_groups"] == 0
    assert report["bars"][-1]["bar_start_broker"] == "2026-09-14 23:51:00.000000"
    assert report["bars"][-1]["bar_end_broker"] == "2026-09-15 00:00:00.000000"
    assert all(bar["bar_count"] == 9 and bar["is_partial"] == 0 for bar in report["bars"])


def test_native_m5_on_a_full_day_keeps_the_clock_floor_including_the_tail():
    data, sessions = _day()
    built = resampling.build_bars(data, sessions, "M5", broker_utc_offset=0, symbol="EURUSD")

    assert len(built) == 288
    assert built[0]["bar_start_broker"] == "2026-09-14 00:00:00.000000"
    assert built[-1]["bar_start_broker"] == "2026-09-14 23:55:00.000000"
    assert built[-1]["bar_count"] == 5


def test_missing_m9_minute_rejects_only_that_group_and_does_not_fill():
    times = [stamp for stamp in pd.date_range("2026-09-14 09:00", periods=18, freq="min")
             if stamp != pd.Timestamp("2026-09-14 09:12")]
    data, sessions = calendar.sessionize_m1(m1_bars(times), "EURUSD", broker_utc_offset=0)
    report = resampling.exact_day_report(data, sessions, "M9", broker_utc_offset=0, symbol="EURUSD")

    assert [bar["bar_start_broker"] for bar in report["bars"]] == ["2026-09-14 09:00:00.000000"]
    assert report["bars"][0]["bar_count"] == 9
    assert report["rejections"]["rejected_groups"] == 1
    assert report["rejections"]["rejected"][0]["start_broker"] == "2026-09-14 09:09:00.000000"
    assert report["rejections"]["rejected"][0]["reasons"] == ["incomplete_minutes"]
    assert report["rejections"]["excluded_source_rows"] == 8


def test_m7_does_not_emit_a_short_bar_across_midnight():
    times = list(pd.date_range("2026-09-14 23:48", periods=7, freq="min"))
    times += list(pd.date_range("2026-09-14 23:55", periods=5, freq="min"))
    times += list(pd.date_range("2026-09-15 00:00", periods=7, freq="min"))
    data, sessions = calendar.sessionize_m1(m1_bars(times), "EURUSD", broker_utc_offset=0)
    report = resampling.exact_day_report(data, sessions, "M7", broker_utc_offset=0, symbol="EURUSD")

    assert [bar["bar_start_broker"] for bar in report["bars"]] == [
        "2026-09-14 23:48:00.000000", "2026-09-15 00:00:00.000000"]
    assert all(bar["bar_count"] == 7 and bar["is_partial"] == 0 for bar in report["bars"])
    assert len({bar["session_id"] for bar in report["bars"]}) == 2
    leftover = report["rejections"]["rejected"]
    assert len(leftover) == 1 and leftover[0]["rows"] == 5 and leftover[0]["crosses_midnight"] is True


def test_mixed_sessions_inside_a_complete_m7_window_are_rejected():
    data, sessions = _day(periods=7)
    data = data.copy()
    data.loc[data.index[-1], "session_id"] = "outra-sessao"
    report = resampling.exact_day_report(data, sessions, "M7", broker_utc_offset=0, symbol="EURUSD")

    assert report["bars"] == []
    assert report["rejections"]["rejected"][0]["mixed_sessions"] is True
    assert "mixed_sessions" in report["rejections"]["rejected"][0]["reasons"]


def test_custom_session_anchor_is_not_stored_as_m7_but_m5_still_builds():
    data, sessions = _day(periods=20, start="2026-09-14 17:00", anchor="17:00")

    with pytest.raises(ValueError, match="00:00"):
        resampling.build_bars(data, sessions, "M7", broker_utc_offset=0, symbol="EURUSD")
    with pytest.raises(ValueError, match="dia exato"):
        resampling.exact_day_report(data, sessions, "M5", broker_utc_offset=0, symbol="EURUSD")

    built = resampling.build_bars(data, sessions, "M5", broker_utc_offset=0, symbol="EURUSD")
    assert built and all(bar["tf"] == "M5" for bar in built)


def test_m7_resample_is_idempotent_and_closed_read_drops_a_short_bar(db):
    source = list(pd.date_range("2026-09-14 00:00", periods=12, freq="min"))
    db.save_ohlcv_data("EURUSD", "1 minuto", m1_bars(source))
    info = SimpleNamespace(name="EURUSD")

    first = resampling.resample_symbol(db, "EURUSD", ["M7"], symbol_info=info)[0]
    second = resampling.resample_symbol(db, "EURUSD", ["M7"], symbol_info=info)[0]

    assert first["timeframe"] == "M7" and first["synthetic"] is True and first["mt5_predefined"] is False
    assert first["day_origin"] == "broker_00:00"
    assert (first["bars"], first["rejected_groups"], first["excluded_source_rows"]) == (1, 1, 5)
    assert (first["inserted"], first["corrected"], first["unchanged"]) == (1, 0, 0)
    assert (second["inserted"], second["corrected"], second["unchanged"]) == (0, 0, 1)
    with db.engine.begin() as conn:
        row = dict(conn.execute(text("SELECT * FROM _bars_derived WHERE tf='M7'")).mappings().one())
        assert row["quality_status"] == "COMPLETE" and row["bar_count"] == 7 and row["is_partial"] == 0
        row["bar_start_utc"] = "2026-09-14T03:07:00.000000+00:00"
        row["bar_end_utc"] = "2026-09-14T03:09:00.000000+00:00"
        row["bar_start_broker"] = "2026-09-14 00:07:00.000000"
        row["bar_end_broker"] = "2026-09-14 00:09:00.000000"
        row["input_hash"] = "short-not-from-builder"
        row["bar_count"] = 2
        names = list(row)
        conn.execute(text(
            f"INSERT INTO _bars_derived ({', '.join(names)}) VALUES ({', '.join(':' + name for name in names)})"),
            row)
    closed = resampling.latest_bars(db, "EURUSD", "M7", closed_only=True)
    inspection = resampling.latest_bars(db, "EURUSD", "M7", closed_only=False)
    assert list(closed["bar_count"]) == [7]
    assert sorted(int(value) for value in inspection["bar_count"]) == [2, 7]


def test_cli_session_aware_m7_says_it_is_synthetic(tmp_path, monkeypatch, capsys):
    from mt5_extracao import cli

    db_path = tmp_path / "cli-m7.sqlite"
    DatabaseManager(db_path=str(db_path)).save_ohlcv_data(
        "EURUSD", "1 minuto", m1_bars(pd.date_range("2026-09-14 00:00", periods=7, freq="min")))
    config = tmp_path / "config.ini"
    config.write_text(f"[DATABASE]\ntype = sqlite\npath = {db_path}\n[APP]\ntime_basis = broker\n",
                      encoding="utf-8")
    info = SimpleNamespace(name="EURUSD", path="Forex\\EURUSD", digits=5, currency_profit="USD")
    monkeypatch.setattr(cli.Context, "connector", lambda self: SimpleNamespace(get_symbol_info=lambda symbol: info))

    result = cli.main(["--config", str(config), "resample", "--symbols", "EURUSD", "--base", "M1",
                       "--tf", "M7", "--session-aware"])

    assert result == cli.EXIT_OK
    output = capsys.readouterr().out
    assert "EURUSD M7: 1 barras" in output
    assert "não é tempo do MT5" in output
    with DatabaseManager(db_path=str(db_path)).engine.connect() as conn:
        stored = conn.execute(text("SELECT tf, bar_count, is_partial FROM _bars_derived")).one()
    assert stored == ("M7", 7, 0)
