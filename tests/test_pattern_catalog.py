"""T13.4-P01: duas bases iguais por símbolo não compartilham identidade."""

import json

import pandas as pd
import pytest
from sqlalchemy import text

from mt5_extracao import cli, sdk
from mt5_extracao.analytics import datasets, pattern_catalog
from mt5_extracao.analytics.resampling import resample_symbol
from mt5_extracao.database_manager import DatabaseManager
from tests.test_backtester import bars


def source(path, *, offset=0):
    db = DatabaseManager(db_path=str(path), broker_utc_offset=offset)
    frame = bars([[100, 101, 99, 100]] * 6, start="2025-01-06 09:00", freq="h")
    frame["tick_volume"] = 10
    frame["real_volume"] = 0
    db.save_ohlcv_data("SYN", "1 hora", frame)
    return db


def test_equal_symbol_in_independent_bases_keeps_source_identity_and_reopens(tmp_path):
    left_path, right_path = tmp_path / "a.sqlite", tmp_path / "b.sqlite"
    left, right = source(left_path), source(right_path)
    try:
        first, second = sdk.pattern_sources(left), sdk.pattern_sources(right)
        assert first["source"]["namespace"] != second["source"]["namespace"]
        assert first["series"][0]["symbol"] == second["series"][0]["symbol"] == "SYN"
        left_report = sdk.qualify_pattern_source(left, symbol="SYN", timeframe="H1", family="reversal")
        right_report = sdk.qualify_pattern_source(right, symbol="SYN", timeframe="H1", family="reversal")
        assert left_report["eligible"] and right_report["eligible"]
        assert left_report["source_namespace"] != right_report["source_namespace"]
        frozen = sdk.freeze_pattern_source(left, symbol="SYN", timeframe="H1", family="reversal")
        assert frozen["source_namespace"] == left_report["source_namespace"]
        assert frozen["rows"] == 6 and frozen["economic_execution"] == "not_qualified"
        assert frozen["quality"] == left_report["quality"]
        assert datasets.load(left, frozen["manifest_id"]).manifest["source"]["namespace"] == frozen["source_namespace"]
        with pytest.raises(ValueError):
            datasets.load(right, frozen["manifest_id"])
    finally:
        left.close(); right.close()
    reopened = DatabaseManager(db_path=str(left_path), broker_utc_offset=0)
    try:
        assert sdk.pattern_sources(reopened)["source"]["namespace"] == frozen["source_namespace"]
        assert datasets.load(reopened, frozen["manifest_id"]).bars.close.tolist() == [100] * 6
    finally:
        reopened.close()


def test_clock_and_session_requirements_block_without_inventing_history(tmp_path):
    db = source(tmp_path / "clock.sqlite", offset=None)
    try:
        report = sdk.qualify_pattern_source(db, symbol="SYN", timeframe="H1", family="reversal")
        assert not report["eligible"] and report["clock"]["offset_state"] == "unknown"
        assert "Relógio" in report["reasons"][-1]
        with pytest.raises(ValueError, match="inelegível"):
            sdk.freeze_pattern_source(db, symbol="SYN", timeframe="H1", family="reversal")
    finally:
        db.close()
    qualified = source(tmp_path / "sessions.sqlite")
    try:
        report = sdk.qualify_pattern_source(qualified, symbol="SYN", timeframe="H1", family="gap")
        assert not report["eligible"] and "observed_session_bars" in report["required"]
        assert report["economic_execution"] == "not_qualified"
        assert report["quality"]["capabilities"]["real_volume"]["state"] == "unknown"
    finally:
        qualified.close()
    zoned = DatabaseManager(db_path=str(tmp_path / "zone.sqlite"), time_basis="utc",
                            broker_timezone="Europe/Athens")
    frame = bars([[100, 101, 99, 100]] * 2, start="2025-01-06 09:00", freq="h")
    frame["tick_volume"], frame["real_volume"] = 10, 0
    zoned.save_ohlcv_data("SYN", "1 hora", frame)
    try:
        report = sdk.qualify_pattern_source(zoned, symbol="SYN", timeframe="H1", family="reversal",
                                            start="2025-01-06 09:00", end="2025-01-06 10:00")
        assert not report["eligible"] and report["clock"]["offset_state"] == "zone"
        assert report["rows"] == 2
        assert "offset fixo" in report["reasons"][-1]
    finally:
        zoned.close()


def test_cli_and_sdk_return_the_same_qualified_source(tmp_path, monkeypatch, capsys):
    db = source(tmp_path / "cli.sqlite")
    config = tmp_path / "qa.ini"
    config.write_text("[APP]\nbroker_utc_offset=0\n", encoding="utf-8")
    monkeypatch.setattr(cli.Context, "db", property(lambda _: db))
    try:
        expected = sdk.qualify_pattern_source(db, symbol="SYN", timeframe="H1", family="reversal")
        assert cli.main(["--config", str(config), "--json", "laboratory", "pattern-qualify",
                         "--symbol", "SYN", "--tf", "H1", "--family", "reversal"]) == 0
        actual = json.loads(capsys.readouterr().out)["data"]
        assert actual["source_namespace"] == expected["source_namespace"]
        assert actual["eligible"] == expected["eligible"]
        assert actual["required"] == expected["required"]
    finally:
        db.close()


def test_oversized_selection_is_counted_before_loading_bars(tmp_path, monkeypatch):
    db = source(tmp_path / "large.sqlite")
    monkeypatch.setattr(pattern_catalog, "MAX_CAPTURE_BARS", 5)
    monkeypatch.setattr(db, "load_ohlcv", lambda *args, **kwargs: pytest.fail("leitura pesada iniciada"))
    try:
        with pytest.raises(ValueError, match="limite de leitura"):
            sdk.qualify_pattern_source(db, symbol="SYN", timeframe="H1", family="reversal")
    finally:
        db.close()


def test_session_family_uses_real_derived_coverage_and_freezes_same_quality(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "sessions.sqlite"), broker_utc_offset=0)
    minute = bars([[100, 101, 99, 100]] * 6, start="2025-01-06 09:00", freq="min")
    minute["tick_volume"], minute["real_volume"] = 10, 0
    db.save_ohlcv_data("SYN", "1 minuto", minute)
    try:
        resample_symbol(db, "SYN", ["M5"])
        with db.engine.connect() as conn:
            frame = pd.read_sql(text("SELECT bar_start_broker AS time,open,high,low,close,"
                                     "tick_volume,real_volume,spread_min AS spread FROM _bars_derived"), conn)
        frame["time"] = pd.to_datetime(frame.time)
        db.save_ohlcv_data("SYN", "5 minutos", frame)
        report = sdk.qualify_pattern_source(db, symbol="SYN", timeframe="M5", family="gap",
                                            end="2025-01-06 09:00:00")
        assert report["eligible"]
        assert report["quality"]["capabilities"]["observed_session_bars"]["state"] == "available"
        frozen = sdk.freeze_pattern_source(db, symbol="SYN", timeframe="M5", family="gap",
                                           end="2025-01-06 09:00:00")
        assert frozen["quality"] == report["quality"]
        assert frozen["economic_execution"] == "not_qualified"
    finally:
        db.close()
