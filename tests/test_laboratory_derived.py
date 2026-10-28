"""Leitura das barras de dia exato já gravadas. Não agrega e não entra na captura nativa."""
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import text

from mt5_extracao.analytics import resampling
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.laboratory import derived, service
from tests.test_analytics_calendar import m1_bars


@pytest.fixture
def db(tmp_path):
    return DatabaseManager(db_type="sqlite", db_path=str(tmp_path / "analytics.sqlite"), broker_utc_offset=-3)


def _store(db, times):
    db.save_ohlcv_data("EURUSD", "1 minuto", m1_bars(times))
    info = SimpleNamespace(name="EURUSD")
    resampling.resample_symbol(db, "EURUSD", ["M5", "M7"], symbol_info=info)


def test_chart_sources_list_exact_day_only_and_read_returns_stored_ohlc(db, monkeypatch):
    _store(db, pd.date_range("2026-09-14 00:00", periods=14, freq="min"))
    monkeypatch.setattr(resampling, "resample_symbol", lambda *args, **kwargs: (_ for _ in ()).throw(
        AssertionError("a leitura não agrega")))

    listed = derived.chart_sources(db)
    assert [(item["timeframe"], item["synthetic"]) for item in listed] == [("M1", False), ("M7", True)]
    assert listed[1]["mt5_predefined"] is False and listed[1]["day_origin"] == "broker_00:00"
    assert "não é tempo do MT5" in listed[1]["note"]

    with db.engine.connect() as conn:
        before = conn.execute(text("SELECT COUNT(*) FROM _bars_derived")).scalar()
        stored = conn.execute(text(
            "SELECT open, high, low, close, bar_count FROM _bars_derived WHERE tf='M7' ORDER BY bar_start_broker"
        )).fetchall()
    read = derived.read_stored(db, "EURUSD", ["M7"])
    series = read["series"][0]
    assert read["kind"] == "stored_derived" and read["synthetic"] is True
    assert series["rows"] == len(stored) == 2
    assert series["gaps"] == 0 and series["partials"] == 0
    assert (series["bar_count_min"], series["bar_count_max"]) == (7, 7)
    assert series["prices"] == [[row[0], row[1], row[2], row[3]] for row in stored]
    with db.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM _bars_derived")).scalar() == before

    with pytest.raises(ValueError, match="não é leitura de dia exato"):
        derived.read_stored(db, "EURUSD", ["M5"])
    with pytest.raises(ValueError, match="timeframes distintos"):
        service.capture_selection(db, symbol="EURUSD", timeframes_selected=["M7"])
    summary = derived.public_summary(read, head=1)
    assert "prices" not in summary["series"][0] and len(summary["series"][0]["head"]) == 1


def test_reader_drops_short_bars_counts_gaps_and_keeps_broker_window(db):
    _store(db, list(pd.date_range("2026-09-14 00:00", periods=7, freq="min"))
           + list(pd.date_range("2026-09-14 00:14", periods=7, freq="min")))
    with db.engine.begin() as conn:
        row = dict(conn.execute(text("SELECT * FROM _bars_derived WHERE tf='M7'")).mappings().first())
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

    read = derived.read_stored(db, "EURUSD", ["M7"])
    series = read["series"][0]
    assert series["rows"] == 2 and series["gaps"] == 1
    assert all(not time.startswith("2026-09-14 00:07") for time in series["times"])
    window = derived.read_stored(db, "EURUSD", ["M7"], start="2026-09-14 00:14", end="2026-09-14 00:14")
    assert window["series"][0]["rows"] == 1
    with pytest.raises(ValueError, match="sem fuso"):
        derived.read_stored(db, "EURUSD", ["M7"], start="2026-09-14T00:00:00Z")
    with pytest.raises(ValueError, match="não cria candles"):
        derived.read_stored(db, "EURUSD", ["M7"], start="2030-01-01")


def test_empty_derived_table_lists_nothing(db):
    assert derived.list_stored(db) == []
    assert derived.selection_plan([]) == "empty"
    assert derived.selection_plan([{"synthetic": True}, {"synthetic": False}]) == "mixed"


def test_cli_derived_read_is_labeled_synthetic(tmp_path, monkeypatch, capsys):
    from mt5_extracao import cli

    db_path = tmp_path / "derived.sqlite"
    db = DatabaseManager(db_path=str(db_path))
    _store(db, pd.date_range("2026-09-14 00:00", periods=7, freq="min"))
    config = tmp_path / "config.ini"
    config.write_text(f"[DATABASE]\ntype = sqlite\npath = {db_path}\n[APP]\ntime_basis = broker\n",
                      encoding="utf-8")

    result = cli.main(["--config", str(config), "laboratory", "derived", "--symbol", "EURUSD", "--tf", "M7",
                       "--head", "1"])

    assert result == cli.EXIT_OK
    output = capsys.readouterr().out
    assert "não é tempo do MT5" in output
    assert "M7" in output
    assert "extraindo" not in output.lower()
