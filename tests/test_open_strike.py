import json

import pandas as pd
import pytest

from mt5_extracao import b3_public, cli, sdk
from mt5_extracao.analytics import datasets, instruments, open_strike
from mt5_extracao.database_manager import DatabaseManager
from tests.test_backtester import bars
from tests.test_opening_coverage import DECEMBER, KNOWN, scheduled


def test_open_strike_m15_signal_and_gap_through_pending_at_launcher_entry_point(tmp_path, capsys):
    path = tmp_path / "win.sqlite"
    db = DatabaseManager(db_path=str(path), **b3_public.CLOCK)
    before, current = "2026-10-27", "2026-10-28"
    warm = bars([[100000, 100005, 99995, 100000]] * 135, start=before + " 09:00", freq="min")
    opening = bars([[100000, 100005, 99995, 100000]] * 14 + [[100000, 101005, 99995, 101000]]
                   + [[101020, 101025, 100995, 101000]] * 15, start=current + " 09:00", freq="min")
    source = pd.concat([warm, opening], ignore_index=True).assign(tick_volume=1, real_volume=1)
    assert b3_public.save(db, DECEMBER, "1 minuto", source)
    instruments.record(db, DECEMBER, "expiration", {"contract": DECEMBER}, source="fixture",
                       known_at=KNOWN, valid_from="2026-12-16T21:00:00Z")
    instruments.record(db, DECEMBER, "specification", b3_public.spec_for(DECEMBER), source="fixture",
                       known_at=KNOWN, valid_from=KNOWN)
    snapshot = datasets.capture(db, DECEMBER, "M1")
    calendar = {"source": "fixture", "sessions": [
        {**scheduled(before, DECEMBER), "close": "11:15"}, {**scheduled(current, DECEMBER), "close": "09:30"}]}
    early = sdk.open_strike_reference(db, snapshot.manifest_id, calendar, as_of=current + "T12:14:59Z")
    assert early["sessions"][1]["signal"] is None
    assert "window_not_closed" in early["sessions"][1]["reasons"]
    at_signal = sdk.open_strike_reference(db, snapshot.manifest_id, calendar, as_of=current + "T12:15:00Z")
    assert at_signal["sessions"][1]["entries"]["pending"]["status"] == "pending"
    assert at_signal["sessions"][1]["entries"]["market"]["status"] == "awaiting_bar"
    file = tmp_path / "calendar.json"
    file.write_text(json.dumps(calendar), encoding="utf-8")
    (tmp_path / "manifest.txt").write_text(snapshot.manifest_id, encoding="utf-8")
    db.close()
    assert cli.main(["--db", str(path), "--json", "analytics", "open-strike-reference", "--manifest", snapshot.manifest_id,
                     "--calendar", str(file), "--as-of", current + "T12:30:00Z"]) == 0
    result = json.loads(capsys.readouterr().out)["data"]["open_strike_reference"]
    warm_row, signal_row = result["sessions"]
    assert warm_row["status"] == "unavailable_bands"
    assert signal_row["available_at_utc"] == current + "T12:15:00+00:00"
    assert signal_row["bands"]["ema"]["mid"] == pytest.approx(100000 + 1000 * 2 / 11)
    assert signal_row["bands"]["sma"]["upper"] == pytest.approx(100400)
    assert signal_row["signal"]["side"] == "long"
    assert signal_row["entries"]["pending"]["trigger"] == 101010
    assert signal_row["entries"]["pending"]["reference_price"] == 101020
    assert signal_row["entries"]["pending"]["event"] == "at_open"


@pytest.mark.parametrize(("op", "close", "expected"), [(90, 110, None), (105, 105, None), (110, 89, "short")])
def test_band_touches_and_neutral_candles_are_not_signals(op, close, expected):
    assert open_strike._side(op, close, 100, 10) == expected


def test_pending_short_expires_before_late_touch_and_preserves_unknown_gaps():
    start = pd.Timestamp("2026-10-28T12:15:00Z")
    times = pd.date_range(start, periods=3, freq="min")
    minutes = pd.DataFrame({"open": [101, 101, 94], "high": [102, 102, 100],
                            "low": [100, 100, 94], "valid": [True, True, True]}, index=times)
    signal = {"side": "short", "high": 105, "low": 100}
    end = times[-1]
    expired = open_strike._entry_models(minutes, start, end, end + pd.Timedelta(minutes=1), signal, 5)
    assert expired["pending"]["status"] == "expired" and expired["pending"]["trigger"] == 95
    gap = open_strike._entry_models(minutes.drop(times[1]), start, end, end, signal, 5)
    assert gap["pending"]["status"] == "unavailable_m1"
    continued = open_strike._entry_models(minutes, start, end + pd.Timedelta(minutes=1), end + pd.Timedelta(minutes=1), signal, 5)
    assert continued["pending"]["reference_price"] == 94 and continued["pending"]["event"] == "at_open"
