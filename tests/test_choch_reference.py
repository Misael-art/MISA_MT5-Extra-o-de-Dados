import json

from mt5_extracao import b3_public, cli, sdk
from mt5_extracao.analytics import datasets, instruments
from mt5_extracao.database_manager import DatabaseManager
from tests.test_backtester import bars


def test_choch_h4_distinguishes_close_turn_from_confirmed_swing_break(tmp_path, capsys):
    path = tmp_path / "h4.sqlite"
    db = DatabaseManager(db_path=str(path), broker_utc_offset=0)
    closes = [100, 105, 110, 103, 99, 95, 102, 110, 120, 115, 110, 105, 112, 118, 115, 109, 102, 100]
    frame = bars([[value, value + 1, value - 1, value] for value in closes],
                 start="2026-10-26", freq="4h").assign(tick_volume=1, real_volume=1)
    symbol = "H4-FIXTURE"
    assert db.save_ohlcv_data(symbol, "4 horas", frame)
    spec = {**b3_public.spec_for("WINZ26"), "point": 1, "tick_size": 1}
    known = "2026-10-01T00:00:00Z"
    instruments.record(db, symbol, "specification", spec, source="fixture", known_at=known, valid_from=known)
    snapshot = datasets.capture(db, symbol, "H4")
    earlier = sdk.choch_reference(db, snapshot.manifest_id, as_of="2026-10-28T12:00:00Z", session_mode="synthetic_utc_day")
    assert earlier["close_turns"][-1]["side"] == "short"
    assert earlier["close_turns"][-1]["available_at_utc"] == "2026-10-28T12:00:00+00:00"
    assert earlier["swing_breaks"] == []
    at_break = sdk.choch_reference(db, snapshot.manifest_id, as_of="2026-10-28T20:00:00Z", session_mode="synthetic_utc_day")
    assert at_break["swing_breaks"][0]["entry"]["status"] == "waiting_next_h4"
    db.close()
    (tmp_path / "manifest.txt").write_text(snapshot.manifest_id, encoding="utf-8")
    assert cli.main(["--db", str(path), "--json", "analytics", "choch-reference", "--manifest", snapshot.manifest_id,
                     "--as-of", "2026-10-29T00:00:00Z", "--session-mode", "synthetic_utc_day"]) == 0
    result = json.loads(capsys.readouterr().out)["data"]["choch_reference"]
    broken, = result["swing_breaks"]
    assert broken["side"] == "short" and broken["broken_swing_price"] == 104
    assert broken["swing_available_at_utc"] == "2026-10-28T08:00:00+00:00"
    assert broken["available_at_utc"] == "2026-10-28T20:00:00+00:00"
    assert broken["entry"] == {"status": "modeled", "reference_price": 100,
                               "open_utc": "2026-10-28T20:00:00+00:00", "observed_at_utc": "2026-10-29T00:00:00+00:00",
                               "stop": 120, "target_1r": 80}
