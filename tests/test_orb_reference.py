import json

import pandas as pd

from mt5_extracao import b3_public, cli, sdk
from mt5_extracao.analytics import datasets, instruments
from mt5_extracao.database_manager import DatabaseManager
from tests.test_opening_coverage import DECEMBER, KNOWN, scheduled


def test_orb_reference_compares_complete_late_and_no_breakout_sessions(tmp_path, capsys):
    db_path = tmp_path / "orb.sqlite"
    db = DatabaseManager(db_path=str(db_path), **b3_public.CLOCK)
    days = ["2026-10-27", "2026-10-28", "2026-10-29"]
    frames = []
    for day, delay, breakout in zip(days, (0, 7, 0), (True, True, False)):
        times = pd.date_range(f"{day} 09:{delay:02d}", periods=45 - delay, freq="min")
        frame = pd.DataFrame({"time": times, "open": 100000, "high": 100005, "low": 99995,
                              "close": 100000, "tick_volume": 1, "real_volume": 1, "spread": 0})
        if breakout:
            frame.loc[30, ["high", "close"]] = 100010
            frame.loc[35, ["high", "close"]] = 100020
        frames.append(frame)
    assert b3_public.save(db, DECEMBER, "1 minuto", pd.concat(frames, ignore_index=True))
    instruments.record(db, DECEMBER, "expiration", {"contract": DECEMBER}, source="fixture",
                       known_at=KNOWN, valid_from="2026-12-16T21:00:00Z")
    snapshot = datasets.capture(db, DECEMBER, "M1")
    schedule = {"source": "fixture", "sessions": [{**scheduled(day, DECEMBER), "close": "10:00"} for day in days]}
    before = sdk.orb_reference(db, snapshot.manifest_id, schedule, as_of="2026-10-27T12:30:59Z")
    assert before["sessions"][0]["range"]["available_at_utc"] == "2026-10-27T12:30:00+00:00"
    assert not before["sessions"][0]["qualified_signals"]
    calendar = tmp_path / "calendar.json"
    calendar.write_text(json.dumps(schedule), encoding="utf-8")
    db.close()
    assert cli.main(["--db", str(db_path), "--json", "analytics", "orb-reference", "--manifest", snapshot.manifest_id,
                     "--calendar", str(calendar), "--as-of", "2026-10-29T13:00:00Z"]) == 0
    result = json.loads(capsys.readouterr().out)["data"]["orb_reference"]
    complete, late, no_break = result["sessions"]
    assert complete["qualified_signals"] == complete["legacy_signals"] == [{
        "bar_open_utc": "2026-10-27T12:30:00+00:00", "available_at_utc": "2026-10-27T12:31:00+00:00",
        "side": "long", "close": 100010.0, "stop": 99995.0, "target": 100025.0}]
    assert late["legacy_signals"][0]["bar_open_utc"] == "2026-10-28T12:37:00+00:00"
    assert late["qualified_signals"] == [] and late["reasons"] == ["incomplete_opening_m1"]
    assert no_break["status"] == "eligible" and not no_break["qualified_signals"] and not no_break["legacy_signals"]
    assert result["first_divergent_session"] == days[1]

    db = DatabaseManager(db_path=str(db_path), **b3_public.CLOCK)
    filtered = sdk.orb_reference(db, snapshot.manifest_id, schedule, as_of="2026-10-29T13:00:00Z",
                                 width_limits=[10, 10], distance_limits=[5, 5])
    full, delayed, flat = (row["filter_decision"] for row in filtered["sessions"])
    assert full["status"] == "accepted" and full["width_points"] == 10 and full["distance_points"] == 5
    assert delayed["status"] == "excluded" and flat["status"] == "no_breakout"
    blocked = sdk.orb_reference(db, snapshot.manifest_id, schedule, as_of="2026-10-29T13:00:00Z",
                                width_limits=[0, 9], distance_limits=[6, 10], direction="short")
    veto = blocked["sessions"][0]["filter_decision"]
    assert veto["status"] == "blocked" and veto["event"] == full["event"]
    assert veto["reasons"] == ["width_outside_limits", "distance_outside_limits", "direction_filtered"]
    assert blocked["filters"]["first_breakout_consumes_session"]
    distance_only = sdk.orb_reference(db, snapshot.manifest_id, schedule, as_of="2026-10-29T13:00:00Z",
                                      distance_limits=[6, 20])["sessions"][0]["filter_decision"]
    assert distance_only["status"] == "blocked" and distance_only["reasons"] == ["distance_outside_limits"]
    assert distance_only["event"]["bar_open_utc"] == "2026-10-27T12:30:00+00:00"
    db.close()
