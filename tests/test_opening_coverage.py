import json

import pandas as pd
import pytest

from mt5_extracao import b3_public, cli, sdk
from mt5_extracao.analytics import datasets, instruments
from mt5_extracao.database_manager import DatabaseManager


OCTOBER = "WINV26"
DECEMBER = "WINZ26"
KNOWN = "2026-10-01T12:00:00Z"
END = "2026-10-30T22:00:00Z"


def scheduled(day, contract=OCTOBER, *, opening="09:00", kind="regular"):
    return {"date": day, "contract": contract, "kind": kind, "known_at": KNOWN,
            "open": opening, "close": f"{opening[:2]}:20"}


def capture(db, contract, sessions, *, expiration):
    frames = []
    for day, opening, skipped, price in sessions:
        minute = pd.date_range(f"{day} {opening}", periods=30, freq="min")
        frames.append(pd.DataFrame({"time": minute, "open": price, "high": price + 5, "low": price - 5,
                                    "close": price, "tick_volume": 1, "real_volume": 1, "spread": 0}).drop(skipped))
    assert b3_public.save(db, contract, "1 minuto", pd.concat(frames, ignore_index=True))
    instruments.record(db, contract, "specification", b3_public.spec_for(contract), source="fixture",
                       valid_from=KNOWN, known_at=KNOWN)
    instruments.record(db, contract, "expiration", {"contract": contract}, source="fixture",
                       valid_from=expiration, known_at=KNOWN)
    return datasets.capture(db, contract, "M1").manifest_id


@pytest.fixture
def market(tmp_path):
    path = tmp_path / "opening.sqlite"
    db = DatabaseManager(db_path=str(path), **b3_public.CLOCK)
    first = capture(db, OCTOBER, [("2026-10-05", "09:00", [], 100000), ("2026-10-06", "09:00", [], 100025),
                                  ("2026-10-07", "09:00", list(range(7)), 100000),
                                  ("2026-10-08", "09:00", [5], 100000), ("2026-10-09", "13:00", [], 100000)],
                    expiration="2026-10-14T21:00:00Z")
    second = capture(db, DECEMBER, [("2026-10-14", "09:00", [], 101000)], expiration="2026-12-16T21:00:00Z")
    calendar = {"source": "fixture", "sessions": [
        *(scheduled(f"2026-10-0{day}") for day in range(5, 9)),
        scheduled("2026-10-09", opening="13:00", kind="special"),
        scheduled("2026-10-14", DECEMBER), scheduled("2026-10-15", DECEMBER)]}
    yield db, path, [first, second], calendar
    db.close()


def test_nominal_opening_gap_missing_minutes_special_and_roll(market):
    db, _, ids, schedule = market
    result = sdk.opening_coverage(db, ids, schedule, as_of=END)
    first, next_session, late, hole, special, roll, empty = result["sessions"]
    assert result["eligible"] == 3
    assert first["expected_open_utc"] == first["observed_open_utc"] == "2026-10-05T12:00:00+00:00"
    assert first["range"]["available_at_utc"] == "2026-10-05T12:15:00+00:00"
    assert first["range"]["width_ticks"] == 2
    assert first["ticks"]["state"] == "not_captured" and first["ticks"]["spread"] is None
    assert first["ticks"]["rows"] is None and first["gap_points"] is None
    assert next_session["gap_points"] == 25 and next_session["prior_atr_points"] == 10
    assert late["observed_open_utc"] == "2026-10-07T12:07:00+00:00"
    assert late["m1"]["observed"] == 8 and len(late["m1"]["missing_utc"]) == 7
    assert late["reasons"] == hole["reasons"] == ["incomplete_opening_m1"]
    assert late["range"] is None and hole["range"] is None
    assert hole["m1"]["missing_utc"] == ["2026-10-08T12:05:00+00:00"]
    assert special["m1"]["observed"] == 15 and special["reasons"] == ["special_session"]
    assert roll["roll_from"] == OCTOBER and roll["status"] == "eligible"
    assert roll["gap_points"] is None and roll["prior_atr_points"] is None
    assert roll["expiration"]["at_utc"].startswith("2026-12-16")
    assert empty["reasons"] == ["no_opening_bars"] and empty["m1"]["observed"] == 0
    assert result == sdk.opening_coverage(db, reversed(ids), schedule, as_of=END)


def test_unclosed_bar_and_unannounced_schedule_cannot_publish_range(market):
    db, _, ids, schedule = market
    first = sdk.opening_coverage(db, ids, schedule, as_of="2026-10-05T12:14:59Z")["sessions"][0]
    assert first["range"] is None and first["reasons"] == ["window_not_closed"]
    assert first["m1"]["observed"] == 14
    schedule["sessions"][0]["known_at"] = "2026-10-05T12:16:00Z"
    first = sdk.opening_coverage(db, ids, schedule, as_of=END)["sessions"][0]
    assert first["range"] is None and first["reasons"] == ["calendar_not_known_at_open"]


def test_cli_returns_coverage_manifest_for_consumer(market, tmp_path, capsys):
    db, path, ids, schedule = market
    calendar = tmp_path / "calendar.json"
    calendar.write_text(json.dumps(schedule), encoding="utf-8")
    db.close()
    args = ["--db", str(path), "--json", "analytics", "opening-coverage", "--calendar", str(calendar),
            "--as-of", END, "--manifest", ids[0], "--manifest", ids[1]]
    assert cli.main(args) == 0
    receipt = json.loads(capsys.readouterr().out)["data"]["opening_coverage"]
    assert receipt["eligible"] == 3
    assert [row["status"] for row in receipt["sessions"]].count("excluded") == 4


def test_ticks_respect_window_boundaries_and_keep_a_measured_zero_spread(market):
    db, _, _, schedule = market
    times = [pd.Timestamp(f"2026-10-05T12:{minute}:00Z") for minute in ("00", "14", "15")]
    ticks = pd.DataFrame({"time_msc": [stamp.value // 1_000_000 for stamp in times], "bid": [100000] * 3,
                          "ask": [100000, 100000, 100035], "last": [100000] * 3,
                          "volume": [1] * 3, "volume_real": [1.0] * 3, "flags": [62] * 3})
    assert db.save_ticks(db.get_table_name_for_symbol(OCTOBER, "ticks"), ticks) == 3
    snapshot = datasets.capture(db, OCTOBER, "M1", families=("ticks",))
    first_day = {**schedule, "sessions": schedule["sessions"][:1]}
    result = sdk.opening_coverage(db, [snapshot.manifest_id], first_day, as_of=END)
    observed = result["sessions"][0]["ticks"]
    assert (observed["rows"], observed["minutes_with_ticks"]) == (2, 2)
    assert observed["spread"]["median"] == 0
    assert observed["origin"] == observed["aggressor"] == "unknown"
    assert observed["queue"] == "missing" and observed["coverage_state"] == "unverified"
