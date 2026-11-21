import json

import pandas as pd
import pytest

from mt5_extracao import cli, sdk
from mt5_extracao.analytics import datasets
from mt5_extracao.database_manager import DatabaseManager
from tests.test_backtester import bars


def capture(tmp_path, candles, *, start="2026-10-26 09:00", omit=()):
    path = tmp_path / "fvg.sqlite"
    db = DatabaseManager(db_path=str(path), broker_utc_offset=0)
    frame = bars(candles, start=start, freq="min").assign(tick_volume=1, real_volume=1)
    assert db.save_ohlcv_data("FVG-FIXTURE", "1 minuto", frame.drop(frame.index[list(omit)]))
    snapshot = datasets.capture(db, "FVG-FIXTURE", "M1")
    return db, path, snapshot.manifest_id


def report(db, manifest, cutoff):
    return sdk.fvg_reference(db, manifest, as_of=cutoff, session_mode="synthetic_utc_day")


def test_fvg_is_visible_at_c_close_and_expires_before_sixth_bar(tmp_path, capsys):
    candles = [[99, 100, 98, 99], [100, 108, 100, 107], [107, 110, 106, 109],
               [108, 110, 106, 108], *[[108, 110, 107, 109]] * 4, [108, 109, 103, 105]]
    db, path, manifest = capture(tmp_path, candles)
    try:
        assert not report(db, manifest, "2026-10-26T09:02:59Z")["zones"]
        formed, = report(db, manifest, "2026-10-26T09:03:00Z")["zones"]
        assert formed["available_at_utc"] == "2026-10-26T09:03:00+00:00"
        assert formed["status"] == "open" and not formed["first_touches"]
        assert [formed[key] for key in ("lower", "upper", "zone_midpoint", "impulse_midpoint")] == [100, 106, 103, 104]
        assert formed["impulse_variant_eligible"]
    finally:
        db.close()
    (tmp_path / "manifest.txt").write_text(manifest, encoding="utf-8")
    assert cli.main(["--db", str(path), "--json", "analytics", "fvg-reference", "--manifest", manifest,
                     "--as-of", "2026-10-26T09:09:00Z", "--session-mode", "synthetic_utc_day"]) == 0
    result = json.loads(capsys.readouterr().out)["data"]["fvg_reference"]
    zone = next(item for item in result["zones"] if item["source_bars_utc"] == formed["source_bars_utc"])
    assert zone["status"] == "expired"
    assert zone["ended_at_utc"] == "2026-10-26T09:08:00+00:00"
    assert sorted(zone["first_touches"]) == ["edge"]
    touch = zone["first_touches"]["edge"]
    assert touch["bars_after_formation"] == 1 and touch["depth_fraction"] == 0
    assert touch["fill_status"] == "unverified_ohlc"


@pytest.mark.parametrize(("impulse", "ratio", "eligible"), [
    ([110, 110, 102, 103], 7 / 8, True),
    ([110, 110, 110, 110], None, False),
    ([105, 105, 99, 101], 4 / 6, False),
])
def test_bearish_fvg_distinguishes_impulse_variant_and_violation(tmp_path, impulse, ratio, eligible):
    db, _, manifest = capture(tmp_path, [[111, 112, 110, 111], impulse, [103, 104, 100, 101], [102, 111, 101, 108]])
    try:
        zone, = report(db, manifest, "2026-10-26T09:04:00Z")["zones"]
    finally:
        db.close()
    assert (zone["side"], zone["lower"], zone["upper"], zone["zone_midpoint"]) == ("short", 104, 110, 107)
    assert zone["impulse_body_ratio"] == ratio
    assert zone["impulse_variant_eligible"] is eligible
    assert zone["status"] == "violated"
    assert zone["first_touches"]["zone_midpoint"]["violation_same_bar"]
    assert ("impulse_midpoint" in zone["first_touches"]) is eligible


@pytest.mark.parametrize(("start", "omit"), [("2026-10-26 09:00", (3,)), ("2026-10-26 23:57", ())])
def test_fvg_validity_is_interrupted_by_missing_minute_or_session_change(tmp_path, start, omit):
    db, _, manifest = capture(tmp_path, [[99, 100, 98, 99], [100, 108, 100, 107], [107, 110, 106, 109],
                                         [108, 110, 106, 108], [107, 108, 103, 105]], start=start, omit=omit)
    try:
        cutoff = pd.Timestamp(start, tz="UTC") + pd.Timedelta(minutes=5)
        zone, = report(db, manifest, cutoff)["zones"]
    finally:
        db.close()
    assert zone["status"] == "interrupted"
    assert zone["first_touches"] == {}
