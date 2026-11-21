import json

from mt5_extracao import cli, sdk
from mt5_extracao.analytics import datasets
from mt5_extracao.database_manager import DatabaseManager
from tests.test_backtester import bars


def test_fractal_break_revisits_are_numbered_and_future_pivot_cannot_change_prefix(tmp_path, capsys):
    prices = [[98, 100, 97, 98], [98, 104, 98, 101], [101, 110, 100, 105],
              [105, 106, 100, 103], [103, 105, 99, 101], [104, 110, 103, 108],
              [108, 109, 103, 108], [108, 112, 106, 111]]
    prices += [[111, 112, 110, 111], [112, 113, 111, 112]] * 4
    path = tmp_path / "retest.sqlite"
    db = DatabaseManager(db_path=str(path), broker_utc_offset=0)
    frame = bars(prices, start="2026-10-27 09:00", freq="min").assign(tick_volume=1, real_volume=1)
    assert db.save_ohlcv_data("SYNTH", "1 minuto", frame)
    snapshot = datasets.capture(db, "SYNTH", "M1")
    kwargs = {"tolerance_price": 0, "horizon_bars": 8, "session_mode": "synthetic_utc_day"}
    cutoff = "2026-10-27T09:09:00Z"
    prefix = sdk.retest_reference(db, snapshot.manifest_id, as_of=cutoff, **kwargs)
    assert [event["kind"] for event in prefix["events"]] == ["break", "retest"]
    assert prefix["events"][0]["prior_touches"] == 1
    assert prefix["events"][0]["pivot_available_at_utc"] == "2026-10-27T09:05:00+00:00"
    assert prefix["events"][0]["available_at_utc"] == "2026-10-27T09:08:00+00:00"

    changed = frame.iloc[11:].copy()
    changed.loc[11, "high"] = 160
    assert db.save_ohlcv_data("SYNTH", "1 minuto", changed)
    changed_snapshot = datasets.capture(db, "SYNTH", "M1")
    assert sdk.retest_reference(db, changed_snapshot.manifest_id, as_of=cutoff, **kwargs)["events"] == prefix["events"]
    shorter = sdk.retest_reference(db, snapshot.manifest_id, as_of="2026-10-27T09:16:00Z", **{**kwargs, "horizon_bars": 3})
    assert [event["retest_number"] for event in shorter["events"] if event["kind"] == "retest"] == [1, 2]
    db.close()
    (tmp_path / "manifest.txt").write_text(snapshot.manifest_id, encoding="utf-8")
    assert cli.main(["--db", str(path), "--json", "analytics", "retest-reference", "--manifest", snapshot.manifest_id,
                     "--as-of", "2026-10-27T09:16:00Z", "--tolerance-price", "0", "--horizon-bars", "8",
                     "--session-mode", "synthetic_utc_day"]) == 0
    report = json.loads(capsys.readouterr().out)["data"]["retest_reference"]
    retests = [event for event in report["events"] if event["kind"] == "retest"]
    assert [event["retest_number"] for event in retests] == [1, 2, 3, 4]
    assert all(event["held_at_close"] and event["level"] == 110 for event in retests)
