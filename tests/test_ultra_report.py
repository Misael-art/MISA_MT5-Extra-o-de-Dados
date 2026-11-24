import json

import pytest

from mt5_extracao import sdk
from mt5_extracao.database_manager import DatabaseManager
from scripts.demo_ultra import prepare


def test_report_contains_actual_series_and_escapes_script_data(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "qa.sqlite"), broker_utc_offset=0)
    try:
        assert sdk.ultra_list(db) == []
        d = prepare(db)
        src = sdk.ultra_freeze(db, d)["receipt"]
        sdk.ultra_replay(db, src)
        comparison = sdk.ultra_compare(db, src)
        output = tmp_path / "report.html"
        sdk.ultra_export_report(db, src, output, reference=comparison["reference_receipt"])
        page = output.read_text(encoding="utf-8")
        payload = json.loads(page.split('id="frozen" type="application/json">')[1].split("</script>")[0])
        assert payload["frames"][216]["events"][-2]["reason"] == "shared_margin"
        assert payload["reference"][-1]["equity"] - payload["frames"][-1]["mark"]["equity"] == 2940
        with pytest.raises(FileExistsError):
            sdk.ultra_export_report(db, src, output)
    finally:
        db.close()


def test_cycle_tracks_show_owner_and_explain_local_suspension(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "vis.sqlite"), broker_utc_offset=0)
    try:
        definition = prepare(db)
        receipt = sdk.ultra_freeze(db, definition)["receipt"]
        sdk.ultra_replay(db, receipt)
        page = tmp_path / "tracks.html"
        sdk.ultra_export_report(db, receipt, page)
        html = page.read_text(encoding="utf-8")
        model = json.loads(html.split('id="frozen" type="application/json">')[1].split("</script>")[0])
        assert 'id="lanes" tabindex="0"' in html
        assert model["limits"]["B"] == 2
        veto = next(e for e in model["frames"][348]["events"] if e["reason"] == "local_loss_streak")
        assert veto["local"]["losses"] == model["limits"][veto["owner"]]
    finally:
        db.close()
