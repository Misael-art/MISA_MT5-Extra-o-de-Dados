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
