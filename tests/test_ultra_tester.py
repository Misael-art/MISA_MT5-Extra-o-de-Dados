from copy import deepcopy
from hashlib import sha256

import pytest

from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.experiments.schema import digest
from mt5_extracao.laboratory import ultra, ultra_tester
from mt5_extracao.presets.compiler import compile_ultra_research
from scripts.demo_ultra import prepare


@pytest.fixture
def campaign(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "qa.sqlite"), broker_utc_offset=0)
    try:
        receipt = ultra.freeze(db, prepare(db))["receipt"]
        ultra.replay(db, receipt)
        yield db, compile_ultra_research(db, receipt)
    finally:
        db.close()


def test_export_frozen_expert_in_priority_order_and_refuse_overwrite(campaign, tmp_path):
    db, package = campaign
    folder = tmp_path / "tester"
    result = ultra_tester.export(db, package, folder)
    assert result["ticks"] == 372
    assert len((folder / (result["symbol"] + ".csv")).read_text().splitlines()) == 372
    expert = (folder / "UltraConcurrent.mq5").read_text()
    assert "#define DON_SLOT 0" in expert
    assert "book.module[1].max_losses=2;" in expert
    assert "MQL_TESTER" in expert and "SymbolInfoTick(_Symbol,quote)" in expert
    assert "OrderSend" not in expert and "OrderSend" not in (folder / "UltraTerminalProbe.mq5").read_text()
    assert "CopyTicksRange(" in (folder / "UltraTerminalProbe.mq5").read_text()
    assert package["package_hash"] in expert
    with pytest.raises(FileExistsError):
        ultra_tester.export(db, package, folder)
    bad = deepcopy(package)
    bad["policy"]["modules"][0]["steps"] += 1
    with pytest.raises(ValueError, match="diverge"):
        ultra_tester.export(db, bad, tmp_path / "bad")
    assert not (tmp_path / "bad").exists()


def fixture_rows(db, package):
    modules, frames = ultra_tester._context(db, package)
    header = ("BEGIN", package["package_hash"], package["source_manifest"], digest(ultra_tester._templates()), 6324)
    records = [header]
    for _, _, kind, _, values in ultra_tester._expected_rows(frames, modules, package["policy"]):
        records.append((kind, *values))
    return records + [("END", len(frames), 0)]


def journal(rows, origin="strategy_tester"):
    return "\n".join("agent\t" + ultra_tester.MARKERS[origin] + "|".join(map(str, row)) for row in rows).encode("utf-16")


def test_first_divergence_in_partial_fill_and_cash_and_source_boundaries(campaign):
    db, package = campaign
    rows = fixture_rows(db, package)
    # 904 registros emitidos por UltraTerminalProbe no MT5 6324, sem cabeçalho e rodapé do journal.
    body = "".join("|".join(map(str, row)) + "\n" for row in rows[1:-1])
    assert sha256(body.encode()).hexdigest() == "5079da7d54157163c4eb19962d71bc873eaffbc3937b1d00dc702d16760a0d74"
    def compare(records, origin="strategy_tester"):
        return ultra_tester.compare(db, package, journal(records, origin), origin=origin)
    passed = compare(rows)
    assert (passed["frames_compared"], passed["events_compared"], passed["end_equity"]) == (372, 80, 93440)
    assert passed["native_order_parity"] == "pending"
    position = next(i for i, row in enumerate(rows) if row[0] == "EVENT" and row[5:7] == ("fill", "partial"))
    changed = list(rows)
    changed[position] = (*rows[position][:7], 2, *rows[position][8:])
    first = compare(changed)["first_divergence"]
    assert first["ordinal"] == 198 and first["field"] == "EVENT.steps" and first["observed"] == "2"
    changed = list(rows)
    changed[position+1] = ("MARK", -1, *rows[position+1][2:])
    assert compare(changed)["first_divergence"]["field"] == "MARK.cash"
    removed = rows[:position] + rows[position+2:]
    assert compare(removed)["first_divergence"]["ordinal"] == 198
    assert compare(rows, "terminal_history")["origin"] == "terminal_history"
    with pytest.raises(ValueError, match="origem"):
        ultra_tester.compare(db, package, journal(rows, "terminal_history"), origin="strategy_tester")
    for broken in [rows[:-1], rows + rows, [("BEGIN", "other", *rows[0][2:])] + rows[1:]]:
        with pytest.raises(ValueError, match="Journal"):
            compare(broken)
