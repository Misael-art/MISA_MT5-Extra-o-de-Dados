import json

import pytest

from mt5_extracao import b3_public, cli, sdk
from mt5_extracao.analytics import datasets, instruments
from mt5_extracao.database_manager import DatabaseManager
from tests.test_backtester import bars


START = "2026-10-01T00:00:00Z"
CUTOFF = "2026-10-26T20:00:00Z"
PLAN = {"version": 1, "lower_cents": 1000, "upper_cents": 8000, "intervals": 3, "cash_cents": 1_500_001,
        "fee_per_buy_cents": 500, "mark_cents": 900, "installment_prices_cents": [8000, 5000, 4500]}


def frozen_asset(tmp_path, symbol):
    path = tmp_path / "cash.sqlite"
    db = DatabaseManager(db_path=str(path), **b3_public.CLOCK)
    data = bars([[80, 80.50, 79, 79.50]], start="2026-10-26", freq="D")
    assert b3_public.save(db, symbol, "1 dia", data.assign(tick_volume=1, real_volume=100))
    revision = instruments.record(db, symbol, "specification", b3_public.spec_for(symbol), source="fixture",
                                  known_at=START, valid_from=START)
    manifest = datasets.capture(db, symbol, "D1", source=b3_public.SOURCE).manifest_id
    db.close()
    return path, manifest, revision


def test_harmonic_cash_floors_lot_and_marks_inventory_with_same_initial_cash(tmp_path, capsys):
    path, manifest, revision = frozen_asset(tmp_path, "PETR4")
    plan = tmp_path / "harmonic.json"
    plan.write_text(json.dumps(PLAN), encoding="utf-8")
    (tmp_path / "manifest.txt").write_text(manifest, encoding="utf-8")
    assert cli.main(["--db", str(path), "--json", "analytics", "harmonic-cash", "--manifest", manifest,
                     "--as-of", CUTOFF, "--file", str(plan)]) == 0
    result = json.loads(capsys.readouterr().out)["data"]["harmonic_cash"]
    assert result["specification_revision"] == revision
    assert result["boundaries_cents"] == [8000, 4000, 2000, 1000]
    assert result["ideal_adjacent_ratio"] == 0.5
    assert result["below_floor"] and result["execution_status"] == "unassessed"
    arms = result["arms"]
    assert [order["units"] for order in arms["harmonic"]["allocations"]] == [100, 200, 400]
    assert arms["harmonic"]["fees_cents"] == 1500
    assert arms["harmonic"]["cash_cents"] == 298501
    assert arms["harmonic"]["equity_cents"] == 928501
    assert arms["buy_and_hold"]["units"] == 100
    assert len(arms["installments"]["allocations"]) == 3
    for arm in arms.values():
        assert arm["cash_cents"] >= 0
        assert arm["cash_cents"] + arm["nominal_turnover_cents"] + arm["fees_cents"] == PLAN["cash_cents"]
        assert arm["equity_cents"] == arm["cash_cents"] + arm["units"] * PLAN["mark_cents"]


def test_harmonic_cash_preserves_cash_below_minimum_and_rejects_repeated_ticks(tmp_path):
    path, manifest, _ = frozen_asset(tmp_path, "PETR4")
    db = DatabaseManager(db_path=str(path), **b3_public.CLOCK)
    try:
        proposal = {**PLAN, "cash_cents": 500}
        result = sdk.harmonic_cash(db, manifest, proposal, as_of=CUTOFF)
        assert all(arm["units"] == 0 and arm["cash_cents"] == 500 for arm in result["arms"].values())
        rounded = {**PLAN, "lower_cents": 1, "upper_cents": 4}
        with pytest.raises(ValueError, match="coincidem"):
            sdk.harmonic_cash(db, manifest, rounded, as_of=CUTOFF)
    finally:
        db.close()


def test_harmonic_cash_rejects_leveraged_contract(tmp_path):
    path, manifest, _ = frozen_asset(tmp_path, "WINZ26")
    db = DatabaseManager(db_path=str(path), **b3_public.CLOCK)
    try:
        with pytest.raises(ValueError, match="à vista"):
            sdk.harmonic_cash(db, manifest, PLAN, as_of=CUTOFF)
    finally:
        db.close()
