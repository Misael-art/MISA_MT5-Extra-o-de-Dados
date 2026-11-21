import json

import pytest

from mt5_extracao import b3_public, cli, sdk
from mt5_extracao.analytics import datasets, instruments
from mt5_extracao.database_manager import DatabaseManager
from tests.test_backtester import bars


SYMBOL = "WINZ26"
CUTOFF = "2026-10-26T10:01:00Z"
PLAN = {"version": 1, "mode": "fixed_adverse", "side": "long", "anchor_ticks": 20000, "stop_ticks": 19940,
        "spacing": 10, "volumes": [1] * 6, "currency": "BRL", "risk_cap_cash": 210,
        "round_trip_cost_per_lot": 0, "adverse_stop_ticks": 0}


@pytest.fixture
def frozen_grid(tmp_path):
    path = tmp_path / "grid.sqlite"
    db = DatabaseManager(db_path=str(path), broker_utc_offset=0)
    data = bars([[100000, 100010, 99990, 100005]], start="2026-10-26 10:00", freq="min")
    assert db.save_ohlcv_data(SYMBOL, "1 minuto", data.assign(tick_volume=1, real_volume=1))
    spec = b3_public.spec_for(SYMBOL)
    rev = instruments.record(db, SYMBOL, "specification", spec, source="fixture",
                             known_at="2026-10-01T00:00:00Z", valid_from="2026-10-01T00:00:00Z")
    instruments.record(db, SYMBOL, "specification", {**spec, "tick_value": 2}, source="fixture",
                       known_at="2026-10-27T00:00:00Z", valid_from="2026-10-02T00:00:00Z")
    manifest = datasets.capture(db, SYMBOL, "M1").manifest_id
    try:
        yield db, path, manifest, rev
    finally:
        db.close()


def test_win_grid_reconciles_six_levels_and_further_adverse_mark(frozen_grid, tmp_path, capsys):
    db, path, manifest, revision = frozen_grid
    intent = tmp_path / "plan.json"
    intent.write_text(json.dumps(PLAN), encoding="utf-8")
    (tmp_path / "manifest.txt").write_text(manifest, encoding="utf-8")
    assert cli.main(["--db", str(path), "--json", "analytics", "grid-risk", "--manifest", manifest,
                     "--file", str(intent), "--as-of", CUTOFF]) == 0
    grid = json.loads(capsys.readouterr().out)["data"]["grid_risk"]
    assert grid["specification_revision"] == revision
    assert grid["spacing_ticks"] == 10 and grid["tick_size"] == 5
    assert [row["price_ticks"] for row in grid["levels"]] == [20000, 19990, 19980, 19970, 19960, 19950]
    assert grid["levels"][-1]["gross_mark_at_level_cash"] == -150
    assert grid["loss_at_stop_cash"] == 210 and grid["total_volume"] == 6
    assert grid["margin_status"] == grid["execution_status"] == "unassessed"
    assert grid["risk_status"] == "within_modeled_cap"
    stressed = sdk.grid_risk(db, manifest, {**PLAN, "adverse_stop_ticks": 3, "round_trip_cost_per_lot": 2},
                             as_of=CUTOFF)
    assert stressed["loss_at_stress_cash"] == 240
    assert stressed["stress_stop_ticks"] == 19937
    assert stressed["risk_status"] == "exceeds_modeled_cap"


@pytest.mark.parametrize(("mode", "spacing", "lots", "entries", "intents"), [
    ("direct", 0, [1], [20000], ["entry"]),
    ("fixed_adverse", 5, [1, 2], [20000, 20005], ["entry", "limit_entry"]),
    ("atr_adverse", {"atr_in_ticks": 3.5, "multiplier": 1.25}, [1, 2], [20000, 20005], ["entry", "limit_entry"]),
    ("directional", 5, [1, 2], [20000, 19995], ["entry", "stop_entry"]),
])
def test_grid_projects_short_mirrors_and_declared_lot_vectors(frozen_grid, mode, spacing, lots, entries, intents):
    db, _, manifest, _ = frozen_grid
    plan = {**PLAN, "mode": mode, "spacing": spacing, "volumes": lots, "side": "short", "stop_ticks": 20015}
    result = sdk.grid_risk(db, manifest, plan, as_of=CUTOFF)
    assert [row["price_ticks"] for row in result["levels"]] == entries
    assert [row["intent"] for row in result["levels"]] == intents
    assert result["loss_at_stop_cash"] == sum((20015 - price) * lot for price, lot in zip(entries, lots))


@pytest.mark.parametrize(("overrides", "message"), [
    ({"currency": "USD"}, "moeda"),
    ({"stop_ticks": 19950}, "Nível 5"),
    ({"volumes": [1, 1.5]}, "passo"),
    ({"adverse_stop_ticks": True}, "inteiro"),
    ({"partial": True}, "desconhecidos"),
])
def test_grid_rejects_unusable_plan(frozen_grid, overrides, message):
    db, _, manifest, _ = frozen_grid
    with pytest.raises(ValueError, match=message):
        sdk.grid_risk(db, manifest, {**PLAN, **overrides}, as_of=CUTOFF)
