"""BD01: o inventário de dados cobre cada definição calculável, sem item sem dono ou estado."""
import json
from collections import Counter
from pathlib import Path

import pytest

from mt5_extracao.analytics import indicator_registry, producers
from test_analytics_profiles import bars_frame

INVENTORY = Path(__file__).resolve().parents[1] / "docs" / "dados" / "inventario_bd01.json"
REQUIRED = ("id", "kind", "class", "bd", "owner_task", "stage", "gap", "test_needed")


@pytest.fixture(scope="module")
def inventory():
    return json.loads(INVENTORY.read_text(encoding="utf-8"))


def _covered(inventory):
    return Counter(ident for item in inventory["items"] for ident in item.get("feature_ids", ()))


def test_every_item_has_owner_state_gap_and_class(inventory):
    ids = [item["id"] for item in inventory["items"]]
    assert len(ids) == len(set(ids))
    for item in inventory["items"]:
        missing = [name for name in REQUIRED if not item.get(name)]
        assert not missing, (item["id"], missing)
        assert item["class"] in inventory["classes"], item["id"]
        assert set(item["stage"]) == {"contract", "implementation", "persistence", "query", "consumers"}


def test_registry_definitions_are_each_inventoried_once(inventory):
    covered = _covered(inventory)
    registry = {spec["feature_id"] for spec in indicator_registry.definitions()}
    assert registry - set(covered) == set(), "definição nova sem inventário BD01"
    assert [ident for ident in registry if covered[ident] != 1] == []


@pytest.mark.parametrize("volume_known", [True, False])
def test_producer_outputs_are_inventoried(inventory, volume_known):
    covered = _covered(inventory)
    bars = bars_frame(sessions=30, bars_per_session=12, real_known=volume_known)
    for family in producers.FAMILIES:
        rows, _ = producers._family_rows(family, bars, producers.DEFAULTS[family], "auto")
        produced = {row["feature_id"] for row in rows}
        assert produced, family
        assert produced - set(covered) == set(), (family, sorted(produced - set(covered)))


def test_natives_and_undefined_rules_stay_in_the_denominator(inventory):
    natives = sorted(item["id"] for item in inventory["items"] if item["kind"] == "native_mt5")
    assert natives == [f"N{index:02d}" for index in range(38)]
    undefined = {item["id"] for item in inventory["items"] if item["class"] == "C"}
    assert {"I03", "I05", "I17", "FRP"} <= undefined
    catalog = {item["id"] for item in inventory["items"] if item["kind"].startswith("catalog_")}
    assert {f"I{index:02d}" for index in range(1, 19)} <= catalog
