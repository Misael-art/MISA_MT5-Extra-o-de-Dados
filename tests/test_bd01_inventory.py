"""BD01: o inventário cobre as definições calculáveis enumeráveis, sem item sem dono ou estado.

Alcance: registry, saídas de produtores, colunas do calculador legado, métodos de
AdvancedIndicators, funções públicas de strategies.indicators e, quando a fonte H é
informada em MT5X_HADOUKEN_ROOT, o catálogo nativo extraído do pin. Não protege
cálculos escritos fora desses pontos (ex.: privados de estratégias, já listados como AX).
"""
import hashlib
import inspect
import json
import os
import re
from collections import Counter
from pathlib import Path

import pytest

from mt5_extracao.advanced_indicators import AdvancedIndicators
from mt5_extracao.analytics import indicator_registry, producers
from mt5_extracao.indicator_calculator import IndicatorCalculator
from mt5_extracao.strategies import indicators as technical
from market_data import random_walk
from test_analytics_profiles import bars_frame

INVENTORY = Path(__file__).resolve().parents[1] / "docs" / "dados" / "inventario_bd01.json"
REQUIRED = ("id", "kind", "class", "bd", "owner_task", "stage", "gap", "test_needed")
NATIVE_CATALOG = re.compile(r"case (\d+):return (i\w+)\(symbol,tf,?([^;]*)\);")


@pytest.fixture(scope="module")
def inventory():
    return json.loads(INVENTORY.read_text(encoding="utf-8"))


def _ids(inventory, kind):
    return {item["id"] for item in inventory["items"] if item["kind"] == kind}


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
    """Confere os IDs emitidos (30 sessões emitem os mesmos 60 IDs de 130); não a maturidade numérica."""
    covered = _covered(inventory)
    bars = bars_frame(sessions=30, bars_per_session=12, real_known=volume_known)
    for family in producers.FAMILIES:
        rows, _ = producers._family_rows(family, bars, producers.DEFAULTS[family], "auto")
        produced = {row["feature_id"] for row in rows}
        assert produced, family
        assert produced - set(covered) == set(), (family, sorted(produced - set(covered)))


def test_legacy_outputs_and_pure_functions_have_children(inventory):
    frame = random_walk(300, freq="min")
    produced = set(IndicatorCalculator().calculate_technical_indicators(frame.copy()).columns) - set(frame.columns)
    assert {"LEG-IC." + column for column in produced} <= _ids(inventory, "legacy_column")
    methods = {name for name, _ in inspect.getmembers(AdvancedIndicators, inspect.isfunction) if not name.startswith("_")}
    assert {"LEG-ADV." + name for name in methods} == _ids(inventory, "legacy_method")
    public = {name for name, obj in inspect.getmembers(technical, inspect.isfunction)
              if not name.startswith("_") and obj.__module__ == technical.__name__}
    assert {"PURE." + name for name in public} == _ids(inventory, "pure_function")


def test_primary_sources_are_mapped(inventory):
    assert {"SRC-OHLCV", "SRC-DERIVED", "SRC-TICKS", "SRC-TRADES", "SRC-BOOK", "SRC-SPEC", "SRC-CAL",
            "SRC-QUALITY", "SRC-REV"} == _ids(inventory, "primary_source")


def test_undefined_rules_stay_in_the_denominator(inventory):
    items = {item["id"]: item for item in inventory["items"]}
    assert {i for i, item in items.items() if item["class"] == "C"} == {"AX03", "AX08", "FRP"}
    assert all(items[i]["pending_kind"] == "formula_missing" for i in ("AX03", "AX08", "FRP"))
    catalog = {i for i, item in items.items() if item["kind"].startswith("catalog_")}
    for index in range(1, 19):
        assert any(ident == f"I{index:02d}" or ident.startswith(f"I{index:02d}-") for ident in catalog), index


def test_native_catalog_matches_pinned_source(inventory):
    natives = sorted((item for item in inventory["items"] if item["kind"] == "native_mt5"),
                     key=lambda item: item["native_index"])
    assert [item["native_index"] for item in natives] == list(range(len(natives)))
    root = os.environ.get("MT5X_HADOUKEN_ROOT")
    if not root:
        pytest.skip("Fonte HADOUKEN não informada (MT5X_HADOUKEN_ROOT); comparação com o pin não executada.")
    source = Path(root) / inventory["sources"]["hadouken_study_catalog"]
    extracted = [(int(i), fn, params.strip()) for i, fn, params in NATIVE_CATALOG.findall(source.read_text())]
    assert extracted == [(item["native_index"], item["native_function"], item["variant"]) for item in natives]
    assert hashlib.sha256(source.read_bytes()).hexdigest() == inventory["sources"]["hadouken_study_catalog_sha256"]
