"""LAB-01: inventário real (registry + definições de Dados + fatos), sem fixture como dado real."""
import pytest

from mt5_extracao.analytics import feature_store, indicator_registry
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.laboratory import inventory


@pytest.fixture
def db(tmp_path):
    database = DatabaseManager(db_path=str(tmp_path / "inv.sqlite"), broker_utc_offset=0)
    yield database
    database.close()


def test_every_registry_definition_is_listed_without_database():
    result = inventory.search(page_size=200)
    assert result["total"] == len(indicator_registry.definitions())
    assert {row["feature_id"] for row in result["items"]} == {row["feature_id"] for row in indicator_registry.definitions()}
    assert all(row["state"] == "COMPUTABLE" and row["action"] and row["outputs"] for row in result["items"])


def test_new_registry_definition_appears_without_editing_selector(monkeypatch):
    original = indicator_registry.definitions()
    extra = dict(original[0], feature_id="momentum.rsi_99", name="RSI (99)", parameters=dict(period=99))
    monkeypatch.setattr(indicator_registry, "definitions", lambda: [*original, extra])
    monkeypatch.setattr(indicator_registry, "get_definition",
                        lambda ident: dict(extra) if ident == extra["feature_id"] else
                        next(dict(row) for row in original if row["feature_id"] == ident))
    found = inventory.search(query="rsi 99")
    assert [row["feature_id"] for row in found["items"]] == ["momentum.rsi_99"]


def test_data_store_variants_and_facts_carry_state_reason_and_source(db):
    spec = indicator_registry.get_definition("momentum.rsi_14")
    feature_store.register_definition(db, "momentum.rsi_14", spec["name"], spec["algorithm_version"],
                                      spec["parameters"], [], family="momentum")
    # SINTÉTICO: variante registrada por Dados sem fórmula neste código.
    feature_store.register_definition(db, "custom.super_atr_line_3", "Super ATR linha 3", "1.0",
                                      dict(line=3), [], family="custom", value_type="NUMBER")
    feature_store.register_definition(db, "custom.orphan", "Órfã", "1.0", {}, [], family="custom")
    for ident, version in (("momentum.rsi_14", spec["algorithm_version"]), ("custom.super_atr_line_3", "1.0")):
        feature_store.save_value(db, "SYN", "H1", ident, version, "2025-01-06T10:00:00Z", 50.0,
                                 available_at="2025-01-06T11:00:00Z", observed_at="2025-01-06T11:00:00Z")
    rows = {row["feature_id"]: row for row in inventory.entries(db)}
    assert rows["momentum.rsi_14"]["state"] == "STORED"
    assert rows["momentum.rsi_14"]["series"][0] == dict(symbol="SYN", timeframe="H1", facts=1,
        first=rows["momentum.rsi_14"]["series"][0]["first"], last=rows["momentum.rsi_14"]["series"][0]["last"],
        last_available_at=rows["momentum.rsi_14"]["series"][0]["last_available_at"])
    assert rows["custom.super_atr_line_3"]["state"] == "STORED_ONLY"
    assert rows["custom.orphan"]["state"] == "UNAVAILABLE" and rows["custom.orphan"]["action"]
    assert inventory.search(db, symbol="SYN", timeframe="H1")["matched"] == 2


def test_pagination_is_stable_and_cursor_bound_to_filters():
    first = inventory.search(page_size=10)
    second = inventory.search(page_size=10, cursor=first["next_cursor"])
    assert not {row["feature_id"] for row in first["items"]} & {row["feature_id"] for row in second["items"]}
    with pytest.raises(ValueError, match="outros filtros"):
        inventory.search(query="rsi", page_size=10, cursor=first["next_cursor"])
    empty = inventory.search(query="inexistente-xyz")
    assert empty["matched"] == 0 and "limpe a busca" in empty["reason"]
