"""BD06: todo o registry tem contrato completo; metadados não versionam valores."""
import json

from mt5_extracao.analytics import indicator_contracts, indicator_materializer, indicator_registry


def test_every_registry_definition_has_the_full_contract():
    assert indicator_contracts.missing_fields() == {}
    ids = {item["feature_id"] for item in indicator_contracts.contracts()}
    assert ids == {item["feature_id"] for item in indicator_registry.definitions()}
    json.dumps(indicator_contracts.contracts(), allow_nan=False)  # serializável para a futura API


def test_direction_codes_are_categories_even_if_stored_as_number():
    alignment = indicator_contracts.contract("trend.alligator_alignment")
    assert alignment["output_kind"] == "category" and alignment["value_type"] == "NUMBER"
    assert alignment["buffers"][0]["categories"]["1"] == "ALIGNED_UP"
    assert indicator_contracts.contract("price.return_1")["output_kind"] == "number"


def test_shift_is_presentation_not_value():
    jaw = indicator_contracts.contract("trend.alligator_jaw_13")
    assert "desenho deslocado 8" in jaw["alignment"] and "valor não" in jaw["alignment"]


def test_contract_metadata_is_outside_the_implementation_signature():
    sources = indicator_materializer.implementation_signature.__code__.co_names
    assert "indicator_contracts" not in sources


def test_pending_references_are_listed_not_hidden():
    pending = indicator_contracts.pending_references()
    assert "momentum.macd_line_12_26_9" in pending and "price.return_1" not in pending


def test_unknown_family_fails_loudly(monkeypatch):
    monkeypatch.setattr(indicator_contracts, "_FAMILIES", ())
    try:
        indicator_contracts.contract("price.return_1")
    except ValueError as exc:
        assert "sem contrato BD06" in str(exc)
    else:
        raise AssertionError("família sem contrato deveria falhar")
