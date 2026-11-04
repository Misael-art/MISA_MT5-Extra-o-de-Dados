"""BD13: identidade da família qualificada, sem versionar por calculador alheio."""
import copy
import json
from pathlib import Path

import pandas as pd
import pytest

from mt5_extracao.analytics import (feature_store, indicator_context, indicator_materializer as materializer,
                                    indicator_registry as registry, producer_jobs, producers, resampling)
from mt5_extracao.strategies import indicators
from tests.test_indicator_materialization import db, facts, seed, source

RETURNS = ["price.return_1", "price.return_5", "price.return_20"]


def versions():
    return {ident: materializer.algorithm_version(ident) for ident in RETURNS}


def change_source(monkeypatch, module, transform):
    target = Path(module.__file__).resolve()
    original = Path.read_text
    def read(path, *args, **kwargs):
        content = original(path, *args, **kwargs)
        return transform(content) if path.resolve() == target else content
    monkeypatch.setattr(Path, "read_text", read)


def test_comments_line_endings_and_unrelated_top_level_functions_do_not_version_returns(monkeypatch):
    before = versions()
    change_source(monkeypatch, registry, lambda content:
        (content + "\n# novo calculador independente\ndef qa_unrelated(x):\n    return x * 2\n").replace("\n", "\r\n"))
    assert versions() == before
    assert len(set(before.values())) == 3  # contratos/horizontes têm identidades próprias


def test_changing_unrelated_legacy_formula_preserves_return_versions(monkeypatch):
    before = versions()
    def other_formula(content):
        old = "technical.keltner(ohlc, 20, 1.5)"
        assert old in content
        return content.replace(old, "technical.keltner(ohlc, 20, 2.0)", 1)
    change_source(monkeypatch, registry, other_formula)
    assert versions() == before


def test_shared_return_formula_change_versions_every_return_descendant(monkeypatch):
    before = versions()
    def formula(content):
        old = "return close / denominator.where(denominator > 0) - 1.0"
        assert old in content
        return content.replace(old, "return close / denominator.where(denominator > 0) - 2.0")
    change_source(monkeypatch, registry, formula)
    after = versions()
    assert all(after[ident] != before[ident] for ident in RETURNS)


def test_changing_A_contract_creates_only_A_version_keeps_B_and_explicit_legacy_context(db, monkeypatch):
    seed(db, source(30))
    ids = RETURNS[:2]
    materializer.materialize(db, "TEST", "M5", feature_ids=ids)
    old_versions = versions()
    old_a = feature_store.query_history(db, symbol="TEST", feature_id=ids[0], algorithm_version=old_versions[ids[0]])
    old_b = feature_store.query_history(db, symbol="TEST", feature_id=ids[1])
    definitions = copy.deepcopy(registry._BY_ID)
    definitions[ids[0]]["warmup_count"] = 3
    monkeypatch.setattr(registry, "_BY_ID", definitions)
    new_versions = versions()
    assert new_versions[ids[0]] != old_versions[ids[0]]
    assert new_versions[ids[1]] == old_versions[ids[1]]
    result = materializer.materialize(db, "TEST", "M5", feature_ids=ids)
    assert result["inserted"] == 30 and result["revised"] == 0 and result["unchanged"] == 30
    pd.testing.assert_frame_equal(old_a, feature_store.query_history(db, symbol="TEST", feature_id=ids[0],
                                                                    algorithm_version=old_versions[ids[0]]))
    pd.testing.assert_frame_equal(old_b, feature_store.query_history(db, symbol="TEST", feature_id=ids[1]))
    decisions = pd.DataFrame({"decision_time": ["2026-09-01T09:10Z"]})
    current = indicator_context.context(db, decisions, symbol="TEST", timeframe="M5", feature_ids=ids).iloc[0]
    assert current[ids[0] + "::state"] == "WARMUP"
    assert current[ids[0] + "::algorithm_version"] == new_versions[ids[0]]
    assert current[ids[1] + "::algorithm_version"] == old_versions[ids[1]]
    legacy = indicator_context.context(db, decisions, symbol="TEST", timeframe="M5", feature_ids=[ids[0]],
                                       algorithm_version=old_versions[ids[0]]).iloc[0]
    assert legacy[ids[0] + "::state"] == "AVAILABLE"
    assert legacy[ids[0] + "::value"] == 101. / 100. - 1


def test_unrelated_technical_calculator_does_not_bypass_checkpoint_or_read_history(db, monkeypatch):
    seed(db, source(30))
    materializer.materialize(db, "TEST", "M5", feature_ids=RETURNS)
    before = facts(db)
    change_source(monkeypatch, indicators, lambda content: content + "\ndef qa_new_indicator(x):\n    return x * 2\n")
    monkeypatch.setattr(resampling, "latest_bars", lambda *a, **k: pytest.fail("releu histórico após edição alheia"))
    result = materializer.materialize(db, "TEST", "M5", feature_ids=RETURNS)
    assert result["skipped"] and result["job"]["status"] == "SKIPPED"
    assert facts(db) == before


@pytest.mark.parametrize("old,new", [
    ("int(window) < 1", "int(window) < 2"),
    ('int(values[positions["revision"]])', 'int(values[positions["revision"]]) + 1'),
])
def test_source_revision_and_availability_components_version_returns(monkeypatch, old, new):
    before = versions()
    def changed(content):
        assert old in content
        return content.replace(old, new)
    change_source(monkeypatch, producers, changed)
    assert all(versions()[ident] != before[ident] for ident in RETURNS)


def test_missing_component_refuses_instead_of_falling_back_to_global(monkeypatch):
    change_source(monkeypatch, registry, lambda content: content.replace("def _return_series(", "def _missing_series("))
    with pytest.raises(ValueError, match="Componentes.*_return_series"):
        materializer.implementation_signature(RETURNS[0])


def test_unqualified_family_keeps_global_scheme_and_unknown_id_is_not_a_fallback():
    assert materializer.implementation_signature("momentum.rsi_14") == materializer.implementation_signature()
    assert materializer.implementation_scheme("momentum.rsi_14") == "legacy_modules_v1"
    with pytest.raises(ValueError, match="desconhecido"):
        materializer.implementation_signature("qa.unknown")


def test_return_formula_and_denominator_use_the_same_declared_horizon(monkeypatch):
    definitions = copy.deepcopy(registry._BY_ID)
    definitions[RETURNS[0]]["parameters"]["horizon_bars"] = 2
    definitions[RETURNS[0]]["warmup_count"] = 3
    monkeypatch.setattr(registry, "_BY_ID", definitions)
    monkeypatch.setattr(registry, "_legacy_calculated_series", lambda *a, **k: pytest.fail("calculou outra família"))
    rows = list(registry.iter_core(pd.DataFrame({"close": [0., 100., 110., 120.]}), feature_ids=[RETURNS[0]]))
    assert rows[2]["state"] == "INVALID_DENOMINATOR" and rows[2]["value"] is None
    assert rows[3]["value"] == 120. / 100. - 1


def test_registered_scheme_checkpoint_and_alias_paths_are_explicit_and_portable(db, monkeypatch):
    seed(db, source(25))
    materializer.materialize(db, "TEST", "M5", feature_ids=RETURNS)
    state = producer_jobs.get_checkpoint(db, "TEST", "M5", materializer.FAMILY)["state"]
    assert set(state["implementations"]) == set(RETURNS)
    assert len(set(state["implementations"].values())) == 3
    definitions = feature_store.list_definitions(db)
    assert all(item["parameters"]["implementation_scheme"] == "return_components_v1" for item in definitions)
    metadata = json.loads(feature_store.query_history(db, symbol="TEST").iloc[0].metadata_json)
    assert metadata["implementation_scheme"] == "return_components_v1"
    before = versions()
    filename = Path(materializer.__file__).resolve()
    monkeypatch.setattr(materializer, "__file__", str(filename.parent / ".." / "analytics" / filename.name))
    assert versions() == before


def test_selection_reads_component_sources_once_without_retaining_a_cross_call_cache(monkeypatch):
    expected = {ident: materializer.implementation_signature(ident) for ident in RETURNS}
    paths = {Path(module.__file__).resolve() for module in (registry, materializer, producers)}
    original = Path.read_text
    reads = []
    def read(path, *args, **kwargs):
        if path.resolve() in paths:
            reads.append(path.resolve())
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, "read_text", read)
    assert materializer._implementation_signatures(RETURNS) == expected
    assert len(reads) == 3 and set(reads) == paths
    assert materializer._implementation_signatures(RETURNS) == expected
    assert len(reads) == 6  # fonte continua conferida em cada operação
    with pytest.raises(ValueError, match="desconhecido"):
        materializer._implementation_signatures(["momentum.rsi_14", "qa.unknown"])
