"""BD13: cada nó liga operador/contrato/pais à proveniência, sem catálogo alheio."""
import json
from pathlib import Path

import pandas as pd
import pytest

from mt5_extracao.analytics import datasets, feature_store, features, indicator_context, indicator_materializer
from tests.test_feature_compositions import A, ABS, ALL, B, DIFF, db, define, evaluate, freeze, put, source


def ready(source):
    define(source)
    put(source, A, 1.)
    put(source, B, 3.)
    return freeze(source)


def rewrite(monkeypatch, module, transform):
    target = Path(module.__file__).resolve()
    original = Path.read_text
    def read(path, *args, **kwargs):
        content = original(path, *args, **kwargs)
        return transform(content) if path.resolve() == target else content
    monkeypatch.setattr(Path, "read_text", read)


def test_old_recipe_schema_is_preserved_and_each_node_records_its_computation_signature(source):
    snapshot = ready(source)
    before = feature_store.query_history(source)
    result = evaluate(snapshot, ["2025-01-01T00:02Z"])
    assert result["composition_version"] == 2 and result["signature_scheme"] == "frozen_node_components_v1"
    definitions = {item["feature_id"]: item for item in result["definitions"]}
    for ident in (DIFF[0], ABS[0]):
        assert definitions[ident]["parameters"]["composition"]["schema_version"] == 1
        row = result["context"][0]
        signature = row[ident + "::computation_sha256"]
        assert len(signature) == 64 and signature == definitions[ident]["computation_sha256"]
        assert json.loads(row[ident + "::metadata_json"])["computation_sha256"] == signature
    pd.testing.assert_frame_equal(before, feature_store.query_history(source))


@pytest.mark.parametrize("module", [features, indicator_context, datasets, feature_store, indicator_materializer])
def test_unrelated_module_additions_do_not_change_frozen_result(source, monkeypatch, module):
    snapshot = ready(source)
    expected = evaluate(snapshot, ["2025-01-01T00:02Z"])
    parents = {ident: indicator_materializer.algorithm_version(ident)
               for ident in ("price.return_1", "price.return_5", "price.return_20")}
    rewrite(monkeypatch, module, lambda content: content + "\n\ndef qa_unrelated(x):\n    return x * 2\n")
    assert evaluate(snapshot, ["2025-01-01T00:02Z"]) == expected
    assert {ident: indicator_materializer.algorithm_version(ident) for ident in parents} == parents


@pytest.mark.parametrize("operator,old,new,changed", [
    ("absolute", "return abs(values[0])", "return float(abs(values[0]))", [ABS[0]]),
    ("difference", "return values[0] - values[1]", "return float(values[0]) - float(values[1])", [DIFF[0], ABS[0]]),
])
def test_operator_source_versions_only_its_descendants_and_binds_provenance_even_for_equal_values(
        source, monkeypatch, operator, old, new, changed):
    snapshot = ready(source)
    before = evaluate(snapshot, ["2025-01-01T00:02Z"])
    def change(content):
        assert old in content
        return content.replace(old, new)
    rewrite(monkeypatch, features, change)
    after = evaluate(snapshot, ["2025-01-01T00:02Z"])
    for ident in (DIFF[0], ABS[0]):
        a, b = before["context"][0], after["context"][0]
        assert a[ident + "::value"] == b[ident + "::value"]
        assert (a[ident + "::computation_sha256"] != b[ident + "::computation_sha256"]) == (ident in changed)
        assert (a[ident + "::source_revision"] != b[ident + "::source_revision"]) == (ident in changed)
    assert before["implementation_sha256"] != after["implementation_sha256"]


def test_live_selection_code_is_outside_frozen_dependency_graph(source, monkeypatch):
    snapshot = ready(source)
    before = evaluate(snapshot, ["2025-01-01T00:02Z"])
    def change(content):
        old = "if algorithm_version is None and set(ids) - local_ids:"
        assert old in content
        return content.replace(old, "if algorithm_version is None and bool(set(ids) - local_ids):")
    rewrite(monkeypatch, indicator_context, change)
    assert evaluate(snapshot, ["2025-01-01T00:02Z"]) == before


def test_asof_policy_change_versions_all_computed_descendants(source, monkeypatch):
    snapshot = ready(source)
    before = evaluate(snapshot, ["2025-01-01T00:02Z"])
    def change(content):
        assert "allow_exact_matches=True" in content
        return content.replace("allow_exact_matches=True", "allow_exact_matches=False")
    rewrite(monkeypatch, feature_store, change)
    after = evaluate(snapshot, ["2025-01-01T00:02Z"])
    for ident in (DIFF[0], ABS[0]):
        assert before["context"][0][ident + "::computation_sha256"] != after["context"][0][ident + "::computation_sha256"]


def test_adding_independent_target_changes_evaluation_but_keeps_original_node_identity(source):
    define(source); put(source, A, 1.); put(source, B, 3.)
    other = ("qa.other", "v1")
    features.register_composite(source, other[0], "Outro ramo", other[1], operation="absolute", inputs=[A])
    snapshot = freeze(source, ALL + [other])
    before = evaluate(snapshot, ["2025-01-01T00:02Z"])
    after = evaluate(snapshot, ["2025-01-01T00:02Z"], targets=[ABS, other])
    for name, value in before["context"][0].items():
        assert after["context"][0][name] == value
    assert before["evaluation_id"] != after["evaluation_id"]


def test_unused_operator_catalog_entry_is_outside_selected_graph(source, monkeypatch):
    snapshot = ready(source)
    before = evaluate(snapshot, ["2025-01-01T00:02Z"])
    monkeypatch.setitem(features._COMPOSITE_OPERATORS, "qa_unused", dict(arity=1, formula="unused", evaluate=lambda x: x[0]))
    assert evaluate(snapshot, ["2025-01-01T00:02Z"]) == before


def test_empty_decisions_comments_and_module_alias_preserve_algorithm_identity(source, monkeypatch):
    snapshot = ready(source)
    before = evaluate(snapshot, [])
    rewrite(monkeypatch, features, lambda content: (content + "\n# comentário\n").replace("\n", "\r\n"))
    filename = Path(features.__file__).resolve()
    monkeypatch.setattr(features, "__file__", str(filename.parent / ".." / "analytics" / filename.name))
    assert evaluate(snapshot, []) == before


def test_actual_frozen_window_is_bound_to_evaluation_not_arithmetic_identity(source, monkeypatch):
    snapshot = ready(source)
    before = evaluate(snapshot, ["2025-01-01T00:02Z"])
    original = datasets.Snapshot.feature_window_utc.fget
    monkeypatch.setattr(datasets.Snapshot, "feature_window_utc", property(lambda snap:
        (original(snap)[0], original(snap)[1] + pd.Timedelta(minutes=1))))
    after = evaluate(snapshot, ["2025-01-01T00:02Z"])
    assert before["implementation_sha256"] == after["implementation_sha256"]
    assert before["evaluation_id"] != after["evaluation_id"] and before["window_utc"] != after["window_utc"]


def test_missing_operator_implementation_fails_without_using_another_calculator(source, monkeypatch):
    snapshot = ready(source)
    rewrite(monkeypatch, features, lambda content: content.replace("def _absolute(", "def _missing_absolute("))
    with pytest.raises(ValueError, match="Componentes.*_absolute"):
        evaluate(snapshot, ["2025-01-01T00:02Z"])
