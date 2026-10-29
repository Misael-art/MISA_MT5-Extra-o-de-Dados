"""BD14: revisar observação antiga, sem confundi-la com a observação mais recente."""
import json

import pandas as pd
import pytest

from mt5_extracao import cli, sdk
from mt5_extracao.analytics import datasets, feature_store, features, indicator_context, indicator_registry
from tests.test_feature_compositions import A, ABS, B, DIFF, db, define, freeze, put, source

OBS = "2025-01-01T00:00Z"
LATE = "2025-01-02T00:00Z"


@pytest.fixture
def frozen(source):
    define(source)
    put(source, A, 1., valid_until="2025-01-01T00:01:30Z")
    put(source, B, 3.)
    put(source, A, 30., observation="2025-01-01T00:01Z", available="2025-01-01T00:02Z")
    put(source, B, 4., observation="2025-01-01T00:01Z", available="2025-01-01T00:02Z")
    put(source, A, 9., available="2025-01-01T00:03Z")
    put(source, B, 6., available=LATE)
    return freeze(source)


def compose(snapshot, times, *, observation=OBS):
    return features.compose_snapshot(snapshot, pd.DataFrame(dict(decision_time=times)),
                                     targets=[ABS], observation_time=observation)


def test_latest_observation_stays_latest_while_fixed_observation_replays_old_revisions(frozen):
    fixed = compose(frozen, ["2025-01-01T00:01Z", "2025-01-01T00:01:30Z", "2025-01-01T00:03Z", LATE])
    assert [row[ABS[0] + "::value"] for row in fixed["context"]] == [2., None, 6., 3.]
    latest = compose(frozen, ["2025-01-01T00:03Z"], observation=None)
    assert latest["context"][0][ABS[0] + "::value"] == 30. - 4.
    assert fixed["context"][2][DIFF[0] + "::value"] == 9. - 3.
    assert fixed["context"][3][DIFF[0] + "::value"] == 9. - 6.
    assert fixed["context_scope"]["mode"] == "fixed_observation"
    assert latest["context_scope"]["mode"] == "latest_observation"
    assert fixed["implementation_sha256"] == latest["implementation_sha256"]
    assert fixed["evaluation_id"] != latest["evaluation_id"]
    assert fixed["context"][0]["selected_observation_time"] == pd.Timestamp(OBS).isoformat()


def test_validity_is_preserved_in_both_levels_and_parent_provenance(frozen):
    result = compose(frozen, ["2025-01-01T00:01Z", "2025-01-01T00:01:30Z", "2025-01-01T00:03Z"])
    expiry = pd.Timestamp("2025-01-01T00:01:30Z").isoformat()
    for ident in (A[0], DIFF[0], ABS[0]):
        assert result["context"][0][ident + "::valid_until_utc"] == expiry
    assert result["context"][1][ABS[0] + "::value"] is None
    assert result["context"][1][ABS[0] + "::state"] == "UNAVAILABLE"
    assert result["context"][2][ABS[0] + "::valid_until_utc"] is None
    metadata = json.loads(result["context"][0][ABS[0] + "::metadata_json"])
    assert metadata["parents"][0]["valid_until_utc"] == expiry


def test_receipt_window_is_explicit_and_does_not_extend_default_market_coverage(frozen):
    result = compose(frozen, [LATE])
    assert result["window_utc"] == [pd.Timestamp(OBS).isoformat(), pd.Timestamp(LATE).isoformat()]
    with pytest.raises(ValueError, match="fora da janela"):
        compose(frozen, [LATE], observation=None)
    with pytest.raises(ValueError, match="fora da janela"):
        compose(frozen, ["2025-01-02T00:00:00.000000001Z"])
    with pytest.raises(ValueError, match="fora da janela"):
        compose(frozen, [LATE], observation="2025-01-01T00:01Z")


@pytest.mark.parametrize("observation,match", [
    ("2025-01-01T00:00", "fuso"),
    ("2024-12-31T23:59Z", "Observação fora"),
    ("2025-01-01T00:06Z", "Observação fora"),
    ("2025-01-01T00:02Z", "não consta"),
])
def test_invalid_or_absent_observations_are_not_invented(frozen, observation, match):
    with pytest.raises(ValueError, match=match):
        compose(frozen, ["2025-01-01T00:03Z"], observation=observation)


def test_frozen_observation_does_not_read_current_database_or_registry(source, frozen, monkeypatch):
    expected = compose(frozen, [LATE])
    put(source, A, 999., available="2025-01-02T00:01Z")
    monkeypatch.setattr(feature_store, "query_history", lambda *a, **k: pytest.fail("base atual"))
    monkeypatch.setattr(indicator_registry, "get_definition", lambda *a, **k: pytest.fail("registry atual"))
    assert compose(frozen, [LATE]) == expected


def test_empty_decisions_keep_scope_and_equivalent_offset_selects_same_observation(frozen):
    empty = compose(frozen, [])
    assert empty["context"] == [] and empty["context_scope"]["mode"] == "fixed_observation"
    assert compose(frozen, [LATE], observation="2024-12-31T21:00-03:00") == compose(frozen, [LATE])
    decisions = pd.DataFrame(dict(decision_time=[LATE], selected_observation_time=[OBS]))
    result = features.compose_snapshot(frozen, decisions, targets=[ABS], observation_time=OBS)
    assert result["context"][0][ABS[0] + "::value"] == 3.
    decisions["selected_observation_time"] = "2025-01-01T00:01Z"
    with pytest.raises(ValueError, match="outra origem"):
        features.compose_snapshot(frozen, decisions, targets=[ABS], observation_time=OBS)


def test_known_expiry_beyond_bars_is_a_finite_declared_event(source):
    define(source)
    put(source, A, 1., valid_until=LATE)
    put(source, B, 3.)
    snapshot = freeze(source)
    result = compose(snapshot, ["2025-01-01T00:06Z", LATE])
    assert result["context"][0][ABS[0] + "::value"] == 2.
    assert result["context"][0][ABS[0] + "::valid_until_utc"] == pd.Timestamp(LATE).isoformat()
    assert result["context"][1][ABS[0] + "::value"] is None
    with pytest.raises(ValueError, match="fora da janela"):
        compose(snapshot, ["2025-01-02T00:01Z"])


def test_superseded_expiry_does_not_extend_the_known_receipt_window(source):
    define(source)
    put(source, A, 1., valid_until="2025-01-03T00:00Z")
    put(source, B, 3.)
    put(source, A, 9., available=LATE)
    snapshot = freeze(source)
    assert compose(snapshot, [LATE])["window_utc"][1] == pd.Timestamp(LATE).isoformat()
    with pytest.raises(ValueError, match="fora da janela"):
        compose(snapshot, ["2025-01-03T00:00Z"])


def test_cli_and_sdk_replay_frozen_observation_with_old_recipe_and_no_child_writes(source, frozen, monkeypatch, capsys):
    monkeypatch.setattr(cli.Context, "db", property(lambda _: source))
    before = feature_store.query_history(source)
    assert cli.main(["--json", "analytics", "features", "compose", "--manifest", frozen.manifest_id,
        "--target", *ABS, "--as-of", LATE, "--observation", OBS]) == 0
    result = json.loads(capsys.readouterr().out)["data"]["composition"]
    assert result == sdk.query_composite_context(source, frozen.manifest_id, [LATE], targets=[ABS], observation_time=OBS)
    assert cli.main(["--json", "analytics", "features", "context", "--manifest", frozen.manifest_id,
        "--symbol", "QA", "--tf", "M1", "--features", A[0], "--algorithm-version", "v1",
        "--as-of", LATE, "--observation", OBS]) == 0
    raw = json.loads(capsys.readouterr().out)["data"]
    assert raw["context"][0][A[0] + "::value"] == 9.
    assert raw["snapshot_context_scope"]["mode"] == "fixed_observation"
    pd.testing.assert_frame_equal(before, feature_store.query_history(source))


def test_cli_observation_requires_frozen_manifest(source, monkeypatch, capsys):
    monkeypatch.setattr(cli.Context, "db", property(lambda _: source))
    assert cli.main(["--json", "analytics", "features", "context", "--symbol", "QA", "--tf", "M1",
        "--features", A[0], "--algorithm-version", "v1", "--as-of", LATE, "--observation", OBS]) != 0
    output = json.loads(capsys.readouterr().out)
    assert not output["ok"] and "exige --manifest" in json.dumps(output, ensure_ascii=False)


def test_import_keeps_original_receipt_scope_and_namespace(source, frozen, tmp_path):
    from mt5_extracao.database_manager import DatabaseManager
    path = tmp_path / "frozen.json"
    datasets.export_snapshot(source, frozen.manifest_id, path)
    other = DatabaseManager(db_path=str(tmp_path / "other.sqlite"), broker_utc_offset=-3)
    try:
        datasets.declare_source(other, dict(provider="other", server="Other"))
        imported = datasets.import_snapshot(other, path)
        assert compose(imported, [LATE]) == compose(frozen, [LATE])
    finally:
        other.close()
