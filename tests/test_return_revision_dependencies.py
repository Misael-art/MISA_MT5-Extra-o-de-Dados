"""BD13: fatos de retorno revisam somente os extremos; ordinalidade continua causal."""
import json

import pandas as pd
import pytest

from mt5_extracao.analytics import feature_store, indicator_materializer as materializer, resampling
from tests.test_indicator_materialization import db, facts, seed, source

RETURNS = ["price.return_1", "price.return_5", "price.return_20"]


def materialize(db, **kwargs):
    return materializer.materialize(db, "TEST", "M5", feature_ids=RETURNS, **kwargs)


def correct(db, frame, position):
    changed = frame.iloc[[position * 5 + 4]].copy()
    changed["close"] += 5
    changed["high"] = changed["close"] + 1
    seed(db, changed)
    bars = resampling.latest_bars(db, "TEST", "M5")
    return bars, bars.loc[bars.bar_start_utc == pd.Timestamp(frame.iloc[position * 5].time, tz="UTC"),
                          "computed_at_utc"].iloc[0]


@pytest.mark.parametrize("position", [25, 30, 79])
def test_correction_revises_only_each_horizon_endpoint_and_keeps_historical_asof(db, position):
    frame = source(80)
    seed(db, frame)
    materialize(db)
    known = pd.Timestamp.now(tz="UTC")
    before = feature_store.query_as_of(db, known, symbol="TEST")
    bars, receipt = correct(db, frame, position)
    result = materialize(db)
    after = feature_store.query_as_of(db, receipt, symbol="TEST")
    actual = after[after.revision > 1]
    expected = {(ident, bars.iloc[i].bar_start_utc) for h, ident in zip((1, 5, 20), RETURNS)
                for i in (position, position + h) if i < len(bars)}
    assert set(zip(actual.feature_id, actual.observation_time)) == expected
    assert result["revised"] == len(expected) and result["inserted"] == 0
    pd.testing.assert_frame_equal(before, feature_store.query_as_of(db, known, symbol="TEST"))
    assert actual.available_at_utc.ge(receipt).all()
    closes = bars.close.tolist()
    for h, ident in zip((1, 5, 20), RETURNS):
        stored = after[after.feature_id == ident].set_index("observation_time")
        for i, when in enumerate(bars.bar_start_utc):
            got = stored.loc[when, "numeric_value"]
            expected_value = None if i < h else closes[i] / closes[i - h] - 1
            assert (expected_value is None and pd.isna(got)) or got == expected_value
    assert materialize(db)["skipped"]


def test_later_correction_outside_horizon_does_not_delay_a_relevant_revision(db):
    frame = source(60)
    seed(db, frame)
    materialize(db)
    bars, relevant_receipt = correct(db, frame, 40)
    _, unrelated_receipt = correct(db, frame, 10)
    assert unrelated_receipt > relevant_receipt
    materialize(db)
    at_relevant = feature_store.query_as_of(db, relevant_receipt, symbol="TEST", feature_id="price.return_1")
    target = at_relevant.set_index("observation_time").loc[bars.iloc[40].bar_start_utc]
    assert target.revision == 2 and target.available_at_utc == relevant_receipt
    now = feature_store.query_as_of(db, unrelated_receipt, symbol="TEST", feature_id="price.return_1")
    pd.testing.assert_series_equal(target, now.set_index("observation_time").loc[bars.iloc[40].bar_start_utc])


def test_removing_and_restoring_interior_bar_cannot_backdate_ordinal_changes(db, monkeypatch):
    seed(db, source(30))
    materialize(db)
    original = feature_store.query_as_of(db, "2026-09-02T00:00Z", symbol="TEST")
    raw = resampling.latest_bars(db, "TEST", "M5", closed_only=False)
    receipt = pd.Timestamp("2026-10-20T12:00Z")
    raw.loc[7, ["revision", "computed_at_utc", "quality_status"]] = [2, receipt, "PARTIAL"]
    closed = raw.drop(index=7).reset_index(drop=True)
    monkeypatch.setattr(resampling, "latest_source_sequence", lambda *a, **k: 2)
    monkeypatch.setattr(resampling, "latest_bars", lambda *a, closed_only=True, **k:
                        closed.copy() if closed_only else raw.copy())
    materialize(db)
    pd.testing.assert_frame_equal(original, feature_store.query_as_of(db, receipt - pd.Timedelta(nanoseconds=1), symbol="TEST"))
    after = feature_store.query_as_of(db, receipt, symbol="TEST", feature_id="price.return_5")
    row = after.set_index("observation_time").loc[raw.iloc[10].bar_start_utc]
    assert row.numeric_value == 110. / 104. - 1
    assert row.available_at_utc == receipt
    assert after.set_index("observation_time").loc[raw.iloc[7].bar_start_utc].quality == "INVALIDATED"
    restored_at = receipt + pd.Timedelta(minutes=1)
    raw.loc[7, ["revision", "computed_at_utc", "quality_status"]] = [3, restored_at, "CORRECTED"]
    closed = raw.copy()
    monkeypatch.setattr(resampling, "latest_source_sequence", lambda *a, **k: 3)
    materialize(db)
    historical = feature_store.query_as_of(db, restored_at - pd.Timedelta(nanoseconds=1), symbol="TEST",
                                         feature_id="price.return_5")
    pd.testing.assert_frame_equal(after, historical)
    restored = feature_store.query_as_of(db, restored_at, symbol="TEST", feature_id="price.return_5")
    row = restored.set_index("observation_time").loc[raw.iloc[10].bar_start_utc]
    assert row.numeric_value == 110. / 105. - 1 and row.available_at_utc == restored_at


def test_adding_other_horizons_preserves_first_branch_and_explicit_source_provenance(db):
    seed(db, source(30))
    materializer.materialize(db, "TEST", "M5", feature_ids=[RETURNS[0]])
    before = facts(db)
    result = materialize(db)
    assert result["inserted"] == 60 and result["revised"] == 0
    assert all(row in facts(db) for row in before)
    rows = feature_store.query_history(db, symbol="TEST", feature_id=RETURNS[0])
    metadata = json.loads(rows.iloc[-1].metadata_json)
    assert metadata["source_scope"] == "return_endpoints_with_eligible_bar_order"
    assert metadata["horizon_bars"] == 1 and len(metadata["dependency_bar_starts_utc"]) == 2


def test_interrupted_correction_resumes_without_duplicating_committed_endpoint_revisions(db, monkeypatch):
    frame = source(60)
    seed(db, frame)
    materialize(db)
    correct(db, frame, 30)
    save = feature_store.save_values
    failed = False
    def fail_after_commit(db_, records, **kwargs):
        nonlocal failed
        result = save(db_, records, **kwargs)
        if result["revised"] and not failed:
            failed = True
            raise RuntimeError("perda do recibo após commit")
        return result
    monkeypatch.setattr(feature_store, "save_values", fail_after_commit)
    with pytest.raises(RuntimeError, match="após commit"):
        materialize(db, batch_size=7)
    committed = facts(db)
    monkeypatch.setattr(feature_store, "save_values", save)
    assert materialize(db, batch_size=7)["job"]["status"] == "COMPLETE"
    assert all(row in facts(db) for row in committed)
    history = feature_store.query_history(db, symbol="TEST")
    assert history.revision.max() == 2 and history.revision.eq(2).sum() == 6
