"""I06: referência racional independente, causalidade e publicação canônica."""
import json
from fractions import Fraction

import numpy as np
import pandas as pd
import pytest

from mt5_extracao.analytics import feature_store, indicator_context, indicator_materializer as materializer, indicator_registry, resampling
from mt5_extracao.data_exporter import DataExporter
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.strategies import indicators
from tests.test_indicator_materialization import db, facts, seed, source


LINES = (("jaw", 13, 8), ("teeth", 8, 5), ("lips", 5, 3))
IDS = [f"trend.alligator_{name}_{n}" for name, n, _ in LINES] + ["trend.alligator_alignment", "trend.alligator_separation"]


def independent_smma(prices, period):
    """Soma fechada de pesos racionais; não recorre ao último resultado calculado."""
    out = []
    segment = []
    q = Fraction(period - 1, period)
    for price in prices:
        if not np.isfinite(price):
            segment = []
        else:
            segment.append(Fraction(float(price)))
        if len(segment) < period:
            out.append(np.nan)
            continue
        k = len(segment) - period
        initial = sum(segment[:period]) / period
        value = initial * q ** k + sum(segment[period + j] * q ** (k - j - 1) / period for j in range(k))
        out.append(float(value))
    return np.array(out)


def independent_alligator(prices):
    out = {name: independent_smma(prices, n) for name, n, _shift in LINES}
    complete = np.isfinite(np.stack(list(out.values()))).all(axis=0)
    out["alignment"] = np.where(complete,
        ((out["lips"] > out["teeth"]) & (out["teeth"] > out["jaw"])).astype(float) -
        ((out["lips"] < out["teeth"]) & (out["teeth"] < out["jaw"])).astype(float), np.nan)
    out["separation"] = np.max(np.stack([out[x] for x in ("jaw", "teeth", "lips")]), axis=0) - np.min(
        np.stack([out[x] for x in ("jaw", "teeth", "lips")]), axis=0)
    return out


@pytest.mark.parametrize("period", [0, -1, True, 1.5])
def test_smma_rejects_invalid_period(period):
    with pytest.raises(ValueError, match="inteiro positivo"):
        indicators.smma(pd.Series([1., 2., 3.]), period)


def test_smma_has_manual_sma_seed_and_preserves_legacy_rma():
    prices = pd.Series([1., 3., 8., 7.], index=[10, 20, 30, 40])
    actual = indicators.smma(prices, 3)
    assert actual.iloc[:2].isna().all()
    assert actual.loc[30] == 4 and actual.loc[40] == 5
    assert indicators.rma(prices, 3).loc[30] != actual.loc[30]
    np.testing.assert_allclose(actual, independent_smma(prices, 3), rtol=2e-14, atol=2e-14, equal_nan=True)
    np.testing.assert_array_equal(indicators.smma(prices, 1), prices)


@pytest.mark.parametrize("prices,direction", [(np.full(40, 42.), 0), (np.arange(40.) + 100, 1), (200 - np.arange(40.), -1)])
def test_alligator_matches_independent_closed_weights_and_warmup(prices, direction):
    frame = pd.DataFrame({"high": prices + 2, "low": prices - 2, "close": prices * 2})
    actual = indicators.alligator(frame)
    expected = independent_alligator(prices)
    for name, period, _shift in LINES:
        assert actual[name].first_valid_index() == period - 1
        np.testing.assert_allclose(actual[name], expected[name], rtol=2e-14, atol=2e-14, equal_nan=True)
    assert actual.alignment.iloc[:12].isna().all()
    assert actual.alignment.iloc[12:].eq(direction).all()
    np.testing.assert_allclose(actual.separation, expected["separation"], rtol=2e-14, atol=2e-14, equal_nan=True)
    if direction == 0:
        assert actual.separation.iloc[12:].eq(0).all()


@pytest.mark.parametrize("bad", [np.nan, np.inf, -np.inf])
def test_missing_source_resets_seed_without_false_zero(bad):
    prices = np.arange(40.) + 100
    frame = pd.DataFrame({"high": prices + 1, "low": prices - 1})
    frame.loc[20, "high"] = bad
    actual = indicators.alligator(frame)
    for name, period, _shift in LINES:
        assert actual.loc[20:20 + period - 1, name].isna().all()
        assert pd.notna(actual.loc[20 + period, name])
    rows = indicator_registry.compute_core(frame, feature_ids=IDS)
    indexed = {(x["feature_id"], x["bar_index"]): x for x in rows}
    for ident in IDS:
        assert indexed[(ident, 20)]["state"] == "SOURCE_VALUE_MISSING"
        assert indexed[(ident, 20)]["value"] is None
        assert indexed[(ident, 21)]["reason"] == "REQUIRES_CONSECUTIVE_FINITE_MEDIAN_BARS"
    missing = indicator_registry.compute_core(frame.drop(columns="low"), feature_ids=IDS)
    assert all(x["state"] == "SOURCE_UNAVAILABLE" and x["value"] is None for x in missing)


def test_invalid_range_and_zero_price_have_explicit_distinct_states():
    frame = pd.DataFrame({"high": np.zeros(20), "low": np.zeros(20)})
    rows = indicator_registry.compute_core(frame, feature_ids=IDS)
    assert all(x["value"] == 0 for x in rows if x["state"] == "AVAILABLE")
    frame.loc[15, "low"] = 1
    rows = indicator_registry.compute_core(frame, feature_ids=IDS)
    assert all(x["state"] == "SOURCE_VALUE_MISSING" for x in rows if x["bar_index"] == 15)


def test_registry_prefix_and_plot_offsets_are_independent_of_future_prices():
    prices = np.array([100 + (i % 7) ** 2 / 4 + i / 8 for i in range(45)])
    frame = pd.DataFrame({"high": prices + 2, "low": prices - 2})
    before = indicator_registry.compute_core(frame.iloc[:24], feature_ids=IDS)
    after = indicator_registry.compute_core(frame, feature_ids=IDS)
    assert before == [row for row in after if row["bar_index"] < 24]
    for name, period, shift in LINES:
        ident = f"trend.alligator_{name}_{period}"
        spec = indicator_registry.get_definition(ident)
        assert spec["warmup_count"] == period and spec["parameters"]["plot_shift_bars"] == shift
        assert spec["parameters"]["value_basis"] == "unshifted_calculation_bar"
        assert all(row["display_offset_bars"] == shift for row in after if row["feature_id"] == ident)


def test_closed_calculation_time_is_not_replaced_by_visual_offset(db):
    seed(db, source(30))
    materializer.materialize(db, "TEST", "M5", feature_ids=IDS)
    for name, period, shift in LINES:
        ident = f"trend.alligator_{name}_{period}"
        start = pd.Timestamp("2026-09-01T09:00Z") + pd.Timedelta(minutes=5 * (period - 1))
        close = start + pd.Timedelta(minutes=5)
        before = indicator_context.context(db, pd.DataFrame({"decision_time": [close - pd.Timedelta(nanoseconds=1)]}),
                                          symbol="TEST", timeframe="M5", feature_ids=[ident])
        assert before.iloc[0][ident + "::state"] != "AVAILABLE"
        after = indicator_context.context(db, pd.DataFrame({"decision_time": [close]}), symbol="TEST", timeframe="M5", feature_ids=[ident])
        row = after.iloc[0]
        assert row[ident + "::state"] == "AVAILABLE"
        assert row[ident + "::available_at_utc"] == close
        meta = json.loads(row[ident + "::metadata_json"])
        assert pd.Timestamp(meta["calculation_bar_end_utc"]) == close
        assert pd.Timestamp(meta["display_anchor_bar_start_utc"]) == start
        assert meta["display_offset_bars"] == shift and meta["display_time_utc"] is None


def test_batch_append_reopen_store_and_csv_match_calculation_and_independent_reference(db, tmp_path):
    seed(db, source(25))
    materializer.materialize(db, "TEST", "M5", feature_ids=IDS, batch_size=13)
    prefix = facts(db)
    seed(db, source(45))
    appended = materializer.materialize(db, "TEST", "M5", feature_ids=IDS, batch_size=13)
    assert appended["write_from"] == 25 and appended["inserted"] == 20 * len(IDS) and appended["revised"] == 0
    assert all(x in facts(db) for x in prefix)
    final = facts(db)
    assert materializer.materialize(db, "TEST", "M5", feature_ids=IDS)["skipped"]
    assert final == facts(db)
    whole = DatabaseManager(db_path=str(tmp_path / "batch.sqlite")); whole.broker_utc_offset = 0
    try:
        seed(whole, source(45))
        materializer.materialize(whole, "TEST", "M5", feature_ids=IDS)
        assert facts(whole) == final
    finally:
        whole.close()
    db.close()
    reopened = DatabaseManager(db_path=db.db_path)
    try:
        assert materializer.materialize(reopened, "TEST", "M5", feature_ids=IDS)["skipped"]
        bars = resampling.latest_bars(reopened, "TEST", "M5")
        calculated = indicator_registry.compute_core(bars, feature_ids=IDS)
        path = tmp_path / "Alligator com espaço.csv"
        DataExporter(reopened).export_indicator_values(path, symbol="TEST", timeframe="M5", as_of="2026-10-19T00:00Z", feature_ids=IDS)
        csv = pd.read_csv(path, keep_default_na=False, float_precision="round_trip")
        assert len(csv) == 45
        references = independent_alligator(np.arange(45.) + 100)
        names = {f"trend.alligator_{name}_{period}": name for name, period, _ in LINES}
        names.update({"trend.alligator_alignment": "alignment", "trend.alligator_separation": "separation"})
        for ident in IDS:
            pure = np.array([x["value"] if x["value"] is not None else np.nan for x in calculated if x["feature_id"] == ident])
            stored = feature_store.query_as_of(reopened, "2026-10-19T00:00Z", symbol="TEST", timeframe="M5", feature_id=ident,
                                               algorithm_version=materializer.algorithm_version(ident))
            exported = np.array([float(value) if value != "" else np.nan for value in csv[ident + "::value"]])
            np.testing.assert_array_equal(stored.numeric_value.to_numpy(), pure)
            np.testing.assert_array_equal(exported, pure)
            np.testing.assert_allclose(pure, references[names[ident]], rtol=2e-14, atol=2e-14, equal_nan=True)
            assert list(csv[ident + "::state"]) == [x["state"] for x in calculated if x["feature_id"] == ident]
            assert all(json.loads(x)["value_basis"] == "unshifted_calculation_bar" for x in csv[ident + "::parameters"])
    finally:
        reopened.close()


def test_open_bar_cannot_publish_alligator_before_close(db, monkeypatch):
    seed(db, source(30))
    ident = "trend.alligator_lips_5"
    monkeypatch.setattr(resampling, "_utc_now", lambda: pd.Timestamp("2026-09-01T09:24:59Z"))
    materializer.materialize(db, "TEST", "M5", feature_ids=[ident])
    first = feature_store.latest_values(db, symbol="TEST", timeframe="M5", feature_ids=[ident])
    assert len(first) == 4 and first.numeric_value.isna().all()
    monkeypatch.setattr(resampling, "_utc_now", lambda: pd.Timestamp("2026-09-01T09:25:00Z"))
    result = materializer.materialize(db, "TEST", "M5", feature_ids=[ident])
    assert result["inserted"] == 1 and not result["skipped"]
    context = indicator_context.context(db, pd.DataFrame({"decision_time": ["2026-09-01T09:25Z"]}), symbol="TEST", timeframe="M5", feature_ids=[ident])
    assert context.iloc[0][ident + "::value"] == 102


def test_late_alligator_correction_preserves_previous_time_and_plot_anchor(db):
    frame = source(30)
    seed(db, frame)
    ident = "trend.alligator_jaw_13"
    materializer.materialize(db, "TEST", "M5", feature_ids=[ident])
    before = feature_store.query_as_of(db, "2026-09-02T00:00Z", symbol="TEST", feature_id=ident)
    changed = frame.iloc[[10]].copy(); changed["high"] += 20
    seed(db, changed)
    raw = resampling.latest_bars(db, "TEST", "M5")
    receipt = raw.loc[raw.revision > 1, "computed_at_utc"].max()
    materializer.materialize(db, "TEST", "M5", feature_ids=[ident])
    pd.testing.assert_frame_equal(before, feature_store.query_as_of(db, receipt - pd.Timedelta(nanoseconds=1), symbol="TEST", feature_id=ident))
    corrected = feature_store.query_as_of(db, receipt, symbol="TEST", feature_id=ident)
    revised = corrected[corrected.revision > 1]
    assert not revised.empty and revised.available_at_utc.ge(receipt).all()
    for row in revised.itertuples():
        meta = json.loads(row.metadata_json)
        assert pd.Timestamp(meta["calculation_bar_end_utc"]) < row.available_at_utc
        assert pd.Timestamp(meta["display_anchor_bar_start_utc"]) == row.observation_time
        assert meta["display_offset_bars"] == 8
