"""IND-CCI-01: independent window reference and the production consumers."""
import math

import numpy as np
import pandas as pd
import pytest

from mt5_extracao.advanced_indicators import AdvancedIndicators


def bars(values):
    prices = pd.Series(values, dtype=float)
    return pd.DataFrame({"high": prices + 1, "low": prices - 1, "close": prices})


def reference(values, n):
    result = []
    for i in range(len(values)):
        window = values[max(0, i - n + 1):i + 1]
        if len(window) != n or not all(math.isfinite(v) for v in window):
            result.append(float("nan"))
            continue
        mean = math.fsum(window) / n
        deviation = math.fsum(abs(v - mean) for v in window) / n
        result.append((window[-1] - mean) / (.015 * deviation) if deviation else 0.)
    return result


def test_original_increasing_series_and_first_eligible_bar():
    frame = bars(range(1, 28))
    actual = AdvancedIndicators().cci(frame.high, frame.low, frame.close, 14)
    np.testing.assert_allclose(actual, reference(list(range(1, 28)), 14), atol=1e-10)
    assert actual.first_valid_index() == 13
    assert actual.iloc[-1] == pytest.approx(123.80952380952381)


def test_translation_precision_and_exact_flat_output_for_linear_prices():
    from mt5_extracao.strategies.indicators import cci
    plain = bars(np.arange(60, dtype=float) / 8)
    shifted = plain + 2**40
    np.testing.assert_allclose(cci(shifted, 14), cci(plain, 14), rtol=1e-13, atol=1e-11)
    # A constant CCI should not turn roundoff into a spurious oscillating trace.
    values = cci(bars(range(100, 160)), 20).dropna()
    assert values.nunique() == 1


@pytest.mark.parametrize("values", [list(range(27, 0, -1)), [5.] * 30,
                                  [0.] * 30, [3., 1., 4., 1., 5., 9.] * 8])
def test_known_paths_and_prefix_equivalence(values):
    from mt5_extracao.strategies.indicators import cci
    frame = bars(values)
    expected = reference(values, 14)
    actual = cci(frame, 14)
    np.testing.assert_allclose(actual, expected, atol=1e-10)
    for i in range(len(frame)):
        tail = frame.iloc[max(0, i - 13):i + 1]
        prefix = cci(frame.iloc[:i + 1], 14).iloc[-1]
        local = cci(tail, 14).iloc[-1]
        if pd.isna(actual.iloc[i]):
            assert pd.isna(prefix) and pd.isna(local)
        else:
            assert prefix == pytest.approx(actual.iloc[i])
            assert local == pytest.approx(actual.iloc[i])


@pytest.mark.parametrize("invalid", [np.nan, np.inf, -np.inf, "missing"])
def test_invalid_observation_requires_a_new_complete_window(invalid):
    from mt5_extracao.strategies.indicators import cci
    frame = bars(range(50)).astype(object)
    frame.loc[20, "close"] = invalid
    actual = cci(frame, 14)
    assert actual.iloc[20:34].isna().all()
    assert actual.iloc[34] == pytest.approx(123.8095238095)
    frame.loc[40, "high"] = -100
    assert cci(frame, 14).iloc[40:].isna().all()


@pytest.mark.parametrize("period", [True, 0, -1, 1.5])
def test_invalid_period_is_explicit(period):
    from mt5_extracao.strategies.indicators import cci
    with pytest.raises(ValueError, match="inteiro positivo"):
        cci(bars(range(30)), period)


def test_empty_short_large_and_threshold_values():
    from mt5_extracao.strategies.indicators import cci
    assert cci(bars([]), 14).empty
    assert cci(bars(range(13)), 14).isna().all()
    huge = pd.DataFrame({name: [1e308] * 14 for name in ("high", "low", "close")})
    assert cci(huge, 14).iloc[-1] == 0
    alternating = pd.DataFrame({"high": [1.7e308] * 14, "low": [-1.7e308] * 14,
                                "close": [1.7e308, -1.7e308] * 7})
    assert cci(alternating, 14).iloc[-1] == pytest.approx(-100 / 1.5)
    tiny = pd.DataFrame({"high": [1., 2.5e-310, 2e-310], "low": [-1., 2.5e-310, 2e-310],
                         "close": [1e-310, 2.5e-310, 2e-310]})
    assert cci(tiny, 3).iloc[-1] == pytest.approx(reference([1/3, 2.5, 2], 3)[-1])
    positive, negative = cci(bars(range(14)), 14).iloc[-1], cci(bars(range(14, 0, -1)), 14).iloc[-1]
    assert positive > 100 and negative < -100


def test_enhanced_consumer_delegates_to_the_same_calculation():
    from mt5_extracao.enhanced_indicators import EnhancedIndicatorCalculator
    from mt5_extracao.strategies.indicators import cci
    frame = bars(range(50))
    frame["time"] = pd.date_range("2026-01-01", periods=50, freq="min")
    frame["open"] = frame.close
    actual = EnhancedIndicatorCalculator().calculate_advanced_indicators(frame.copy(), ["cci"], period=14)
    pd.testing.assert_series_equal(actual.cci, cci(frame, 14), check_names=False)


def test_collector_replaces_overlapping_bar_before_calculation():
    from types import SimpleNamespace
    from mt5_extracao.data_collector import DataCollector
    from mt5_extracao.indicator_calculator import IndicatorCalculator
    from mt5_extracao.strategies.indicators import cci
    frame = bars(range(50))
    frame["time"] = pd.date_range("2026-01-01", periods=50, freq="min")
    frame["open"] = frame.close
    frame["volume"] = 10
    history = frame.copy()
    history.loc[49, ["high", "low", "close"]] = [1001, 999, 1000]
    saved = []
    db = SimpleNamespace(get_table_name_for_symbol=lambda *a: "qa_m1",
                         get_recent_data=lambda *a, **k: history.copy(),
                         save_data=lambda data, *a: saved.append(data.copy()) or True)
    connector = SimpleNamespace(is_initialized=True, get_last_bars=lambda *a, **k: frame.tail(2).copy())
    collector = DataCollector(connector, db, IndicatorCalculator())
    collector.set_symbols(["QA"])
    assert collector._fetch_and_save_data("QA")
    assert len(saved) == 1 and len(saved[0]) == 1
    assert saved[0].cci.iloc[0] == pytest.approx(cci(frame, 20).iloc[-1])


def test_materialized_cci_is_versioned_and_append_matches_full(tmp_path):
    from mt5_extracao.analytics import feature_store, indicator_materializer as materializer
    from mt5_extracao.database_manager import DatabaseManager
    from tests.test_indicator_materialization import facts, seed, source
    from sqlalchemy import text
    db = DatabaseManager(db_path=str(tmp_path / "cci.sqlite"))
    db.broker_utc_offset = 0
    try:
        frame = source(45)
        frame["cci"] = 66.6666667  # prior unversioned result must remain untouched
        seed(db, frame.iloc[:125])
        first = materializer.materialize(db, "TEST", "M5", feature_ids=["momentum.cci_20"])
        old = facts(db)
        assert first["inserted"] == 25
        assert all(row[1].startswith("2.0+") for row in old)
        seed(db, frame)
        result = materializer.materialize(db, "TEST", "M5", feature_ids=["momentum.cci_20"])
        assert result["write_from"] == 25 and result["inserted"] == 20
        assert all(row in facts(db) for row in old)
        values = feature_store.latest_values(db, symbol="TEST", timeframe="M5", feature_ids=["momentum.cci_20"])
        assert len(values) == 45 and values.iloc[:19].quality.eq("UNAVAILABLE").all()
        assert values.iloc[-1].numeric_value == pytest.approx(126.6666666667)
        table = db.get_table_name_for_symbol("TEST", "1 minuto")
        with db.engine.connect() as conn:
            assert all(row[0] == 66.6666667 for row in conn.execute(text(f'SELECT cci FROM "{table}"')))
        full = DatabaseManager(db_path=str(tmp_path / "cci-full.sqlite"))
        full.broker_utc_offset = 0
        try:
            seed(full, frame)
            materializer.materialize(full, "TEST", "M5", feature_ids=["momentum.cci_20"])
            assert facts(full) == facts(db)
        finally:
            full.close()
    finally:
        db.close()


def test_known_ineligible_bar_resets_materialization_and_preserves_old_asof(tmp_path, monkeypatch):
    from mt5_extracao.analytics import feature_store, indicator_materializer as materializer, resampling
    from mt5_extracao.database_manager import DatabaseManager
    from tests.test_indicator_materialization import seed, source
    db = DatabaseManager(db_path=str(tmp_path / "cci-gap.sqlite"))
    db.broker_utc_offset = 0
    try:
        seed(db, source(70))
        materializer.materialize(db, "TEST", "M5", feature_ids=["momentum.cci_20"])
        before = feature_store.query_as_of(db, "2026-09-02T00:00Z", symbol="TEST")
        raw = resampling.latest_bars(db, "TEST", "M5", closed_only=False)
        cutoff = pd.Timestamp("2026-10-20T12:00Z")
        raw.loc[30, ["quality_status", "revision", "computed_at_utc"]] = ["PARTIAL", 2, cutoff]
        closed = raw.drop(index=30).reset_index(drop=True)
        monkeypatch.setattr(resampling, "latest_source_sequence", lambda *a, **k: 2)
        monkeypatch.setattr(resampling, "latest_bars", lambda *a, closed_only=True, **k: closed.copy() if closed_only else raw.copy())
        materializer.materialize(db, "TEST", "M5", feature_ids=["momentum.cci_20"])
        pd.testing.assert_frame_equal(before, feature_store.query_as_of(db, cutoff - pd.Timedelta(nanoseconds=1), symbol="TEST"))
        after = feature_store.query_as_of(db, cutoff, symbol="TEST", feature_id="momentum.cci_20")
        for offset in range(1, 20):
            row = after[after.observation_time == raw.iloc[30 + offset].bar_start_utc].iloc[0]
            assert row.quality == "UNAVAILABLE" and row.available_at_utc >= cutoff
        assert after[after.observation_time == raw.iloc[50].bar_start_utc].iloc[0].quality == "VALID"
    finally:
        db.close()
