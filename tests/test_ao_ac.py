"""I07/I09: soma racional independente e consumo pelo serviço canônico."""
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


OUTPUTS = {"momentum.ao_5_34": ("ao", 34), "momentum.ao_5_34_slope_1": ("ao_slope_1", 35),
           "momentum.ao_5_34_acceleration_1": ("ao_acceleration_1", 36),
           "momentum.ao_5_34_zero_cross": ("ao_zero_cross", 35), "momentum.ao_5_34_color": ("ao_color", 35),
           "momentum.ac_5_34_5": ("ac", 38), "momentum.ac_5_34_5_slope_1": ("ac_slope_1", 39),
           "momentum.ac_5_34_5_color": ("ac_color", 39)}
IDS = list(OUTPUTS)


def independent_ao_ac(prices):
    """Pesos racionais explícitos sobre listas; sem rolling/funções do produto."""
    values = [Fraction(float(p)) if np.isfinite(p) else None for p in prices]
    ao, ac = [], []
    for i in range(len(values)):
        window = values[max(0, i - 33):i + 1]
        a = (sum(window[-5:]) / 5 - sum(window) / 34
             if len(window) == 34 and None not in window else None)
        ao.append(a)
        previous = ao[max(0, i - 4):i + 1]
        ac.append(a - sum(previous) / 5 if len(previous) == 5 and None not in previous else None)
    return {name: np.array([float(v) if v is not None else np.nan for v in values])
            for name, values in (("ao", ao), ("ac", ac))}


@pytest.mark.parametrize("prices", [np.zeros(70), np.full(70, 42.), np.arange(70.) + 100,
                                    200 - np.arange(70.), np.array([100 + (i % 11) ** 2 + i // 11 for i in range(70)])])
def test_reference_manual_warmup_and_color_do_not_change_levels(prices):
    frame = pd.DataFrame({"high": prices + 1, "low": prices - 1, "close": prices * 3 + 17})
    result = indicators.ao_ac(frame)
    expected = independent_ao_ac(prices)
    for name, warmup in (("ao", 34), ("ac", 38)):
        assert result[name].first_valid_index() == warmup - 1
        np.testing.assert_allclose(result[name], expected[name], atol=2e-14, rtol=2e-14, equal_nan=True)
        np.testing.assert_allclose(result[name + "_slope_1"], np.diff(expected[name], prepend=np.nan),
                                   atol=4e-14, rtol=2e-14, equal_nan=True)
        np.testing.assert_array_equal(result[name + "_color"], np.sign(result[name + "_slope_1"]))
    if np.array_equal(prices, np.arange(70.) + 100):
        assert result.ao.iloc[33] == 14.5 and result.ac.iloc[37] == 0
    if np.ptp(prices) == 0:
        assert result.ao.iloc[33:].eq(0).all() and result.ac.iloc[37:].eq(0).all()
        assert result.ao_color.iloc[34:].eq(0).all() and result.ac_color.iloc[38:].eq(0).all()


@pytest.mark.parametrize("bad", [np.nan, np.inf, -np.inf, "invalid_range"])
def test_invalid_source_requires_full_new_windows_and_never_fills_zero(bad):
    prices = np.arange(90.) + 100
    frame = pd.DataFrame({"high": prices + 1, "low": prices - 1})
    frame.loc[40, "high"] = frame.loc[40, "low"] - 1 if isinstance(bad, str) else bad
    result = indicators.ao_ac(frame)
    assert result.ao.loc[40:73].isna().all() and pd.notna(result.ao.loc[74])
    assert result.ac.loc[40:77].isna().all() and pd.notna(result.ac.loc[78])
    rows = indicator_registry.compute_core(frame, feature_ids=IDS)
    assert all(r["state"] == "SOURCE_VALUE_MISSING" and r["value"] is None for r in rows if r["bar_index"] == 40)
    assert all(r["state"] == "DEPENDENCY_UNAVAILABLE" for r in rows if r["bar_index"] == 41)
    missing = indicator_registry.compute_core(frame.drop(columns="low"), feature_ids=IDS)
    assert all(r["state"] == "SOURCE_UNAVAILABLE" and r["value"] is None for r in missing)


@pytest.mark.parametrize("price", [1e307, -1e307])
def test_extreme_finite_windows_do_not_overflow(price):
    frame = pd.DataFrame({"high": np.full(45, price), "low": np.full(45, price)})
    result = indicators.ao_ac(frame)
    assert result.ao.iloc[33:].eq(0).all() and result.ac.iloc[37:].eq(0).all()
    rows = indicator_registry.compute_core(frame, feature_ids=["momentum.ao_5_34", "momentum.ac_5_34_5"])
    assert all(r["state"] == "AVAILABLE" and r["value"] == 0 for r in rows if r["bar_index"] >= 37)


def test_registry_warmup_prefix_causality_and_transform_contracts():
    prices = np.array([100 + (i % 11) ** 2 + i // 11 for i in range(80)])
    frame = pd.DataFrame({"high": prices + 1, "low": prices - 1}, index=np.arange(80) * 3)
    prefix = indicator_registry.compute_core(frame.iloc[:45], feature_ids=IDS)
    whole = indicator_registry.compute_core(frame, feature_ids=IDS)
    assert prefix == [r for r in whole if r["bar_index"] < 45 * 3]
    for ident, (_column, warmup) in OUTPUTS.items():
        spec = indicator_registry.get_definition(ident)
        assert spec["warmup_count"] == warmup and spec["parameters"]["method"] == "SMA_full_finite_window"
        rows = [r for r in whole if r["feature_id"] == ident]
        assert rows[warmup - 2]["state"] == "WARMUP" and rows[warmup - 1]["state"] == "AVAILABLE"
    out = indicators.ao_ac(frame)
    np.testing.assert_allclose(out.ao_acceleration_1, out.ao.diff().diff(), equal_nan=True)
    prior = out.ao.shift(1)
    expected = ((prior <= 0) & (out.ao > 0)).astype(float) - ((prior >= 0) & (out.ao < 0)).astype(float)
    np.testing.assert_array_equal(out.ao_zero_cross, expected.where(prior.notna() & out.ao.notna()))
    assert indicator_registry.get_definition("momentum.ao_5_34_color")["parameters"]["colors"]["0"] == "FLAT_NEUTRAL"
    assert list(indicators.ao_ac(frame.iloc[:0]).columns) == list(out.columns)


def test_zero_touch_departure_crossing_and_acceleration_have_manual_reference():
    prices = np.concatenate([np.zeros(40), np.ones(40), -np.ones(40)])
    frame = pd.DataFrame({"high": prices, "low": prices})
    result = indicators.ao_ac(frame)
    expected = independent_ao_ac(prices)
    assert result.ao_zero_cross.iloc[40] == 1 and result.ao_zero_cross.iloc[80] == -1
    assert result.ao_zero_cross.iloc[73] == 0  # Encostar em zero não cruza.
    np.testing.assert_allclose(result.ao_acceleration_1, np.diff(expected["ao"], n=2, prepend=[np.nan, np.nan]),
                               atol=2e-14, rtol=2e-14, equal_nan=True)


def test_materialization_append_reopen_and_csv_match_reference_exactly(db, tmp_path):
    seed(db, source(40))
    materializer.materialize(db, "TEST", "M5", feature_ids=IDS, batch_size=23)
    prefix = facts(db)
    seed(db, source(70))
    added = materializer.materialize(db, "TEST", "M5", feature_ids=IDS, batch_size=23)
    assert added["inserted"] == 30 * len(IDS) and added["revised"] == 0 and added["write_from"] == 40
    assert all(row in facts(db) for row in prefix)
    final = facts(db)
    assert materializer.materialize(db, "TEST", "M5", feature_ids=IDS)["skipped"] and facts(db) == final
    full = DatabaseManager(db_path=str(tmp_path / "batch.sqlite")); full.broker_utc_offset = 0
    try:
        seed(full, source(70))
        materializer.materialize(full, "TEST", "M5", feature_ids=IDS)
        assert facts(full) == final
    finally:
        full.close()
    db.close()
    reopened = DatabaseManager(db_path=db.db_path)
    try:
        assert materializer.materialize(reopened, "TEST", "M5", feature_ids=IDS)["skipped"]
        bars = resampling.latest_bars(reopened, "TEST", "M5")
        calculated = indicator_registry.compute_core(bars, feature_ids=IDS)
        reference = independent_ao_ac(np.arange(70.) + 100)
        path = tmp_path / "AO e AC.csv"
        DataExporter(reopened).export_indicator_values(path, symbol="TEST", timeframe="M5",
                                                      as_of="2026-10-19T00:00Z", feature_ids=IDS)
        csv = pd.read_csv(path, keep_default_na=False, float_precision="round_trip")
        assert len(csv) == 70
        for ident in IDS:
            pure = np.array([r["value"] if r["value"] is not None else np.nan for r in calculated if r["feature_id"] == ident])
            stored = feature_store.query_as_of(reopened, "2026-10-19T00:00Z", symbol="TEST", timeframe="M5",
                                               feature_id=ident, algorithm_version=materializer.algorithm_version(ident))
            exported = np.array([float(v) if v != "" else np.nan for v in csv[ident + "::value"]])
            np.testing.assert_array_equal(pure, stored.numeric_value.to_numpy())
            np.testing.assert_array_equal(pure, exported)
            assert list(csv[ident + "::state"]) == [r["state"] for r in calculated if r["feature_id"] == ident]
            assert all(json.loads(v)["method"] == "SMA_full_finite_window" for v in csv[ident + "::parameters"])
            if ident in ("momentum.ao_5_34", "momentum.ac_5_34_5"):
                np.testing.assert_allclose(pure, reference[OUTPUTS[ident][0]], atol=2e-14, rtol=2e-14, equal_nan=True)
    finally:
        reopened.close()


def test_ao_ac_are_available_at_calculation_close_and_never_before(db):
    seed(db, source(45))
    materializer.materialize(db, "TEST", "M5", feature_ids=IDS)
    for ident, warmup in (("momentum.ao_5_34", 34), ("momentum.ac_5_34_5", 38)):
        close = pd.Timestamp("2026-09-01T09:00Z") + pd.Timedelta(minutes=5 * warmup)
        context = indicator_context.context(db, pd.DataFrame({"decision_time": [close - pd.Timedelta(nanoseconds=1), close]}),
                                            symbol="TEST", timeframe="M5", feature_ids=[ident])
        assert context.iloc[0][ident + "::state"] != "AVAILABLE"
        assert context.iloc[1][ident + "::state"] == "AVAILABLE"
        assert context.iloc[1][ident + "::available_at_utc"] == close


def test_late_source_correction_preserves_previous_asof(db):
    frame = source(70)
    seed(db, frame)
    materializer.materialize(db, "TEST", "M5", feature_ids=IDS)
    before = feature_store.query_as_of(db, "2026-09-02T00:00Z", symbol="TEST")
    changed = frame.iloc[[10]].copy(); changed["high"] += 20
    seed(db, changed)
    receipt = resampling.latest_bars(db, "TEST", "M5").loc[lambda x: x.revision > 1, "computed_at_utc"].max()
    materializer.materialize(db, "TEST", "M5", feature_ids=IDS)
    pd.testing.assert_frame_equal(before, feature_store.query_as_of(db, receipt - pd.Timedelta(nanoseconds=1), symbol="TEST"))
    after = feature_store.query_as_of(db, receipt, symbol="TEST")
    revisions = after[after.revision > 1]
    assert not revisions.empty and revisions.available_at_utc.ge(receipt).all()
