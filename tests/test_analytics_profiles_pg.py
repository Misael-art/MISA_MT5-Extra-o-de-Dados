"""T13.3 no PostgreSQL isolado, sem terminal MetaTrader."""
import os

import numpy as np
import pandas as pd
import pytest

from mt5_extracao.analytics import feature_store, features, producer_jobs, producers, resampling
from tests.postgres_helpers import isolated_postgresql_database
from tests.test_analytics_profiles import INFO, m1_days

PG_URL = os.environ.get("MT5X_TEST_PG_URL")
pytestmark = pytest.mark.skipif(not PG_URL, reason="defina MT5X_TEST_PG_URL para testar com PostgreSQL")


@pytest.fixture
def pg():
    pytest.importorskip("psycopg")
    with isolated_postgresql_database(PG_URL) as db:
        db.broker_utc_offset = 0
        yield db


def test_bulk_history_invalidation_and_expiry_are_point_in_time(pg):
    features.register(pg, "context", "Contexto", "1", dependencies=["bars:M1"])
    def record(symbol, value, available, source, **kwargs):
        return dict(symbol=symbol, timeframe="M1", feature_id="context", algorithm_version="1",
                    observation_time="2026-10-01T09:00:00Z", value=value,
                    available_at=available, observed_at=available, source_revision=source, **kwargs)
    initial = [record("A", 10, "2026-10-01T10:00:00Z", "a1"),
               record("B", 20, "2026-10-01T10:00:00Z", "b1",
                      valid_until="2026-10-01T12:00:00Z")]
    assert feature_store.save_values(pg, initial) == dict(inserted=2, revised=0, unchanged=0)
    assert feature_store.save_values(pg, initial) == dict(inserted=0, revised=0, unchanged=2)
    invalidation = record("A", None, "2026-10-01T12:00:00Z", "a2", quality="INVALIDATED")
    assert feature_store.save_values(pg, [invalidation])["revised"] == 1
    history = feature_store.query_history(pg)
    decisions = pd.DataFrame({"decision_time": ["2026-10-01T11:00:00Z", "2026-10-01T12:00:00Z"]})
    joined = feature_store.asof_join(decisions, history)
    assert joined.iloc[:2].numeric_value.tolist() == [10, 20]
    assert joined.iloc[2:].numeric_value.isna().all()
    assert joined.iloc[2].quality == "INVALIDATED"
    latest = feature_store.latest_values(pg, feature_ids=["context"])
    assert len(latest) == 2
    assert latest.set_index("symbol").loc["A", "quality"] == "INVALIDATED"
    # A entrada válida anterior ao erro não pode ser gravada parcialmente.
    bad = record("C", 30, "2026-10-01T13:00:00Z", "c1")
    with pytest.raises(ValueError):
        feature_store.save_values(pg, [bad, dict(bad, feature_id="não registrada")])
    pd.testing.assert_frame_equal(feature_store.query_history(pg), history)


def test_producer_higher_timeframe_closes_before_join(pg):
    pg.broker_utc_offset = 0
    pg.save_ohlcv_data("TEST", "1 minuto", m1_days(days=3))
    resampling.resample_symbol(pg, "TEST", ["H1"], symbol_info=INFO)
    summary = producers.compute(pg, "TEST", "H1", families=["calendar", "volatility"])
    assert all(row["values"] > 0 for row in summary)
    history = feature_store.query_history(pg, feature_id="volatility.range_pct")
    first = history.iloc[0]
    decisions = pd.DataFrame({"decision_time": [first.available_at_utc - pd.Timedelta(nanoseconds=1),
                                                first.available_at_utc]})
    joined = feature_store.asof_join(decisions, history)
    assert pd.isna(joined.iloc[0].numeric_value)
    assert pd.notna(joined.iloc[1].numeric_value)
    bars = resampling.latest_bars(pg, "TEST", "H1")
    bar = bars[bars.bar_start_utc == first.observation_time].iloc[0]
    assert first.available_at_utc >= bar.bar_end_utc
    repeat = producers.compute(pg, "TEST", "H1", families=["calendar", "volatility"])
    assert all(row["inserted"] == row["revised"] == 0 for row in repeat)


def test_producer_timeframes_and_configurations_coexist_without_mutating_history(pg):
    pg.broker_utc_offset = 0
    pg.save_ohlcv_data("TEST", "1 minuto", m1_days(days=2))
    resampling.resample_symbol(pg, "TEST", ["M15", "H1"], symbol_info=INFO)
    m15 = producers.compute(pg, "TEST", "M15", families=["volatility"])[0]
    original = feature_store.query_history(pg, timeframe="M15")
    h1 = producers.compute(pg, "TEST", "H1", families=["volatility"])[0]
    alternate = producers.compute(pg, "TEST", "M15", families=["volatility"],
                                  overrides={"volatility": {"atr_n": 7}})[0]
    assert len({row["algorithm_version"] for row in [m15, h1, alternate]}) == 3
    pd.testing.assert_frame_equal(feature_store.query_history(pg, timeframe="M15",
                                  algorithm_version=m15["algorithm_version"]), original)
    # A versão H1 mais recente não esconde M15; a configuração alternativa também não.
    selected = producers.recalculate_affected(pg, "TEST", "M15", ["bars_derived:M15"])
    assert selected
    assert all(item["algorithm_version"] == m15["algorithm_version"] for item in selected)
    assert all(item["result"][0]["inserted"] == item["result"][0]["revised"] == 0 for item in selected)
    assert producers.recalculate_affected(pg, "TEST", "M15", ["bars_derived:H1"]) == []


def _m1_minutes(start):
    times = pd.date_range(start, periods=20, freq="min")
    close = 100 + np.arange(len(times), dtype=float) / 100
    return pd.DataFrame({"time": times, "open": close - 0.01, "high": close + 0.02,
                         "low": close - 0.02, "close": close, "tick_volume": 20,
                         "spread": 2, "real_volume": 10})



def _m1_sessions(days=8, minutes=60):
    frames = []
    for day in range(days):
        times = pd.date_range(pd.Timestamp("2026-09-01 09:00") + pd.Timedelta(days=day), periods=minutes,
                              freq="min")
        close = 100 + np.cumsum(np.sin(np.arange(minutes) / 7.0)) * 0.1 + day
        frames.append(pd.DataFrame({"time": times, "open": close - 0.05, "high": close + 0.2,
                                    "low": close - 0.2, "close": close, "tick_volume": 20,
                                    "spread": 2, "real_volume": 10}))
    return pd.concat(frames, ignore_index=True)



def test_postgresql_checkpoint_recovers_committed_batch_and_appends_new_session(pg, monkeypatch):
    pg.save_ohlcv_data("TEST", "1 minuto", _m1_minutes("2026-09-01 09:00"))
    resampling.resample_symbol(pg, "TEST", ["M5"], symbol_info=INFO)

    original = feature_store.save_values
    calls = {"count": 0}

    def fail_after_first_commit(db, records, **kwargs):
        counts = original(db, records, **kwargs)
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("falha simulada após lote confirmado")
        return counts

    monkeypatch.setattr(feature_store, "save_values", fail_after_first_commit)
    with pytest.raises(RuntimeError, match="falha simulada"):
        producers.compute(pg, "TEST", "M5", families=["calendar"], batch_size=1, max_retries=0)

    checkpoint = producer_jobs.get_checkpoint(pg, "TEST", "M5", "calendar")
    assert checkpoint["status"] == "RUNNING" and checkpoint["last_bar_start_utc"] is not None
    assert producer_jobs.latest_run(pg, "TEST", "M5", "calendar")["status"] == "FAILED"

    monkeypatch.setattr(feature_store, "save_values", original)
    resumed = producers.compute(pg, "TEST", "M5", families=["calendar"], batch_size=1)
    assert resumed[0]["job"]["status"] == "COMPLETE"
    assert len(feature_store.query_history(pg, symbol="TEST", timeframe="M5",
                                           algorithm_version=resumed[0]["algorithm_version"])) == 16

    pg.save_ohlcv_data("TEST", "1 minuto", _m1_minutes("2026-09-02 09:00"))
    resampling.resample_symbol(pg, "TEST", ["M5"], symbol_info=INFO)
    appended = producers.compute(pg, "TEST", "M5", families=["calendar"], batch_size=1)
    checkpoint = producer_jobs.get_checkpoint(pg, "TEST", "M5", "calendar")
    assert checkpoint["source_sequence"] == 2 and checkpoint["status"] == "COMPLETE"
    assert appended[0]["inserted"] == 16

    unchanged = producers.compute(pg, "TEST", "M5", families=["calendar"], batch_size=1)
    assert unchanged[0]["skipped"]
    history = feature_store.query_history(pg, symbol="TEST", timeframe="M5",
                                          algorithm_version=resumed[0]["algorithm_version"])
    assert len(history) == 32



def test_postgresql_late_correction_invalidation_survives_resume(pg, monkeypatch):
    pg.save_ohlcv_data("TEST", "1 minuto", _m1_sessions())
    resampling.resample_symbol(pg, "TEST", ["M15"], symbol_info=INFO)
    params = {"lookback": 1, "min_sessions": 1, "same_weekday": False}
    producers.compute(pg, "TEST", "M15", families=["rvol"], volume_source="real",
                      overrides={"rvol": params})

    bars = resampling.latest_bars(pg, "TEST", "M15")
    sessions = bars.groupby("session_id")["bar_start_utc"].min().sort_values().index.tolist()
    reference_session, current_session = sessions[-2:]
    observations = bars.loc[bars.session_id == current_session, "bar_start_utc"]
    previous = feature_store.query_as_of(pg, "2030-01-01T00:00:00Z", symbol="TEST", timeframe="M15",
                                         feature_id="rvol.real")
    previous = previous[previous.observation_time.isin(observations)]
    assert previous["numeric_value"].notna().all()

    reference = bars[bars.session_id == reference_session]
    m1 = pg.load_ohlcv("TEST", "1 minuto")
    raw_times = pd.to_datetime(m1["time"])
    start = reference["bar_start_utc"].min().tz_convert(None)
    end = reference["bar_end_utc"].max().tz_convert(None)
    hit = (raw_times >= start) & (raw_times < end)
    m1.loc[hit, "real_volume"] = 0
    pg.save_ohlcv_data("TEST", "1 minuto", m1.loc[hit])
    resampling.resample_symbol(pg, "TEST", ["M15"], symbol_info=INFO)
    corrected = resampling.latest_bars(pg, "TEST", "M15", closed_only=False)
    corrected_at = corrected.loc[corrected.session_id == reference_session, "computed_at_utc"].max()

    original = feature_store.save_values
    calls = {"count": 0}

    def fail_after_first_commit(db, records, **kwargs):
        counts = original(db, records, **kwargs)
        if records:
            calls["count"] += 1
            if calls["count"] == 1:
                raise RuntimeError("falha simulada após lote confirmado")
        return counts

    monkeypatch.setattr(feature_store, "save_values", fail_after_first_commit)
    with pytest.raises(RuntimeError, match="falha simulada após lote confirmado"):
        producers.compute(pg, "TEST", "M15", families=["rvol"], volume_source="real",
                          overrides={"rvol": params}, batch_size=1, max_retries=0)
    monkeypatch.setattr(feature_store, "save_values", original)
    result = producers.compute(pg, "TEST", "M15", families=["rvol"], volume_source="real",
                               overrides={"rvol": params}, batch_size=1)
    assert result[0]["invalidations"] > 0
    assert producer_jobs.get_checkpoint(pg, "TEST", "M15", "rvol")["status"] == "COMPLETE"

    history = feature_store.query_as_of(pg, "2030-01-01T00:00:00Z", symbol="TEST", timeframe="M15",
                                        feature_id="rvol.real")
    current = history[history.observation_time.isin(observations)]
    assert current["quality"].eq("INVALIDATED").all()
    assert current["numeric_value"].isna().all() and current["categorical_value"].isna().all()
    assert current["available_at_utc"].eq(corrected_at).all()



def test_typed_registry_additive_migration_and_target_idempotency(pg):
    from tests import test_analytics_relations_contract as contracts

    contracts.test_feature_definition_migration_only_adds_typed_columns(pg)
    contracts.test_target_registry_tracks_horizon_maturity_and_write_guard(pg)


@pytest.mark.parametrize("bulk", [False, True])
@pytest.mark.parametrize("value_type,value", [("NUMBER", 0.25), ("CATEGORY", "TREND"), ("BOOLEAN", True)])
def test_typed_registry_invalidation_is_point_in_time(pg, bulk, value_type, value):
    from tests import test_analytics_relations_contract as contracts

    contracts.test_typed_invalidation_preserves_asof_history(pg, bulk, value_type, value)


def test_typed_registry_upgrade_preserves_legacy_checkpoint_and_values(pg, monkeypatch):
    from tests.test_analytics_profiles import test_typed_contract_upgrade_preserves_legacy_values_and_rebuilds_checkpoint

    test_typed_contract_upgrade_preserves_legacy_values_and_rebuilds_checkpoint(pg, monkeypatch)


def test_native_registry_materialization_preserves_raw_and_missing_states(pg):
    from tests.test_indicator_materialization import test_materialize_preserves_raw_bars_and_explicit_missing_states
    test_materialize_preserves_raw_bars_and_explicit_missing_states(pg)


def test_native_registry_failed_ack_recovers_checkpoint(pg, monkeypatch):
    from tests.test_indicator_materialization import test_failed_ack_resumes_only_confirmed_cursor
    test_failed_ack_resumes_only_confirmed_cursor(pg, monkeypatch)


def test_native_registry_late_correction_is_point_in_time(pg):
    from tests.test_indicator_materialization import test_late_revision_never_changes_previous_asof
    test_late_revision_never_changes_previous_asof(pg)


def test_native_registry_export_retains_states_and_destination(pg, tmp_path):
    from tests.test_indicator_materialization import test_export_preserves_states_version_and_previous_destination
    test_export_preserves_states_version_and_previous_destination(pg, tmp_path)


def test_alligator_calculation_close_and_plot_anchor_are_independent(pg):
    from tests.test_alligator import test_closed_calculation_time_is_not_replaced_by_visual_offset
    test_closed_calculation_time_is_not_replaced_by_visual_offset(pg)


def test_alligator_late_revision_preserves_history_and_plot_anchor(pg):
    from tests.test_alligator import test_late_alligator_correction_preserves_previous_time_and_plot_anchor
    test_late_alligator_correction_preserves_previous_time_and_plot_anchor(pg)


@pytest.mark.parametrize("boundary", ["first_batch", "completion", "cache_skip"])
def test_native_registry_rejects_concurrent_source_change(pg, monkeypatch, boundary):
    from tests.test_indicator_consistency import test_source_change_aborts_snapshot_and_resume_uses_current_source
    test_source_change_aborts_snapshot_and_resume_uses_current_source(pg, monkeypatch, boundary)


def test_feature_store_overlapping_retries_are_idempotent(pg):
    from tests.test_indicator_consistency import test_overlapping_store_retries_are_idempotent
    test_overlapping_store_retries_are_idempotent(pg)


def test_explicit_legacy_registry_version_remains_queryable(pg, monkeypatch):
    from tests.test_indicator_consistency import test_global_version_transition_keeps_explicit_legacy_context
    test_global_version_transition_keeps_explicit_legacy_context(pg, monkeypatch)


def test_ao_ac_closed_bar_availability(pg):
    from tests.test_ao_ac import test_ao_ac_are_available_at_calculation_close_and_never_before
    test_ao_ac_are_available_at_calculation_close_and_never_before(pg)


def test_ao_ac_late_revision_preserves_history(pg):
    from tests.test_ao_ac import test_late_source_correction_preserves_previous_asof
    test_late_source_correction_preserves_previous_asof(pg)



def test_source_sequence_schema_inspection_stays_in_snapshot_transaction(pg):
    from tests.test_indicator_consistency import test_source_sequence_uses_the_supplied_transaction_for_schema_inspection
    test_source_sequence_uses_the_supplied_transaction_for_schema_inspection(pg)
