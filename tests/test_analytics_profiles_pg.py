"""T13.3 no PostgreSQL isolado, sem terminal MetaTrader."""
import os

import pandas as pd
import pytest

from mt5_extracao.analytics import feature_store, features, producers, resampling
from tests.postgres_helpers import isolated_postgresql_database
from tests.test_analytics_profiles import INFO, m1_days

PG_URL = os.environ.get("MT5X_TEST_PG_URL")
pytestmark = pytest.mark.skipif(not PG_URL, reason="defina MT5X_TEST_PG_URL para testar com PostgreSQL")


@pytest.fixture
def pg():
    pytest.importorskip("psycopg")
    with isolated_postgresql_database(PG_URL) as db:
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
