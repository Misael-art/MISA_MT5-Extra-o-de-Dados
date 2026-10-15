"""T13.3b producer checkpoint/recovery integration on PostgreSQL (optional local/CI service)."""
import os
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from mt5_extracao.analytics import feature_store, producer_jobs, producers, resampling
from mt5_extracao.database_manager import DatabaseManager

PG_URL = os.environ.get("MT5X_TEST_PG_URL")
pytestmark = pytest.mark.skipif(not PG_URL, reason="defina MT5X_TEST_PG_URL para testar com PostgreSQL")
INFO = SimpleNamespace(name="TEST", description="sintético", path="Teste\\TEST", digits=2,
                       currency_profit="BRL")


@pytest.fixture
def pg():
    pytest.importorskip("psycopg")
    database_name = (make_url(PG_URL).database or "").lower()
    if "test" not in database_name:
        pytest.fail("MT5X_TEST_PG_URL precisa apontar para uma base de teste descartável")
    engine = create_engine(PG_URL)
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
    engine.dispose()
    db = DatabaseManager(db_type="postgresql", db_path=PG_URL, broker_utc_offset=0)
    assert db.is_connected()
    yield db
    db.engine.dispose()


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
                                           algorithm_version=producers.ALGORITHM_VERSION)) == 16

    pg.save_ohlcv_data("TEST", "1 minuto", _m1_minutes("2026-09-02 09:00"))
    resampling.resample_symbol(pg, "TEST", ["M5"], symbol_info=INFO)
    appended = producers.compute(pg, "TEST", "M5", families=["calendar"], batch_size=1)
    checkpoint = producer_jobs.get_checkpoint(pg, "TEST", "M5", "calendar")
    assert checkpoint["source_sequence"] == 2 and checkpoint["status"] == "COMPLETE"
    assert appended[0]["inserted"] == 16

    unchanged = producers.compute(pg, "TEST", "M5", families=["calendar"], batch_size=1)
    assert unchanged[0]["skipped"]
    history = feature_store.query_history(pg, symbol="TEST", timeframe="M5",
                                          algorithm_version=producers.ALGORITHM_VERSION)
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
