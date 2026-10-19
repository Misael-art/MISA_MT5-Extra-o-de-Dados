"""T13.3: volume, RVOL, volatilidade, regime e estrutura da sessão, com disponibilidade point-in-time."""
from types import SimpleNamespace
import json

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import inspect, text
from mt5_extracao.analytics import (feature_store, features, market_regimes, producers, resampling,
                                    session_structure, volume_profiles)
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.strategies import indicators
from mt5_extracao.timeframes import Timeframe

UTC = "UTC"


def bars_frame(sessions=8, bars_per_session=6, minutes=5, volume=None, real_known=True, start="2026-09-01 12:00"):
    """Barras derivadas sintéticas: uma sessão por dia, ``bars_per_session`` barras de ``minutes``."""
    rows = []
    day0 = pd.Timestamp(start, tz=UTC)
    price = 100.0
    for s in range(sessions):
        open_time = day0 + pd.Timedelta(days=s)
        for b in range(bars_per_session):
            begin = open_time + pd.Timedelta(minutes=minutes * b)
            vol = volume(s, b) if volume else 10
            rows.append({"symbol": "TEST", "tf": f"M{minutes}", "session_id": f"s{s}", "bar_start_utc": begin,
                         "bar_end_utc": begin + pd.Timedelta(minutes=minutes), "open": price, "high": price + 1,
                         "low": price - 1, "close": price + 0.5, "tick_volume": vol * 3,
                         "real_volume": vol if real_known else None, "real_volume_known": int(real_known),
                         "revision": 1, "computed_at_utc": pd.Timestamp("2026-10-14", tz=UTC)})
            price += 0.5
    return pd.DataFrame(rows)


def test_session_volume_uses_only_prior_sessions_and_never_mixes_sources():
    # Volume constante 10 por barra, exceto a sessão 7 com 40: mediana anterior = 10 → razão 4.
    bars = bars_frame(sessions=9, volume=lambda s, b: 40 if s == 7 else 10)
    frame, source = volume_profiles.session_volume_features(bars, windows=(5,), min_sessions=5)
    assert source == "real"
    ratio = frame[frame["feature_id"] == "volume.session_ratio_median_5.real"].set_index("session_id")["value"]
    assert ratio["s7"] == pytest.approx(4.0)
    assert ratio["s6"] == pytest.approx(1.0)
    assert "s8" not in ratio.index                    # última sessão da base: pode estar aberta
    assert "s4" not in ratio.index                    # menos de 5 sessões anteriores: ausente, não 1,0
    available = frame.set_index(["session_id", "feature_id"])["available_at"]
    assert available[("s7", "volume.session_ratio_median_5.real")] == bars[bars.session_id == "s7"].bar_end_utc.max()

    # Uma barra com volume real desconhecido: o modo auto cai para ticks; real explícito deixa a sessão ausente.
    partial = bars.copy()
    partial.loc[3, ["real_volume", "real_volume_known"]] = [None, 0]
    _, auto_source = volume_profiles.session_volume_features(partial)
    assert auto_source == "tick"
    real_only, _ = volume_profiles.session_volume_features(partial, volume_source="real", windows=(5,))
    assert "s0" not in set(real_only[real_only.feature_id == "volume.session_per_minute.real"].session_id)


def test_minimum_session_count_larger_than_window_yields_missing_values():
    bars = bars_frame(sessions=12)
    frame, _ = volume_profiles.session_volume_features(bars, windows=(3,), min_sessions=5,
                                                       percentile_window=3)
    assert frame[frame.feature_id.str.contains("median_3|mean_3|percentile_3")].empty
    rvol, _ = volume_profiles.intraday_rvol(bars, lookback=3, min_sessions=5)
    assert rvol.empty


def test_declining_volume_slope_is_negative_and_robust():
    bars = bars_frame(sessions=20, volume=lambda s, b: 100 * (0.9 ** s) if s != 10 else 1000)
    frame, _ = volume_profiles.session_volume_features(bars, slope_window=15)
    slope = frame[frame.feature_id == "volume.session_slope_15.real"]["value"]
    assert len(slope) and (slope < 0).all()
    assert slope.iloc[-1] == pytest.approx(np.log(0.9), rel=0.05)    # o pico isolado não muda a mediana


def test_rvol_compares_same_minute_and_is_missing_without_history():
    # Sessões 0-5 com 10 por barra; sessão 6 com 20 por barra: RVOL = 2 em todos os minutos.
    bars = bars_frame(sessions=7, volume=lambda s, b: 20 if s == 6 else 10)
    frame, source = volume_profiles.intraday_rvol(bars, lookback=20, min_sessions=5)
    rvol = frame[frame.feature_id == f"rvol.{source}"]
    last = rvol[rvol.session_id == "s6"]
    assert np.allclose(last["value"], 2.0) and len(last) == 6
    assert set(rvol.session_id) == {"s5", "s6"}                       # s0-s4: menos de 5 sessões de referência
    refs = frame[(frame.feature_id == "rvol.reference_sessions") & (frame.session_id == "s6")]["value"]
    assert (refs == 6).all()
    assert last["available_at"].tolist() == bars[bars.session_id == "s6"]["bar_end_utc"].tolist()


def test_regime_matches_screener_with_duration_one_and_states_on_known_series():
    rng = np.random.default_rng(7)
    n = 300
    trend = np.cumsum(np.full(n, 0.5) + rng.normal(0, 0.05, n)) + 100
    flat = 100 + rng.normal(0, 0.5, n)
    for series, expected in ((trend, market_regimes.TREND), (flat, market_regimes.RANGE)):
        bars = pd.DataFrame({"open": series, "high": series + 0.6, "low": series - 0.6, "close": series})
        frame = market_regimes.regime_frame(bars, min_duration=1)
        assert frame["regime"].iloc[0] == market_regimes.UNCERTAIN           # sem ADX ainda: incerto
        assert frame["regime"].iloc[-1] == expected
        assert frame["regime_confidence"].iloc[-1] > 0.6
        # Paridade com a triagem: mesmo corte de ADX na última barra.
        screener_adx = float(indicators.adx(bars, 14)["adx"].iloc[-1])
        label = "tendência" if screener_adx > 25 else "lateral" if screener_adx < 20 else "transição"
        assert market_regimes.SCREENER_LABELS[label] == frame["regime_raw"].iloc[-1] == frame["regime"].iloc[-1]


def test_min_duration_delays_regime_switch():
    raw = [market_regimes.RANGE] * 5 + [market_regimes.TREND] * 2 + [market_regimes.RANGE] * 3 + \
          [market_regimes.TREND] * 4
    adx = [10 if label == market_regimes.RANGE else 40 for label in raw]
    original = market_regimes.ind.adx
    market_regimes.ind.adx = lambda data, n: pd.DataFrame({"adx": adx})
    try:
        data = pd.DataFrame({"open": np.ones(len(raw)), "high": np.ones(len(raw)), "low": np.ones(len(raw)),
                             "close": np.linspace(1, 2, len(raw))})
        frame = market_regimes.regime_frame(data, er_n=2, r2_n=3, min_duration=3)
    finally:
        market_regimes.ind.adx = original
    # Começa incerto até o primeiro estado durar 3 barras; 2 barras TREND não bastam para trocar.
    assert frame["regime"].tolist()[:10] == [market_regimes.UNCERTAIN] * 2 + [market_regimes.RANGE] * 8
    assert frame["regime"].tolist()[-2:] == [market_regimes.TREND] * 2
    assert frame["regime_duration"].iloc[-1] == 4


def test_volatility_percentile_excludes_current_bar_and_flags_compression():
    close = pd.Series(np.r_[100 + np.sin(np.arange(200)) * 3, np.full(40, 100.0)])
    bars = pd.DataFrame({"open": close, "high": close + 0.01, "low": close - 0.01, "close": close})
    frame = market_regimes.volatility_frame(bars, percentile_window=100)
    assert frame["volatility_state"].iloc[-1] == "COMPRESSION"
    pct = market_regimes.prior_percentile(pd.Series([1.0, 2.0, 3.0, 0.5]), window=3)
    assert pct.iloc[-1] == 0.0 and np.isnan(pct.iloc[2])


def test_structure_gap_and_opening_range_availability():
    bars = bars_frame(sessions=3, bars_per_session=12, minutes=5)
    frame = session_structure.structure_features(bars, opening_minutes=30)
    gap = frame[(frame.feature_id == "session.gap") & (frame.session_id == "s1")].iloc[0]
    first_bar_end = bars[bars.session_id == "s1"]["bar_end_utc"].min()
    assert gap["available_at"] == first_bar_end
    high = frame[(frame.feature_id == "opening_range.high_30") & (frame.session_id == "s1")].iloc[0]
    assert high["available_at"] == bars[bars.session_id == "s1"]["bar_start_utc"].min() + pd.Timedelta(minutes=30)
    breakout = frame[(frame.feature_id == "opening_range.breakout_30") & (frame.session_id == "s1")].iloc[0]
    assert breakout["value"] == 1.0 and breakout["available_at"] > high["available_at"]
    assert "s2" not in set(frame[frame.feature_id == "session.return"].session_id)    # última sessão: aberta


def test_calendar_features_reuse_existing_calendar_columns():
    from mt5_extracao.trade_analytics.enrich import calendar_columns

    bars = bars_frame(sessions=1, bars_per_session=2)
    expected = calendar_columns(bars["bar_start_utc"])
    rows = producers.calendar_rows(bars)
    by_feature = {}
    for row in rows:
        by_feature.setdefault(row["feature_id"], []).append(row["value"])
    assert by_feature["calendar.weekday_utc"] == expected["day_of_week"].astype(float).tolist()
    assert by_feature["calendar.week_of_month_utc"] == expected["week_of_month"].astype(float).tolist()
    assert by_feature["calendar.hour_utc"] == expected["hour"].astype(float).tolist()


def test_calendar_session_minute_uses_only_its_open_time_dependency():
    bars = bars_frame(sessions=2, bars_per_session=4)
    bars["computed_at_utc"] = pd.Timestamp("2026-09-01", tz=UTC)
    bars.loc[1, "revision"] = 2
    correction = pd.Timestamp("2026-10-01", tz=UTC)
    bars.loc[1, "computed_at_utc"] = correction
    rows = producers.calendar_rows(bars)
    minute = [row for row in rows if row["feature_id"] == "calendar.minute_of_session"]
    clock = [row for row in rows if row["feature_id"] == "calendar.hour_utc"]
    first_end = bars[bars.session_id == "s0"]["bar_end_utc"].iloc[0]
    assert minute[0]["available_at"] == first_end
    assert minute[1]["available_at"] == minute[1]["observation_time"]
    assert clock[0]["available_at"] == bars["bar_start_utc"].iloc[0]
    assert minute[0]["source_revision"].startswith("calendar:")


def test_ensure_schema_adds_partial_flag_and_source_sequence_to_legacy_table(db):
    legacy_schema = resampling._CREATE_BARS.replace("    is_partial INTEGER NOT NULL DEFAULT 0,\n", "").replace(
        "    source_seq BIGINT NOT NULL DEFAULT 0,\n", "")
    with db.engine.begin() as conn:
        conn.execute(text(legacy_schema))
    resampling.ensure_schema(db)
    with db.engine.connect() as conn:
        columns = {column["name"] for column in inspect(conn).get_columns("_bars_derived")}
    assert "is_partial" in columns
    assert "source_seq" in columns
    assert "ix_bars_derived_source_sequence" in {index["name"]
                                                   for index in inspect(db.engine).get_indexes("_bars_derived")}


def test_legacy_corrected_partial_bar_stays_excluded_without_partial_column(db):
    from mt5_extracao.analytics import calendar

    legacy_schema = resampling._CREATE_BARS.replace("    is_partial INTEGER NOT NULL DEFAULT 0,\n", "")
    data, sessions = calendar.sessionize_m1(m1_days(days=1, minutes=4, start="2026-09-01 09:01"), "TEST",
                                            broker_utc_offset=0)
    partial = resampling.build_bars(data, sessions, "M5", broker_utc_offset=0, symbol="TEST")[0]
    assert partial["is_partial"] == 1 and partial["coverage_ratio"] == pytest.approx(1.0)
    first = {key: value for key, value in partial.items() if key != "is_partial"}
    first.update(revision=1, computed_at_utc=pd.Timestamp("2026-09-02", tz=UTC).isoformat())
    corrected = dict(first, revision=2, quality_status="CORRECTED", input_hash="corrigida",
                     computed_at_utc=pd.Timestamp.now(tz="UTC").isoformat())
    names = list(first)
    sql = f"INSERT INTO _bars_derived ({', '.join(names)}) VALUES ({', '.join(':' + name for name in names)})"
    with db.engine.begin() as conn:
        conn.execute(text(legacy_schema))
        conn.execute(text(sql), [first, corrected])

    assert resampling.latest_bars(db, "TEST", "M5").empty


def test_incomplete_sessions_are_not_used_as_historical_volume_or_levels():
    bars = bars_frame(sessions=8, bars_per_session=6)
    bars["session_complete"] = True
    bars.loc[bars.session_id == "s2", "session_complete"] = False
    volume, _ = volume_profiles.session_volume_features(bars, windows=(3,), min_sessions=3,
                                                        percentile_window=3)
    assert "s3" not in set(volume[volume.feature_id == "volume.session_median_3.real"].session_id)
    rvol, _ = volume_profiles.intraday_rvol(bars, lookback=3, min_sessions=3)
    assert "s3" not in set(rvol[rvol.feature_id == "rvol.real"].session_id)
    structure = session_structure.structure_features(bars, atr_n=1)
    assert "s3" not in set(structure[structure.feature_id == "session.gap"].session_id)


def test_late_session_corrections_propagate_only_to_dependent_outputs():
    bars = bars_frame(sessions=10, bars_per_session=6)
    bars["computed_at_utc"] = pd.Timestamp("2026-09-01", tz=UTC)
    bars.loc[bars.session_id == "s1", "revision"] = 2
    bars.loc[bars.session_id == "s1", "computed_at_utc"] = pd.Timestamp("2026-10-01", tz=UTC)

    params = {"windows": (5,), "slope_window": 3, "percentile_window": 5, "min_sessions": 1,
              "last_session_open": False}
    volume, _ = volume_profiles.session_volume_features(bars, **params)
    rows = producers._long_rows(volume, bars, "volume", params)
    median_id = "volume.session_median_5.real"
    s6 = next(row for row in rows if row["feature_id"] == median_id and row["observation_time"] ==
              bars[bars.session_id == "s6"]["bar_start_utc"].min())
    s7 = next(row for row in rows if row["feature_id"] == median_id and row["observation_time"] ==
              bars[bars.session_id == "s7"]["bar_start_utc"].min())
    correction = pd.Timestamp("2026-10-01", tz=UTC)
    assert s6["available_at"] == correction
    assert s7["available_at"] < correction
    assert s6["source_revision"].startswith("bars:")

    rvol_params = {"lookback": 5, "min_sessions": 1, "same_weekday": False}
    rvol, _ = volume_profiles.intraday_rvol(bars, **rvol_params)
    rvol_rows = producers._long_rows(rvol, bars, "rvol", rvol_params)
    rvol_s6 = next(row for row in rvol_rows if row["feature_id"] == "rvol.real" and row["session_id"] == "s6")
    rvol_s7 = next(row for row in rvol_rows if row["feature_id"] == "rvol.real" and row["session_id"] == "s7")
    assert rvol_s6["available_at"] == correction
    assert rvol_s7["available_at"] < correction

    structure_bars = bars.copy()
    structure_bars.loc[structure_bars.session_id == "s5", "revision"] = 2
    structure_bars.loc[structure_bars.session_id == "s5", "computed_at_utc"] = correction
    structure_params = {"opening_minutes": 10, "atr_n": 1, "last_session_open": True}
    structure = session_structure.structure_features(structure_bars, **structure_params)
    structure_rows = producers._long_rows(structure, structure_bars, "structure", structure_params)
    level = next(row for row in structure_rows if row["feature_id"] == "level.dist_prev_high_atr" and
                 row["session_id"] == "s6")
    assert level["available_at"] == correction


def test_finite_correction_window_expires_but_recursive_provenance_does_not():
    bars = bars_frame(sessions=1, bars_per_session=100)
    correction = pd.Timestamp("2026-10-01", tz=UTC)
    bars.loc[2, "revision"] = 2
    bars.loc[2, "computed_at_utc"] = correction
    available = producers._correction_times(bars, window=70)
    assert available.iloc[2] == correction and available.iloc[71] == correction
    assert pd.isna(available.iloc[72])
    recursive = producers._correction_times(bars, cumulative=True)
    assert recursive.iloc[2] == correction and recursive.iloc[-1] == correction


def test_corrected_regime_state_propagates_beyond_adx_memory_until_reconfirmed():
    bars = bars_frame(sessions=1, bars_per_session=200)
    bars["computed_at_utc"] = pd.Timestamp("2026-09-01", tz=UTC)
    bars.loc[2, "revision"] = 2
    correction = pd.Timestamp("2026-10-01", tz=UTC)
    bars.loc[2, "computed_at_utc"] = correction
    params = {"adx_n": 2, "er_n": 2, "r2_n": 3, "trend_adx": 25, "range_adx": 20,
              "er_cut": 0.3, "r2_cut": 0.5, "min_duration": 1}
    rows, _ = producers._family_rows("regime", bars, params, "auto")
    last_start = bars["bar_start_utc"].iloc[-1]
    state = next(row for row in rows if row["feature_id"] == "regime.state" and
                 row["observation_time"] == last_start)
    adx = next(row for row in rows if row["feature_id"] == "regime.adx" and
               row["observation_time"] == last_start)
    assert state["available_at"] == correction
    assert adx["available_at"] == correction
    assert state["source_revision"] == adx["source_revision"]
    assert state["metadata"]["source_window_bars"] == len(bars)


@pytest.fixture
def db(tmp_path):
    manager = DatabaseManager(db_type="sqlite", db_path=str(tmp_path / "profiles.sqlite"), broker_utc_offset=0)
    yield manager
    manager.close()


def m1_days(days=12, minutes=180, start="2026-09-01 09:00", bump=None):
    frames = []
    for d in range(days):
        times = pd.date_range(pd.Timestamp(start) + pd.Timedelta(days=d), periods=minutes, freq="min")
        close = 100 + np.cumsum(np.sin(np.arange(minutes) / 7.0)) * 0.1 + d
        vol = np.full(minutes, 10)
        if bump is not None:
            vol = bump(d, vol)
        frames.append(pd.DataFrame({"time": times, "open": close - 0.05, "high": close + 0.2, "low": close - 0.2,
                                    "close": close, "tick_volume": vol * 2, "spread": 2, "real_volume": vol}))
    return pd.concat(frames, ignore_index=True)


INFO = SimpleNamespace(name="TEST", description="sintético", path="Teste\\TEST", digits=2, currency_profit="BRL")


def test_latest_bars_filters_partial_windows_and_marks_incomplete_session(db):
    db.save_ohlcv_data("TEST", "1 minuto", m1_days(days=1, minutes=10, start="2026-09-01 09:01"))
    resampling.resample_symbol(db, "TEST", ["M5"], symbol_info=INFO)
    all_bars = resampling.latest_bars(db, "TEST", "M5", closed_only=False)
    assert all_bars["is_partial"].sum() == 2
    complete = resampling.latest_bars(db, "TEST", "M5")
    assert len(complete) == 1
    assert complete.iloc[0]["quality_status"] == "COMPLETE"
    assert not complete.iloc[0]["session_complete"]


def test_compute_writes_point_in_time_values_and_is_idempotent(db):
    db.save_ohlcv_data("TEST", "1 minuto", m1_days())
    resampling.resample_symbol(db, "TEST", ["M15"], symbol_info=INFO)
    first = producers.compute(db, "TEST", "M15")
    again = producers.compute(db, "TEST", "M15")
    assert sum(item["inserted"] for item in first) > 0
    assert sum(item["inserted"] + item["revised"] for item in again) == 0
    regime_def = features.get_feature(db, "regime.state")
    assert "não é probabilidade" in regime_def["parameters"]["confidence"]

    bars = resampling.latest_bars(db, "TEST", "M15")
    bar = bars.iloc[40]
    # Na metade da barra, o valor dela ainda não existe; no fim, existe.
    mid = bar["bar_start_utc"] + pd.Timedelta(minutes=7)
    seen_mid = feature_store.query_as_of(db, mid, symbol="TEST", timeframe="M15", feature_id="volatility.atr")
    assert seen_mid["observation_time"].max() < bar["bar_start_utc"]
    seen_end = feature_store.query_as_of(db, bar["bar_end_utc"], symbol="TEST", timeframe="M15",
                                         feature_id="volatility.atr")
    assert seen_end["observation_time"].max() == bar["bar_start_utc"]
    meta = feature_store.query_as_of(db, bar["bar_end_utc"], symbol="TEST", timeframe="M15",
                                     feature_id="volatility.atr")["metadata_json"].iloc[-1]
    assert '"reconstructed":true' in meta


def test_resampling_source_sequence_advances_only_for_new_revisions(db):
    db.save_ohlcv_data("TEST", "1 minuto", m1_days(days=1, minutes=20))
    resampling.resample_symbol(db, "TEST", ["M5"], symbol_info=INFO)
    assert resampling.latest_source_sequence(db, "TEST", "M5") == 1
    resampling.resample_symbol(db, "TEST", ["M5"], symbol_info=INFO)
    assert resampling.latest_source_sequence(db, "TEST", "M5") == 1

    m1 = db.load_ohlcv("TEST", Timeframe.M1.label)
    hit = m1["time"] == m1["time"].iloc[2]
    m1.loc[hit, "high"] = m1.loc[hit, "high"] + 1
    db.save_ohlcv_data("TEST", "1 minuto", m1[hit])
    resampling.resample_symbol(db, "TEST", ["M5"], symbol_info=INFO)
    assert resampling.latest_source_sequence(db, "TEST", "M5") == 2
    with db.engine.connect() as conn:
        sequences = conn.execute(text(
            "SELECT DISTINCT source_seq FROM _bars_derived WHERE symbol = 'TEST' AND tf = 'M5' "
            "ORDER BY source_seq")).scalars().all()
    assert sequences == [1, 2]


def test_producer_checkpoint_skips_unchanged_source_before_loading_history(db, monkeypatch):
    from mt5_extracao.analytics import producer_jobs

    db.save_ohlcv_data("TEST", "1 minuto", m1_days(days=1, minutes=20))
    resampling.resample_symbol(db, "TEST", ["M5"], symbol_info=INFO)
    first = producers.compute(db, "TEST", "M5", families=["calendar"])
    assert first[0]["job"]["status"] == "COMPLETE"
    checkpoint = producer_jobs.get_checkpoint(db, "TEST", "M5", "calendar")
    assert checkpoint["source_sequence"] == 1 and checkpoint["status"] == "COMPLETE"
    run = producer_jobs.latest_run(db, "TEST", "M5", "calendar")
    assert run["status"] == "COMPLETE" and run["duration_ms"] >= 0
    assert run["peak_rss_bytes"] > 0 and run["database_bytes_after"] >= run["database_bytes_before"]

    monkeypatch.setattr(resampling, "latest_bars", lambda *args, **kwargs: pytest.fail(
        "compute recarregou histórico apesar do checkpoint válido"))
    again = producers.compute(db, "TEST", "M5", families=["calendar"])
    assert again[0]["skipped"] and again[0]["job"]["status"] == "SKIPPED"


def test_compute_reprocesses_newly_closed_bars_without_a_source_revision(db, monkeypatch):
    from mt5_extracao.analytics import producer_jobs

    first = pd.date_range("2026-09-01 09:00", periods=10, freq="min")
    second = pd.date_range("2026-09-02 09:00", periods=5, freq="min")
    times = first.append(second)
    close = 100 + np.arange(len(times), dtype=float) / 100.0
    m1 = pd.DataFrame({"time": times, "open": close - 0.01, "high": close + 0.02,
                       "low": close - 0.02, "close": close, "tick_volume": 10,
                       "spread": 2, "real_volume": 10})
    db.save_ohlcv_data("TEST", "1 minuto", m1)
    resampling.resample_symbol(db, "TEST", ["M5"], symbol_info=INFO)
    clock = {"now": pd.Timestamp("2026-09-01 09:11", tz=UTC)}
    monkeypatch.setattr(resampling, "_utc_now", lambda: clock["now"])

    first_run = producers.compute(db, "TEST", "M5", families=["calendar"])
    assert first_run[0]["inserted"] == 8
    source_sequence = resampling.latest_source_sequence(db, "TEST", "M5")
    checkpoint = producer_jobs.get_checkpoint(db, "TEST", "M5", "calendar")
    assert checkpoint["source_sequence"] == source_sequence == 1

    clock["now"] = pd.Timestamp("2026-09-02 09:16", tz=UTC)
    original = resampling.latest_bars
    calls = []

    def spy(db_, symbol, timeframe, **kwargs):
        frame = original(db_, symbol, timeframe, **kwargs)
        calls.append((kwargs, len(frame)))
        return frame

    monkeypatch.setattr(resampling, "latest_bars", spy)
    next_run = producers.compute(db, "TEST", "M5", families=["calendar"])
    assert resampling.latest_source_sequence(db, "TEST", "M5") == source_sequence
    assert calls[0][0]["source_seq_after"] == source_sequence
    assert calls[0][0]["bar_start_after"] == checkpoint["last_bar_start_utc"]
    assert calls[0][1] == 1  # uma barra lida apesar de o histórico conter três
    assert next_run[0]["inserted"] == 4 and not next_run[0].get("skipped", False)


def test_write_guard_is_checked_for_each_output_batch_and_final_checkpoint(db):
    db.save_ohlcv_data("TEST", "1 minuto", m1_days(days=1, minutes=20))
    resampling.resample_symbol(db, "TEST", ["M5"], symbol_info=INFO)
    calls = []

    def guard():
        calls.append(1)
        return None

    producers.compute(db, "TEST", "M5", families=["calendar"], batch_size=1, write_guard=guard)
    assert len(calls) == 5  # quatro instantes com quatro características, mais o checkpoint final


def test_calendar_append_uses_only_changed_bars_and_matches_full_recompute(db, monkeypatch):
    db.save_ohlcv_data("TEST", "1 minuto", m1_days(days=1, minutes=20))
    resampling.resample_symbol(db, "TEST", ["M5"], symbol_info=INFO)
    producers.compute(db, "TEST", "M5", families=["calendar"])
    db.save_ohlcv_data("TEST", "1 minuto", m1_days(days=1, minutes=20, start="2026-09-02 09:00"))
    resampling.resample_symbol(db, "TEST", ["M5"], symbol_info=INFO)

    original = resampling.latest_bars
    calls = []

    def spy(db_, symbol, timeframe, **kwargs):
        calls.append(kwargs.get("source_seq_after"))
        return original(db_, symbol, timeframe, **kwargs)

    monkeypatch.setattr(resampling, "latest_bars", spy)
    result = producers.compute(db, "TEST", "M5", families=["calendar"])
    assert calls == [1] and result[0]["inserted"] == 16

    all_bars = original(db, "TEST", "M5")
    full_rows = producers.calendar_rows(all_bars)
    current_session = all_bars["session_id"].iloc[-1]
    start = all_bars.loc[all_bars.session_id == current_session, "bar_start_utc"].min()
    expected = {(row["feature_id"], pd.Timestamp(row["observation_time"]), row["value"],
                 pd.Timestamp(row["available_at"]), row["source_revision"])
                for row in full_rows if pd.Timestamp(row["observation_time"]) >= start}
    history = feature_store.query_history(db, symbol="TEST", timeframe="M5",
                                          algorithm_version=result[0]["algorithm_version"])
    actual = {(row.feature_id, row.observation_time, row.numeric_value if pd.notna(row.numeric_value)
               else row.categorical_value, row.available_at_utc, row.source_revision)
              for row in history.itertuples() if row.observation_time >= start}
    assert actual == expected


def test_volume_and_rvol_append_reuse_bounded_session_context(db, monkeypatch):
    db.save_ohlcv_data("TEST", "1 minuto", m1_days(days=8, minutes=20))
    resampling.resample_symbol(db, "TEST", ["M5"], symbol_info=INFO)
    producers.compute(db, "TEST", "M5", families=["volume", "rvol"])
    db.save_ohlcv_data("TEST", "1 minuto", m1_days(days=1, minutes=20, start="2026-09-08 09:20"))
    resampling.resample_symbol(db, "TEST", ["M5"], symbol_info=INFO)

    original = resampling.latest_bars
    calls = []

    def spy(db_, symbol, timeframe, **kwargs):
        calls.append(kwargs.get("source_seq_after"))
        return original(db_, symbol, timeframe, **kwargs)

    monkeypatch.setattr(resampling, "latest_bars", spy)
    result = producers.compute(db, "TEST", "M5", families=["volume", "rvol"])
    assert calls == [1]
    assert result[0]["inserted"] == 0  # sessão atual continua excluída do perfil diário
    assert result[1]["inserted"] == 8  # dois fatos por cada uma das quatro barras novas

    all_bars = original(db, "TEST", "M5")
    full_rows, _ = producers._family_rows("rvol", all_bars, producers.DEFAULTS["rvol"], "real")
    new_starts = set(all_bars[all_bars["bar_start_utc"] >= pd.Timestamp("2026-09-08T09:20:00Z")]
                     ["bar_start_utc"])
    expected = {(row["feature_id"], pd.Timestamp(row["observation_time"]), row["value"],
                 pd.Timestamp(row["available_at"]), row["source_revision"])
                for row in full_rows if row["observation_time"] in new_starts}
    history = feature_store.query_history(db, symbol="TEST", timeframe="M5",
                                          algorithm_version=next(item["algorithm_version"] for item in result if item["family"] == "rvol"),
                                          start="2026-09-08T09:20:00Z")
    actual = {(row.feature_id, row.observation_time, row.numeric_value if pd.notna(row.numeric_value)
               else row.categorical_value, row.available_at_utc, row.source_revision)
              for row in history.itertuples()}
    assert actual == expected


def test_auto_volume_source_switches_version_when_new_history_is_unknown(db):
    db.save_ohlcv_data("TEST", "1 minuto", m1_days(days=8, minutes=20))
    resampling.resample_symbol(db, "TEST", ["M5"], symbol_info=INFO)
    initial = producers.compute(db, "TEST", "M5", families=["volume", "rvol"])
    new_bars = m1_days(days=1, minutes=20, start="2026-09-08 09:20")
    new_bars.loc[0, "real_volume"] = None
    db.save_ohlcv_data("TEST", "1 minuto", new_bars)
    resampling.resample_symbol(db, "TEST", ["M5"], symbol_info=INFO)

    updated = producers.compute(db, "TEST", "M5", families=["volume", "rvol"])
    assert all(item["volume_source"] == "tick" for item in updated)
    before_versions = {item["family"]: item["algorithm_version"] for item in initial}
    after_versions = {item["family"]: item["algorithm_version"] for item in updated}
    assert set(before_versions) == set(after_versions) == {"volume", "rvol"}
    assert all(before_versions[family] != after_versions[family] for family in before_versions)
    definitions = feature_store.list_definitions(db)
    for family in before_versions:
        assert any(item["algorithm_version"] == before_versions[family] and
                   item["parameters"].get("family") == family and
                   item["parameters"].get("volume_source") == "real" for item in definitions)
        assert any(item["algorithm_version"] == after_versions[family] and
                   item["parameters"].get("family") == family and
                   item["parameters"].get("volume_source") == "tick" for item in definitions)


def test_session_transition_incrementally_completes_volume_and_structure(db, monkeypatch):
    db.save_ohlcv_data("TEST", "1 minuto", m1_days(days=8, minutes=20))
    resampling.resample_symbol(db, "TEST", ["M5"], symbol_info=INFO)
    producers.compute(db, "TEST", "M5", families=["volume", "rvol", "structure"])
    prior_bars = resampling.latest_bars(db, "TEST", "M5")
    old_session = str(prior_bars["session_id"].iloc[-1])
    db.save_ohlcv_data("TEST", "1 minuto", m1_days(days=1, minutes=20, start="2026-09-09 09:00"))
    resampling.resample_symbol(db, "TEST", ["M5"], symbol_info=INFO)

    original = resampling.latest_bars
    calls = []

    def spy(db_, symbol, timeframe, **kwargs):
        calls.append(kwargs.get("source_seq_after"))
        return original(db_, symbol, timeframe, **kwargs)

    monkeypatch.setattr(resampling, "latest_bars", spy)
    result = producers.compute(db, "TEST", "M5", families=["volume", "rvol", "structure"])
    assert calls == [1] and all(item["inserted"] > 0 for item in result)

    all_bars = original(db, "TEST", "M5")
    current_session = str(all_bars["session_id"].iloc[-1])
    current_open = all_bars.loc[all_bars.session_id == current_session, "bar_start_utc"].min()
    expected = {}
    for family in ("volume", "rvol", "structure"):
        rows, _ = producers._family_rows(family, all_bars, producers.DEFAULTS[family], "real")
        for row in rows:
            newly_relevant = (row.get("session_id") == old_session and family == "volume") or \
                (pd.Timestamp(row["observation_time"]) >= current_open) or \
                (row.get("feature_id") == "session.return" and row.get("session_id") == old_session)
            if newly_relevant:
                expected[(row["feature_id"], pd.Timestamp(row["observation_time"]))] = (
                    row["value"], pd.Timestamp(row["available_at"]), row["source_revision"])
    history = feature_store.query_history(db, symbol="TEST", timeframe="M5",
                                          )
    history = history[history.algorithm_version.isin({item["algorithm_version"] for item in result})]
    actual_all = {(row.feature_id, row.observation_time): (row.numeric_value if pd.notna(row.numeric_value)
                  else row.categorical_value, row.available_at_utc, row.source_revision)
                  for row in history.itertuples()}
    actual = {key: actual_all[key] for key in expected if key in actual_all}
    assert actual.keys() == expected.keys()
    for key, (expected_value, expected_available, expected_revision) in expected.items():
        actual_value, actual_available, actual_revision = actual[key]
        assert actual_available == expected_available and actual_revision == expected_revision
        assert actual_value == pytest.approx(expected_value, rel=1e-12, abs=1e-14)


def test_volatility_append_continues_recursive_atr_and_matches_full_recompute(db, monkeypatch):
    db.save_ohlcv_data("TEST", "1 minuto", m1_days(days=12, minutes=180))
    resampling.resample_symbol(db, "TEST", ["M5"], symbol_info=INFO)
    producers.compute(db, "TEST", "M5", families=["volatility"])
    db.save_ohlcv_data("TEST", "1 minuto", m1_days(days=1, minutes=30, start="2026-09-12 12:00"))
    resampling.resample_symbol(db, "TEST", ["M5"], symbol_info=INFO)

    original = resampling.latest_bars
    calls = []

    def spy(db_, symbol, timeframe, **kwargs):
        calls.append(kwargs.get("source_seq_after"))
        return original(db_, symbol, timeframe, **kwargs)

    monkeypatch.setattr(resampling, "latest_bars", spy)
    result = producers.compute(db, "TEST", "M5", families=["volatility"])
    assert calls == [1] and result[0]["inserted"] > 0

    all_bars = original(db, "TEST", "M5")
    full_rows, _ = producers._family_rows("volatility", all_bars, producers.DEFAULTS["volatility"], "auto")
    start = pd.Timestamp("2026-09-12T12:00:00Z")
    expected = {(row["feature_id"], pd.Timestamp(row["observation_time"])):
                (row["value"], pd.Timestamp(row["available_at"]), row["source_revision"])
                for row in full_rows if row["observation_time"] >= start}
    history = feature_store.query_history(db, symbol="TEST", timeframe="M5",
                                          algorithm_version=result[0]["algorithm_version"],
                                          start=start)
    actual = {(row.feature_id, row.observation_time): (row.numeric_value if pd.notna(row.numeric_value)
              else row.categorical_value, row.available_at_utc, row.source_revision)
              for row in history.itertuples()}
    assert actual.keys() == expected.keys()
    for key, (expected_value, expected_available, expected_revision) in expected.items():
        actual_value, actual_available, actual_revision = actual[key]
        assert actual_available == expected_available and actual_revision == expected_revision
        if isinstance(expected_value, (float, int)):
            # Pinned to below 1e-12 relative: only IEEE summation order may differ in rolling std.
            assert actual_value == pytest.approx(expected_value, rel=1e-12, abs=1e-14)
        else:
            assert actual_value == expected_value


def test_regime_append_continues_adx_and_confirmation_state(db, monkeypatch):
    db.save_ohlcv_data("TEST", "1 minuto", m1_days(days=12, minutes=180))
    resampling.resample_symbol(db, "TEST", ["M5"], symbol_info=INFO)
    producers.compute(db, "TEST", "M5", families=["regime"])
    db.save_ohlcv_data("TEST", "1 minuto", m1_days(days=1, minutes=30, start="2026-09-12 12:00"))
    resampling.resample_symbol(db, "TEST", ["M5"], symbol_info=INFO)

    original = resampling.latest_bars
    calls = []

    def spy(db_, symbol, timeframe, **kwargs):
        calls.append(kwargs.get("source_seq_after"))
        return original(db_, symbol, timeframe, **kwargs)

    monkeypatch.setattr(resampling, "latest_bars", spy)
    result = producers.compute(db, "TEST", "M5", families=["regime"])
    assert calls == [1] and result[0]["inserted"] > 0

    all_bars = original(db, "TEST", "M5")
    full_rows, _ = producers._family_rows("regime", all_bars, producers.DEFAULTS["regime"], "auto")
    start = pd.Timestamp("2026-09-12T12:00:00Z")
    expected = {(row["feature_id"], pd.Timestamp(row["observation_time"])):
                (row["value"], pd.Timestamp(row["available_at"]), row["source_revision"], row["metadata"])
                for row in full_rows if row["observation_time"] >= start}
    history = feature_store.query_history(db, symbol="TEST", timeframe="M5",
                                          algorithm_version=result[0]["algorithm_version"], start=start)
    actual = {(row.feature_id, row.observation_time): (row.numeric_value if pd.notna(row.numeric_value)
              else row.categorical_value, row.available_at_utc, row.source_revision,
              json.loads(row.metadata_json)) for row in history.itertuples()}
    assert actual.keys() == expected.keys()
    for key, (expected_value, expected_available, expected_revision, expected_metadata) in expected.items():
        actual_value, actual_available, actual_revision, actual_metadata = actual[key]
        assert actual_available == expected_available and actual_revision == expected_revision
        assert actual_metadata["source_window_bars"] == expected_metadata["source_window_bars"]
        if isinstance(expected_value, (float, int)):
            assert actual_value == pytest.approx(expected_value, rel=1e-12, abs=1e-14)
        else:
            assert actual_value == expected_value


def test_structure_append_continues_atr_and_matches_full_recompute(db, monkeypatch):
    db.save_ohlcv_data("TEST", "1 minuto", m1_days(days=12, minutes=180))
    resampling.resample_symbol(db, "TEST", ["M5"], symbol_info=INFO)
    producers.compute(db, "TEST", "M5", families=["structure"])
    db.save_ohlcv_data("TEST", "1 minuto", m1_days(days=1, minutes=30, start="2026-09-12 12:00"))
    resampling.resample_symbol(db, "TEST", ["M5"], symbol_info=INFO)

    original = resampling.latest_bars
    calls = []

    def spy(db_, symbol, timeframe, **kwargs):
        calls.append(kwargs.get("source_seq_after"))
        return original(db_, symbol, timeframe, **kwargs)

    monkeypatch.setattr(resampling, "latest_bars", spy)
    result = producers.compute(db, "TEST", "M5", families=["structure"])
    assert calls == [1] and result[0]["inserted"] > 0

    all_bars = original(db, "TEST", "M5")
    full_rows, _ = producers._family_rows("structure", all_bars, producers.DEFAULTS["structure"], "auto")
    start = pd.Timestamp("2026-09-12T12:00:00Z")
    expected = {(row["feature_id"], pd.Timestamp(row["observation_time"])):
                (row["value"], pd.Timestamp(row["available_at"]), row["source_revision"])
                for row in full_rows if row["observation_time"] >= start}
    history = feature_store.query_history(db, symbol="TEST", timeframe="M5",
                                          algorithm_version=result[0]["algorithm_version"], start=start)
    actual = {(row.feature_id, row.observation_time): (row.numeric_value if pd.notna(row.numeric_value)
              else row.categorical_value, row.available_at_utc, row.source_revision)
              for row in history.itertuples()}
    assert actual.keys() == expected.keys()
    for key, (expected_value, expected_available, expected_revision) in expected.items():
        actual_value, actual_available, actual_revision = actual[key]
        assert actual_available == expected_available and actual_revision == expected_revision
        assert actual_value == pytest.approx(expected_value, rel=1e-12, abs=1e-14)


def test_late_correction_forces_full_recompute_and_revises_dependent_rvol(db, monkeypatch):
    db.save_ohlcv_data("TEST", "1 minuto", m1_days(days=12, minutes=180))
    resampling.resample_symbol(db, "TEST", ["M5"], symbol_info=INFO)
    producers.compute(db, "TEST", "M5")
    m1 = db.load_ohlcv("TEST", Timeframe.M1.label)
    hit = m1["time"] == m1["time"].iloc[2]
    m1.loc[hit, "real_volume"] = 100
    db.save_ohlcv_data("TEST", "1 minuto", m1[hit])
    resampling.resample_symbol(db, "TEST", ["M5"], symbol_info=INFO)
    corrected_bars = resampling.latest_bars(db, "TEST", "M5", closed_only=False)
    correction_available = corrected_bars["computed_at_utc"].max()

    original = resampling.latest_bars
    calls = []

    def spy(db_, symbol, timeframe, **kwargs):
        calls.append(kwargs.get("source_seq_after"))
        return original(db_, symbol, timeframe, **kwargs)

    monkeypatch.setattr(resampling, "latest_bars", spy)
    result = producers.compute(db, "TEST", "M5")
    assert calls == [1, None]  # change histórico detectada; as seis famílias usam a reconstrução completa
    assert sum(item["revised"] for item in result) > 0
    history = feature_store.query_history(db, symbol="TEST", timeframe="M5", feature_id="rvol.real",
                                          algorithm_version=next(item["algorithm_version"] for item in result if item["family"] == "rvol"))
    revised = history[history["revision"] > 1]
    assert not revised.empty
    assert revised["available_at_utc"].max() >= correction_available


def test_producer_batch_failure_resumes_after_committed_checkpoint(db, monkeypatch):
    from mt5_extracao.analytics import producer_jobs

    db.save_ohlcv_data("TEST", "1 minuto", m1_days(days=1, minutes=20))
    resampling.resample_symbol(db, "TEST", ["M5"], symbol_info=INFO)
    original = feature_store.save_values
    calls = {"count": 0}

    def fail_after_first_commit(db_, records, **kwargs):
        counts = original(db_, records, **kwargs)
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("falha simulada após lote confirmado")
        return counts

    monkeypatch.setattr(feature_store, "save_values", fail_after_first_commit)
    with pytest.raises(RuntimeError, match="falha simulada"):
        producers.compute(db, "TEST", "M5", families=["calendar"], batch_size=1, max_retries=0)
    checkpoint = producer_jobs.get_checkpoint(db, "TEST", "M5", "calendar")
    assert checkpoint["status"] == "RUNNING" and checkpoint["last_bar_start_utc"] is not None
    assert producer_jobs.latest_run(db, "TEST", "M5", "calendar")["status"] == "FAILED"

    monkeypatch.setattr(feature_store, "save_values", original)
    result = producers.compute(db, "TEST", "M5", families=["calendar"], batch_size=1)
    assert result[0]["job"]["status"] == "COMPLETE"
    checkpoint = producer_jobs.get_checkpoint(db, "TEST", "M5", "calendar")
    assert checkpoint["status"] == "COMPLETE" and checkpoint["source_sequence"] == 1
    history = feature_store.query_history(db, symbol="TEST", timeframe="M5",
                                          algorithm_version=result[0]["algorithm_version"])
    assert len(history) == 16


def test_producer_memory_limit_fails_before_feature_writes(db, monkeypatch):
    from mt5_extracao.analytics import producer_jobs

    db.save_ohlcv_data("TEST", "1 minuto", m1_days(days=1, minutes=20))
    resampling.resample_symbol(db, "TEST", ["M5"], symbol_info=INFO)
    monkeypatch.setattr(producer_jobs, "current_rss_bytes", lambda: 2 * 1024 * 1024)
    with pytest.raises(MemoryError, match="limite de memória"):
        producers.compute(db, "TEST", "M5", families=["calendar"], max_memory_mb=1)
    assert feature_store.query_history(db, symbol="TEST", timeframe="M5").empty
    run = producer_jobs.latest_run(db, "TEST", "M5", "calendar")
    assert run["status"] == "FAILED" and "limite de memória" in run["error"]


def test_asof_join_across_timeframes_never_uses_open_higher_bar(db):
    db.save_ohlcv_data("TEST", "1 minuto", m1_days(days=3))
    resampling.resample_symbol(db, "TEST", ["M5", "H1"], symbol_info=INFO)
    producers.compute(db, "TEST", "H1", families=["calendar", "volatility"])
    h1 = resampling.latest_bars(db, "TEST", "H1")
    m5 = resampling.latest_bars(db, "TEST", "M5")
    decisions = pd.DataFrame({"decision_time": m5["bar_end_utc"]})
    values = feature_store.query_as_of(db, m5["bar_end_utc"].max(), symbol="TEST", timeframe="H1",
                                       feature_id="volatility.range_pct")
    joined = feature_store.asof_join(decisions, values)
    for row in joined.dropna(subset=["observation_time"]).itertuples():
        h1_bar = h1[h1.bar_start_utc == row.observation_time].iloc[0]
        assert h1_bar["bar_end_utc"] <= row.decision_time           # só H1 já fechada
    first_h1_end = h1["bar_end_utc"].min()
    early = joined[joined.decision_time < first_h1_end]
    assert early["numeric_value"].isna().all()                       # ausente, nunca zero


def test_query_history_asof_join_preserves_corrections_and_series_identity(db):
    features.register(db, "context.test", "Contexto de teste", "1.0", dependencies=["bars:M1"])
    t1, t2, t3, t4 = [pd.Timestamp(f"2026-10-01T{hour:02d}:00:00Z") for hour in (1, 2, 3, 4)]
    observation = pd.Timestamp("2026-10-01T00:00:00Z")
    records = [
        {"symbol": "A", "timeframe": "M1", "feature_id": "context.test", "algorithm_version": "1.0",
         "observation_time": observation, "value": 10, "available_at": t1, "observed_at": t1,
         "source_revision": "a1"},
        {"symbol": "A", "timeframe": "M1", "feature_id": "context.test", "algorithm_version": "1.0",
         "observation_time": observation, "value": 11, "available_at": t3, "observed_at": t3,
         "source_revision": "a2"},
        {"symbol": "A", "timeframe": "M15", "feature_id": "context.test", "algorithm_version": "1.0",
         "observation_time": observation, "value": 150, "available_at": t1, "observed_at": t1,
         "source_revision": "a15"},
        {"symbol": "B", "timeframe": "M1", "feature_id": "context.test", "algorithm_version": "1.0",
         "observation_time": observation, "value": 20, "available_at": t1, "observed_at": t1,
         "source_revision": "b1"},
    ]
    counts = feature_store.save_values(db, records)
    assert counts == {"inserted": 3, "revised": 1, "unchanged": 0}
    history = feature_store.query_history(db, feature_id="context.test")
    decisions = pd.DataFrame({"decision_time": [t2, t4]})
    joined = feature_store.asof_join(decisions, history).set_index(["decision_time", "symbol", "tf"])
    assert joined.loc[(t2, "A", "M1"), "numeric_value"] == 10
    assert joined.loc[(t4, "A", "M1"), "numeric_value"] == 11
    assert joined.loc[(t4, "A", "M15"), "numeric_value"] == 150
    assert joined.loc[(t4, "B", "M1"), "numeric_value"] == 20
    only_a = feature_store.asof_join(pd.DataFrame({"decision_time": [t4], "symbol": ["A"]}), history)
    assert set(only_a["symbol"]) == {"A"}


def test_late_bar_correction_is_not_anticipated(db):
    db.save_ohlcv_data("TEST", "1 minuto", m1_days(days=8))
    resampling.resample_symbol(db, "TEST", ["M15"], symbol_info=INFO)
    producers.compute(db, "TEST", "M15", families=["volatility"])
    before = resampling.latest_bars(db, "TEST", "M15")
    target = before.iloc[30]
    # Corrige uma barra M1 dentro da barra alvo (chegou depois) e regera as barras.
    m1 = db.load_ohlcv("TEST", Timeframe.M1.label)
    hit = m1["time"] == (target["bar_start_utc"].tz_convert(None) + pd.Timedelta(minutes=3))
    m1.loc[hit, "high"] = m1.loc[hit, "high"] + 50
    db.save_ohlcv_data("TEST", "1 minuto", m1[hit])
    resampling.resample_symbol(db, "TEST", ["M15"], symbol_info=INFO)
    after = resampling.latest_bars(db, "TEST", "M15")
    fixed = after[after.bar_start_utc == target["bar_start_utc"]].iloc[0]
    assert int(fixed["revision"]) == 2
    producers.compute(db, "TEST", "M15", families=["volatility"])
    at_bar_end = feature_store.query_as_of(db, target["bar_end_utc"], symbol="TEST", timeframe="M15",
                                           feature_id="volatility.range_pct")
    old = at_bar_end[at_bar_end.observation_time == target["bar_start_utc"]]["numeric_value"].iloc[0]
    assert old == pytest.approx((target["high"] - target["low"]) / target["close"])
    now = feature_store.query_as_of(db, fixed["computed_at_utc"], symbol="TEST", timeframe="M15",
                                    feature_id="volatility.range_pct")
    new = now[now.observation_time == target["bar_start_utc"]]["numeric_value"].iloc[0]
    assert new > old


def test_late_reference_correction_invalidates_rvol_after_incremental_resume(db, monkeypatch):
    db.save_ohlcv_data("TEST", "1 minuto", m1_days(days=8, minutes=60))
    resampling.resample_symbol(db, "TEST", ["M15"], symbol_info=INFO)
    params = {"lookback": 1, "min_sessions": 1, "same_weekday": False}
    producers.compute(db, "TEST", "M15", families=["rvol"], volume_source="real",
                      overrides={"rvol": params})

    before_bars = resampling.latest_bars(db, "TEST", "M15")
    sessions = (before_bars.groupby("session_id")["bar_start_utc"].min().sort_values().index.tolist())
    reference_session, current_session = sessions[-2:]
    current_observations = before_bars.loc[before_bars.session_id == current_session, "bar_start_utc"]
    old = feature_store.query_as_of(db, "2030-01-01T00:00:00Z", symbol="TEST", timeframe="M15",
                                    feature_id="rvol.real")
    old = old[old.observation_time.isin(current_observations)]
    assert len(old) == len(current_observations)
    assert old["numeric_value"].notna().all()

    reference = before_bars[before_bars.session_id == reference_session]
    m1 = db.load_ohlcv("TEST", Timeframe.M1.label)
    raw_times = pd.to_datetime(m1["time"])
    start = reference["bar_start_utc"].min().tz_convert(None)
    end = reference["bar_end_utc"].max().tz_convert(None)
    hit = (raw_times >= start) & (raw_times < end)
    assert hit.any()
    m1.loc[hit, "real_volume"] = 0
    db.save_ohlcv_data("TEST", Timeframe.M1.label, m1.loc[hit])
    resampling.resample_symbol(db, "TEST", ["M15"], symbol_info=INFO)
    all_bars = resampling.latest_bars(db, "TEST", "M15", closed_only=False)
    corrected_at = all_bars.loc[all_bars.session_id == reference_session, "computed_at_utc"].max()
    assert pd.notna(corrected_at)

    original_save = feature_store.save_values
    committed = {"batches": 0}

    def fail_after_first_batch(db_, records, **kwargs):
        counts = original_save(db_, records, **kwargs)
        if records:
            committed["batches"] += 1
            if committed["batches"] == 1:
                raise RuntimeError("falha simulada após lote confirmado")
        return counts

    monkeypatch.setattr(feature_store, "save_values", fail_after_first_batch)
    with pytest.raises(RuntimeError, match="falha simulada após lote confirmado"):
        producers.compute(db, "TEST", "M15", families=["rvol"], volume_source="real",
                          overrides={"rvol": params}, batch_size=1, max_retries=0)
    monkeypatch.setattr(feature_store, "save_values", original_save)
    result = producers.compute(db, "TEST", "M15", families=["rvol"], volume_source="real",
                               overrides={"rvol": params}, batch_size=1)
    assert result[0]["invalidations"] > 0
    assert result[0]["job"]["status"] == "COMPLETE"

    historical = feature_store.query_as_of(db, corrected_at - pd.Timedelta(nanoseconds=1), symbol="TEST",
                                           timeframe="M15", feature_id="rvol.real")
    historical = historical[historical.observation_time.isin(current_observations)]
    assert historical["numeric_value"].notna().all()
    current = feature_store.query_as_of(db, "2030-01-01T00:00:00Z", symbol="TEST", timeframe="M15",
                                        feature_id="rvol.real")
    current = current[current.observation_time.isin(current_observations)]
    assert len(current) == len(current_observations)
    assert current["quality"].eq("INVALIDATED").all()
    assert current["numeric_value"].isna().all() and current["categorical_value"].isna().all()
    assert current["available_at_utc"].eq(corrected_at).all()


def test_write_guard_blocks_before_writing(db):
    db.save_ohlcv_data("TEST", "1 minuto", m1_days(days=2))
    resampling.resample_symbol(db, "TEST", ["M15"], symbol_info=INFO)
    with pytest.raises(producers.WriteBlocked, match="limite"):
        producers.compute(db, "TEST", "M15", write_guard=lambda: "disco abaixo do mínimo")
    assert feature_store.query_as_of(db, "2030-01-01T00:00:00Z").empty


def test_recalculate_affected_runs_each_family_once(db):
    db.save_ohlcv_data("TEST", "1 minuto", m1_days(days=8))
    resampling.resample_symbol(db, "TEST", ["M15"], symbol_info=INFO)
    producers.compute(db, "TEST", "M15", families=["regime", "calendar"])
    handlers = producers.recalculators(db, "TEST", "M15")
    definitions = features.list_features(db)
    assert all("bars_derived:M15" in item["dependencies"] for item in definitions)
    results = features.recalculate_affected(db, ["bars_derived:M15"], handlers)
    families = {item["result"][0]["family"] for item in results}
    assert families == {"regime", "calendar"}
    assert all(item["result"][0]["inserted"] == 0 for item in results)


def test_cli_compute_reports_families(db, monkeypatch, capsys):
    from mt5_extracao import cli
    db.save_ohlcv_data("TEST", "1 minuto", m1_days(days=3))
    resampling.resample_symbol(db, "TEST", ["M15"], symbol_info=INFO)
    ctx = SimpleNamespace(db=db, config=None)
    monkeypatch.setattr("mt5_extracao.resources.guard.make", lambda db_, config: None)
    args = cli.build_parser().parse_args(["analytics", "features", "compute", "--symbol", "TEST", "--tf", "M15",
                                          "--families", "regime,calendar"])
    assert args.func(args, ctx) == cli.EXIT_OK
    out = capsys.readouterr().out
    assert "regime" in out and "calendar" in out and "reconstruídos" in out


@pytest.mark.parametrize("empty", [False, True])
def test_asof_join_rejects_decisions_without_timezone(empty):
    values = pd.DataFrame() if empty else _join_clock_values()
    with pytest.raises(ValueError, match="fuso"):
        feature_store.asof_join(pd.DataFrame({"decision_time": ["2026-10-01T12:00:00"]}), values)


def _join_clock_values():
    return pd.DataFrame({"observation_time": ["2026-10-01T11:00:00Z"],
                         "available_at_utc": ["2026-10-01T12:00:00.000000901Z"],
                         "valid_until_utc": [None], "numeric_value": [7.0]})


@pytest.mark.parametrize("column", ["observation_time", "available_at_utc", "valid_until_utc"])
@pytest.mark.parametrize("invalid", ["2026-10-01T12:00:00", "data inválida"])
def test_asof_join_rejects_invalid_or_naive_history(column, invalid):
    values = _join_clock_values()
    values[column] = invalid
    with pytest.raises(ValueError, match=column):
        feature_store.asof_join(pd.DataFrame({"decision_time": ["2026-10-01T13:00:00Z"]}), values)


def test_asof_join_preserves_nanosecond_boundary_and_input_frames():
    values = _join_clock_values()
    decisions = pd.DataFrame({"decision_time": ["2026-10-01T09:00:00.000000900-03:00",
                                                 "2026-10-01T09:00:00.000000901-03:00"]})
    original_values, original_decisions = values.copy(deep=True), decisions.copy(deep=True)
    result = feature_store.asof_join(decisions, values)
    assert pd.isna(result.iloc[0].numeric_value)
    assert result.iloc[1].numeric_value == 7
    pd.testing.assert_frame_equal(values, original_values)
    pd.testing.assert_frame_equal(decisions, original_decisions)


def test_compute_preserves_legacy_definitions_and_supports_multiple_timeframes(db):
    features.register(db, "calendar.hour_utc", "calendar hour_utc", "1.1",
                      parameters=producers._definition_parameters("calendar", {}, None),
                      dependencies=["bars_derived:M15", "market_calendar"])
    legacy = feature_store.list_definitions(db)
    db.save_ohlcv_data("TEST", "1 minuto", m1_days(days=2))
    resampling.resample_symbol(db, "TEST", ["M15", "H1"], symbol_info=INFO)
    first = producers.compute(db, "TEST", "M15", families=["calendar"])
    before = feature_store.query_history(db, symbol="TEST", timeframe="M15")
    second = producers.compute(db, "TEST", "H1", families=["calendar"])
    after = feature_store.query_history(db, symbol="TEST", timeframe="M15")
    pd.testing.assert_frame_equal(before, after)
    assert first[0]["algorithm_version"] != second[0]["algorithm_version"]
    assert first[0]["algorithm_version"] != "1.1"
    assert feature_store.list_definitions(db)[0] == legacy[0]
    repeat = producers.compute(db, "TEST", "15min", families=["calendar"])
    assert repeat[0]["algorithm_version"] == first[0]["algorithm_version"]
    assert repeat[0]["inserted"] == repeat[0]["revised"] == 0
    assert set(feature_store.query_history(db)["tf"]) == {"M15", "H1"}


def test_compute_configuration_versions_coexist_and_reuse_the_same_store(db):
    db.save_ohlcv_data("TEST", "1 minuto", m1_days(days=2))
    resampling.resample_symbol(db, "TEST", ["M15"], symbol_info=INFO)
    first = producers.compute(db, "TEST", "M15", families=["volatility"])
    old = feature_store.query_history(db, algorithm_version=first[0]["algorithm_version"])
    second = producers.compute(db, "TEST", "M15", families=["volatility"],
                               overrides={"volatility": {"atr_n": 7}})
    assert first[0]["algorithm_version"] != second[0]["algorithm_version"]
    pd.testing.assert_frame_equal(feature_store.query_history(db,
                                  algorithm_version=first[0]["algorithm_version"]), old)
    repeat = producers.compute(db, "TEST", "M15", families=["volatility"],
                               overrides={"volatility": {"atr_n": 7}})
    assert repeat[0]["algorithm_version"] == second[0]["algorithm_version"]
    assert repeat[0]["inserted"] == repeat[0]["revised"] == 0
    # A versão global mais nova não pode substituir a configuração solicitada.
    default_results = producers.recalculate_affected(db, "TEST", "M15", ["bars_derived:M15"])
    assert default_results
    assert all(item["algorithm_version"] == first[0]["algorithm_version"] for item in default_results)
    assert all(item["result"][0]["inserted"] == item["result"][0]["revised"] == 0 for item in default_results)
    alternate_results = producers.recalculate_affected(db, "TEST", "M15", ["bars_derived:M15"],
                                                     overrides={"volatility": {"atr_n": 7}})
    assert alternate_results
    assert all(item["algorithm_version"] == second[0]["algorithm_version"] for item in alternate_results)


def test_scoped_recalculation_after_newer_h1_definition_keeps_h1_unchanged(db):
    db.save_ohlcv_data("TEST", "1 minuto", m1_days(days=2))
    resampling.resample_symbol(db, "TEST", ["M15", "H1"], symbol_info=INFO)
    producers.compute(db, "TEST", "M15", families=["volatility"])
    producers.compute(db, "TEST", "H1", families=["volatility"])
    h1 = feature_store.query_history(db, symbol="TEST", timeframe="H1")
    initial = feature_store.query_history(db, symbol="TEST", timeframe="M15",
                                         feature_id="volatility.range_pct")
    observed = initial.iloc[0].observation_time
    history = feature_store.query_history(db, symbol="TEST", feature_id="volatility.range_pct")
    first_m15, first_h1 = initial.iloc[0].available_at_utc, h1[h1.feature_id == "volatility.range_pct"].iloc[0].available_at_utc
    joined = feature_store.asof_join(pd.DataFrame({"decision_time": [first_m15, first_h1]}), history)
    joined = joined.set_index(["decision_time", "tf"])
    assert pd.notna(joined.loc[(first_m15, "M15"), "numeric_value"])
    assert pd.isna(joined.loc[(first_m15, "H1"), "numeric_value"])
    assert pd.notna(joined.loc[(first_h1, "H1"), "numeric_value"])
    for increase in [10, 20]:
        raw = db.load_ohlcv("TEST", Timeframe.M1.label)
        hit = raw.time == observed.tz_convert(None) + pd.Timedelta(minutes=3)
        raw.loc[hit, "high"] += increase
        db.save_ohlcv_data("TEST", "1 minuto", raw.loc[hit])
        resampling.resample_symbol(db, "TEST", ["M15"], symbol_info=INFO)
        results = producers.recalculate_affected(db, "TEST", "M15", ["bars_derived:M15"])
        assert results
        assert all(item["result"][0]["timeframe"] == "M15" for item in results)
        assert any(item["result"][0]["revised"] > 0 for item in results)
        pd.testing.assert_frame_equal(feature_store.query_history(db, symbol="TEST", timeframe="H1"), h1)
    revised = feature_store.query_history(db, symbol="TEST", timeframe="M15",
                                         feature_id="volatility.range_pct")
    assert revised[revised.observation_time == observed].revision.tolist() == [1, 2, 3]
    old = feature_store.asof_join(pd.DataFrame({"decision_time": [initial.iloc[0].available_at_utc]}), revised)
    assert old.iloc[0].numeric_value == initial.iloc[0].numeric_value
    # Uma origem H1 não deve disparar os produtores de M15 neste contexto.
    assert producers.recalculate_affected(db, "TEST", "M15", ["bars_derived:H1"]) == []


def test_scoped_recalculation_without_derived_bars_has_actionable_error(db):
    with pytest.raises(ValueError, match="mt5x resample"):
        producers.recalculate_affected(db, "TEST", "M15", ["bars_derived:M15"])


def test_long_history_bollinger_append_matches_full_without_relaxing_precision():
    bars = bars_frame(sessions=161, bars_per_session=6, minutes=5, start="2026-01-05 09:00")
    minute_offsets = np.arange(30)
    intraday = np.cumsum(0.00002 * np.sin(minute_offsets / 9.0) + 0.000003)
    close = np.concatenate([(1.1 + day * 0.0001 + intraday)[4::5] for day in range(161)])
    bars["close"] = close
    bars["open"], bars["high"], bars["low"] = close - 0.00001, close + 0.00003, close - 0.00003
    params = producers.DEFAULTS["volatility"]
    initial, delta = bars.iloc[:-6].copy(), bars.iloc[-6:].copy()
    prior = {"state": producers._family_checkpoint_state("volatility", initial, params)}
    actual, _ = producers._volatility_append_rows(prior, delta, params, initial.bar_start_utc.iloc[-1])
    complete, _ = producers._family_rows("volatility", bars, params, "auto")
    expected = {(row["feature_id"], row["observation_time"]): row for row in complete
                if row["observation_time"] >= delta.bar_start_utc.iloc[0]}
    assert len(actual) == len(expected)
    for row in actual:
        reference = expected[(row["feature_id"], row["observation_time"])]
        assert row["available_at"] == reference["available_at"]
        assert row["source_revision"] == reference["source_revision"]
        assert row["metadata"] == reference["metadata"]
        if row["feature_id"] in {"volatility.bb_width", "volatility.realized_vol"}:
            assert row["value"] == reference["value"]
        if isinstance(reference["value"], (float, int)):
            assert row["value"] == pytest.approx(reference["value"], rel=1e-12, abs=1e-14)
        else:
            assert row["value"] == reference["value"]


def test_stable_bollinger_uses_same_finite_window_and_matches_decimal_oracle():
    from decimal import Decimal, localcontext
    import math
    close = pd.Series(np.concatenate([(1.1 + day * 0.0001 + np.cumsum(
        0.00002 * np.sin(np.arange(30) / 9.0) + 0.000003))[4::5] for day in range(161)]))
    full = indicators.bollinger(close, 20, stable_window=True)
    tail = indicators.bollinger(close.iloc[-25:], 20, stable_window=True)
    pd.testing.assert_frame_equal(full.iloc[-6:], tail.iloc[-6:], check_exact=True)
    with localcontext() as context:
        context.prec = 80
        for position in range(len(close) - 6, len(close)):
            values = [Decimal.from_float(float(value)) for value in close.iloc[position-19:position+1]]
            mean = sum(values) / Decimal(20)
            std = (sum((value - mean) ** 2 for value in values) / Decimal(20)).sqrt()
            for name, expected in [("mid", mean), ("upper", mean + 2 * std), ("lower", mean - 2 * std)]:
                assert abs(full.iloc[position][name] - float(expected)) <= 4 * math.ulp(float(mean))
    returns = np.log(close).diff()
    realized = indicators.rolling_std(returns, 20, stable_window=True)
    realized_tail = indicators.rolling_std(returns.iloc[-25:], 20, stable_window=True)
    pd.testing.assert_series_equal(realized.iloc[-6:], realized_tail.iloc[-6:], check_exact=True)
    with localcontext() as context:
        context.prec = 80
        for position in range(len(returns) - 6, len(returns)):
            values = [Decimal.from_float(float(value)) for value in returns.iloc[position-19:position+1]]
            mean = sum(values) / Decimal(20)
            expected = (sum((value - mean) ** 2 for value in values) / Decimal(19)).sqrt()
            assert abs(realized.iloc[position] - float(expected)) <= 4 * math.ulp(float(expected))
    pd.testing.assert_series_equal(indicators.rolling_std(returns, 20),
                                   returns.rolling(20, min_periods=20).std(), check_exact=True)
    # A opção nova não altera a definição padrão usada pelas estratégias.
    default = indicators.bollinger(close, 20)
    mid = close.rolling(20, min_periods=20).mean()
    std = close.rolling(20, min_periods=20).std(ddof=0)
    pd.testing.assert_frame_equal(default, pd.DataFrame({"mid": mid, "upper": mid + 2 * std,
                                                       "lower": mid - 2 * std}), check_exact=True)


@pytest.mark.parametrize("invalid", [-1, 6, 5.9, float("nan"), float("inf"), "2"])
def test_producer_retries_reject_invalid_counts_before_job_or_feature_writes(db, invalid):
    with pytest.raises(ValueError, match="max_retries"):
        producers.compute(db, "TEST", "M5", families=["calendar"], max_retries=invalid)
    assert not inspect(db.engine).has_table("_analytics_producer_runs")
    assert not inspect(db.engine).has_table("_feature_values")
