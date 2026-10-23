"""BD05: disponibilidade, alinhamento causal, sessões, lacunas, W1/MN1 e limites de fuso.

Primeiro recorte: qualifica os mecanismos existentes (data_eligibility.as_of, calendar,
resampling, trajectories.clocks) e mede, sem corrigir, o risco do offset fixo no horário de verão.
"""
from types import SimpleNamespace

import pandas as pd
import pytest

import bench
from mt5_extracao.analytics import calendar, data_eligibility, resampling
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.trade_analytics.trajectories.clocks import bar_end_in_source
from tests.test_analytics_calendar import m1_bars

SYMBOL = "EURUSD"


def _bars(tf, starts, minutes):
    starts = pd.to_datetime(starts, utc=True)
    end = starts + pd.Timedelta(minutes=minutes)
    return pd.DataFrame({"symbol": SYMBOL, "timeframe": tf, "event_start": starts, "event_end": end,
                         "available_at": end + pd.Timedelta(seconds=2), "value": range(len(starts))})


def test_m5_decision_never_sees_the_h1_bar_still_open():
    h1 = _bars("H1", ["2026-09-14 09:00", "2026-09-14 10:00"], 60)
    seen = data_eligibility.as_of(h1, pd.Timestamp("2026-09-14 10:35", tz="UTC"))
    assert seen["event_start"].tolist() == [pd.Timestamp("2026-09-14 09:00", tz="UTC")]
    after = data_eligibility.as_of(h1, pd.Timestamp("2026-09-14 11:00:02", tz="UTC"))
    assert len(after) == 2
    # Fechou às 11:00 mas só foi recebida 2 s depois: às 11:00:01 ainda não está disponível.
    assert len(data_eligibility.as_of(h1, pd.Timestamp("2026-09-14 11:00:01", tz="UTC"))) == 1


def test_late_revision_is_invisible_to_earlier_as_of_query():
    first = _bars("H1", ["2026-09-14 09:00"], 60)
    late = first.assign(value=99, available_at=pd.Timestamp("2026-09-15 08:00", tz="UTC"))
    both = pd.concat([first, late], ignore_index=True)
    assert data_eligibility.as_of(both, pd.Timestamp("2026-09-14 12:00", tz="UTC"))["value"].tolist() == [0]
    assert data_eligibility.as_of(both, pd.Timestamp("2026-09-15 09:00", tz="UTC"))["value"].tolist() == [99]


def test_impossible_bench_mass_is_rejected_not_accepted():
    invalid = bench.invalid_computed_before_close()
    frame = pd.DataFrame({"symbol": invalid["symbol"], "timeframe": invalid["tf"], "event_end": invalid["bar_end_utc"],
                          "available_at": invalid["computed_at_utc"]})
    with pytest.raises(ValueError, match="anterior ao fechamento"):
        data_eligibility.as_of(frame, pd.Timestamp("2026-12-01", tz="UTC"))


def test_unknown_time_zone_is_refused_by_the_causal_join():
    frame = _bars("H1", ["2026-09-14 09:00"], 60)
    frame["available_at"] = frame["available_at"].dt.tz_localize(None)
    with pytest.raises(ValueError, match="sem fuso"):
        data_eligibility.as_of(frame, pd.Timestamp("2026-09-14 12:00", tz="UTC"))
    with pytest.raises(ValueError, match="fuso"):
        data_eligibility.as_of(_bars("H1", ["2026-09-14 09:00"], 60), pd.Timestamp("2026-09-14 12:00"))


def test_overnight_session_with_anchor_and_holiday_gap_produce_no_fake_bars(tmp_path):
    db = DatabaseManager(db_type="sqlite", db_path=str(tmp_path / "bd05.sqlite"), broker_utc_offset=0)
    night1 = pd.date_range("2026-09-14 22:00", "2026-09-15 03:59", freq="min")
    night2 = pd.date_range("2026-09-16 22:00", "2026-09-17 03:59", freq="min")  # 15→16: feriado, sem sessão
    db.save_ohlcv_data(SYMBOL, "1 minuto", m1_bars(night1.append(night2)))
    resampling.resample_symbol(db, SYMBOL, ["H1"], symbol_info=SimpleNamespace(name=SYMBOL), session_anchor="17:00")
    with db.engine.connect() as conn:
        sessions = pd.read_sql("SELECT session_id, open_utc, close_utc FROM _market_calendar ORDER BY open_utc", conn)
        bars = pd.read_sql("SELECT session_id, bar_start_utc FROM _bars_derived WHERE tf='H1'", conn)
    assert len(sessions) == 2                       # cada noite é uma sessão, atravessando a meia-noite
    assert bars.groupby("session_id").size().tolist() == [6, 6]  # nenhuma barra no dia sem pregão


def test_week_and_month_are_calendar_intervals_not_fixed_minutes():
    with pytest.raises(ValueError, match="W1|1D"):
        resampling.parse_target("W1")
    with pytest.raises(ValueError):
        resampling.parse_target("MN1")
    assert bar_end_in_source("2024-02-01", "MN1") == pd.Timestamp("2024-03-01")  # 29 dias (bissexto)
    assert bar_end_in_source("2026-01-01", "MN1") == pd.Timestamp("2026-02-01")  # 31 dias
    assert bar_end_in_source("2026-10-18", "W1") == pd.Timestamp("2026-10-25")


def test_fixed_broker_offset_collapses_the_dst_fold_measured_limit(tmp_path):
    """Limite medido: com base de tempo do corretor e offset fixo, a hora repetida do fim do
    horário de verão vira o MESMO carimbo local; o upsert por ``time`` guarda só uma barra."""
    dst = bench.dst_transition()
    local = dst["bar_start_local"]
    assert local.duplicated().sum() == 1
    db = DatabaseManager(db_type="sqlite", db_path=str(tmp_path / "fold.sqlite"), broker_utc_offset=3)
    frame = pd.DataFrame({"time": local, "open": dst["open"], "high": dst["high"], "low": dst["low"],
                          "close": dst["close"], "tick_volume": 10, "spread": 1, "real_volume": 0})
    db.save_ohlcv_data(SYMBOL, "1 hora", frame)
    assert len(db.load_ohlcv(SYMBOL, "1 hora")) == len(dst) - 1  # uma hora real perdida no fold
    assert calendar.DEFAULT_SESSION_GAP_MINUTES > 0
