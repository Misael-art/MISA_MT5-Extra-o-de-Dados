"""T19.14: referências pequenas para idade, barras e tempo negociável."""
import pandas as pd
import pytest

from mt5_extracao.trade_analytics.trajectories.clocks import (
    Clock, bar_intervals, complete_bars, elapsed_minutes, trading_minutes,
)


UTC = Clock("utc")


def test_declared_broker_offset_and_aware_utc_represent_same_instant():
    broker = Clock("broker", -3)
    assert broker.timestamp("2026-10-18 09:00") == UTC.timestamp("2026-10-18 12:00Z")
    assert UTC.timestamp("2026-10-18 09:00-03:00") == broker.timestamp("2026-10-18 09:00")
    assert elapsed_minutes("2026-10-18 09:00", "2026-10-18 09:07", broker) == 7
    with pytest.raises(ValueError, match="relógio declarado"):
        broker.timestamp("2026-10-18 09:00+02:00")


@pytest.mark.parametrize("basis,offset", [("local", None), ("utc", 0), ("broker", None),
                                         ("broker", True), ("broker", float("nan")), ("broker", 15)])
def test_unknown_or_inconsistent_clock_is_rejected(basis, offset):
    with pytest.raises(ValueError, match="Relógio"):
        Clock(basis, offset)


@pytest.mark.parametrize("value", [None, pd.NaT, "inválido", 1792287317, True])
def test_missing_or_unitless_time_is_not_zero(value):
    with pytest.raises(ValueError, match="Horário"):
        UTC.timestamp(value)


def test_month_end_uses_source_calendar_including_leap_year_and_broker_offset():
    broker = Clock("broker", 3)
    frame = bar_intervals(["2024-02-01", "2024-03-01"], "MN1", broker)
    assert frame.end_utc.tolist() == [UTC.timestamp("2024-02-29 21:00Z"),
                                    UTC.timestamp("2024-03-31 21:00Z")]
    assert [(b - a).days for a, b in frame.itertuples(index=False)] == [29, 31]


def test_complete_bars_excludes_partial_boundaries_and_does_not_fill_a_gap():
    frame = bar_intervals(["2026-01-05 10:00", "2026-01-05 10:01", "2026-01-05 10:05"], "M1", UTC)
    assert complete_bars("2026-01-05 10:00:30", "2026-01-05 10:05:30", frame, UTC) == 1
    assert complete_bars("2026-01-05 10:00", "2026-01-05 10:06", frame, UTC) == 3
    assert elapsed_minutes("2026-01-05 10:00", "2026-01-05 10:06", UTC) == 6
    assert complete_bars("2026-01-05", "2026-01-06", bar_intervals([], "M1", UTC), UTC) == 0


@pytest.mark.parametrize("times", [["2026-01-05 10:00"] * 2,
                                   ["2026-01-05 10:00", "2026-01-05 10:00:30"]])
def test_duplicate_or_overlapping_bars_cannot_count_twice(times):
    with pytest.raises(ValueError, match="sobrepostas"):
        bar_intervals(times, "M1", UTC)


def test_unsupported_series_does_not_become_minutes():
    with pytest.raises(ValueError, match="resolução"):
        bar_intervals(["2026-01-05"], "RENKO", UTC)


def test_trading_clock_unions_sessions_and_excludes_pause():
    sessions = pd.DataFrame({"open_utc": ["2026-01-05 09:00", "2026-01-05 09:30", "2026-01-05 10:05"],
                             "close_utc": ["2026-01-05 10:00", "2026-01-05 10:00", "2026-01-05 11:00"]})
    period = ("2026-01-05 09:59", "2026-01-05 10:06")
    coverage = ("2026-01-05", "2026-01-06")
    assert elapsed_minutes(*period, UTC) == 7
    assert trading_minutes(*period, sessions, UTC, coverage=coverage) == 2
    assert trading_minutes(*period, sessions, UTC) is None
    assert trading_minutes(*period, sessions, UTC, coverage=("2026-01-05 10:00", coverage[1])) is None
    empty = sessions.iloc[:0]
    assert trading_minutes(*period, empty, UTC, coverage=coverage) == 0
    assert trading_minutes(*period, None, UTC, coverage=coverage) is None


def test_reversed_period_or_invalid_session_is_rejected():
    with pytest.raises(ValueError, match="Fim anterior"):
        elapsed_minutes("2026-01-06", "2026-01-05", UTC)
    sessions = pd.DataFrame({"open_utc": ["2026-01-05"], "close_utc": ["2026-01-05"]})
    with pytest.raises(ValueError, match="Sessão sem duração"):
        trading_minutes("2026-01-05", "2026-01-06", sessions, UTC, coverage=("2026-01-05", "2026-01-06"))
