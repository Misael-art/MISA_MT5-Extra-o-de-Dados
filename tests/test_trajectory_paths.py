"""T19.14: trajetórias pequenas com valores e limites temporais calculados à mão."""
from copy import deepcopy
import json

import numpy as np
import pandas as pd
import pytest

from mt5_extracao.trade_analytics.trajectories.clocks import Clock
from mt5_extracao.trade_analytics.trajectories.paths import reconstruct

UTC = Clock("utc")


def trade(**changes):
    row = dict(run_id="r", seq=0, source="python", symbol="X", timeframe="M1", direction=1,
               entry_time="2026-01-05 10:00", exit_time="2026-01-05 10:04", entry_price=100)
    return {**row, **changes}


def quotes(prices=(101, 98, 103, 102)):
    return pd.DataFrame({"event_time_utc": pd.date_range("2026-01-05 10:01", periods=len(prices), freq="min"),
                         "bid": prices, "ask": [v + 1 for v in prices]})


@pytest.mark.parametrize("source", ["python", "mql5_tester", "demo", "real"])
def test_quote_extremes_and_gain_before_loss_preserve_source_and_raw_trade(source):
    row = trade(source=source)
    original = deepcopy(row)
    data = quotes()
    saved = data.copy(deep=True)
    result = reconstruct(row, clock=UTC, quotes=data, thresholds=[1, -1])
    assert result["identity"]["source"] == source and row == original
    pd.testing.assert_frame_equal(data, saved)
    assert result["mfe_observed"] == 3 and result["mae_observed"] == 2
    assert result["mfe_time"]["age_start_min"] == result["mfe_time"]["age_end_min"] == 3
    assert result["mae_time"]["age_start_min"] == 2
    assert [t["window"]["age_start_min"] for t in result["thresholds"]] == [1, 2]
    assert all(not t["first_absolute_crossing_known"] for t in result["thresholds"])
    assert result["coverage_state"] == "observed_only"


def test_short_liquidates_at_ask_and_mirrors_long_when_quotes_are_mirrored():
    data = quotes()
    data["ask"] = [99, 102, 97, 98]
    data["bid"] = data.ask - 10  # usar bid fabricaria um ganho dez unidades maior
    result = reconstruct(trade(direction=-1), clock=UTC, quotes=data, thresholds=[1, -1])
    assert result["mfe_observed"] == 3 and result["mae_observed"] == 2
    assert result["mfe_time"]["age_start_min"] == 3
    assert [t["window"]["age_start_min"] for t in result["thresholds"]] == [1, 2]


def test_as_of_does_not_use_future_exit_or_future_quotes():
    result = reconstruct(trade(pnl=100000), clock=UTC, quotes=quotes(), as_of="2026-01-05 10:02")
    assert result["right_censored"] and result["age_min"] == 2
    assert result["mfe_observed"] == 1 and result["mae_observed"] == 2
    assert len(result["points"]) == 2 and "pnl" not in result
    assert reconstruct(trade(exit_time=None), clock=UTC, quotes=quotes())["limitations"] == [
        "open_trade_requires_as_of"]
    assert reconstruct(trade(), clock=UTC, quotes=quotes(), as_of="2026-01-05 09:59")["age_min"] is None


def test_ohlc_extrema_keep_interval_and_availability_without_intrabar_order():
    data = pd.DataFrame({"time": ["2026-01-05 10:01"], "open": [100], "high": [103],
                         "low": [98], "close": [101]})
    result = reconstruct(trade(), clock=UTC, bars=data, thresholds=[2, -1])
    assert result["mfe_observed"] == 3 and result["mae_observed"] == 2
    for event in [result["mfe_time"], result["mae_time"]] + [t["window"] for t in result["thresholds"]]:
        assert event["age_start_min"] == 1 and event["age_end_min"] == 2
    interval = next(p for p in result["points"] if p["basis"] == "ohlc_interval")
    assert interval["available_at_utc"].startswith("2026-01-05T10:02")


def test_partial_boundary_bars_cannot_supply_extrema_before_entry_or_after_exit():
    data = pd.DataFrame({"time": ["2026-01-05 10:00", "2026-01-05 10:01", "2026-01-05 10:02"],
                         "open": [100] * 3, "high": [999, 103, 999], "low": [-999, 98, -999],
                         "close": [100] * 3})
    result = reconstruct(trade(entry_time="2026-01-05 10:00:30", exit_time="2026-01-05 10:02:30"),
                         clock=UTC, bars=data)
    assert result["complete_bars"] == 1
    assert sum(p["basis"] == "ohlc_interval" for p in result["points"]) == 1
    assert result["mfe_observed"] == 3 and result["mae_observed"] == 2
    assert "partial_boundary_bar_excluded" in result["limitations"]


def test_bid_bars_do_not_invent_short_ask_extremes_from_minimum_spread():
    data = pd.DataFrame({"time": ["2026-01-05 10:01"], "open": [100], "high": [103],
                         "low": [98], "close": [101], "spread": [1]})
    result = reconstruct(trade(direction=-1), clock=UTC, bars=data)
    assert result["mfe_observed"] is None and "missing_ask_bars" in result["limitations"]


def test_absent_entry_does_not_become_exit_based_age_or_zero_extrema():
    result = reconstruct(trade(entry_time=None), clock=UTC, quotes=quotes())
    assert result["status"] == "unavailable" and result["age_min"] is None
    assert result["mfe_observed"] is None and result["mae_observed"] is None


def test_unhit_threshold_with_unproven_coverage_is_unknown_not_censored_non_hit():
    result = reconstruct(trade(exit_time=None), clock=UTC, quotes=quotes(), as_of="2026-01-05 10:04",
                         thresholds=[100])
    assert result["right_censored"] and result["thresholds"][0]["state"] == "unknown"


@pytest.mark.parametrize("unit,extra,expected", [("points", {"point": 0.5}, 6),
                                               ("R", {"risk_distance": 2}, 1.5)])
def test_declared_units_apply_to_values_and_thresholds_together(unit, extra, expected):
    result = reconstruct(trade(), clock=UTC, quotes=quotes(), unit=unit, thresholds=[expected], **extra)
    assert result["mfe_observed"] == expected
    assert result["thresholds"][0]["window"]["age_start_min"] == 3


@pytest.mark.parametrize("options", [{"unit": "money"}, {"unit": "points"}, {"unit": "R", "risk_distance": 0},
                                     {"thresholds": [0]}, {"thresholds": [1, 1]}, {"thresholds": [float("nan")]}])
def test_invalid_units_or_thresholds_are_rejected(options):
    with pytest.raises(ValueError):
        reconstruct(trade(), clock=UTC, quotes=quotes(), **options)


@pytest.mark.parametrize("changes", [{"seq": True}, {"seq": -1}, {"seq": 0.5},
                                     {"source": None}, {"run_id": float("nan")}])
def test_missing_or_invalid_ledger_identity_is_rejected(changes):
    with pytest.raises(ValueError, match="identidade"):
        reconstruct(trade(**changes), clock=UTC, quotes=quotes())


def test_overflow_is_unavailable_not_an_infinite_excursion():
    data = quotes(prices=(1e308,))
    result = reconstruct(trade(entry_price=-1e308), clock=UTC, quotes=data)
    assert result["mfe_observed"] is None and "invalid_or_missing_quote" in result["limitations"]


def test_ledger_scalar_types_are_serializable_without_nonfinite_json():
    result = reconstruct(trade(seq=np.int64(0), direction=np.int64(1)), clock=UTC, quotes=quotes())
    assert json.loads(json.dumps(result, allow_nan=False))["identity"]["seq"] == 0


def test_units_too_small_for_finite_precision_are_rejected():
    with pytest.raises(ValueError, match="precisão"):
        reconstruct(trade(), clock=UTC, quotes=quotes(), unit="points", point=1e-320)


def test_provider_sequence_orders_same_timestamp_without_inventing_venue_priority():
    data = quotes((101, 98))
    data["event_time_utc"] = pd.Timestamp("2026-01-05 10:01")
    data["seq"] = [2, 1]
    result = reconstruct(trade(), clock=UTC, quotes=data, thresholds=[1, -1])
    assert [p["source_sequence"] for p in result["points"]] == [1, 2]
    assert result["thresholds"][0]["window"]["source_sequence"] == 2
    assert result["thresholds"][1]["window"]["source_sequence"] == 1
    assert "same_timestamp_order_unknown" not in result["limitations"]
    assert all(not v["first_absolute_crossing_known"] for v in result["thresholds"])
    json.dumps(result, allow_nan=False)
    unknown = reconstruct(trade(), clock=UTC, quotes=data.drop(columns="seq"))
    assert "same_timestamp_order_unknown" in unknown["limitations"]
    data["seq"] = 1
    with pytest.raises(ValueError, match="Quotes conflitantes"):
        reconstruct(trade(), clock=UTC, quotes=data)


def test_known_execution_window_includes_final_bar_when_position_lives_until_close():
    data = pd.DataFrame({"time": ["2026-01-05 10:00", "2026-01-05 10:01"],
                         "open": [100, 103], "high": [101, 110], "low": [99, 102], "close": [100, 105]})
    raw = trade(exit_time="2026-01-05 10:01", exit_price=105)
    saved = deepcopy(raw)
    window = dict(start_time="2026-01-05 10:02", end_time="2026-01-05 10:02",
                  available_at="2026-01-05 10:02", price=105, phase="close")
    result = reconstruct(raw, clock=UTC, bars=data, exit_window=window, thresholds=[2])
    assert raw == saved and result["age_min"] == 2 and result["complete_bars"] == 2
    assert result["mfe_observed"] == 10 and result["mae_observed"] == 1
    assert result["thresholds"][0]["window"]["age_start_min"] == 1
    assert result["thresholds"][0]["window"]["age_end_min"] == 1  # abertura conhecida, não fim da barra


def test_intrabar_exit_keeps_fill_and_known_open_without_future_extremes():
    data = pd.DataFrame({"time": ["2026-01-05 10:00"], "open": [101], "high": [999],
                         "low": [-999], "close": [100]})
    window = dict(start_time="2026-01-05 10:00", end_time="2026-01-05 10:01",
                  available_at="2026-01-05 10:01", price=98, phase="intrabar")
    result = reconstruct(trade(exit_time="2026-01-05 10:00", exit_price=98), clock=UTC,
                         bars=data, exit_window=window)
    assert result["age_min"] is None and result["age_lower_min"] == 0 and result["age_upper_min"] == 1
    assert result["mfe_observed"] == 1 and result["mae_observed"] == 2
    assert {p["basis"] for p in result["points"]} == {"bar_open", "producer_execution_fill"}
    with pytest.raises(ValueError, match="indisponível"):
        reconstruct(trade(), clock=UTC, bars=data, exit_window=window, as_of="2026-01-05 10:00:30")


def test_execution_endpoint_is_observable_but_does_not_invent_continuous_path():
    result = reconstruct(trade(exit_price=102), clock=UTC, thresholds=[1])
    assert result["mfe_observed"] == 2 and result["coverage_state"] == "execution_endpoints_only"
    assert result["points"][0]["basis"] == "ledger_execution_endpoint"
    assert not result["thresholds"][0]["first_absolute_crossing_known"]
    cut = reconstruct(trade(exit_price=102), clock=UTC, as_of="2026-01-05 10:01")
    assert cut["points"] == [] and cut["mfe_observed"] is None


def test_quote_at_exit_boundary_does_not_extend_position_after_known_fill():
    data = quotes((101, 999))
    result = reconstruct(trade(exit_time="2026-01-05 10:02", exit_price=102), clock=UTC, quotes=data)
    assert result["mfe_observed"] == 2
    assert result["points"][-1]["basis"] == "ledger_execution_endpoint"
