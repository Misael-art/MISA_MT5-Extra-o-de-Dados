"""Consulta de ciclos explícitos com censura limitada ao último estado conhecido."""
from copy import deepcopy

import pytest

from mt5_extracao.strategies import ExitRules
from mt5_extracao.strategies.backtester import BacktestConfig, SymbolSpec, run_backtest
from mt5_extracao.trade_analytics.trajectories.clocks import Clock
from mt5_extracao.trade_analytics.trajectories.cycles import summarize
from tests.test_backtester import Manual, bars

UTC = Clock("utc")


@pytest.fixture
def payload():
    data = bars([[100, 101, 99, 100], [100, 101, 99, 100],
                 [100, 111, 100, 110], [105, 106, 104, 105]], start="2026-01-05")
    spec = SymbolSpec(point=1, tick_size=1, tick_value=1, swap_mode=1, swap_long=-1)
    cfg = BacktestConfig(initial_equity=10000, risk_per_trade=0.01, commission_per_lot=0.5)
    strategy = Manual(long=[0], stop={0: 90}, rules=ExitRules(partial_r=1, partial_fraction=0.5,
                                                          breakeven_after_partial=False))
    return run_backtest(data, strategy, spec, cfg, observation_timeframe="D1").trajectory


def test_one_partial_is_not_a_second_unit_or_a_second_charge(payload):
    saved = deepcopy(payload)
    units = summarize(payload, run_id="r", source="python", clock=UTC, thresholds=[80, -4])
    assert len(units) == 1 and payload == saved
    unit = units[0]
    assert unit["identity"] == {"run_id": "r", "seq": 0, "source": "python"}
    assert unit["final_net_pnl"] == 40 and unit["mfe_observed"] == 82.5 and unit["mae_observed"] == 5
    assert unit["age_min"] == 3 * 1440 and not unit["right_censored"]
    assert unit["unit"] == "account_money" and unit["currency"] is None
    assert [v["window"]["age_start_min"] for v in unit["thresholds"]] == [2 * 1440, 1440]
    assert all(not v["first_absolute_crossing_known"] for v in unit["thresholds"])


def test_as_of_does_not_publish_future_partial_profit_or_future_close(payload):
    unit = summarize(payload, run_id="r", source="python", clock=UTC,
                     as_of="2026-01-07 12:00", thresholds=[80])[0]
    assert unit["right_censored"] and unit["age_min"] == 1440
    assert unit["final_net_pnl"] is None and unit["mfe_observed"] == 0 and unit["mae_observed"] == 5
    assert unit["thresholds"][0]["state"] == "unknown"
    assert [p["kind"] for p in unit["points"]] == ["entry", "mark"]
    assert "as_of_between_observations" in unit["limitations"]


def test_intrabar_close_does_not_gain_an_exact_duration_or_alive_until_midbar():
    data = bars([[100, 101, 99, 100], [100, 101, 89, 90]], start="2026-01-05 10:00", freq="min")
    result = run_backtest(data, Manual(long=[0], stop={0: 90}), SymbolSpec(point=1, tick_size=1, tick_value=1),
                          BacktestConfig(), observation_timeframe="M1")
    unit = summarize(result.trajectory, run_id="r", source="python", clock=UTC)[0]
    assert not unit["right_censored"] and unit["age_min"] is None
    assert unit["age_lower_min"] == 0 and unit["age_upper_min"] == 1
    cut = summarize(result.trajectory, run_id="r", source="python", clock=UTC,
                    as_of="2026-01-05 10:01:30")[0]
    assert cut["right_censored"] and cut["age_min"] == 0 and cut["final_net_pnl"] is None


def test_model_observations_cannot_be_relabelled_as_real_fills(payload):
    with pytest.raises(ValueError, match="Origem"):
        summarize(payload, run_id="r", source="real", clock=UTC)
    with pytest.raises(ValueError, match="Origem"):
        summarize(payload, run_id="r", source="resim:ponto_fixo", clock=UTC)


@pytest.mark.parametrize("damage", ["missing_entry", "duplicate_event", "after_exit", "negative_exposure"])
def test_invalid_event_stream_is_rejected(payload, damage):
    records = payload["records"]
    if damage == "missing_entry":
        records[0]["kind"] = "mark"
    elif damage == "duplicate_event":
        records[1]["event_seq"] = 0
    elif damage == "after_exit":
        extra = deepcopy(records[-1]); extra["event_seq"] += 1; extra["kind"] = "mark"
        records.append(extra)
    else:
        records[1]["volume_open"] = -1
    with pytest.raises(ValueError):
        summarize(payload, run_id="r", source="python", clock=UTC)


def test_open_stream_requires_as_of_and_missing_net_is_not_zero(payload):
    payload["records"].pop()
    with pytest.raises(ValueError, match="aberto exige"):
        summarize(payload, run_id="r", source="python", clock=UTC)
    for record in payload["records"]:
        record["net_pnl"] = None
    unit = summarize(payload, run_id="r", source="python", clock=UTC, as_of="2026-01-09")[0]
    assert unit["right_censored"] and unit["mfe_observed"] is None and unit["mae_observed"] is None
    assert unit["cash_state"] == "unavailable" and "net_valuation_unavailable" in unit["limitations"]
