"""TM08: todas as unidades, intervalos/censura e n em risco, com contas pequenas."""
from copy import deepcopy
import json

import pandas as pd
import pytest

from mt5_extracao.trade_analytics.stats import duration_curve


def unit(seq, lower, upper=None, censored=False, **changes):
    start = pd.Timestamp("2026-01-01", tz="UTC") + pd.Timedelta(days=seq)
    result = {"identity": {"run_id": "r", "source": "python", "seq": seq},
              "observation_unit": "cycle", "sequence_basis": "modeled_execution_order",
              "age_lower_min": lower, "age_upper_min": lower if upper is None else upper,
              "right_censored": censored, "start_utc": start.isoformat()}
    return {**result, **changes}


def test_exact_and_right_censored_units_keep_identification_uncertainty():
    data = [unit(0, 1), unit(1, 2, censored=True), unit(2, 3), unit(3, 4, censored=True)]
    saved = deepcopy(data)
    result = duration_curve(data, [1, 2, 3, 4, 5])
    group = result["groups"][0]
    assert data == saved and group["n"] == 4
    assert group["n_closed_exact"] == 2 and group["n_right_censored"] == 2
    curve = group["curve"]
    assert [p["continuation_lower"] for p in curve] == [0.75, 0.75, 0.25, 0.25, 0]
    assert [p["continuation_upper"] for p in curve] == [0.75, 0.75, 0.5, 0.5, 0.5]
    assert [p["n_risk_lower"] for p in curve] == [4, 3, 2, 1, 0]
    assert [p["n_unknown_after_censor"] for p in curve] == [0, 0, 1, 1, 2]
    assert group["small"] and group["ci_unavailable_reason"] == "small_sample"
    assert all(p["outer_ci_low"] is None for p in curve)
    assert group["quantile_bounds"][-1]["identified_upper_min"] is None
    assert group["quantile_bounds"][-1]["upper_unbounded"]
    assert result["probability_of_winning"] is None
    json.dumps(result, allow_nan=False)


def test_interval_close_is_in_the_population_not_imputed_or_dropped():
    result = duration_curve([unit(0, 1, 2), unit(1, 3), unit(2, 2, censored=True)], [1.5, 2, 3])
    group = result["groups"][0]
    assert group["n"] == 3 and group["n_closed_interval"] == 1
    first, second, third = group["curve"]
    assert first["n_risk_lower"] == 2 and first["n_risk_upper"] == 3
    assert first["continuation_lower"] == 2 / 3 and first["continuation_upper"] == 1
    assert second["continuation_lower"] == second["continuation_upper"] == 2 / 3
    assert third["continuation_lower"] == 0 and third["continuation_upper"] == 1 / 3
    assert result["n_excluded"] == 0


def test_overlapping_followup_windows_merge_entry_day_blocks():
    data = [unit(0, 3 * 1440), unit(1, 1), unit(5, 1)]
    result = duration_curve(data, [1], minimum=1, n_boot=200)
    assert result["groups"][0]["n_blocks"] == 2
    assert result["groups"][0]["ci_unavailable_reason"] is None
    merged = duration_curve(data[:2], [1], minimum=1)
    assert merged["groups"][0]["n_blocks"] == 1
    assert merged["groups"][0]["ci_unavailable_reason"] == "insufficient_blocks"


def test_seed_reproduces_sampling_band_without_mixing_runs_or_sources():
    data = [unit(i, 1 if i % 2 else 3) for i in range(40)]
    first = duration_curve(data, [2], seed=11)
    assert first == duration_curve(data, [2], seed=11)
    point = first["groups"][0]["curve"][0]
    assert point["continuation_lower"] == point["continuation_upper"] == 0.5
    assert point["outer_ci_low"] < 0.5 < point["outer_ci_high"]
    other = unit(0, 2, identity={"run_id": "second", "source": "demo", "seq": 0})
    grouped = duration_curve(data + [other], [2])
    assert len(grouped["groups"]) == 2 and sorted(g["n"] for g in grouped["groups"]) == [1, 40]


def test_unavailable_entry_is_explicitly_excluded_not_counted_as_zero():
    data = [unit(0, None, right_censored=None), unit(1, 2)]
    result = duration_curve(data, [0])
    assert result["n_input"] == 2 and result["n_eligible"] == 1 and result["n_excluded"] == 1
    assert result["excluded"][0]["reason"] == "duration_unavailable"
    with pytest.raises(ValueError, match="duplicada"):
        duration_curve([data[1], data[1]], [0])


@pytest.mark.parametrize("changes", [{"age_lower_min": -1}, {"age_upper_min": 0},
                                     {"right_censored": True, "age_upper_min": 3}, {"age_upper_min": float("inf")}])
def test_incoherent_duration_is_rejected(changes):
    with pytest.raises(ValueError, match="duração"):
        duration_curve([unit(0, 2, **changes)], [1])


@pytest.mark.parametrize("age", [True, None, -1, float("nan")])
def test_invalid_age_does_not_become_zero(age):
    with pytest.raises(ValueError, match="Idades"):
        duration_curve([unit(0, 2)], [age])


def test_missing_clock_does_not_invent_sampling_blocks_from_host_time():
    result = duration_curve([unit(0, 1, start_utc=None), unit(1, 2)], [1], minimum=1)
    assert result["groups"][0]["ci_unavailable_reason"] == "missing_followup_clock"


def test_cooperative_cancel_is_observed_before_calculation():
    def cancel():
        raise InterruptedError("Consulta cancelada")
    with pytest.raises(InterruptedError, match="cancelada"):
        duration_curve([unit(0, 1)], [1], check=cancel)
