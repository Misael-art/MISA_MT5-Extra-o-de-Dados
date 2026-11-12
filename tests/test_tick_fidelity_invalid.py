"""Dados inválidos geram diagnóstico finito sem mudar a captura original."""
import json

import pandas as pd
import pytest

from mt5_extracao.analytics.tick_fidelity import audit, grade


@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), -float("inf"), "inválido", None])
def test_invalid_timestamp_is_reported_without_crashing_or_nonfinite_json(invalid):
    ticks = pd.DataFrame({"time_msc": [1000, invalid, 3000], "bid": [1., 1., 1.], "ask": [1.1, 1.1, 1.1]})
    original = ticks.copy(deep=True)
    report = audit(ticks)
    assert report["rows"] == 3 and report["invalid_time"] == 1
    assert report["first_time_msc"] == 1000 and report["last_time_msc"] == 3000
    assert "invalid_time" in report["defects"]
    json.dumps(report, allow_nan=False)
    pd.testing.assert_frame_equal(ticks, original)
    capabilities = {"capabilities": {"ticks": {"state": "available", "origin": "observed"},
                                     "complete_real_ticks": {"state": "available"}}}
    assert grade(capabilities, report)["grade"] != "A"


def test_all_invalid_timestamps_have_no_invented_extremes():
    report = audit(pd.DataFrame({"time_msc": [float("inf"), float("nan")]}))
    assert report["defects"] == ["invalid_time"]
    assert report["invalid_time"] == report["rows"] == 2
    assert "first_time_msc" not in report
    json.dumps(report, allow_nan=False)
