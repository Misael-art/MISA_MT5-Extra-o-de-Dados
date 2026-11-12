"""Metadados incompletos não comprovam a ordem dos ticks nem a classe A."""
import json

import pandas as pd
import pytest

from mt5_extracao.analytics import tick_fidelity


def ticks(**changes):
    frame = pd.DataFrame({"time_msc": [1000, 1000, 2000], "seq": [0, 1, 0],
                          "bid": [1., 1., 1.], "ask": [1.1, 1.1, 1.1], "flags": [2, 4, 8]})
    return frame.assign(**changes)


def capabilities():
    return {"capabilities": {"ticks": {"state": "available", "origin": "observed"},
                              "complete_real_ticks": {"state": "available"}}}


@pytest.mark.parametrize("invalid", [float("inf"), -float("inf"), "inválido", None, 1.5, -1, 2**65, True])
def test_invalid_flags_produce_finite_diagnostic_without_modifying_capture(invalid):
    frame = ticks(flags=[2, invalid, 8])
    original = frame.copy(deep=True)
    result = tick_fidelity.audit(frame)
    assert result["invalid_flags"] == 1
    assert "invalid_flags" in result["defects"]
    assert tick_fidelity.grade(capabilities(), result)["grade"] != "A"
    json.dumps(result, allow_nan=False)
    pd.testing.assert_frame_equal(frame, original)


@pytest.mark.parametrize("sequence", [[0, 0, 0], [None, 1, 0], [-1, 1, 0], [0, "inválido", 0],
                                      [0, .5, 0], [0, True, 0]])
def test_invalid_sequence_does_not_certify_intrabar_order(sequence):
    frame = ticks(seq=sequence)
    result = tick_fidelity.audit(frame)
    assert result["provider_sequence"] is False
    assert not tick_fidelity.grade(capabilities(), result)["certifies"]["intrabarra"]
    json.dumps(result, allow_nan=False)


@pytest.mark.parametrize("audit", [None, {}, {"rows": 0, "defects": []}])
def test_absent_or_empty_audit_cannot_certify_intrabar_or_full_grade(audit):
    result = tick_fidelity.grade(capabilities(), audit)
    assert result["grade"] != "A"
    assert result["certifies"]["intrabarra"] is False


@pytest.mark.parametrize("point", [float("inf"), float("nan"), "inválido", -1., 0., 1e-320])
def test_invalid_or_unrepresentable_point_preserves_price_units(point):
    result = tick_fidelity.audit(ticks(), point=point)
    assert result["point_state"] == "invalid" and result["spread"]["unit"] == "price"
    assert "invalid_point" in result["defects"]
    json.dumps(result, allow_nan=False)


@pytest.mark.parametrize("column", ["seq", "flags", "time_msc"])
def test_nullable_integer_metadata_is_diagnosed(column):
    frame = ticks().assign(**{column: pd.Series([1, None, 2], dtype="Int64")})
    result = tick_fidelity.audit(frame)
    assert result["defects"]
    assert not tick_fidelity.grade(capabilities(), result)["certifies"]["intrabarra"]
    json.dumps(result, allow_nan=False)
