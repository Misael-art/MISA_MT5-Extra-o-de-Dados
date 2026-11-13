"""T16.14: auditoria da sequência de ticks e classe de fidelidade A–E."""
import pandas as pd
import pytest

from mt5_extracao.analytics import data_eligibility as q, tick_fidelity
from tests.market_data import ohlc_from_close

EPOCH = int(pd.Timestamp("2025-01-06T00:00:00Z").value // 1_000_000)


def _bars():
    return ohlc_from_close([100, 101, 100, 102, 101, 103], start="2025-01-06", freq="h")


def _ticks(offsets=(1000, 2000, 3000, 4000), **changes):
    frame = pd.DataFrame({"time_msc": [EPOCH + n for n in offsets], "seq": 0, "bid": 100.0, "ask": 100.2,
                          "last": 100.1, "volume": 1, "volume_real": 1.0, "flags": 56})
    for column, values in changes.items():
        frame[column] = values
    return frame


def _report(ticks=None, origin="observed"):
    evidence = [] if ticks is None else [q.acquisition(ticks, "ticks", origin=origin, symbol=None)]
    return q.assess(_bars(), bar_minutes=60, ticks=ticks, evidence=evidence)


def test_audit_measures_the_stored_sequence_without_fixing_it():
    ticks = _ticks(offsets=(1000, 1000, 500, 200_000), seq=[0, 1, 0, 0], ask=[100.2, 99.0, 100.2, 105.0],
                   flags=[2, 4, 56, 88])
    got = tick_fidelity.audit(ticks, point=0.1)
    assert got["rows"] == 4 and got["stored_order_regressions"] == 1
    assert got["same_ms_groups"] == 1 and got["max_same_ms"] == 2 and got["crossed_quotes"] == 1
    assert got["gaps_over_60s"] == 1 and got["largest_gaps_s"][0] == pytest.approx(199)
    assert got["flags"]["bid"] == 1 and got["flags"]["buy"] == 1 and got["flags"]["sell"] == 1
    assert got["spread"]["unit"] == "points" and got["spread"]["median"] == pytest.approx(2)
    assert {"stored_order_regressions", "crossed_quotes", "gaps_not_reconciled_with_sessions"} <= set(got["defects"])
    assert tick_fidelity.audit(None)["defects"] == ["no_ticks"]
    assert tick_fidelity.audit(_ticks(time_msc=[None] * 4))["defects"] == ["invalid_time"]


def test_grade_never_rises_by_inference():
    assert tick_fidelity.grade(None)["grade"] == "E"
    only_bars = tick_fidelity.grade(_report())
    assert only_bars["grade"] == "D" and only_bars["certifies"] == {"intrabarra": False, "fechamento": True}
    ticks = _ticks()
    assert tick_fidelity.grade(_report(ticks, origin="generated"))["grade"] == "C"
    assert tick_fidelity.grade(_report(ticks, origin="unknown"))["grade"] == "C"
    observed, clean = _report(ticks), tick_fidelity.audit(ticks)
    assert clean["defects"] == []
    limited = tick_fidelity.grade(observed, clean)                  # origem observada, cobertura integral não provada
    assert limited["grade"] == "B" and tick_fidelity.supports(limited, "intrabarra")
    assert any("Cobertura integral" in reason for reason in limited["reasons"])
    full = {"capabilities": {"complete_real_ticks": {"state": "available"}}}
    partial = {"capabilities": {"complete_real_ticks": {"state": "missing"}}}
    assert tick_fidelity.grade(observed, clean, tester=full)["grade"] == "A"
    assert tick_fidelity.grade(observed, clean, tester=partial)["grade"] == "B"
    assert tick_fidelity.grade(observed, None, tester=full)["grade"] == "B"          # sem auditoria não é A
    crossed = tick_fidelity.audit(_ticks(ask=[99.0, 100.2, 100.2, 100.2]))
    assert tick_fidelity.grade(observed, crossed, tester=full)["grade"] == "B"       # defeito na sequência
    with pytest.raises(ValueError, match="desconhecido"):
        tick_fidelity.supports(limited, "palpite")
