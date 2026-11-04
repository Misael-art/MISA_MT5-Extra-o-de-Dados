"""P06: quatro famílias têm regras causais distintas e parâmetros explícitos."""

import pytest

from mt5_extracao.analytics import pattern_engine
from mt5_extracao.analytics.pattern_families import compile_family, prior_breakout_references
from tests.test_pattern_engine import bar


COMMON = dict(symbol="SYN", timeframe="M1", clock_version="UTC_fixed_v1",
              calendar_version="observed_session_v1", cost_model_id="qa_cost", horizon_bars=1)


def family(name, parameters):
    return compile_family(name, parameters, **COMMON)


def test_reversal_continuation_and_gap_generate_only_their_declared_events():
    reversal = family("reversal", {"drop_fraction": "0.013"})
    first = [bar(0, 100, session_minute=0), bar(1, 98, session_minute=1)]
    _, events = pattern_engine.evaluate_batch(reversal, "db-a", "SYN", first)
    assert len(events) == 1 and events[0]["side"] == "long"
    continuation = family("continuation_time", {"rise_fraction": "0.01",
                                                 "session_start_minute": 0, "session_end_minute": 10})
    rising = [bar(0, 100, session_minute=0), bar(1, 102, session_minute=5)]
    _, events = pattern_engine.evaluate_batch(continuation, "db-a", "SYN", rising)
    assert len(events) == 1
    rising[1]["session_minute"] = 11
    assert pattern_engine.evaluate_batch(continuation, "db-a", "SYN", rising)[1] == []
    gap = family("gap", {"gap_fraction": "0.01"})
    opening = [bar(0, 100, session_minute=120), bar(1, 102, session_minute=0, open="102")]
    _, events = pattern_engine.evaluate_batch(gap, "db-a", "SYN", opening)
    assert len(events) == 1 and events[0]["pattern_id"] == "gap"


def test_breakout_failure_reference_uses_only_prior_highs():
    breakout = family("breakout_failure", {"breakout_fraction": "0.02", "lookback_bars": 2})
    observations = [bar(0, 100, high="100", low="99"),
                    bar(1, 100, high="101", low="99"),
                    bar(2, 100, high="105", low="99")]
    with_refs = prior_breakout_references(observations, 2)
    assert with_refs[2]["reference_price"] == "101"
    _, events = pattern_engine.evaluate_batch(breakout, "db-a", "SYN", with_refs)
    assert len(events) == 1 and events[0]["side"] == "short"
    observations.append(bar(3, 100, high="1000", low="99"))
    more = prior_breakout_references(observations, 2)
    assert pattern_engine.evaluate_batch(breakout, "db-a", "SYN", more[:3])[1] == events


def test_family_parameters_and_future_reference_are_rejected():
    with pytest.raises(ValueError, match="fração decimal"):
        family("reversal", {"drop_fraction": 0.013})
    with pytest.raises(ValueError, match="Faixa"):
        family("continuation_time", {"rise_fraction": "0.01",
                                     "session_start_minute": 10, "session_end_minute": 5})
    with pytest.raises(ValueError, match="parâmetros"):
        family("gap", {"gap_fraction": "0.01", "extra": 1})
    observations = [bar(0, 100, available="2025-01-06T09:05:00Z"), bar(1, 100)]
    with pytest.raises(ValueError, match="disponível"):
        prior_breakout_references(observations, 2)
