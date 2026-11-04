"""T13.4-P02: sinais causais e labels futuros não participam da decisão."""

import json
from decimal import Decimal

import pytest

from mt5_extracao.analytics import hypotheses, pattern_engine
from mt5_extracao.strategies.declared import SignalSpec


def field(name):
    return {"op": "field", "name": name}


def literal(value, unit="fraction"):
    return {"op": "literal", "value": value, "unit": unit}


def definition(**changes):
    body = {"schema_version": 1, "pattern_id": "reversal.example", "strategy_version": "1",
            "original_hypothesis": "Reversão após queda conhecida", "origin": "user",
            "parent_attempt_ids": [], "universe_definition": {"mode": "explicit", "symbols": ["SYN"]},
            "data_requirements": ["ohlcv"], "timeframe": "M1",
            "timezone_calendar_versions": {"clock": "UTC", "calendar": "observed"},
            "features_and_versions": [], "context_expression": None,
            "condition_expression": {"op": "compare", "comparison": "lte",
                                     "left": {"op": "percent_change", "value": field("close"),
                                              "reference": field("prior_close")},
                                     "right": literal("-0.013")},
            "decision_clock": "closed_bar_available", "price_reference": "prior_close",
            "side": "long", "simulated_entry": "next_executable_open",
            "outcome_definition": {"horizon_bars": 1}, "repetition_policy": "all",
            "invalidation_rules": [], "execution_policy": "research_only", "cost_model_id": None,
            "complexity": {"terms": 1}, "implementation_signature": "pattern_engine_v1"}
    body.update(changes)
    return hypotheses.compile_definition(body)


def bar(minute, price, *, revision=1, available=None, namespace="db-a", symbol="SYN", **extra):
    start = f"2025-01-06T09:{minute:02d}:00Z"
    end = f"2025-01-06T09:{minute + 1:02d}:00Z"
    return {"source_namespace": namespace, "symbol": symbol, "timeframe": "M1",
            "bar_start_utc": start, "bar_end_utc": end,
            "available_at_utc": available or end, "revision": revision,
            "session_id": "session-1", "open": str(price), "high": str(price + 1),
            "low": str(price - 1), "close": str(price), **extra}


def test_definition_hash_covers_side_horizon_and_rejects_ambiguous_percent():
    base = definition()
    assert base.definition_hash != definition(side="short").definition_hash
    assert base.definition_hash != definition(outcome_definition={"horizon_bars": 2}).definition_hash
    assert base.definition_hash == definition(timeframe="1 minuto").definition_hash
    assert hypotheses.compile_definition(base.body).definition_hash == base.definition_hash
    mutable_copy = base.body
    mutable_copy["side"] = "short"
    assert base.body["side"] == "long"
    condition = {"op": "compare", "comparison": "gte",
                 "left": {"op": "percent_change", "value": field("close"),
                          "reference": field("prior_close")}, "right": literal("1.3")}
    with pytest.raises(ValueError, match="0.013"):
        definition(condition_expression=condition)
    with pytest.raises(ValueError, match="unidades"):
        definition(condition_expression={"op": "compare", "comparison": "gt",
                                         "left": field("close"), "right": literal("0.013")})
    with pytest.raises(ValueError, match="desconhecido"):
        definition(condition_expression={"op": "eval", "source": "future_close"})
    with pytest.raises(ValueError, match="não consome"):
        definition(features_and_versions=[{"feature_id": "rsi", "version": "1"}])
    with pytest.raises(ValueError, match="universo explícito"):
        definition(universe_definition={"mode": "all_local"})
    with pytest.raises(ValueError, match="fora do universo"):
        pattern_engine.initial_state(base, "db-a", "OTHER")


def test_batch_prefix_restart_revision_and_source_boundary_match():
    compiled = definition()
    observations = [bar(0, 100), bar(1, 98), bar(2, 99),
                    bar(1, 97, revision=2, available="2025-01-06T09:04:00Z")]
    batch_state, batch_events = pattern_engine.evaluate_batch(compiled, "db-a", "SYN", observations)
    assert len(batch_events) == 2 and batch_events[0]["event_id"] != batch_events[1]["event_id"]
    assert batch_events[0]["decision_at_utc"] == "2025-01-06T09:02:00+00:00"
    assert batch_events[0]["signal_values"]["reference"] == "100"
    assert batch_events[0]["signal_values"]["close"] == "98"
    state = pattern_engine.initial_state(compiled, "db-a", "SYN")
    prefix_events = []
    for observation in observations[:2]:
        state, event = pattern_engine.evaluate_prefix(compiled, state, observation)
        if event:
            prefix_events.append(event)
    state = json.loads(json.dumps(state))
    for observation in observations[2:]:
        state, event = pattern_engine.evaluate_prefix(compiled, state, observation)
        if event:
            prefix_events.append(event)
    assert prefix_events == batch_events and state == batch_state
    with pytest.raises(ValueError, match="misture"):
        pattern_engine.evaluate_prefix(compiled, state, bar(4, 99, namespace="db-b"))
    with pytest.raises(ValueError, match="misture"):
        pattern_engine.evaluate_prefix(compiled, state, bar(4, 99, symbol="OTHER"))


def test_future_reference_and_open_bar_are_refused_before_signal():
    compiled = definition()
    state = pattern_engine.initial_state(compiled, "db-a", "SYN")
    with pytest.raises(ValueError, match="futuros"):
        pattern_engine.evaluate_prefix(compiled, state, bar(0, 100, available="2025-01-06T09:00:30Z"))
    with pytest.raises(ValueError, match="futura"):
        pattern_engine.evaluate_prefix(compiled, state, bar(0, 100, reference_price="100",
            reference_available_at_utc="2025-01-06T09:02:00Z"))
    assert state["known"] == {}
    state, event = pattern_engine.evaluate_prefix(compiled, state, bar(0, 100, high=None))
    assert event is None


def test_outcome_remains_censored_until_future_bar_is_received():
    compiled = definition()
    observations = [bar(0, 100), bar(1, 98), bar(2, 99)]
    _, events = pattern_engine.evaluate_batch(compiled, "db-a", "SYN", observations)
    assert len(events) == 1 and events[0]["outcome"] == {"status": "pending"}
    early = pattern_engine.resolve_outcomes(compiled, events, observations, as_of="2025-01-06T09:02:30Z")
    assert early[0]["outcome"]["status"] == "censored"
    complete = pattern_engine.resolve_outcomes(compiled, events, observations, as_of="2025-01-06T09:03:00Z")
    assert complete[0]["outcome"]["status"] == "complete"
    assert complete[0]["outcome"]["entry_bar_start_utc"] == "2025-01-06T09:02:00+00:00"
    assert complete[0]["outcome"]["terminal_return"] == "0"
    assert events[0]["outcome"] == {"status": "pending"}
    with pytest.raises(ValueError, match="conflitantes"):
        pattern_engine.resolve_outcomes(compiled, events, observations + [observations[2]],
                                        as_of="2025-01-06T09:03:00Z")
    with pytest.raises(ValueError, match="indisponível"):
        pattern_engine.resolve_outcomes(compiled, events, observations, as_of="2025-01-06T09:01:00Z")


def test_horizon_gap_is_censored_without_calendar_proof():
    compiled = definition(outcome_definition={"horizon_bars": 2})
    observations = [bar(0, 100), bar(1, 98), bar(2, 99), bar(4, 100)]
    _, events = pattern_engine.evaluate_batch(compiled, "db-a", "SYN", observations)
    labeled = pattern_engine.resolve_outcomes(compiled, events, observations, as_of="2025-01-06T09:05:00Z")
    assert labeled[0]["outcome"] == {"status": "censored", "reason": "gap_inside_horizon_without_calendar_proof"}
    single = definition()
    _, events = pattern_engine.evaluate_batch(single, "db-a", "SYN", [bar(0, 100), bar(1, 98)])
    delayed = pattern_engine.resolve_outcomes(single, events, [bar(0, 100), bar(1, 98), bar(4, 99)],
                                              as_of="2025-01-06T09:05:00Z")
    assert delayed[0]["outcome"]["reason"] == "gap_or_delay_before_entry_without_calendar_proof"


def test_first_per_session_survives_late_revision_after_another_session():
    compiled = definition(repetition_policy="first_per_session")
    first, second, third = bar(0, 100), bar(1, 98), bar(2, 96)
    third["session_id"] = "session-2"
    late = bar(1, 95, revision=2, available="2025-01-06T09:04:00Z")
    _, events = pattern_engine.evaluate_batch(compiled, "db-a", "SYN", [first, second, third, late])
    assert [item["session_id"] for item in events] == ["session-1", "session-2"]
    with pytest.raises(ValueError, match="posterior"):
        pattern_engine.evaluate_batch(compiled, "db-a", "SYN",
            [first, second, bar(1, 95, revision=2, available="2025-01-06T09:02:00Z")])


def test_signal_spec_adapter_has_only_declared_close_threshold_subset():
    spec = SignalSpec.from_dict({"key": "simple", "name": "Simple", "indicators":
        {"unused": {"kind": "sma", "period": 2}}, "long": [["above", "close", 100]], "short": []})
    expression = hypotheses.signal_spec_condition(spec, "long")
    compiled = definition(condition_expression=expression, implementation_signature="signal_spec_condition_subset_v1")
    _, events = pattern_engine.evaluate_batch(compiled, "db-a", "SYN", [bar(0, 99), bar(1, 101)])
    assert len(events) == 1 and events[0]["bar_start_utc"] == "2025-01-06T09:01:00+00:00"
    spec.long = [("above", "unused", 100)]
    with pytest.raises(ValueError, match="materializada"):
        hypotheses.signal_spec_condition(spec, "long")


def test_crossing_needs_previous_known_value_and_missing_is_not_false():
    crossing = {"op": "crosses_above", "left": field("close"), "right": literal("100", "price")}
    compiled = definition(condition_expression=crossing, price_reference="close")
    _, first_only = pattern_engine.evaluate_batch(compiled, "db-a", "SYN", [bar(0, 101)])
    assert first_only == []
    _, crossed = pattern_engine.evaluate_batch(compiled, "db-a", "SYN", [bar(0, 99), bar(1, 101)])
    assert len(crossed) == 1
    missing = bar(0, 99, close=None)
    _, unknown = pattern_engine.evaluate_batch(compiled, "db-a", "SYN", [missing, bar(1, 101)])
    assert unknown == []


def test_decimal_observations_remain_json_replayable():
    compiled = definition()
    source = bar(0, 100, close=Decimal("100.00"))
    state, event = pattern_engine.evaluate_prefix(
        compiled, pattern_engine.initial_state(compiled, "db-a", "SYN"), source)
    assert event is None
    assert json.loads(json.dumps(state))["known"]["2025-01-06T09:00:00+00:00"]["close"] == "100.00"
