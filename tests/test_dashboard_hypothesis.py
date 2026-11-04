"""DASH-06: faixa de hipótese sem produtor T13.4; adaptador estrito para o DTO futuro."""
from datetime import datetime, timedelta, timezone

import pytest

from mt5_extracao.dashboard import document, generator, sections

CUT = datetime(2026, 10, 30, 12, tzinfo=timezone.utc)


def data():
    return {"as_of_utc": CUT.isoformat()}


def record(**changes):
    base = {"status_namespace": "hypothesis", "hypothesis_id": "qa-h1", "version": "v1", "status": "APPROVED",
            "title": "QA · rompimento após abertura", "available_at_utc": (CUT - timedelta(days=1)).isoformat(),
            "n_events": 412, "p_condition": 0.58, "p_base": 0.43, "lift": 1.35,
            "confidence": {"method": "bootstrap em blocos de dias", "level": 0.95, "low": 1.08, "high": 1.61},
            "oos": {"test_from": "2026-06-01T00:00:00+00:00", "test_to": "2026-09-30T00:00:00+00:00"}}
    base.update(changes)
    return base


def test_generator_declares_pending_producer_without_inventing_hypotheses():
    snapshot = generator.generate(None, symbol="QA", timeframe="M5", as_of=CUT.isoformat())
    section = next(s for s in snapshot["sections"] if s["kind"] == "hypothesis")
    assert section["display_state"] == "UNAVAILABLE" and section["default_visible"]
    assert section["evidence"] == {"reason_code": "T13_4_PENDING"} and section["n_observations"] is None
    assert "bateria de robô" in section["details"]["reason"]
    assert snapshot["capabilities"]["hypotheses"]["state"] == "DEPENDENCY_PENDING"
    generator.validate(snapshot)


def test_approved_card_keeps_rings_scales_and_no_recommendation():
    section = sections.hypothesis(data(), record())
    assert section["default_visible"] and section["source_status"] == "APPROVED"
    rings = {r["ring"]: r for r in section["visual"]["rings"]}
    assert rings["sample"]["value"] == 412 and rings["lift"]["reference"] == 1.0
    assert rings["confidence"]["value"]["is_probability_of_profit"] is False
    assert section["evidence"]["approved_is_recommendation"] is False
    assert "sem limite superior" in section["evidence"]["lift_scale"]


@pytest.mark.parametrize("confidence", [None, {"level": 0.95, "low": 1, "high": 2},
                                        {"method": "x", "level": 95, "low": 1, "high": 2},
                                        {"method": "x", "level": 0.95, "low": 2, "high": 1}])
def test_undefined_confidence_is_neutral_not_invented(confidence):
    section = sections.hypothesis(data(), record(confidence=confidence))
    ring = section["visual"]["rings"][2]
    assert ring["value"] is None and ring["label"] == "Não informada"


@pytest.mark.parametrize("status", ["EXPLORATORY", "DRAFT", "MONITORING", "RETIRED"])
def test_non_approved_states_stay_hidden_with_reason(status):
    section = sections.hypothesis(data(), record(status=status))
    assert not section["default_visible"] and section["hidden_reason_code"] == "STATUS_" + status
    assert section["source_status"] == status


@pytest.mark.parametrize("changes,code", [
    ({"status_namespace": "robot", "status": "APPROVED"}, "namespace"),       # bateria de robô
    ({"status_namespace": "relation"}, "namespace"),
    ({"status": "bateria"}, "estado"),
    ({"oos": None}, "fora da amostra"),
    ({"available_at_utc": (CUT + timedelta(minutes=1)).isoformat()}, "FUTURE_EVIDENCE"),
    ({"n_events": -1}, "n_events"), ({"n_events": True}, "n_events"),
    ({"lift": float("inf")}, "lift"), ({"lift": -0.1}, "lift"),
    ({"p_base": 1.2}, "p_base"),
])
def test_foreign_or_invalid_evidence_is_refused(changes, code):
    with pytest.raises(ValueError, match=code):
        sections.hypothesis(data(), record(**changes))


def test_validating_without_oos_is_allowed_and_labelled():
    section = sections.hypothesis(data(), record(status="VALIDATING", oos=None))
    assert section["default_visible"] and section["details"]["status_label"] == "Em validação"


def test_document_embeds_hypothesis_renderer():
    assert "js/charts/hypothesis.js" in document.RESOURCES
