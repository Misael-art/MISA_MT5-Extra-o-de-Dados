"""DASH-11: política de ordenação visual explicável, sem promover estados."""
import copy
from datetime import datetime, timezone

import pytest

from mt5_extracao.dashboard import generator, priority, sections

CUT = datetime(2026, 10, 30, 12, tzinfo=timezone.utc)


def base():
    data = {"as_of_utc": CUT.isoformat(), "provenance": {}}
    return data


def hyp(ident, status="APPROVED", lift=1.3, n=200, ci=True, oos=True):
    from tests.test_dashboard_hypothesis import record
    r = record(hypothesis_id=ident, status=status, lift=lift, n_events=n,
               confidence={"method": "m", "level": .95, "low": 1, "high": 2} if ci else None,
               oos={"test_from": "2026-06-01", "test_to": "2026-07-01"} if oos else None)
    return sections.hypothesis(base(), r)


def ordered(*items):
    data = base()
    data["sections"] = list(items)
    return priority.prioritize(data)["sections"]


def test_groups_follow_mandatory_order_before_scores():
    surprise = sections.empty(base(), "surprise", "surprise", "S", "x")
    surprise.update(default_visible=True)
    validating = hyp("v", status="VALIDATING", lift=5.0, n=5000, oos=False)        # score alto, grupo posterior
    approved = hyp("a", lift=1.05, n=10)
    out = ordered(validating, approved, surprise)
    assert [s["id"] for s in out] == ["surprise", "hypothesis:a", "hypothesis:v"]
    assert [s["priority"]["position"] for s in out] == [1, 2, 3]


def test_scores_are_normalised_within_group_and_explained():
    strong, weak = hyp("forte", lift=2.0, n=400), hyp("fraca", lift=1.1, n=400)
    out = ordered(weak, strong)
    assert [s["id"] for s in out] == ["hypothesis:forte", "hypothesis:fraca"]
    assert sum(s["priority"]["score"] for s in out) == pytest.approx(1.0)
    p = out[0]["priority"]
    assert p["factors"]["N"]["label"] == "Sem comparação anterior" and p["factors"]["N"]["value"] > 0
    assert "ln" in p["factors"]["I"]["method"] and p["policy"] == priority.POLICY


def test_missing_factor_is_unavailable_not_favourable():
    known, missing = hyp("k", lift=1.2), hyp("m", lift=None)
    out = ordered(missing, known)
    assert out[0]["id"] == "hypothesis:k"
    assert out[1]["priority"]["score_state"] == "UNAVAILABLE" and out[1]["priority"]["score"] is None


def test_lift_below_one_is_impact_too_and_direction_kept():
    up, down = hyp("up", lift=1.5), hyp("down", lift=1 / 1.5)
    out = ordered(up, down)
    assert out[0]["priority"]["factors"]["I"]["value"] == pytest.approx(out[1]["priority"]["factors"]["I"]["value"])
    assert {s["evidence"]["lift"] for s in out} == {1.5, 1 / 1.5}                  # sinal/direção preservados


def test_ties_are_deterministic_by_id():
    a, b = hyp("b", lift=1.3), hyp("a", lift=1.3)
    assert [s["id"] for s in ordered(a, b)] == ["hypothesis:a", "hypothesis:b"]
    assert [s["id"] for s in ordered(b, a)] == ["hypothesis:a", "hypothesis:b"]


def test_zero_sum_does_not_divide_or_drop():
    flat = hyp("flat", lift=1.0)                                                  # |ln 1| = 0
    out = ordered(flat)
    assert out[0]["priority"]["score_state"] == "ZERO_SUM" and out[0]["priority"]["score"] is None


def test_hidden_never_promoted_and_retired_is_audit_only():
    explore, retired = hyp("e", status="EXPLORATORY", lift=9.0, n=9999), hyp("r", status="RETIRED")
    out = ordered(explore, retired)
    assert all(not s["default_visible"] for s in out)
    assert all(s["priority"]["score_state"] == "NOT_RANKED" for s in out)
    assert retired["details"]["audit_only"] is True and not explore["details"].get("audit_only")


def test_policy_is_in_snapshot_and_generation_is_deterministic():
    one = generator.generate(None, symbol="QA", timeframe="M5", as_of=CUT.isoformat())
    assert one["provenance"]["priority_policy"]["policy"] == priority.POLICY
    data = copy.deepcopy(one)
    assert [s["id"] for s in priority.prioritize(data)["sections"]] == [s["id"] for s in one["sections"]]
    generator.validate(one)
