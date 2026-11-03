"""T13.4-P03: protocolo, tentativas e acessos no registro de experimentos."""

from dataclasses import asdict
from concurrent.futures import ThreadPoolExecutor
import copy

import pytest

from mt5_extracao import experiments
from mt5_extracao.analytics import datasets
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.experiments import store
from mt5_extracao.robots.budget import Limits
from tests.test_backtester import bars
from tests.test_pattern_engine import definition


def campaign(db, *, name="pattern_qa", max_attempts=2):
    snap = datasets.capture(db, "SYN", "M1")
    namespace = snap.manifest["source"]["namespace"]
    long, short = definition(), definition(side="short")
    body = {"schema_version": 2, "type": "pattern_campaign_v1", "name": name,
            "revision": 1, "roster": [{"data_manifest_id": snap.manifest_id,
                                      "source_namespace": namespace, "symbol": "SYN", "timeframe": "M1"}],
            "roles": {"train": {"start_utc": "2025-01-06T09:00:00Z", "end_utc": "2025-01-06T09:05:00Z"},
                      "validation": {"start_utc": "2025-01-06T09:05:00Z", "end_utc": "2025-01-06T09:10:00Z"},
                      "holdout": {"start_utc": "2025-01-06T09:10:00Z", "end_utc": "2025-01-06T09:15:00Z"}},
            "endpoints": [{"id": "terminal_return", "kind": "primary"}],
            "grid": [long.definition_hash, short.definition_hash],
            "budget": {"scope": "campaign", "max_attempts": max_attempts,
                       "limits": asdict(Limits(max_jobs=2, max_seconds=300))},
            "criteria": {"alpha": 0.05, "confidence": 0.95,
                         "minimum_effect": 0.01, "maximum_drawdown": 0.2},
            "seed": 7, "validation_plan": {}, "economic_plan": {}}
    return body, long, short


@pytest.fixture
def source(tmp_path):
    path = tmp_path / "campaign.sqlite"
    db = DatabaseManager(db_path=str(path), broker_utc_offset=0)
    frame = bars([[100, 101, 99, 100]] * 15, start="2025-01-06 09:00", freq="min")
    frame["tick_volume"], frame["real_volume"] = 10, 0
    db.save_ohlcv_data("SYN", "1 minuto", frame)
    yield db, path
    db.close()


def test_campaign_freezes_roster_global_windows_and_reopens(source):
    db, path = source
    body, long, _ = campaign(db)
    created = experiments.create_pattern_campaign(db, body)
    same = experiments.create_pattern_campaign(db, body)
    assert created["protocol_id"] == same["protocol_id"]
    context = created["context"]
    assert [row["rows"] for row in context["coverage"]["train"]] == [5]
    assert [row["rows"] for row in context["coverage"]["holdout"]] == [5]
    assert context["prior_external_exposure"] == "unknown"
    attempt = experiments.register_pattern_attempt(db, created["protocol_id"], "first", long)
    assert attempt["state"] == "requested" and attempt["payload"]["planned"]
    assert not experiments.pattern_campaign_status(db, created["protocol_id"])["qualified_final_holdout"]
    reopened = DatabaseManager(db_path=str(path), broker_utc_offset=0)
    try:
        state = experiments.pattern_campaign_status(reopened, created["protocol_id"])
        assert state["requests"][0]["request_id"] == attempt["request_id"]
    finally:
        reopened.close()


def test_global_attempt_budget_counts_alternatives_and_preserves_rejections(source):
    db, _ = source
    body, long, short = campaign(db)
    protocol = experiments.create_pattern_campaign(db, body)["protocol_id"]
    one = experiments.register_pattern_attempt(db, protocol, "one", long)
    again = experiments.register_pattern_attempt(db, protocol, "one", long)
    duplicate = experiments.register_pattern_attempt(db, protocol, "one_again", long)
    two = experiments.register_pattern_attempt(db, protocol, "two", short, origin="automatic")
    extra = experiments.register_pattern_attempt(db, protocol, "extra", definition(outcome_definition={"horizon_bars": 2}))
    assert one["request_id"] == again["request_id"]
    assert duplicate["alternative_id"] == one["alternative_id"]
    assert two["state"] == "requested"
    assert extra["state"] == "rejected" and "esgotado" in extra["error"]
    state = experiments.pattern_campaign_status(db, protocol)
    assert len(state["requests"]) == 4
    assert sum(event["kind"] == "request_registered" for event in state["events"]) == 4
    with pytest.raises(ValueError, match="outro conteúdo"):
        experiments.register_pattern_attempt(db, protocol, "one", short)


def test_holdout_access_is_recorded_before_later_proposal_across_campaigns(source):
    db, _ = source
    body, long, _ = campaign(db)
    first = experiments.create_pattern_campaign(db, body)["protocol_id"]
    receipt = experiments.pattern_campaign_access(db, first, "review_1", role="holdout", purpose="tuning")
    assert receipt["recorded"] and not receipt["qualified_final_holdout"]
    assert experiments.pattern_campaign_status(db, first)["exposure"]["holdout"]["status"] == "used_for_tuning"
    again = experiments.pattern_campaign_access(db, first, "review_1", role="holdout", purpose="tuning")
    assert again == receipt
    successor = dict(body, name="pattern_successor")
    second = experiments.create_pattern_campaign(db, successor)["protocol_id"]
    later = experiments.register_pattern_attempt(db, second, "after_access", long)
    assert later["payload"]["prior_holdout_exposure"]
    forbidden = experiments.register_pattern_attempt(db, second, "after_auto", long, origin="automatic")
    assert forbidden["state"] == "rejected" and "exposição" in forbidden["error"]
    holdout_search = experiments.register_pattern_attempt(db, second, "holdout_tuning", long,
                                                          role="holdout", purpose="tuning")
    assert holdout_search["state"] == "rejected" and "fechada" in holdout_search["error"]
    state = experiments.pattern_campaign_status(db, second)
    assert any(event["kind"] == "proposal_after_access" for event in state["events"])
    assert not state["qualified_final_holdout"]


def test_two_bases_with_same_symbol_keep_distinct_campaign_scopes(source, tmp_path):
    db, _ = source
    other = DatabaseManager(db_path=str(tmp_path / "other.sqlite"), broker_utc_offset=0)
    frame = bars([[200, 201, 199, 200]] * 15, start="2025-01-06 09:00", freq="min")
    frame["tick_volume"], frame["real_volume"] = 10, 0
    other.save_ohlcv_data("SYN", "1 minuto", frame)
    try:
        secondary = datasets.capture(other, "SYN", "M1")
        path = tmp_path / "secondary.json"
        datasets.export_snapshot(other, secondary.manifest_id, path)
        imported = datasets.import_snapshot(db, path)
        body, _, _ = campaign(db)
        body["roster"].append({"data_manifest_id": imported.manifest_id,
                               "source_namespace": imported.manifest["source"]["namespace"],
                               "symbol": "SYN", "timeframe": "M1"})
        created = experiments.create_pattern_campaign(db, body)
        scopes = created["context"]["campaign_roles"]["holdout"]
        assert len(scopes) == 2 and scopes[0]["origin_key"] != scopes[1]["origin_key"]
        experiments.pattern_campaign_access(db, created["protocol_id"], "open", role="holdout")
        assert experiments.pattern_campaign_status(db, created["protocol_id"])["exposure"]["holdout"]["recorded_accesses"] == 2
    finally:
        other.close()


def test_sqlite_concurrent_attempts_cannot_exceed_campaign_cap(source):
    db, _ = source
    body, long, short = campaign(db, max_attempts=1)
    body["grid"] = [long.definition_hash, short.definition_hash][:1]
    protocol = experiments.create_pattern_campaign(db, body)["protocol_id"]
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda pair: experiments.register_pattern_attempt(
            db, protocol, pair[0], pair[1]), [("left", long), ("right", short)]))
    assert sorted(item["state"] for item in results) == ["rejected", "requested"]


def test_campaign_schema_and_roster_refuse_ambiguous_or_unbound_inputs(source):
    db, _ = source
    body, long, _ = campaign(db)
    bad_clock = copy.deepcopy(body)
    bad_clock["roles"]["holdout"]["start_utc"] = "2025-01-06 09:10:00"
    with pytest.raises(ValueError, match="UTC explícito"):
        experiments.create_pattern_campaign(db, bad_clock)
    bad_budget = copy.deepcopy(body)
    bad_budget["budget"]["max_attempts"] = 1
    with pytest.raises(ValueError, match="Grade excede"):
        experiments.create_pattern_campaign(db, bad_budget)
    bad_identity = copy.deepcopy(body)
    bad_identity["roster"][0]["source_namespace"] = "foreign"
    with pytest.raises(ValueError, match="Roster diverge"):
        experiments.create_pattern_campaign(db, bad_identity)
    protocol = experiments.create_pattern_campaign(db, body)["protocol_id"]
    attempt = experiments.register_pattern_attempt(db, protocol, "one", long)
    with pytest.raises(ValueError, match="executor de composição"):
        experiments.evaluate(db, attempt["request_id"])
    assert store.request(db, attempt["request_id"])["state"] == "requested"


def test_failed_access_transaction_does_not_expose_holdout(source, monkeypatch):
    db, _ = source
    body, _, _ = campaign(db)
    protocol = experiments.create_pattern_campaign(db, body)["protocol_id"]
    original = store.event

    def fail_access(conn, event_id, protocol_id, request_id, kind, payload):
        if kind == "data_access":
            raise RuntimeError("falha sintética antes do commit")
        return original(conn, event_id, protocol_id, request_id, kind, payload)

    monkeypatch.setattr(store, "event", fail_access)
    with pytest.raises(RuntimeError, match="sintética"):
        experiments.pattern_campaign_access(db, protocol, "failed")
    monkeypatch.setattr(store, "event", original)
    assert experiments.pattern_campaign_status(db, protocol)["exposure"]["holdout"]["recorded_accesses"] == 0
