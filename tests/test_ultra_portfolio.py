import json
import subprocess
import sys
from copy import deepcopy
from datetime import date
from pathlib import Path

import pytest

from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.laboratory import ultra, ultra_tester
from scripts.demo_ultra import prepare, study_plan

APP = Path(__file__).resolve().parents[1]


@pytest.fixture
def campaign(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "qa.sqlite"), broker_utc_offset=0)
    try:
        yield db, prepare(db)
    finally:
        db.close()


def cli(db, *args):
    run = subprocess.run([sys.executable, "-m", "mt5_extracao.cli", "--db", db.db_path, "--json",
                          "laboratory", *args], cwd=APP, capture_output=True, text=True, check=True)
    return json.loads(run.stdout)["data"]


def test_reopen_partial_then_compare_portfolio_with_counterfactual(tmp_path, campaign):
    db, definition = campaign
    source = tmp_path / "policy.json"
    source.write_text(json.dumps(definition), encoding="utf-8")
    frozen = cli(db, "ultra-freeze", "--definition", str(source))
    receipt = frozen["receipt"]
    assert frozen["status"] == "frozen" and frozen["progress"] == 0
    prefix = cli(db, "ultra-replay", "--receipt", receipt, "--until", "199")
    assert prefix["account"]["cycles"]["A"]["filled"] == prefix["account"]["cycles"]["A"]["remaining"] == 1
    assert prefix["account"]["cycles"]["B"]["remaining"] == 2
    assert prefix == cli(db, "ultra-replay", "--receipt", receipt, "--until", "199")
    final = cli(db, "ultra-replay", "--receipt", receipt)
    assert final["status"] == "complete" and final["progress"] == 372
    assert final["account"]["cash"] == 93440 and final["account"]["cost"] == 160
    assert final["account"]["cycles"] == {}
    assert final == cli(db, "ultra-replay", "--receipt", receipt)
    frames = ultra.inspect_receipt(db, receipt, timeline=True)["timeline"]
    def decisions(i):
        return [(e["owner"], e["event"], e["reason"]) for e in frames[i]["events"]]
    assert decisions(198) == [("A", "armed", "closed_bar"), ("A", "order", "reserved"),
                              ("B", "armed", "closed_bar"), ("B", "order", "reserved"), ("A", "fill", "partial")]
    assert frames[198]["events"][1]["stop"] == 102 and frames[198]["events"][1]["target"] == 113
    assert decisions(199) == [("A", "fill", "complete")]
    assert decisions(200) == [("B", "fill", "partial")] and frames[201]["mark"]["exposure_steps"] == 4
    assert ("B", "veto", "shared_margin") in decisions(216)
    assert ("A", "management", "breakeven") in decisions(274)
    assert ("A", "invalidated", "reference_lost") in decisions(288)
    assert ("A", "order", "reserved") in decisions(348) and ("B", "veto", "local_loss_streak") in decisions(348)
    c = cli(db, "ultra-compare", "--receipt", receipt)
    assert c["verdict"] == "reject_on_sample" and c["net_difference"] == -2940
    assert [e["reference_net"] for e in c["paired_vetoes"]] == [2180, 2180, -620]
    assert c["conditional"]["max_drawdown"] == 9530 and c["conditional"]["max_reserved_margin"] == 98800
    transition = c["management"]["eligible_transitions"]
    assert any(e["owner"] == "A" and e["key"] == frames[274]["mark"]["key"] for e in transition)
    assert all(e["difference"] == e["managed_net"] - e["hold_net"] for e in transition if e["difference"] is not None)
    assert c["management"]["net_difference"] == c["conditional"]["end_equity"] - c["management"]["hold_metrics"]["end_equity"]
    assert c["conditional"]["completed_cycles"] == 8 and not c["economic_approval"]


def test_control_insufficient_unknown_mode_and_duplicate_owner(campaign):
    db, definition = campaign
    assert ultra.freeze(db, dict(definition, modules=[]))["status"] == "no_candidate"
    history = ultra.freeze(db, dict(definition, sample_kind="market_history"))
    assert history["status"] == "insufficient_data" and history["reasons"]
    assert ultra.replay(db, history["receipt"]) == history
    with pytest.raises(ValueError, match="modo de conta"):
        ultra.freeze(db, dict(definition, account_mode="netting"))
    modules = deepcopy(definition["modules"])
    modules[1]["owner"] = modules[0]["owner"]
    with pytest.raises(ValueError, match="Dono duplicado"):
        ultra.freeze(db, dict(definition, modules=modules))


def test_study_requires_temporal_support_and_package_detects_changes(campaign):
    from mt5_extracao.laboratory import ultra_study
    from mt5_extracao.presets.compiler import compile_ultra_research, verify_ultra_research
    db, definition = campaign
    receipt = ultra.freeze(db, definition)["receipt"]
    plan = study_plan()
    with pytest.raises(ValueError, match="orçamento"):
        ultra_study.register(db, receipt, dict(plan, attempt_budget=2))
    registration = ultra_study.register(db, receipt, plan)
    assert ultra_study.evaluate(db, registration["study_id"])["status"] == "insufficient_data"
    ultra.replay(db, receipt)
    package = compile_ultra_research(db, receipt)
    assert verify_ultra_research(db, package)["status"] == "verified_research"
    with pytest.raises(ValueError, match="diverge"):
        verify_ultra_research(db, dict(package, economic_approval=True))


def test_review_only_sees_completed_sessions_and_can_select_cash(tmp_path):
    from mt5_extracao.laboratory import ultra_study
    from mt5_extracao.analytics import datasets
    db = DatabaseManager(db_path=str(tmp_path / "study.sqlite"), broker_utc_offset=0)
    try:
        definition = prepare(db, sessions=21)
        receipt = ultra.freeze(db, definition)["receipt"]
        plan = study_plan()
        study = ultra_study.register(db, receipt, plan)
        result = ultra_study.evaluate(db, study["study_id"])
        assert result == ultra_study.evaluate(db, study["study_id"])
        assert len(result["attempts"]) == len(plan["trials"]) and not result["economic_approval"]
        assert result["selection_effect"]["status"] == "insufficient_sessions"
        paths = result["policies"]
        reserve = [d for d in paths["static_portfolio"]["daily"] if d["role"] == "reserve"]
        summary = paths["static_portfolio"]["reserve_only"]
        assert summary["sessions"] == len(reserve) and summary["first_day"] == reserve[0]["day"]
        assert summary["net"] == sum(d["delta"] for d in reserve)
        assert summary["opening_equity"] == reserve[0]["opening_equity"] and summary["commission"] > 0
        assert summary["max_exposure_steps"] > 0 and summary["max_margin"] > 0
        assert paths["cash"]["reserve_only"]["profit_factor_status"] == "no_trades"
        assert paths["every_15_days"]["selection_status"] in ("research_candidate", "no_candidate")
        assert paths["cash"]["net"] == 0 and paths["fixed"]["net"] == 0
        assert len(paths["weekly"]["reviews"]) == 4 and len(paths["fortnightly"]["reviews"]) == 2
        epoch = date(1970, 1, 1)
        for name, interval, second in (("fortnightly", 14, date(2026, 10, 26)),
                                       ("every_15_days", 15, date(2026, 10, 27))):
            assert paths[name]["review_interval_calendar_days"] == interval
            assert [r["day"] for r in paths[name]["reviews"]] == [
                (date(2026, 10, 12) - epoch).days, (second - epoch).days]
        assert len(paths["fixed"]["reviews"]) == 1
        for path in paths.values():
            assert all(r["last_training_day"] < r["day"] for r in path["reviews"])
        policy = ultra.inspect_receipt(db, receipt)["policy"]
        ticks, signals = ultra._inputs(datasets.load(db, policy["manifest_id"]), policy)
        longer = ultra_study.simulate(policy, dict(plan, validation_start="2026-10-14"), ticks, signals)
        assert longer["selection_effect"]["status"] == "estimated"
        # Mudar a última cotação da reserva jamais muda decisões de revisão anteriores.
        ticks[-1] = dict(ticks[-1], bid=ticks[-1]["bid"] + 10, ask=ticks[-1]["ask"] + 10)
        alternative = ultra_study.simulate(policy, plan, ticks, signals)
        assert alternative["policies"]["weekly"]["reviews"] == paths["weekly"]["reviews"]
    finally:
        db.close()


def test_new_pattern_replays_causal_rules_and_counterfactual(campaign, tmp_path):
    from test_pattern_engine import definition as define, field, literal
    from mt5_extracao.laboratory import ultra_evaluation
    db, campaign_def = campaign
    condition = {"op": "compare", "comparison": "gte", "left": field("close"), "right": literal("105", "price")}
    rule = {"id": "high_close", "when": condition, "action": "cancel_before_entry"}
    body = define(universe_definition={"mode": "explicit", "symbols": ["QA_ULTRA"]}, origin="exploratory",
                  side="short", price_reference="close", condition_expression=condition, invalidation_rules=[rule]).body
    module = dict(campaign_def["modules"][0], strategy="pattern_definition", params={
        "definition": body, "apply_invalidation": True}, invalidate=0)
    run = ultra.freeze(db, dict(campaign_def, modules=[module]))
    ultra.replay(db, run["receipt"])
    timeline = ultra.inspect_receipt(db, run["receipt"], timeline=True)["timeline"]
    rejected = [e for f in timeline for e in f["events"] if e["reason"] == "pattern_invalidation"]
    assert rejected and all(e["entry_decision"]["rules"] == [{"id": "high_close", "value": True}] for e in rejected)
    c = ultra_evaluation.compare(db, run["receipt"])
    assert c["conditional"]["completed_cycles"] == 0 < c["static"]["completed_cycles"]
    assert any(e["reference_net"] is not None for e in c["paired_vetoes"])
    from mt5_extracao.laboratory import ultra_pattern_native, ultra_tester
    from mt5_extracao.presets.compiler import compile_ultra_research
    from decimal import Decimal
    package = compile_ultra_research(db, run["receipt"])
    cap = ultra_tester.package_capabilities(db, package)
    assert cap["supported_origins"] == list(ultra_tester.PHYSICAL_ORIGINS)
    assert cap["owner_signals"] == {module["owner"]: "pattern_definition"}
    export = ultra_tester.export(db, package, tmp_path / "mql", prepare_tester=True)
    expert = (tmp_path / "mql/UltraPlatformOrders.mq5").read_text()
    assert "hadouken_headers" not in export and "PatternCmp(closing,105,3)" in expert
    marker = "MT5X_ULTRA_PREDICATE1|"
    records = []
    for frame in timeline:
        for event in frame["events"]:
            if event["event"] == "armed":
                records.append(f"{marker}0|{event['key'][0]}|{event['available_msc']}|0|1")
    armed = [e for frame in timeline for e in frame["events"] if e["event"] == "armed"]
    signals = [f"MT5X_ULTRA_SIGNAL1|0|{e['key'][0]}|{e['available_msc']}|{e['side']}|{e['reference']}" for e in armed]
    assert ultra_pattern_native.audit_signals(package, timeline, signals)["first_divergence"] is None
    missing = ultra_pattern_native.audit_signals(package, timeline, signals[1:])["first_divergence"]
    assert missing["field"] == "owner.signal" and missing["key"] == armed[0]["key"]
    assert len(records) == len(rejected) > 0
    check = ultra_pattern_native.audit(db, package, timeline, records)
    assert check["first_divergence"] is None and check["expected_records"] == len(records)
    for wrong in (records[0][:-1]+"0", marker+"invalid", records[0].replace(marker+"0|", marker+"2|")):
        check = ultra_pattern_native.audit(db, package, timeline, [wrong, *records[1:]])
        assert check["first_divergence"]["key"] == rejected[0]["key"]
    with pytest.raises(ValueError, match="grade"):
        ultra_pattern_native._price(literal("105.00000000000001", "price"), Decimal("0.25"))
    composite = {"op": "or", "terms": [condition, {"op": "not", "term": condition}]}
    first = dict(module, params=dict(module["params"], definition=dict(body,
                 condition_expression=composite, repetition_policy="first_per_session")))
    template = (ultra_tester.TEMPLATES / "UltraPattern.mqh.tmpl").read_text()
    generated = ultra_pattern_native.kernel(template, first, 1, Decimal("1"))
    assert "PatternOr(PatternCmp(closing,105,3),PatternNot(" in generated and "last_day!=minute/1440" in generated
    crossing = {"op": "crosses_above", "left": field("close"), "right": field("prior_close")}
    unsupported = dict(module, params=dict(module["params"], definition=dict(body, condition_expression=crossing)))
    with pytest.raises(ValueError, match="crosses_above"):
        ultra_pattern_native.kernel(template, unsupported, 0, Decimal("1"))
    outside = dict(body, universe_definition={"mode": "explicit", "symbols": ["OTHER"]})
    module["params"] = dict(definition=outside, apply_invalidation=True)
    assert "symbol_outside_pattern_universe" in ultra.freeze(db, dict(campaign_def, modules=[module]))["reasons"]


def test_comparator_locates_earliest_normalized_disagreement(campaign):
    from hashlib import sha256
    from mt5_extracao.laboratory.ultra_parity import compare_trace
    from mt5_extracao.presets.compiler import compile_ultra_research
    db, definition = campaign
    receipt = ultra.freeze(db, definition)["receipt"]
    ultra.replay(db, receipt)
    package = compile_ultra_research(db, receipt)
    assert compare_trace(db, package)["status"] == "missing_tester_trace"
    run = ultra.inspect_receipt(db, receipt, timeline=True)
    journal = b"synthetic QA of the comparator"
    trace = dict(kind="mt5_strategy_tester_ultra_v1", package_hash=package["package_hash"],
                 manifest_id=package["source_manifest"], tester_build=1, journal_sha256=sha256(journal).hexdigest(),
                 timeline=run["timeline"], account=run["account"])
    result = compare_trace(db, package, trace, journal=journal)
    assert result["status"] == "normalized_trace_matches" and result["native_acceptance"].startswith("pending")
    trace["account"]["cash"] += 10
    trace["timeline"][198]["events"][-1]["steps"] += 1
    mismatch = compare_trace(db, package, trace, journal=journal)["first_divergence"]
    assert mismatch["ordinal"] == 198 and mismatch["field"] == "frame.events[4].steps"
    with pytest.raises(ValueError, match="Journal"):
        compare_trace(db, package, trace, journal=b"other")


def test_shared_margin_reserves_execution_cost_of_both_pending_intentions():
    from mt5_extracao.composition import ultra as engine
    policy = {"cash": 210, "risk_limit": 1000, "margin": 100, "fee": 1,
              "tick_cash": 1, "fill_steps_per_tick": 1, "revision": "qa", "modules": []}
    for owner in ("A", "B"):
        policy["modules"].append(dict(owner=owner, params={}, steps=1, stop=15, target=20, breakeven=0,
                                      invalidate=0, ttl_ms=1000, max_losses=1, daily_goal=100))
    events, mark = engine.advance(engine.initial_state(210), policy, dict(time_msc=1, seq=0, bid=90, ask=100),
                                  {m["owner"]: dict(side=1, reference=90, available_msc=1) for m in policy["modules"]})
    assert any(e["owner"] == "B" and e["reason"] == "shared_margin" for e in events)
    assert mark["exposure_steps"] == 1 and mark["margin"] <= mark["equity"]


def test_fractional_instrument_uses_own_price_volume_and_tick_economics(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "fx.sqlite"), broker_utc_offset=0)
    try:
        definition = prepare(db, instrument="fractional")
        receipt = ultra.freeze(db, definition)["receipt"]
        result = ultra.replay(db, receipt)
        assert result["policy"]["tick_cash"] == 50 and result["status"] == "complete"
        tape = ultra.inspect_receipt(db, receipt, timeline=True)["timeline"]
        assert tape[0]["mark"]["ask"] - tape[0]["mark"]["bid"] == 2
        assert any(e["event"] == "fill" for f in tape for e in f["events"])
    finally:
        db.close()

def test_reserve_drawdown_resets_at_cut_and_respects_tick_order():
    from mt5_extracao.laboratory.ultra_study import _reserve
    # Um pico da validação de 3000 e seu drawdown histórico nunca entram na reserva.
    common = {"max_exposure_steps": 2, "max_margin": 25, "commission": 2, "switch_cost": 0,
              "spread_cost_twice": 3, "drawdown": 2000}
    days = [dict(common, day=20, opening_equity=1000, high=1040, low=990, intraday_drawdown=10,
                 equity=1040, delta=40, closed_cycle_net=[40]),
            dict(common, day=23, opening_equity=1040, high=1060, low=1010, intraday_drawdown=30,
                 equity=1050, delta=10, closed_cycle_net=[20, -10])]
    result = _reserve(days)
    assert (result["net"], result["max_drawdown"], result["profit_factor"], result["losses"]) == (50, 30, 6, 1)
    assert result["gross_profit"] == 60 and result["gross_loss"] == 10
    assert result["commission"] == 4 and result["spread_cost_vs_mid"] == 3
    assert result["mean_session_net_interval"]["status"] == "insufficient_sessions"

def test_batch_continues_to_three_owner_explanation(tmp_path, campaign):
    db, definition = campaign
    three = deepcopy(definition)
    three["modules"].append(dict(three["modules"][0], owner="C", steps=4))
    args = []
    for index, item in enumerate((dict(definition, sample_kind="market_history"), dict(definition, modules=[]), three)):
        source = tmp_path / f"policy-{index}.json"
        source.write_text(json.dumps(item))
        args.extend(("--definition", str(source)))
    batch = cli(db, "ultra-batch", *args)["items"]
    assert [r["status"] for r in batch] == ["insufficient_data", "no_candidate", "complete"]
    receipt = batch[-1]["receipt"]
    tick = cli(db, "ultra-explain", "--receipt", receipt, "--ordinal", "348")
    decisions = {(e["owner"], e["event"], e["reason"]) for e in tick["events"]}
    assert {("B", "veto", "local_loss_streak"), ("A", "order", "reserved"), ("C", "veto", "shared_margin")} <= decisions
    assert set(tick["rules"]) == {"A", "B", "C"}
    package = tmp_path / "package.json"
    cli(db, "ultra-compile", "--receipt", receipt, "--out", str(package))
    cap = cli(db, "ultra-tester-capabilities", "--package", str(package))
    assert cap["status"] == "exportable" and cap["owners"] == ["A", "B", "C"]
    assert cap["supported_origins"] == list(ultra_tester.PHYSICAL_ORIGINS)
