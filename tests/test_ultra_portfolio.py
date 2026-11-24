import json
import subprocess
import sys
from copy import deepcopy
from pathlib import Path

import pytest

from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.laboratory import ultra
from scripts.demo_ultra import prepare

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


def study_plan():
    return {"objective": "net_marked_equity", "train_start": "2026-10-05", "validation_start": "2026-10-12",
            "reserve_start": "2026-10-26", "end_exclusive": "2026-11-14",
            "trials": [{"owners": owners, "invalidation": flag} for owners, flag in (
                ([], False), (["A"], False), (["B"], False), (["A", "B"], False), (["A", "B"], True))],
            "attempt_budget": 5, "min_sessions": 3, "switch_cost": 10, "minimum_gain": 0, "max_drawdown": 8000}


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
        paths = result["policies"]
        assert paths["cash"]["net"] == 0 and paths["fixed"]["net"] == 0
        assert len(paths["weekly"]["reviews"]) == 4 and len(paths["fortnightly"]["reviews"]) == 2
        assert len(paths["fixed"]["reviews"]) == 1
        for path in paths.values():
            assert all(r["last_training_day"] < r["day"] for r in path["reviews"])
        policy = ultra.inspect_receipt(db, receipt)["policy"]
        ticks, signals = ultra._inputs(datasets.load(db, policy["manifest_id"]), policy)
        # Mudar a última cotação da reserva jamais muda decisões de revisão anteriores.
        ticks[-1] = dict(ticks[-1], bid=ticks[-1]["bid"] + 10, ask=ticks[-1]["ask"] + 10)
        alternative = ultra_study.simulate(policy, plan, ticks, signals)
        assert alternative["policies"]["weekly"]["reviews"] == paths["weekly"]["reviews"]
    finally:
        db.close()
