from copy import deepcopy
import hashlib
import json

import pytest

from mt5_extracao import cli
from mt5_extracao.composition import base_composition
from mt5_extracao.experiments import f19, f19_summary
from mt5_extracao.presets.research import build_paired_research_preset, validate_research_preset


@pytest.fixture
def evidence():
    reference = base_composition("orb", symbol="WINZ26", timeframe="M1").to_dict()
    candidate = deepcopy(reference)
    candidate["strategy"]["params"]["range_minutes"] = 15
    arms = {"reference": reference, "candidate": candidate}
    role = {"data_manifest_id": "a" * 64, "evaluation_start": "2026-10-05T09:00:00"}
    protocol = {"protocol_id": "f19-protocol", "context": {"identity": {"origin": "fixture"}},
                "definition": {"objective": {"id": f19.OBJECTIVE}, "criteria": {"id": "daily_delta"},
                               "roles": {"train": role}, "seed": 42,
                               "grid": [{"arm": arm, "composition": comp} for arm, comp in arms.items()]}}
    report = {"protocol_id": protocol["protocol_id"], "role": "train", "snapshot_id": role["data_manifest_id"],
              "coverage_manifest_id": "b" * 64, "executor_sha256": "c" * 64,
              "runs": {"base": {"reference": "base-1", "candidate": "base-2"},
                       "stress": {"reference": "stress-1", "candidate": "stress-2"}}}
    return protocol, report, arms


def test_paired_preset_cli_reopens_research_artifact(evidence, tmp_path, monkeypatch, capsys):
    protocol, report, arms = evidence
    monkeypatch.setattr(f19_summary, "query", lambda db, ident, role: report)
    monkeypatch.setattr(f19_summary.store, "protocol", lambda db, ident: protocol)
    assert cli.main(["--db", str(tmp_path / "qa.sqlite"), "--json", "experiments", "paired-preset",
                     protocol["protocol_id"], "--role", "train"]) == 0
    preset = json.loads(capsys.readouterr().out)["data"]
    assert validate_research_preset(preset) == preset
    assert preset["arms"] == arms and preset["runs"] == report["runs"]
    assert preset["data_manifest_id"] == report["snapshot_id"] and preset["scope"] == protocol["definition"]["roles"]["train"]
    encoded = json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    assert preset["results_sha256"] == hashlib.sha256(encoded).hexdigest()
    assert preset["kind"] == "f19_paired_research_only"
    assert preset["native_set_equivalent"] is preset["activation_authorized"] is preset["economic_approval"] is False
    with pytest.raises(ValueError, match="alterado"):
        validate_research_preset({**preset, "activation_authorized": True})


def test_paired_preset_refuses_evidence_of_another_snapshot(evidence):
    protocol, report, _ = evidence
    with pytest.raises(ValueError, match="mesmo protocolo e snapshot"):
        build_paired_research_preset(protocol, {**report, "snapshot_id": "other"})
