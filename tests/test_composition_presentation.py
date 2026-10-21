from copy import deepcopy

from mt5_extracao.composition import base_composition
from mt5_extracao.composition.presentation import recorded_summary
from mt5_extracao.strategies.session_entry import SessionEntryPolicy
from mt5_extracao.strategies.temporal import TemporalPolicy


def test_registered_selection_describes_exact_units_without_mutating_metadata():
    value = base_composition("madrid", symbol="QA", timeframe="H1", risk_fraction=.0025, compound=False).to_dict()
    value["protection"] = {"mode": "fixed_points", "params": {"stop_points": 100}}
    value["profit_exit"] = {"mode": "fixed_r", "params": {"target_r": 1.5}}
    value["conditional_policy"] = {"mode": "temporal_v1", "params": TemporalPolicy(max_seconds=3600).to_dict()}
    value["filters"] = [{"mode": "session_v1", "params": SessionEntryPolicy(max_cycles=1).to_dict()},
                        {"mode": "cooldown_v1", "params": {"seconds": 3600}}]
    run = {"metrics": {"composition": value}}
    original = deepcopy(run)
    text = "\n".join(recorded_summary(run))
    for expected in ("Composição registrada", "0,25%", "capital inicial", "100 pontos", "1,5 R", "temporal", "prazo 3600 s", "1 entrada", "3600 s"):
        assert expected in text
    assert run == original


def test_legacy_result_has_no_new_selection_label():
    assert recorded_summary({"metrics": {"trades": 10}}) == []


def test_unknown_composition_is_visible_without_claiming_default():
    text = recorded_summary({"metrics": {"composition": {"composition_schema_version": 999}}})
    assert len(text) == 1 and "indisponível" in text[0]
