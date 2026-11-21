from configparser import ConfigParser
from copy import deepcopy

import pytest

from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.experiments import f19, service, store
from tests.experiment_helpers import setup


@pytest.fixture
def source(tmp_path):
    path = tmp_path / "f19.sqlite"
    db = DatabaseManager(db_path=str(path), broker_utc_offset=-3)
    definition, base, bars = setup(db, name="f19")
    generic = deepcopy(definition)
    arms = {"reference": base.to_dict(), "candidate": base.to_dict()}
    arms["candidate"]["strategy"]["params"]["fast"] = 10
    definition.update(objective={"id": f19.OBJECTIVE, "version": 1, "focus": "signal", "horizon_seconds": 3600,
                                 "embargo_seconds": 3600, "initial_equity": 100000, "block_sessions": 2,
                                 "bootstrap_replicates": 10,
                                 "costs": {"base": {"commission_per_lot": 0, "slippage_points": 0},
                                           "stress": {"commission_per_lot": 1, "slippage_points": 1}}},
                      criteria={"id": "daily_delta", "version": 1, "minimum_net_cash_difference": 0,
                                "confidence": .95},
                      grid=[{"arm": arm, "composition": value} for arm, value in arms.items()])
    definition["roles"]["validation"]["evaluation_start"] = bars.time.iloc[361].isoformat()
    definition["roles"]["holdout"]["evaluation_start"] = bars.time.iloc[481].isoformat()
    yield db, path, definition, generic, arms
    db.close()


def _request(db, protocol_id, arms, arm="reference", scenario="base", **kwargs):
    config = ConfigParser()
    if scenario == "stress":
        config["STRATEGY"] = {"commission_per_lot": "1", "slippage_points": "1"}
    return service.request(db, protocol_id, f"holdout.{arm}.{scenario}", arms[arm],
                           role="holdout", config=config, **kwargs)


def test_freeze_caps_risk_cost_and_embargo_before_access(source):
    db, _, definition, _, arms = source
    bad = deepcopy(definition)
    bad["grid"][1]["composition"]["sizing"]["params"]["risk_fraction"] = .01
    with pytest.raises(ValueError, match="dimensionamento"):
        service.create(db, bad)
    bad = deepcopy(definition)
    bad["objective"]["embargo_seconds"] = 7200
    with pytest.raises(ValueError, match="embargo"):
        service.create(db, bad)

    protocol = service.create(db, definition)
    pid = protocol["protocol_id"]
    assert len(pid) == 64 and service.create(db, definition)["protocol_id"] == pid
    assert store.status(db, pid)["exposure"]["holdout"]["status"] == "no_recorded_access"
    wrong = service.request(db, pid, "holdout.reference.stress", arms["reference"], role="holdout")
    assert wrong["state"] == "rejected" and "custos" in wrong["error"]
    assert _request(db, pid, arms, scenario="stress")["state"] == "requested"


def test_preregistration_refuses_reserve_with_prior_access(source):
    db, _, definition, generic, _ = source
    other = service.create(db, generic)
    service.holdout_access(db, other["protocol_id"], "read-before-registering")
    with pytest.raises(ValueError, match="já teve acesso"):
        service.create(db, definition)
    assert len(store.list_protocols(db)) == 1


def test_holdout_requires_four_planned_requests_before_any_access(source):
    db, _, definition, _, arms = source
    pid = service.create(db, definition)["protocol_id"]
    first = _request(db, pid, arms)
    assert first["payload"]["use_cache"] is False
    blocked = service.evaluate(db, first["request_id"])
    assert "dois braços" in blocked["evaluation_error"]
    assert store.status(db, pid)["exposure"]["holdout"]["status"] == "no_recorded_access"
    for arm, scenario in (("reference", "stress"), ("candidate", "base"), ("candidate", "stress")):
        assert _request(db, pid, arms, arm, scenario)["state"] == "requested"
    with store.transaction(db) as conn:
        protocol = store.protocol(db, pid, conn)
        store.check_f19_holdout(db, conn, protocol)
        store.access(conn, pid, "holdout", protocol["context"]["roles"]["holdout"], "prior-planned",
                     "evaluation", first["request_id"])
        store.check_f19_holdout(db, conn, protocol)
    service.holdout_access(db, pid, "outside-the-plan", purpose="tuning")
    blocked = service.evaluate(db, first["request_id"])
    assert "fora dos braços" in blocked["evaluation_error"]
    assert not blocked["qualified_final_holdout"]
