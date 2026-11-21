from configparser import ConfigParser
from copy import deepcopy
from dataclasses import asdict
import json

import pandas as pd
import pytest

from mt5_extracao import b3_public, cli
from mt5_extracao.analytics import datasets, instruments
from mt5_extracao.composition import base_composition
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.experiments import f19, service, store
from mt5_extracao.robots import backtest
from mt5_extracao.robots.budget import Limits
from tests.experiment_helpers import setup
from tests.test_opening_coverage import DECEMBER, scheduled


@pytest.fixture
def source(tmp_path):
    path = tmp_path / "f19.sqlite"
    db = DatabaseManager(db_path=str(path), broker_utc_offset=-3)
    definition, base, bars = setup(db, name="f19")
    generic = deepcopy(definition)
    arms = {"reference": base.to_dict(), "candidate": base.to_dict()}
    arms["candidate"]["strategy"]["params"]["fast"] = 10
    definition.update(_paired_fields(arms))
    definition["roles"]["validation"]["evaluation_start"] = bars.time.iloc[361].isoformat()
    definition["roles"]["holdout"]["evaluation_start"] = bars.time.iloc[481].isoformat()
    yield db, path, definition, generic, arms
    db.close()


def _paired_fields(arms):
    return {"objective": {"id": f19.OBJECTIVE, "version": 1, "focus": "signal", "horizon_seconds": 3600,
                          "embargo_seconds": 3600, "initial_equity": 100000, "block_sessions": 1,
                          "bootstrap_replicates": 10,
                          "costs": {"base": {"commission_per_lot": 0, "slippage_points": 0},
                                    "stress": {"commission_per_lot": 1, "slippage_points": 1}}},
            "criteria": {"id": "daily_delta", "version": 1, "minimum_net_cash_difference": 0, "confidence": .95},
            "grid": [{"arm": arm, "composition": value} for arm, value in arms.items()]}


def _request(db, protocol_id, arms, arm="reference", scenario="base", role="holdout", **kwargs):
    config = ConfigParser()
    if scenario == "stress":
        config["STRATEGY"] = {"commission_per_lot": "1", "slippage_points": "1"}
    return service.request(db, protocol_id, f"{role}.{arm}.{scenario}", arms[arm], role=role, config=config, **kwargs)


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


def test_paired_summary_uses_reexecuted_costs_and_zero_trade_session(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(backtest, "_execution_code_digest", lambda: "fixture-code")
    path = tmp_path / "paired.sqlite"
    db = DatabaseManager(db_path=str(path), **b3_public.CLOCK)
    days = [f"2026-10-{day:02d}" for day in range(5, 11)]
    frames = []
    for i, day in enumerate(days):
        times = pd.date_range(f"{day} 09:00", periods=60, freq="min")
        frame = pd.DataFrame({"time": times, "open": 100000, "high": 100005, "low": 99995,
                              "close": 100000, "tick_volume": 1, "real_volume": 1, "spread": 0})
        if i == 0:
            frame.loc[20:21, ["high", "close"]] = 100010
            frame.loc[22:24, ["high", "close"]] = 100030
            frame.loc[21:24, "open"] = 100010
            frame.loc[21:24, "low"] = 100005
        frames.append(frame)
    assert b3_public.save(db, DECEMBER, "1 minuto", pd.concat(frames, ignore_index=True))
    instruments.record(db, DECEMBER, "expiration", {"contract": DECEMBER}, source="fixture",
                       known_at="2026-10-01T00:00:00Z", valid_from="2026-12-16T21:00:00Z")
    instruments.record(db, DECEMBER, "specification", b3_public.spec_for(DECEMBER), source="fixture",
                       known_at="2026-10-01T00:00:00Z", valid_from="2026-10-01T00:00:00Z")
    roles = {}
    for index, role in enumerate(("train", "validation", "holdout")):
        start, end = f"{days[index * 2]} 09:00", f"{days[index * 2 + 1]} 09:59"
        snap = datasets.capture(db, DECEMBER, "M1", start, end)
        roles[role] = {"data_manifest_id": snap.manifest_id, "evaluation_start": start}
    reference = base_composition("orb", symbol=DECEMBER, timeframe="M1", risk_fraction=.003).to_dict()
    candidate = deepcopy(reference)
    candidate["strategy"]["params"]["range_minutes"] = 15
    arms = {"reference": reference, "candidate": candidate}
    protocol = dict(schema_version=1, name="paired", revision=1, seed=42, roles=roles,
                    budget=dict(scope="evaluation_request", limits=asdict(Limits(min_free_mb=0))),
                    **_paired_fields(arms))
    protocol["objective"]["calendar"] = {"source": "fixture", "sessions": [
        {**scheduled(day, DECEMBER), "close": "10:00"} for day in days]}
    pid = service.create(db, protocol)["protocol_id"]
    requests = [_request(db, pid, arms, arm, scenario, role="train")["request_id"]
                for arm in arms for scenario in ("base", "stress")]
    for ident in requests:
        result = service.evaluate(db, ident)
        assert result["request"]["state"] == "run_recorded", result
    db.close()
    (tmp_path / "protocol_id.txt").write_text(pid, encoding="utf-8")
    assert cli.main(["--db", str(path), "--json", "experiments", "paired-summary", pid, "--role", "train"]) == 0
    result = json.loads(capsys.readouterr().out)["data"]
    first, zero = result["sessions"]
    assert first["scenarios"]["base"]["difference"] > 0
    assert first["scenarios"]["stress"]["difference"] < first["scenarios"]["base"]["difference"]
    assert zero["scenarios"]["base"] == {"reference": 0.0, "candidate": 0.0, "difference": 0.0}
    assert result["estimates"]["base"]["sessions"] == 2 and result["estimates"]["base"]["replicates"] == 10
    diagnostic = result["sensitivity"]["base"]
    assert diagnostic["exploratory"]
    assert diagnostic["without_best_session"] == {"excluded_date": first["date"],
                                                  "excluded_difference": first["scenarios"]["base"]["difference"],
                                                  "sessions": 1, "mean_difference": 0}
    alternate, = diagnostic["block_sizes"]
    assert alternate["block_sessions"] == 2 and alternate["status"] == "insufficient_sessions"
    assert result["tests_counted"] == 2 and result["economic_approval"] is False
