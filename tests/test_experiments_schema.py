"""Declaração fechada e imutável, sem executar ou qualificar critérios financeiros."""
import copy
from dataclasses import asdict

import pytest

from mt5_extracao.experiments import ProtocolRevision
from mt5_extracao.robots.budget import Limits


def definition():
    return dict(schema_version=1, name="declared", revision=1, criteria=dict(id="legacy", version=1),
                objective=dict(id="legacy_total_r", version=1), grid=[], seed=42,
                budget=dict(scope="evaluation_request", limits=asdict(Limits())),
                roles=dict(train=dict(data_manifest_id="a"*64, evaluation_start="2026-01-01T00:00:00")))


@pytest.mark.parametrize("path,value", [
    ("schema_version",True),("schema_version",2),("revision",0),("revision",1.1),
    ("seed",True),("seed",-1),("name","../path"),("grid",{}),
    ("criteria",{}),("objective",{"id":"x","version":False}),
    ("budget.scope","campaign"),("budget.limits.max_jobs",1.1),
    ("budget.limits.max_jobs",True),("budget.limits.max_jobs",10**400),
    ("budget.limits.max_seconds",-1),("budget.limits.hard_cpu_pct",101),
    ("budget.limits.min_free_mb",float("inf")),
    ("roles",{}),("roles.train.data_manifest_id","current_data"),
    ("roles.train.evaluation_start",None),("roles.train.evaluation_start","NaT"),
])
def test_invalid_or_implicit_fields_are_rejected(path, value):
    data = definition();node=data
    pieces=path.split(".")
    for key in pieces[:-1]:node=node[key]
    node[pieces[-1]]=value
    with pytest.raises(ValueError):ProtocolRevision(data)


def test_immutable_copy_and_canonical_identity():
    data=definition();protocol=ProtocolRevision(data)
    other=copy.deepcopy(data)
    other=dict(reversed(list(other.items())))
    assert ProtocolRevision(other).protocol_id==protocol.protocol_id
    data["criteria"]["version"]=2
    returned=protocol.to_dict();returned["seed"]=99
    assert protocol.to_dict()["criteria"]["version"]==1 and protocol.to_dict()["seed"]==42
    assert ProtocolRevision(data).protocol_id!=protocol.protocol_id
