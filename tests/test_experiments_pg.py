"""Mesmo journal no PostgreSQL da CI, sempre em schema UUID isolado."""
from concurrent.futures import ThreadPoolExecutor
import copy
import os

import pytest
from sqlalchemy import text

from mt5_extracao import experiments
from mt5_extracao.analytics import datasets
from mt5_extracao.robots import store as runs
from tests.experiment_helpers import setup
from tests.postgres_helpers import isolated_postgresql_database

PG_URL=os.environ.get("MT5X_TEST_PG_URL")
pytestmark=pytest.mark.skipif(not PG_URL,reason="defina MT5X_TEST_PG_URL para PostgreSQL isolado; CI inclui test_*_pg.py")


def test_pg_concurrent_idempotency_immutable_revision_and_shared_holdout():
    with isolated_postgresql_database(PG_URL) as db:
        definition,composition,_=setup(db)
        pid=experiments.create(db,definition)["protocol_id"]
        with ThreadPoolExecutor(max_workers=2) as pool:
            asked=list(pool.map(lambda _:experiments.request(db,pid,"manual",composition),range(2)))
        assert asked[0]["request_id"]==asked[1]["request_id"]
        with db.engine.connect() as conn:
            assert conn.execute(text("SELECT COUNT(*) FROM _experiment_requests")).scalar_one()==1
        changed=copy.deepcopy(definition);changed["seed"]+=1
        with pytest.raises(ValueError,match="imutável"):experiments.create(db,changed)
        other=copy.deepcopy(definition);other["name"]="second"
        other_id=experiments.create(db,other)["protocol_id"]
        experiments.holdout_access(db,pid,"adjustment",purpose="tuning")
        assert experiments.status(db,other_id)["exposure"]["holdout"]["status"]=="used_for_tuning"


def test_pg_publication_rollback_and_resume_reserved_run(monkeypatch):
    with isolated_postgresql_database(PG_URL) as db:
        definition,composition,_=setup(db)
        pid=experiments.create(db,definition)["protocol_id"]
        item=experiments.request(db,pid,"one",composition)
        original=datasets.bind;calls=[]
        def fail_once(*args,**kwargs):
            original(*args,**kwargs);calls.append(1)
            if len(calls)==1:raise ValueError("falha depois do bind PG")
        monkeypatch.setattr(datasets,"bind",fail_once)
        first=experiments.evaluate(db,item["request_id"])
        assert first["request"]["state"]=="failed",first
        assert not runs.load_runs(db)
        with db.engine.connect() as conn:
            assert conn.execute(text("SELECT COUNT(*) FROM _dataset_references")).scalar_one()==0
        again=experiments.evaluate(db,item["request_id"])
        assert again["request"]["state"]=="run_recorded",again
        assert again["run"]["run_id"]==first["request"]["run_id"] and len(runs.load_runs(db))==1
