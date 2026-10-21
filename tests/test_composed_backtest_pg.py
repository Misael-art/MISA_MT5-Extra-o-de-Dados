"""O consumidor composto usa a publicação/journal existentes também no PostgreSQL."""
import os

import pytest

from mt5_extracao import experiments
from mt5_extracao.composition import Composition
from mt5_extracao.robots import store
from mt5_extracao.strategies.temporal import TemporalPolicy
from tests.experiment_helpers import setup
from tests.postgres_helpers import isolated_postgresql_database

PG_URL = os.environ.get("MT5X_TEST_PG_URL")
pytestmark = pytest.mark.skipif(not PG_URL, reason="defina MT5X_TEST_PG_URL; CI inclui test_*_pg.py em schema isolado")


def test_pg_composition_metadata_and_single_publication_survive_reopen():
    with isolated_postgresql_database(PG_URL) as db:
        definition, composition, _ = setup(db)
        value = composition.to_dict()
        value["sizing"]["params"]["compound"] = False
        value["profit_exit"] = {"mode": "fixed_r", "params": {"target_r": 1.5}}
        value["conditional_policy"] = {"mode": "temporal_v1", "params": TemporalPolicy(max_seconds=3600).to_dict()}
        composition = Composition(value)
        protocol = experiments.create(db, definition)["protocol_id"]
        item = experiments.request(db, protocol, "composed", composition, executor="composition_v1")
        first = experiments.evaluate(db, item["request_id"])
        assert first["request"]["state"] == "run_recorded", first
        reopened = store.load_runs(db, run_id=first["run"]["run_id"])[0]
        assert reopened["metrics"]["composition"] == composition.to_dict()
        assert reopened["metrics"]["compound"] is False
        again = experiments.evaluate(db, item["request_id"])
        assert again["run"]["run_id"] == reopened["run_id"] and len(store.load_runs(db)) == 1
        assert not again["ledger_qualified"] and not again["qualified_final_holdout"]
