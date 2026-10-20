"""Evidência de preset no PostgreSQL existente, sem MT5 e sem aceite nativo."""
import os
import pytest
from tests.postgres_helpers import isolated_postgresql_database
from mt5_extracao.presets import execution

PG_URL=os.environ.get("MT5X_TEST_PG_URL")
pytestmark=pytest.mark.skipif(not PG_URL,reason="defina MT5X_TEST_PG_URL para testar com PostgreSQL")


def test_native_evidence_is_immutable_and_reopens_pg():
    pytest.importorskip("psycopg")
    proof=dict(preset_native_evidence_schema_version=1,run_id="fake-contract-only",execution_hash="a"*64,
               mode="backtest",status="contract_only",trades=0,functional_cycle=False,allows_live=False,
               statistical_approval=False,created_at="2026-10-20T00:00:00+00:00")
    with isolated_postgresql_database(PG_URL) as db:
        execution._save_evidence(db,proof);execution._save_evidence(db,proof)
        assert execution.load_evidence(db,execution_hash="a"*64)==[proof]
        with pytest.raises(ValueError,match="outra evidência"):
            execution._save_evidence(db,dict(proof,status="modified"))
