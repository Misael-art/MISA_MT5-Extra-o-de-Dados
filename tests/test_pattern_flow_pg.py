"""P06: fatos e recibo compartilhados em transação no PostgreSQL da CI."""

import os

import pytest

from mt5_extracao import experiments
from mt5_extracao.analytics import feature_store
from mt5_extracao.analytics.pattern_families import compile_family
from mt5_extracao.analytics.pattern_flow import evaluate_registered_attempt
from mt5_extracao.experiments import store
from tests.postgres_helpers import isolated_postgresql_database
from tests.test_backtester import bars
from tests.test_pattern_campaign import campaign


PG_URL = os.environ.get("MT5X_TEST_PG_URL")
pytestmark = pytest.mark.skipif(not PG_URL, reason="defina MT5X_TEST_PG_URL; CI executa test_*_pg.py")


def test_pg_pattern_evaluation_reopens_same_receipt_and_facts():
    with isolated_postgresql_database(PG_URL, broker_utc_offset=0) as db:
        frame = bars([[100, 101, 99, 100], [98, 99, 97, 98],
                      *([[99, 100, 98, 99]] * 13)], start="2025-01-06 09:00", freq="min")
        frame["tick_volume"], frame["real_volume"] = 10, 0
        db.save_ohlcv_data("SYN", "1 minuto", frame)
        body, _, _ = campaign(db, max_attempts=1)
        hypothesis = compile_family("reversal", {"drop_fraction": "0.013"},
                                    symbol="SYN", timeframe="M1", clock_version="UTC_fixed_v1",
                                    calendar_version="synthetic_utc_day", cost_model_id="qa_cost",
                                    horizon_bars=1)
        body["grid"] = [hypothesis.definition_hash]
        protocol = experiments.create_pattern_campaign(db, body)["protocol_id"]
        attempt = experiments.register_pattern_attempt(db, protocol, "pg_reversal", hypothesis)
        first = evaluate_registered_attempt(db, attempt["request_id"],
                                            body["roster"][0]["data_manifest_id"],
                                            session_mode="synthetic_utc_day")
        again = evaluate_registered_attempt(db, attempt["request_id"],
                                            body["roster"][0]["data_manifest_id"],
                                            session_mode="synthetic_utc_day")
        assert again["receipt"] == first["receipt"]
        assert store.pattern_evaluations(db, protocol) == [first["receipt"]]
        assert len(feature_store.query_as_of(db, "2025-01-06T09:03:00Z",
                                             feature_id=first["outcome_feature_id"])) == 1
