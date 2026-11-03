"""P03: orçamento global serializado também no PostgreSQL isolado da CI."""

from concurrent.futures import ThreadPoolExecutor
import os

import pytest

from mt5_extracao import experiments
from tests.postgres_helpers import isolated_postgresql_database
from tests.test_backtester import bars
from tests.test_pattern_campaign import campaign


PG_URL = os.environ.get("MT5X_TEST_PG_URL")
pytestmark = pytest.mark.skipif(not PG_URL, reason="defina MT5X_TEST_PG_URL; CI inclui test_*_pg.py")


def test_pg_pattern_attempts_share_one_campaign_budget_and_exposure():
    with isolated_postgresql_database(PG_URL, broker_utc_offset=0) as db:
        frame = bars([[100, 101, 99, 100]] * 15, start="2025-01-06 09:00", freq="min")
        frame["tick_volume"], frame["real_volume"] = 10, 0
        db.save_ohlcv_data("SYN", "1 minuto", frame)
        body, long, short = campaign(db, max_attempts=1)
        body["grid"] = [long.definition_hash]
        protocol = experiments.create_pattern_campaign(db, body)["protocol_id"]
        with ThreadPoolExecutor(max_workers=2) as pool:
            requests = list(pool.map(lambda pair: experiments.register_pattern_attempt(
                db, protocol, pair[0], pair[1]), [("long", long), ("short", short)]))
        assert {item["state"] for item in requests} == {"requested", "rejected"}
        assert len(experiments.pattern_campaign_status(db, protocol)["requests"]) == 2
        experiments.pattern_campaign_access(db, protocol, "review", purpose="tuning")
        assert experiments.pattern_campaign_status(db, protocol)["exposure"]["holdout"]["status"] == "used_for_tuning"
