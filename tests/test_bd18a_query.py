"""BD18a: serviço de consulta sobre a cadeia do piloto (retorno), sem UI e sem MT5."""
import json

import pandas as pd

from mt5_extracao import sdk
from mt5_extracao.analytics import feature_store, query_service
from tests.test_bd08a_returns import RETURNS, SYMBOL, _chain, _materialize, no_sleep  # noqa: F401

NOW = pd.Timestamp("2100-01-01", tz="UTC")


def _db(connector, tmp_path):
    db = _chain(connector, tmp_path / "q.sqlite")
    _materialize(db)
    return db


def test_envelope_carries_base_time_definitions_and_store_facts(connector, fake_mt5, tmp_path):
    db = _db(connector, tmp_path)
    env = query_service.series(db, symbol=SYMBOL, timeframe="M5", feature_ids=["price.return_5"], as_of=NOW,
                               page_size=5000)
    assert env["status"] == "ok" and env["errors"] == []
    assert env["base"]["provider"] == "mt5" and env["base"]["server"] == "Fake-Demo"
    assert env["time_context"]["offset_state"] == "declared"
    assert env["definitions"]["price.return_5"]["unit"] == "fraction"
    store = feature_store.query_as_of(db, NOW, symbol=SYMBOL, timeframe="M5", feature_id="price.return_5")
    assert len(env["values"]) == len(store)
    by_time = {row["observation_time"]: row["value"] for row in env["values"]}
    for item in store.to_dict("records"):
        want = item["numeric_value"]
        got = by_time[query_service._iso(item["observation_time"])]
        assert (got is None and pd.isna(want)) or got == want
    json.dumps(env, allow_nan=False)


def test_pagination_is_stable_and_cursor_is_bound_to_selection(connector, fake_mt5, tmp_path):
    db = _db(connector, tmp_path)
    full = query_service.series(db, symbol=SYMBOL, timeframe="M5", feature_ids=RETURNS, as_of=NOW, page_size=5000)
    pages, cursor = [], None
    while True:
        env = query_service.series(db, symbol=SYMBOL, timeframe="M5", feature_ids=RETURNS, as_of=NOW,
                                   page_size=97, cursor=cursor)
        pages += env["values"]
        cursor = env["page"]["next_cursor"]
        if not cursor:
            break
    assert pages == full["values"]
    other = query_service.series(db, symbol=SYMBOL, timeframe="M5", feature_ids=["price.return_1"], as_of=NOW,
                                 cursor=query_service.series(db, symbol=SYMBOL, timeframe="M5", feature_ids=RETURNS,
                                                             as_of=NOW, page_size=1)["page"]["next_cursor"])
    assert other["status"] == "error" and other["errors"][0]["code"] == "CURSOR_MISMATCH"


def test_as_of_before_availability_returns_nothing_with_reason(connector, fake_mt5, tmp_path):
    db = _db(connector, tmp_path)
    env = query_service.series(db, symbol=SYMBOL, timeframe="M5", feature_ids=["price.return_1"],
                               as_of=pd.Timestamp("2024-01-01", tz="UTC"))
    assert env["values"] == [] and env["reasons"]


def test_structured_errors_and_no_writes(connector, fake_mt5, tmp_path):
    db = _db(connector, tmp_path)
    before = len(feature_store.query_history(db, symbol=SYMBOL))
    cases = {"NAIVE_TIME": dict(as_of="2026-01-01"), "UNKNOWN_FEATURE": dict(feature_ids=["x.y"]),
             "BAD_PAGE_SIZE": dict(page_size=0), "BAD_CURSOR": dict(cursor="@@")}
    for code, override in cases.items():
        args = {**dict(symbol=SYMBOL, timeframe="M5", feature_ids=["price.return_1"], as_of=NOW), **override}
        env = query_service.series(db, **args)
        assert env["status"] == "error" and env["errors"][0]["code"] == code, code
    assert len(feature_store.query_history(db, symbol=SYMBOL)) == before


def test_sdk_returns_the_same_facts(connector, fake_mt5, tmp_path):
    db = _db(connector, tmp_path)
    selection = dict(symbol=SYMBOL, timeframe="M5", feature_ids=["price.return_20"], as_of=NOW)
    assert sdk.query_series(db, **selection) == query_service.series(db, **selection)
    assert "price.return_20" in {d["feature_id"] for d in sdk.query_catalog()["definitions"]}
