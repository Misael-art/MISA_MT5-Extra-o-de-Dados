"""V3-01..05: contrato v2 da consulta pública (serviço, SDK e CLI)."""
import base64
import json
import os
import shutil
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import event, inspect, text

from mt5_extracao import cli, sdk
from mt5_extracao.analytics import datasets, feature_store, indicator_materializer, query_service, resampling
from mt5_extracao.database_manager import DatabaseManager
from tests.test_analytics_calendar import m1_bars

SYMBOL = "EURUSD"
RECEIVED = pd.Timestamp("2026-10-23 12:00", tz="UTC")
MARKET_DAY = pd.Timestamp("2026-09-15", tz="UTC")
FAR = pd.Timestamp("2100-01-01", tz="UTC")


def _prepared(path, start="2026-09-14 09:00", received=RECEIVED):
    db = DatabaseManager(db_path=str(path), broker_utc_offset=0)
    assert db.save_ohlcv_data(SYMBOL, "1 minuto", m1_bars(pd.date_range(start, periods=90, freq="min")),
                              received_at=received)
    datasets.identity(db)  # inicialização explícita (escrita), fora da consulta
    resampling.resample_symbol(db, SYMBOL, ["M5"], symbol_info=SimpleNamespace(name=SYMBOL))
    indicator_materializer.materialize(db, SYMBOL, "M5", feature_ids=["price.return_1", "price.return_5"])
    return db


def _series(db, **kw):
    args = dict(symbol=SYMBOL, timeframe="M5", feature_ids=["price.return_1"], as_of=FAR)
    args.update(kw)
    return query_service.series(db, **args)


def _cli(tmp_path, db_path, *extra, capsys):
    config = tmp_path / "cli.ini"
    config.write_text(f"[DATABASE]\ntype = sqlite\npath = {db_path}\n[APP]\nbroker_utc_offset = 0\n", encoding="utf-8")
    code = cli.main(["--config", str(config), "--json", "analytics", "features", "series", "--symbol", SYMBOL,
                     "--tf", "M5", *extra])
    return code, json.loads(capsys.readouterr().out)


# --- V3-01: reconstrução × conhecimento até o consumidor --------------------------------------------

def test_v3_01_research_is_labelled_and_knowledge_waits_for_the_system(tmp_path, capsys):
    db = _prepared(tmp_path / "late.sqlite")
    research = _series(db, as_of=MARKET_DAY)
    assert research["temporal_basis"] == "research" and len(research["values"]) == 18
    assert {row["knowledge_state"] for row in research["values"]} == {"reconstructed_after_cut"}
    assert all(row["reconstructed"] is True for row in research["values"])
    assert any("reconstrução de pesquisa" in reason for reason in research["reasons"])
    knowledge = _series(db, as_of=MARKET_DAY, basis="knowledge")
    assert knowledge["values"] == [] and knowledge["reasons"]
    assert _series(db, as_of=RECEIVED, basis="knowledge")["values"] == []  # recebido, ainda não calculado
    known = _series(db, basis="knowledge")
    assert len(known["values"]) == 18 and {r["knowledge_state"] for r in known["values"]} == {"known_at_cut"}
    via_sdk = sdk.query_series(db, symbol=SYMBOL, timeframe="M5", feature_ids=["price.return_1"], as_of=MARKET_DAY,
                               basis="knowledge")
    assert via_sdk["values"] == []
    code, out = _cli(tmp_path, db.db_path, "--feature", "price.return_1", "--as-of", MARKET_DAY.isoformat(),
                     "--basis", "knowledge", capsys=capsys)
    assert code == 0 and out["data"]["envelope"]["values"] == []


def test_v3_01_correction_is_known_only_after_it_is_recorded(tmp_path):
    db = _prepared(tmp_path / "fix.sqlite")
    before_fix = pd.Timestamp.now(tz="UTC")
    target = db.load_ohlcv(SYMBOL, "1 minuto").iloc[[14]].copy()  # fecha a barra M5 09:10
    target["close"] = target["close"] * 2
    target["high"] = target[["high", "close"]].max(axis=1)
    assert db.save_ohlcv_data(SYMBOL, "1 minuto", target, received_at=pd.Timestamp.now(tz="UTC"))
    resampling.resample_symbol(db, SYMBOL, ["M5"], symbol_info=SimpleNamespace(name=SYMBOL))
    indicator_materializer.materialize(db, SYMBOL, "M5", feature_ids=["price.return_1"])
    old = {r["observation_time"]: r["value"] for r in _series(db, as_of=before_fix, basis="knowledge")["values"]}
    new = {r["observation_time"]: r["value"] for r in _series(db, basis="knowledge")["values"]}
    assert old.keys() == new.keys() and old != new


def test_v3_01_legacy_facts_are_not_knowledge(tmp_path):
    db = _prepared(tmp_path / "legacy.sqlite")
    with db.engine.begin() as conn:
        conn.execute(text("UPDATE _feature_values SET recorded_at_utc = NULL, generation = NULL"))
    research = _series(db)
    assert {r["knowledge_state"] for r in research["values"]} == {"recording_unknown"}
    assert any("sem instante de gravação" in reason for reason in research["reasons"])
    assert _series(db, basis="knowledge")["values"] == []


# --- V3-02: definição histórica imutável ------------------------------------------------------------

def _old_version(db):
    feature_store.register_definition(db, "price.return_1", "Return (1 bar) %", "old-percent-v0",
                                      {"horizon_bars": 1, "scale": 100}, ["bars_derived:M5"], unit="percent",
                                      value_type="NUMBER", formula="100 * (close/close[t-1] - 1)")
    stamp = pd.Timestamp("2026-09-14T10:00Z")
    feature_store.save_value(db, SYMBOL, "M5", "price.return_1", "old-percent-v0", stamp, 1.5,
                             observed_at=stamp, available_at=stamp)


def test_v3_02_each_fact_uses_the_definition_of_its_version(tmp_path):
    db = _prepared(tmp_path / "versions.sqlite")
    _old_version(db)
    both = _series(db)
    versions = both["definitions"]["price.return_1"]["versions"]
    assert versions["old-percent-v0"]["unit"] == "percent" and versions["old-percent-v0"]["parameters"]["scale"] == 100
    current = [v for k, v in versions.items() if k != "old-percent-v0"]
    assert current and all(v["unit"] == "fraction" for v in current)
    assert both["definitions"]["price.return_1"]["current_contract"]["unit"] == "fraction"
    only_old = _series(db, algorithm_version="old-percent-v0")
    assert list(only_old["definitions"]["price.return_1"]["versions"]) == ["old-percent-v0"]
    reopened = DatabaseManager(db_path=db.db_path, broker_utc_offset=0, read_only=True)
    assert _series(reopened, algorithm_version="old-percent-v0")["definitions"] == only_old["definitions"]


def test_v3_02_missing_version_definition_is_explicit(tmp_path):
    db = _prepared(tmp_path / "missing.sqlite")
    _old_version(db)
    with db.engine.begin() as conn:
        conn.execute(text("DELETE FROM _feature_definitions WHERE algorithm_version = 'old-percent-v0'"))
    env = _series(db, algorithm_version="old-percent-v0")
    assert env["definitions"]["price.return_1"]["versions"]["old-percent-v0"]["state"] == "missing"
    assert any("definição de versão não está gravada" in r for r in env["reasons"])


# --- V3-03: leitura sem escrita --------------------------------------------------------------------

def _trace(db):
    statements = []

    def record(_conn, _cursor, statement, *_args):
        statements.append(statement.strip().split()[0].upper())

    event.listen(db.engine, "before_cursor_execute", record)
    return statements


def test_v3_03_query_sends_no_ddl_or_dml(tmp_path):
    db = _prepared(tmp_path / "trace.sqlite")
    statements = _trace(db)
    assert _series(db)["status"] == "ok"
    assert set(statements) <= {"SELECT", "PRAGMA", "WITH"}, set(statements)


def test_v3_03_read_only_connection_works_and_unidentified_base_is_not_initialized(tmp_path, capsys):
    db = _prepared(tmp_path / "ro.sqlite")
    db.engine.dispose()
    reader = DatabaseManager(db_path=db.db_path, broker_utc_offset=0, read_only=True)
    with reader.engine.connect() as conn:
        with pytest.raises(Exception):
            conn.execute(text("CREATE TABLE intruso (x INTEGER)"))
    assert len(_series(reader)["values"]) == 18
    bare = DatabaseManager(db_path=str(tmp_path / "bare.sqlite"))
    bare.save_ohlcv_data(SYMBOL, "1 minuto", m1_bars(pd.date_range("2026-09-14 09:00", periods=3, freq="min")))
    bare.engine.dispose()
    before = set(inspect(DatabaseManager(db_path=bare.db_path, read_only=True).engine).get_table_names())
    env = _series(DatabaseManager(db_path=bare.db_path, read_only=True))
    assert env["base"]["state"] == "unidentified" and env["base"]["namespace"] is None
    assert set(inspect(DatabaseManager(db_path=bare.db_path, read_only=True).engine).get_table_names()) == before
    os.chmod(db.db_path, 0o444)  # CLI completa sobre arquivo somente leitura
    try:
        code, out = _cli(tmp_path, db.db_path, "--feature", "price.return_1", "--as-of", FAR.isoformat(),
                         capsys=capsys)
    finally:
        os.chmod(db.db_path, 0o644)
    assert code == 0 and len(out["data"]["envelope"]["values"]) == 18


# --- V3-04: cursor ligado à base, ao modo e à fotografia -------------------------------------------

def test_v3_04_cursor_is_bound_to_base_and_basis(tmp_path):
    a = _prepared(tmp_path / "a.sqlite")
    b = _prepared(tmp_path / "b.sqlite")
    first = _series(a, page_size=5)
    other_base = _series(b, page_size=5, cursor=first["page"]["next_cursor"])
    assert other_base["status"] == "error" and other_base["errors"][0]["code"] == "CURSOR_MISMATCH"
    other_basis = _series(a, page_size=5, basis="knowledge", cursor=first["page"]["next_cursor"])
    assert other_basis["errors"][0]["code"] == "CURSOR_MISMATCH"
    a.engine.dispose()
    shutil.copy(a.db_path, tmp_path / "backup.sqlite")
    backup = DatabaseManager(db_path=str(tmp_path / "backup.sqlite"), broker_utc_offset=0)
    assert _series(backup, page_size=5, cursor=first["page"]["next_cursor"])["status"] == "ok"  # mesma base
    datasets.fork(backup)
    forked = _series(backup, page_size=5, cursor=first["page"]["next_cursor"])
    assert forked["errors"][0]["code"] == "CURSOR_MISMATCH"


def test_v3_04_pages_keep_the_snapshot_of_the_first_page(tmp_path):
    db = _prepared(tmp_path / "snap.sqlite")
    full = _series(db, feature_ids=["price.return_1", "price.return_5"], page_size=5000)["values"]
    page = _series(db, feature_ids=["price.return_1", "price.return_5"], page_size=7)
    collected, cursor = list(page["values"]), page["page"]["next_cursor"]
    target = db.load_ohlcv(SYMBOL, "1 minuto").iloc[[14]].copy()  # revisão no meio da paginação
    target["close"] = target["close"] * 3
    target["high"] = target[["high", "close"]].max(axis=1)
    db.save_ohlcv_data(SYMBOL, "1 minuto", target, received_at=pd.Timestamp.now(tz="UTC"))
    resampling.resample_symbol(db, SYMBOL, ["M5"], symbol_info=SimpleNamespace(name=SYMBOL))
    indicator_materializer.materialize(db, SYMBOL, "M5", feature_ids=["price.return_1", "price.return_5"])
    while cursor:
        page = _series(db, feature_ids=["price.return_1", "price.return_5"], page_size=7, cursor=cursor)
        collected += page["values"]
        cursor = page["page"]["next_cursor"]
    assert collected == full  # nem perda, nem duplicação, nem mistura com a revisão nova


# --- V3-05: validação e erros estruturados ---------------------------------------------------------

def _cursor(payload):
    return base64.urlsafe_b64encode(json.dumps(payload).encode()).decode()


def test_v3_05_invalid_inputs_have_stable_codes(tmp_path, capsys):
    db = _prepared(tmp_path / "errors.sqlite")
    binding = _series(db, page_size=1)["page"]["next_cursor"]
    good = json.loads(base64.urlsafe_b64decode(binding.encode()))
    cases = {
        "BAD_TIME": dict(as_of="nao-e-data"),
        "NAIVE_TIME": dict(as_of="2026-01-01"),
        "BAD_RANGE": dict(start="2026-09-15T00:00Z", end="2026-09-14T00:00Z"),
        "BAD_BASIS": dict(basis="conhecido"),
        "MISSING_SELECTION": dict(feature_ids="price.return_1"),
        "BAD_SELECTION": dict(feature_ids=[1]),
        "BAD_PAGE_SIZE": dict(page_size=True),
        "BAD_CURSOR": dict(cursor=_cursor([])),
    }
    cases_cursor = {"missing_key": {"s": good["s"], "t": good["t"], "g": good["g"]},
                    "wrong_types": {**good, "k": [1, 2, 3]}, "bad_generation": {**good, "g": "1"},
                    "huge": "x" * 5000}
    for code, override in cases.items():
        assert _series(db, **override)["errors"][0]["code"] == code, code
    for name, payload in cases_cursor.items():
        cursor = payload if isinstance(payload, str) else _cursor(payload)
        assert _series(db, page_size=1, cursor=cursor)["errors"][0]["code"] == "BAD_CURSOR", name
    code, out = _cli(tmp_path, db.db_path, "--feature", "price.return_1", "--as-of", "nao-e-data", capsys=capsys)
    assert code == cli.EXIT_USAGE and out["data"]["envelope"]["errors"][0]["code"] == "BAD_TIME"


def test_v3_05_repeated_features_are_normalized_and_pages_concatenate(tmp_path):
    db = _prepared(tmp_path / "dup.sqlite")
    repeated = ["price.return_1", "price.return_5", "price.return_1"]
    single = _series(db, feature_ids=["price.return_1", "price.return_5"], page_size=5000)
    dup = _series(db, feature_ids=repeated, page_size=5000)
    assert dup["selection"]["feature_ids"] == ["price.return_1", "price.return_5"]
    assert dup["values"] == single["values"]
    pages, cursor = [], None
    while True:
        page = _series(db, feature_ids=repeated, page_size=4, cursor=cursor)
        pages += page["values"]
        cursor = page["page"]["next_cursor"]
        if not cursor:
            break
    keys = [(r["feature_id"], r["observation_time"], r["algorithm_version"]) for r in pages]
    assert len(keys) == len(set(keys)) and pages == single["values"]


def test_v3_03_read_only_pg_url_keeps_existing_options():
    url = "postgresql+psycopg://u:p@h:5432/db?options=-csearch_path%3Dmt5x_test_x"
    options = DatabaseManager.read_only_pg_url(url).split("options=", 1)[1]
    assert "search_path" in options and "default_transaction_read_only" in options
    plain = DatabaseManager.read_only_pg_url("postgresql+psycopg://u:p@h:5432/db")
    assert "default_transaction_read_only" in plain
