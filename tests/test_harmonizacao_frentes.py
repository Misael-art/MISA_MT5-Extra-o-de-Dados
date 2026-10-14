"""Cruzamentos entre frentes (harmonização de 2026-10-14): o que uma frente cria não pode quebrar outra."""
from sqlalchemy import inspect

from mt5_extracao.analytics import calendar as session_calendar
from mt5_extracao.analytics import resampling
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.resources import hygiene, policy, storage
from tests.market_data import random_walk


def test_t13_tables_are_internal_and_do_not_become_fake_assets(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "b.db"))
    db.save_ohlcv_data("EURUSD", "1 dia", random_walk(50, freq="D"))
    session_calendar.ensure_schema(db)
    resampling.ensure_schema(db)
    names = set(inspect(db.engine).get_table_names())
    assert {"_market_calendar", "_bars_derived"} <= names and not {"market_calendar", "bars_derived"} & names
    assert [i["symbol"] for i in db.data_inventory()] == ["EURUSD"]          # nada de ativo falso, nada quebra
    kinds = {t["table"]: t["kind"] for t in storage.measure(db, with_dirs=False)["tables"]}
    assert kinds["_bars_derived"] == storage.KIND_INTERNAL == kinds["_market_calendar"]
    found = hygiene.scan(db, policy.Policy())
    assert all(not c["table"].startswith("_") for c in found["candidates"])   # Recursos nunca propõe arquivá-las
    db.close()
