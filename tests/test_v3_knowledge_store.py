"""V3-01 (armazenamento): reconstrução de pesquisa × conhecimento efetivo do sistema."""
from types import SimpleNamespace

import pandas as pd
from sqlalchemy import text

from mt5_extracao.analytics import feature_store, indicator_materializer, resampling
from mt5_extracao.database_manager import DatabaseManager
from tests.test_analytics_calendar import m1_bars

SYMBOL = "EURUSD"
RECEIVED = pd.Timestamp("2026-10-23 12:00", tz="UTC")
MARKET_DAY = pd.Timestamp("2026-09-15", tz="UTC")


def _late_chain(path):
    db = DatabaseManager(db_path=str(path), broker_utc_offset=0)
    bars = m1_bars(pd.date_range("2026-09-14 09:00", periods=90, freq="min"))
    assert db.save_ohlcv_data(SYMBOL, "1 minuto", bars, received_at=RECEIVED)
    resampling.resample_symbol(db, SYMBOL, ["M5"], symbol_info=SimpleNamespace(name=SYMBOL))
    indicator_materializer.materialize(db, SYMBOL, "M5", feature_ids=["price.return_1"])
    return db


def _query(db, as_of, basis, **kw):
    return feature_store.query_as_of(db, as_of, symbol=SYMBOL, timeframe="M5", feature_id="price.return_1",
                                     basis=basis, **kw)


def test_research_reconstructs_but_knowledge_waits_for_the_system(tmp_path):
    db = _late_chain(tmp_path / "late.sqlite")
    assert db.load_ohlcv_as_of(SYMBOL, "1 minuto", MARKET_DAY).empty     # bruto: ainda não recebido
    assert len(_query(db, MARKET_DAY, "research")) == 18                 # reconstrução de pesquisa
    assert _query(db, MARKET_DAY, "knowledge").empty                     # o sistema não sabia
    assert _query(db, RECEIVED, "knowledge").empty                        # recebido, ainda não calculado
    now = pd.Timestamp.now(tz="UTC")
    known = _query(db, now, "knowledge", with_recorded=True)
    assert len(known) == 18 and (pd.to_datetime(known["recorded_at_utc"], utc=True) <= now).all()


def test_legacy_facts_without_recording_time_are_not_knowledge(tmp_path):
    db = _late_chain(tmp_path / "legacy.sqlite")
    with db.engine.begin() as conn:
        conn.execute(text("UPDATE _feature_values SET recorded_at_utc = NULL"))
    now = pd.Timestamp.now(tz="UTC")
    assert len(_query(db, now, "research")) == 18
    assert _query(db, now, "knowledge").empty


def test_recorded_through_freezes_a_snapshot(tmp_path):
    db = _late_chain(tmp_path / "snap.sqlite")
    frozen = pd.Timestamp.now(tz="UTC")
    target = db.load_ohlcv(SYMBOL, "1 minuto").iloc[[14]].copy()  # 09:14: última M1 da barra M5 09:10
    target["close"] = target["close"] * 2
    target["high"] = target[["high", "close"]].max(axis=1)
    assert db.save_ohlcv_data(SYMBOL, "1 minuto", target, received_at=pd.Timestamp.now(tz="UTC"))
    resampling.resample_symbol(db, SYMBOL, ["M5"], symbol_info=SimpleNamespace(name=SYMBOL))
    indicator_materializer.materialize(db, SYMBOL, "M5", feature_ids=["price.return_1"])
    later = pd.Timestamp.now(tz="UTC")
    before = _query(db, later, "research", recorded_through=frozen).set_index("observation_time")["numeric_value"]
    after = _query(db, later, "research").set_index("observation_time")["numeric_value"]
    assert not before.equals(after)
    assert _query(db, frozen, "research").set_index("observation_time")["numeric_value"].equals(before)


def test_invalid_basis_is_refused(tmp_path):
    db = _late_chain(tmp_path / "basis.sqlite")
    try:
        _query(db, RECEIVED, "conhecido")
    except ValueError as exc:
        assert "basis" in str(exc)
    else:
        raise AssertionError("basis inválido deveria falhar")
