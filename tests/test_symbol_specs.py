"""T6.5: especificações do símbolo e leitura de OHLCV do banco."""
import pandas as pd
import pytest
from sqlalchemy import text

from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.strategies import specs
from tests.market_data import random_walk


@pytest.fixture
def db(tmp_path):
    return DatabaseManager(db_path=str(tmp_path / "t.db"))


def test_fetch_and_save_specs_from_mt5(connector, db, fake_mt5):
    symbol = fake_mt5.symbols[0]
    result = specs.fetch_and_save_specs(connector, db, [symbol, "NAO_EXISTE"])
    assert result[symbol] == "ok" and "não encontrado" in result["NAO_EXISTE"]
    spec = specs.load_spec(db, symbol)
    assert spec.known and spec.tick_size == 5.0 and spec.tick_value == 0.2 and spec.volume_min == 1.0
    assert spec.spread_points == 5 and spec.currency_profit == "BRL"
    # WIN: tick de 5 pontos vale R$ 0,20 por contrato -> 1 ponto vale R$ 0,04
    assert spec.money_per_price_unit == pytest.approx(0.04)


def test_unknown_spec_is_generic(db):
    spec = specs.load_spec(db, "XPTO")
    assert not spec.known and spec.symbol == "XPTO"


def test_spec_upsert(db):
    db.save_symbol_spec("A", {"point": 0.01, "digits": 2, "tick_size": 0.01, "tick_value": 1, "volume_min": 1,
                              "volume_step": 1, "volume_max": 100, "contract_size": 1, "currency_profit": "BRL",
                              "spread": 1})
    db.save_symbol_spec("A", {**db.get_symbol_spec("A"), "spread": 3})
    with db.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM _symbol_specs")).scalar() == 1
    assert db.get_symbol_spec("A")["spread"] == 3
    assert "_symbol_specs" not in db.get_all_tables()


def test_spec_from_symbol_info_tolerates_missing_fields():
    class Info:
        point = 0.0001
    d = specs.spec_from_symbol_info(Info())
    assert d["tick_size"] == 0.0001 and d["tick_value"] == 1.0 and d["volume_min"] == 1.0


def test_load_ohlcv_roundtrip_and_period(db):
    df = random_walk(100, freq="h", start="2024-01-02 09:00")
    assert db.save_ohlcv_data("WIN$N", "1 hora", df)
    out = db.load_ohlcv("WIN$N", "1 hora")
    assert len(out) == 100 and list(out.columns[:5]) == ["time", "open", "high", "low", "close"]
    assert out["time"].is_monotonic_increasing
    assert out["close"].iloc[-1] == pytest.approx(df["close"].iloc[-1])
    part = db.load_ohlcv("WIN$N", "1 hora", start=df["time"][10], end=df["time"][19])
    assert len(part) == 10 and part["time"].iloc[0] == df["time"][10]


def test_load_ohlcv_missing_symbol_does_not_create_mapping(db):
    assert db.load_ohlcv("NADA", "1 hora").empty
    with db.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM _symbol_tables")).scalar() == 0
