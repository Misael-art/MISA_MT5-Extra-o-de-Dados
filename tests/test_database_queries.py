"""T4.3: consultas auxiliares do DatabaseManager usadas pela interface e pela exportação."""
import pandas as pd
import pytest
from sqlalchemy import text

from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.error_handler import DatabaseError
from tests.market_data import random_walk

TABLE = "win_n_1_minuto"


@pytest.fixture
def db(tmp_path):
    manager = DatabaseManager(db_path=str(tmp_path / "q.db"))
    df = random_walk(120, freq="min", start="2024-01-02 09:00")
    assert manager.save_ohlcv_data("WIN$N", "1 minuto", df)
    return manager


def test_existing_symbols_and_tables(db):
    assert db.get_existing_symbols() == [TABLE]
    assert db.get_all_tables() == [TABLE]


def test_symbol_data_summary(db):
    s = db.get_symbol_data_summary(TABLE)
    assert s["total_registros"] == 120
    assert s["data_inicio"].startswith("2024-01-02 09:00")
    assert s["data_fim"].startswith("2024-01-02 10:59")
    assert s["intervalo_medio_minutos"] == pytest.approx(1.0)
    assert s["completude"] == pytest.approx(100.0, abs=1)
    assert db.get_symbol_data_summary("nao_existe") is None


def test_table_summary(db):
    s = db.get_table_summary(TABLE)
    assert s["total_registros"] == 120
    assert pd.Timestamp(s["data_inicial"]) == pd.Timestamp("2024-01-02 09:00")


def test_recent_data_is_sorted_and_limited(db):
    df = db.get_recent_data(TABLE, limit=10)
    assert len(df) == 10
    assert df["time"].is_monotonic_increasing
    assert pd.Timestamp(df["time"].iloc[-1]) == pd.Timestamp("2024-01-02 10:59")
    assert db.get_recent_data("nao_existe") is None


def test_execute_query(db):
    df = db.execute_query(f'SELECT COUNT(*) AS n FROM "{TABLE}" WHERE close > :x', {"x": 0})
    assert int(df["n"].iloc[0]) == 120
    with pytest.raises(DatabaseError):
        db.execute_query("SELECT * FROM tabela_inexistente")


def test_optimize_creates_time_index(db):
    assert db.optimize_database() is True
    with db.engine.connect() as conn:
        names = [r[0] for r in conn.execute(text(
            "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=:t"), {"t": TABLE})]
    assert f"idx_{TABLE}_time" in names


def test_save_data_appends_new_rows(db):
    extra = random_walk(5, freq="min", start="2024-01-02 11:00")
    assert db.save_data(extra, TABLE, "WIN$N")
    assert db.get_table_summary(TABLE)["total_registros"] == 125


def test_disconnected_manager_is_safe(tmp_path):
    db = DatabaseManager(db_type="postgresql_nao_suportado", db_path=str(tmp_path / "x"))
    assert not db.is_connected()
    assert db.get_symbol_data_summary(TABLE) is None
    assert db.get_recent_data(TABLE) is None
    assert db.optimize_database() is False
    assert db.save_ohlcv_data("WIN$N", "1 minuto", random_walk(3)) is False
