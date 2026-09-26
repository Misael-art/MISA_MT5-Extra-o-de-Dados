import datetime as dt
import threading

import pytest
from sqlalchemy import text

import mt5_extracao.historical_extractor as he_module
import mt5_extracao.mt5_connector as connector_module
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.historical_extractor import HistoricalExtractor
from mt5_extracao.indicator_calculator import IndicatorCalculator

M1 = 1


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(he_module.time, "sleep", lambda s: None)
    monkeypatch.setattr(connector_module.time, "sleep", lambda s: None)


def _setup(connector, tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "t.db"))
    ext = HistoricalExtractor(connector, db, IndicatorCalculator(), chunk_config={"m1": 30, "m5_m15": 90, "default": 365})
    return ext, db


def _update(ext, symbols, now):
    done = threading.Event()
    result = {}

    def finished(s, f, c):
        result.update(success=s, failed=f, canceled=c)
        done.set()

    starts = ext.update_symbols(symbols, M1, "1 minuto", include_indicators=False, now=now, finished_callback=finished)
    assert done.wait(60), "atualização não terminou"
    return starts, result


def _count(db, table="win_n_1_minuto"):
    with db.engine.connect() as c:
        return c.execute(text(f'SELECT COUNT(*), COUNT(DISTINCT time) FROM "{table}"')).fetchone()


def test_get_last_timestamp(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "t.db"))
    assert db.get_last_timestamp("nao_existe") is None


def test_update_from_last_record_twice_without_duplicates(connector, fake_mt5, tmp_path):
    ext, db = _setup(connector, tmp_path)
    assert ext._process_symbol("WIN$N", M1, "1 minuto", dt.datetime(2024, 1, 2), dt.datetime(2024, 1, 3),
                               include_indicators=False, overwrite=False)
    assert db.get_last_timestamp("win_n_1_minuto") == dt.datetime(2024, 1, 2, 17, 59)

    now = dt.datetime(2024, 1, 3, 12, 0)
    starts, result = _update(ext, ["WIN$N"], now)
    assert starts["WIN$N"] == dt.datetime(2024, 1, 2, 17, 58)  # 1 barra de sobreposição
    assert result["success"] == 1
    total, distinct = _count(db)
    assert total == distinct == 540 + 3 * 60 + 1  # dia 02 completo + 03 das 09:00 às 12:00

    # 2ª atualização no mesmo instante: nada novo, nada duplicado
    _update(ext, ["WIN$N"], now)
    assert _count(db) == (total, total)


def test_update_symbol_without_data_starts_default_days_ago(connector, fake_mt5, tmp_path):
    ext, db = _setup(connector, tmp_path)
    now = dt.datetime(2024, 1, 10, 10, 0)
    starts, result = _update(ext, ["PETR4"], now)
    assert starts["PETR4"] == now - dt.timedelta(days=30)
    assert result["success"] == 1
    assert _count(db, "petr4_1_minuto")[0] > 0
