"""T1.6: base de tempo explícita (broker = padrão, utc = convertido), sem misturar bases no mesmo banco."""
import datetime as dt

import pandas as pd
import pytest
from sqlalchemy import text

import mt5_extracao.historical_extractor as he_module
import mt5_extracao.mt5_connector as connector_module
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.historical_extractor import HistoricalExtractor
from mt5_extracao.indicator_calculator import IndicatorCalculator


def _df(times):
    n = len(times)
    return pd.DataFrame({"time": pd.to_datetime(times), "open": [1.0] * n, "high": [1.0] * n, "low": [1.0] * n,
                         "close": list(range(n)), "tick_volume": [1] * n, "spread": [1] * n, "real_volume": [1] * n})


def _stored_times(db, table="win_n_1_minuto"):
    with db.engine.connect() as c:
        return [r[0][:16] for r in c.execute(text(f'SELECT time FROM "{table}" ORDER BY time'))]


def test_default_broker_keeps_times_and_records_basis(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "t.db"))
    db.save_ohlcv_data("WIN$N", "1 minuto", _df(["2024-01-02 09:00"]))
    assert _stored_times(db) == ["2024-01-02 09:00"]
    assert db.get_metadata("time_basis") == "broker"


def test_utc_converts_on_write_and_back_on_read(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "t.db"), time_basis="utc", broker_utc_offset=-3)
    db.save_ohlcv_data("WIN$N", "1 minuto", _df(["2024-01-02 09:00", "2024-01-02 09:01", "2024-01-02 09:02"]))
    assert _stored_times(db) == ["2024-01-02 12:00", "2024-01-02 12:01", "2024-01-02 12:02"]
    # leitura devolve o horário da corretora
    assert db.get_last_timestamp("win_n_1_minuto") == dt.datetime(2024, 1, 2, 9, 2)
    warm = db.get_rows_before("win_n_1_minuto", dt.datetime(2024, 1, 2, 9, 2), 10)
    assert list(warm["time"].dt.strftime("%H:%M")) == ["09:00", "09:01"]
    # exclusão recebe horário da corretora
    assert db.delete_data_periodo("win_n_1_minuto", dt.datetime(2024, 1, 2, 9, 1), dt.datetime(2024, 1, 2, 9, 1))
    assert _stored_times(db) == ["2024-01-02 12:00", "2024-01-02 12:02"]


def test_mixing_bases_is_refused(tmp_path):
    path = str(tmp_path / "t.db")
    DatabaseManager(db_path=path).save_ohlcv_data("WIN$N", "1 minuto", _df(["2024-01-02 09:00"]))
    db_utc = DatabaseManager(db_path=path, time_basis="utc")
    assert db_utc.save_ohlcv_data("WIN$N", "1 minuto", _df(["2024-01-02 09:05"])) is False
    assert db_utc.save_data(_df(["2024-01-02 09:05"]), "win_n_1_minuto") is False
    assert _stored_times(db_utc) == ["2024-01-02 09:00"]


def test_invalid_basis():
    with pytest.raises(ValueError):
        DatabaseManager(db_path=":memory:", time_basis="local")


def test_extraction_in_utc_mode(connector, fake_mt5, tmp_path, monkeypatch):
    monkeypatch.setattr(he_module.time, "sleep", lambda s: None)
    monkeypatch.setattr(connector_module.time, "sleep", lambda s: None)
    db = DatabaseManager(db_path=str(tmp_path / "t.db"), time_basis="utc", broker_utc_offset=-3)
    ext = HistoricalExtractor(connector, db, IndicatorCalculator(), chunk_config={"m1": 1, "m5_m15": 1, "default": 1})
    assert ext._process_symbol("WIN$N", 1, "1 minuto", dt.datetime(2024, 1, 2), dt.datetime(2024, 1, 4),
                               include_indicators=True, overwrite=False)
    times = _stored_times(db)
    assert len(times) == 2 * 540 and times[0] == "2024-01-02 12:00" and times[-1] == "2024-01-03 20:59"
    assert db.get_last_timestamp("win_n_1_minuto") == dt.datetime(2024, 1, 3, 17, 59)
