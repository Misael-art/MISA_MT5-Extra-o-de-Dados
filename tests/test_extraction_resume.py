import calendar
import datetime as dt

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import text

import mt5_extracao.historical_extractor as he_module
import mt5_extracao.mt5_connector as connector_module
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.historical_extractor import HistoricalExtractor
from mt5_extracao.indicator_calculator import IndicatorCalculator

M1 = 1
START, END = dt.datetime(2024, 1, 2), dt.datetime(2024, 1, 5)  # 3 blocos de 1 dia (ter, qua, qui)


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(he_module.time, "sleep", lambda s: None)
    monkeypatch.setattr(connector_module.time, "sleep", lambda s: None)


def _epoch(d):
    return calendar.timegm(d.timetuple())


def _extractor(connector, db_path, chunk_days=1):
    db = DatabaseManager(db_path=str(db_path))
    return HistoricalExtractor(connector, db, IndicatorCalculator(),
                               chunk_config={"m1": chunk_days, "m5_m15": chunk_days, "default": chunk_days}), db


def _run(ext, indicators=False, overwrite=False):
    return ext._process_symbol("WIN$N", M1, "1 minuto", START, END, include_indicators=indicators, overwrite=overwrite)


def _days(db):
    with db.engine.connect() as c:
        return sorted({r[0][:10] for r in c.execute(text("SELECT time FROM win_n_1_minuto")).fetchall()})


def _log(db):
    with db.engine.connect() as c:
        return c.execute(text("SELECT block_start, status, rows FROM _extraction_log ORDER BY block_start")).fetchall()


def test_failed_block_does_not_block_others_and_resume_fetches_only_missing(connector, fake_mt5, tmp_path):
    # Falha simulada no 2º dia (03/01)
    fake_mt5.fail_ranges = [(_epoch(dt.datetime(2024, 1, 3, 9)), _epoch(dt.datetime(2024, 1, 3, 18)))]
    ext, db = _extractor(connector, tmp_path / "t.db")
    assert _run(ext) is False
    assert _days(db) == ["2024-01-02", "2024-01-04"]
    assert [r[1] for r in _log(db)] == ["ok", "failed", "ok"]

    # Retomada: sem falha, só o bloco que faltou é buscado de novo
    fake_mt5.fail_ranges = []
    fake_mt5.calls.clear()
    assert _run(ext) is True
    fetched = fake_mt5.calls_named("copy_rates_range")
    assert len(fetched) == 1
    assert fetched[0][1][2] == _epoch(dt.datetime(2024, 1, 3, 0, 0, 1))
    assert _days(db) == ["2024-01-02", "2024-01-03", "2024-01-04"]
    assert [r[1] for r in _log(db)] == ["ok", "ok", "ok"]


def test_rerun_skips_completed_blocks(connector, fake_mt5, tmp_path):
    ext, db = _extractor(connector, tmp_path / "t.db")
    assert _run(ext)
    fake_mt5.calls.clear()
    assert _run(ext)
    assert fake_mt5.calls_named("copy_rates_range") == []


def test_overwrite_refetches_everything(connector, fake_mt5, tmp_path):
    ext, db = _extractor(connector, tmp_path / "t.db")
    assert _run(ext)
    fake_mt5.calls.clear()
    assert _run(ext, overwrite=True)
    assert len(fake_mt5.calls_named("copy_rates_range")) == 3


def test_empty_blocks_are_recorded_and_skipped(connector, fake_mt5, tmp_path):
    ext, db = _extractor(connector, tmp_path / "t.db")
    args = ("WIN$N", M1, "1 minuto", dt.datetime(2024, 1, 6), dt.datetime(2024, 1, 8))  # fim de semana
    assert ext._process_symbol(*args, include_indicators=False, overwrite=False)
    assert [r[1] for r in _log(db)] == ["empty", "empty"]
    fake_mt5.calls.clear()
    assert ext._process_symbol(*args, include_indicators=False, overwrite=False)
    assert fake_mt5.calls_named("copy_rates_range") == []


def test_indicators_per_block_match_single_block(connector, fake_mt5, tmp_path):
    cols = ["rsi", "ma_20", "atr", "true_range", "macd_line", "macd_signal", "bollinger_upper"]
    ext_blocks, db_blocks = _extractor(connector, tmp_path / "blocks.db", chunk_days=1)
    ext_single, db_single = _extractor(connector, tmp_path / "single.db", chunk_days=10)
    assert _run(ext_blocks, indicators=True)
    assert _run(ext_single, indicators=True)
    q = "SELECT time, " + ", ".join(cols) + " FROM win_n_1_minuto ORDER BY time"
    a = pd.read_sql(q, db_blocks.engine)
    b = pd.read_sql(q, db_single.engine)
    assert len(a) == len(b) == 3 * 540
    assert (a["time"] == b["time"]).all()
    for c in cols:
        x, y = a[c].to_numpy(dtype=float), b[c].to_numpy(dtype=float)
        assert np.array_equal(np.isnan(x), np.isnan(y)), c
        assert np.allclose(x[~np.isnan(x)], y[~np.isnan(y)], rtol=1e-9, atol=1e-9), c


def test_control_table_is_hidden_from_symbol_lists(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "t.db"))
    assert "_extraction_log" not in db.get_all_tables()
    assert "_extraction_log" not in db.get_existing_symbols()


def test_delete_period_includes_boundaries(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "t.db"))
    times = pd.date_range("2024-01-02 09:00", periods=5, freq="min")
    df = pd.DataFrame({"time": times, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0,
                       "tick_volume": 1, "spread": 1, "real_volume": 1})
    db.save_ohlcv_data("WIN$N", "1 minuto", df)
    assert db.delete_data_periodo("win_n_1_minuto", times[1].to_pydatetime(), times[3].to_pydatetime())
    with db.engine.connect() as c:
        left = [r[0][11:16] for r in c.execute(text("SELECT time FROM win_n_1_minuto ORDER BY time"))]
    assert left == ["09:00", "09:04"]
