import numpy as np
import pandas as pd
from sqlalchemy import text

from mt5_extracao.database_manager import DatabaseManager


def _df(close1=2.0, extra=False):
    times = ['2024-01-01 10:00', '2024-01-01 10:01'] + (['2024-01-01 10:02'] if extra else [])
    n = len(times)
    return pd.DataFrame({'time': pd.to_datetime(times),
                         'open': [1.0, 2.0, 3.0][:n], 'high': [1.0, 2.0, 3.0][:n],
                         'low': [1.0, 2.0, 3.0][:n], 'close': [1.0, close1, 3.0][:n],
                         # MT5 devolve volumes como uint64
                         'tick_volume': np.array([1, 2, 3][:n], dtype='uint64'),
                         'spread': np.array([1, 1, 1][:n], dtype='int32'),
                         'real_volume': np.array([5, 6, 7][:n], dtype='uint64')})


def _closes(db):
    with db.engine.connect() as c:
        return [r[0] for r in c.execute(text('SELECT close FROM win_n_1_minuto ORDER BY time')).fetchall()]


def test_upsert(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / 't.db'))
    assert db.save_ohlcv_data('WIN$N', '1 minuto', _df())
    assert db.save_ohlcv_data('WIN$N', '1 minuto', _df(close1=99.0))
    assert _closes(db) == [1.0, 99.0]


def test_upsert_mixed_new_and_existing_rows(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / 't.db'))
    assert db.save_ohlcv_data('WIN$N', '1 minuto', _df())
    assert db.save_ohlcv_data('WIN$N', '1 minuto', _df(extra=True))
    assert _closes(db) == [1.0, 2.0, 3.0]


def test_upsert_keeps_time_format_of_existing_bases(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / 't.db'))
    db.save_ohlcv_data('WIN$N', '1 minuto', _df())
    with db.engine.connect() as c:
        first = c.execute(text('SELECT time FROM win_n_1_minuto ORDER BY time')).fetchone()[0]
    assert first == '2024-01-01 10:00:00.000000'  # mesmo formato que o to_sql gravava


def test_save_data_upserts_when_table_has_time_pk(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / 't.db'))
    db.save_ohlcv_data('WIN$N', '1 minuto', _df())
    assert db.save_data(_df(close1=77.0), 'win_n_1_minuto', symbol='WIN$N')
    assert _closes(db) == [1.0, 77.0]


def test_upsert_many_rows_uses_chunks(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / 't.db'))
    times = pd.date_range('2024-01-01 09:00', periods=1000, freq='min')
    df = pd.DataFrame({'time': times, 'open': 1.0, 'high': 1.0, 'low': 1.0, 'close': 1.0,
                       'tick_volume': 1, 'spread': 1, 'real_volume': 1})
    assert db.save_ohlcv_data('WIN$N', '1 minuto', df)
    assert db.save_ohlcv_data('WIN$N', '1 minuto', df)
    with db.engine.connect() as c:
        assert c.execute(text('SELECT COUNT(*) FROM win_n_1_minuto')).scalar() == 1000
