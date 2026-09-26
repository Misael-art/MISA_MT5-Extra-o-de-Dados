import datetime as dt

import pandas as pd
from sqlalchemy import text

from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.historical_extractor import HistoricalExtractor
from mt5_extracao.indicator_calculator import IndicatorCalculator

M1 = 1
H1 = 16385


def _extractor(connector, tmp_path, chunk_days=1):
    db = DatabaseManager(db_path=str(tmp_path / "t.db"))
    ext = HistoricalExtractor(connector, db, IndicatorCalculator(),
                              chunk_config={"m1": chunk_days, "m5_m15": chunk_days, "default": chunk_days})
    return ext, db


def _rows(db, table):
    with db.engine.connect() as c:
        return c.execute(text(f'SELECT time, spread FROM "{table}" ORDER BY time')).fetchall()


def test_connector_empty_period_returns_empty_dataframe(connector, fake_mt5):
    # Sábado: resposta válida, sem barras -> DataFrame vazio (não None)
    df = connector.get_historical_data("WIN$N", M1, start_dt=dt.datetime(2024, 1, 6, 9),
                                       end_dt=dt.datetime(2024, 1, 6, 18))
    assert df is not None and df.empty
    assert list(df.columns) == ["time", "open", "high", "low", "close", "tick_volume", "spread", "real_volume"]


def test_m1_uses_copy_rates_range_per_block(connector, fake_mt5, tmp_path):
    ext, db = _extractor(connector, tmp_path)
    ok = ext._process_symbol("WIN$N", M1, "1 minuto", dt.datetime(2024, 1, 2), dt.datetime(2024, 1, 4),
                             include_indicators=False, overwrite=False)
    assert ok
    assert fake_mt5.calls_named("copy_rates_from") == []
    ranges = fake_mt5.calls_named("copy_rates_range")
    assert len(ranges) == 2  # 2 blocos de 1 dia
    rows = _rows(db, "win_n_1_minuto")
    assert len(rows) == 2 * 9 * 60  # 2 pregões de 9h
    assert rows[0][0].startswith("2024-01-02 09:00:00")


def test_spread_is_preserved_with_indicators(connector, fake_mt5, tmp_path):
    ext, db = _extractor(connector, tmp_path)
    assert ext._process_symbol("WIN$N", M1, "1 minuto", dt.datetime(2024, 1, 2, 9), dt.datetime(2024, 1, 2, 12),
                               include_indicators=True, overwrite=False)
    spreads = {r[1] for r in _rows(db, "win_n_1_minuto")}
    # O FakeMT5 gera spread 1..3 por barra; symbol_info.spread (5) não pode sobrescrever
    assert spreads == {1, 2, 3}


def test_weekend_block_is_not_a_failure(connector, fake_mt5, tmp_path):
    ext, db = _extractor(connector, tmp_path)
    # 2024-01-06/07 é fim de semana; o bloco vazio não pode derrubar o símbolo
    assert ext._process_symbol("WIN$N", H1, "1 hora", dt.datetime(2024, 1, 5), dt.datetime(2024, 1, 9),
                               include_indicators=False, overwrite=False)
    days = {r[0][:10] for r in _rows(db, "win_n_1_hora")}
    assert days == {"2024-01-05", "2024-01-08"}  # end_date 09/01 00:00 exclui o pregão do dia 09


def test_reextraction_does_not_fail(connector, fake_mt5, tmp_path):
    ext, db = _extractor(connector, tmp_path)
    args = ("WIN$N", M1, "1 minuto", dt.datetime(2024, 1, 2), dt.datetime(2024, 1, 3))
    assert ext._process_symbol(*args, include_indicators=False, overwrite=False)
    assert ext._process_symbol(*args, include_indicators=False, overwrite=False)
    assert len(_rows(db, "win_n_1_minuto")) == 9 * 60
