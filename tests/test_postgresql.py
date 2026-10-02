"""
T5.2: DatabaseManager com PostgreSQL.

Roda contra um servidor real quando MT5X_TEST_PG_URL está definido
(ex.: postgresql+psycopg://postgres:teste@localhost:5432/mt5x_test). No CI do Linux há um
serviço postgres; sem servidor (ex.: Windows), estes testes são pulados.
"""
import configparser
import datetime as dt
import os

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

import mt5_extracao.historical_extractor as he_module
import mt5_extracao.mt5_connector as connector_module
from mt5_extracao import services
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.historical_extractor import HistoricalExtractor
from mt5_extracao.indicator_calculator import IndicatorCalculator
from tests.market_data import random_walk

PG_URL = os.environ.get("MT5X_TEST_PG_URL")
pytestmark = pytest.mark.skipif(not PG_URL, reason="defina MT5X_TEST_PG_URL para testar com PostgreSQL")


@pytest.fixture
def pg():
    pytest.importorskip("psycopg")
    engine = create_engine(PG_URL)
    with engine.begin() as conn:   # banco limpo a cada teste
        conn.execute(text("DROP SCHEMA public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
    engine.dispose()
    db = DatabaseManager(db_type="postgresql", db_path=PG_URL)
    assert db.is_connected()
    yield db
    db.engine.dispose()


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(he_module.time, "sleep", lambda s: None)
    monkeypatch.setattr(connector_module.time, "sleep", lambda s: None)


def test_save_upsert_and_read_back(pg):
    df = random_walk(300, freq="min", start="2024-01-02 09:00")
    assert pg.save_ohlcv_data("WIN$N", "1 minuto", df)
    changed = df.copy()
    changed.loc[:9, "close"] = 1.0
    assert pg.save_ohlcv_data("WIN$N", "1 minuto", changed.iloc[:50])   # upsert: atualiza, não duplica
    assert pg.get_all_tables() == ["win_n_1_minuto"]
    out = pg.load_ohlcv("WIN$N", "1 minuto")
    assert len(out) == 300 and out["close"].iloc[0] == 1.0
    assert pd.api.types.is_datetime64_any_dtype(out["time"])
    part = pg.load_ohlcv("WIN$N", "1 minuto", start=df["time"][10], end=df["time"][19])
    assert len(part) == 10
    assert pg.get_last_timestamp("win_n_1_minuto") == dt.datetime(2024, 1, 2, 13, 59)
    before = pg.get_rows_before("win_n_1_minuto", df["time"][100], 20)
    assert len(before) == 20 and before["time"].iloc[-1] == df["time"][99]


def test_control_tables_and_helpers(pg):
    pg.save_ohlcv_data("WIN$N", "1 minuto", random_walk(120, freq="min", start="2024-01-02 09:00"))
    a, b = dt.datetime(2024, 1, 2), dt.datetime(2024, 1, 3)
    pg.record_block("win_n_1_minuto", a, b, 120, "ok", "mt5", quality={"rows": 120})
    pg.record_block("win_n_1_minuto", a, b, 120, "ok", "mt5", quality={"rows": 121})   # upsert
    assert pg.completed_blocks("win_n_1_minuto") == {(a, b)}
    reports = pg.quality_reports("win_n_1_minuto")
    assert reports[0][0].startswith("2024-01-02") and reports[0][1]["rows"] == 121
    summary = pg.get_symbol_data_summary("win_n_1_minuto")
    assert summary["total_registros"] == 120 and summary["intervalo_medio_minutos"] == pytest.approx(1.0)
    assert len(pg.get_recent_data("win_n_1_minuto", 5)) == 5
    assert int(pg.execute_query('SELECT COUNT(*) AS n FROM "win_n_1_minuto"')["n"].iloc[0]) == 120
    assert pg.optimize_database() is True
    pg.delete_data_periodo("win_n_1_minuto", dt.datetime(2024, 1, 2, 9), dt.datetime(2024, 1, 2, 9, 9))
    assert pg.get_table_summary("win_n_1_minuto")["total_registros"] == 110


def test_table_name_collision_specs_ticks_book(pg, connector):
    assert pg.get_table_name_for_symbol("WIN$N", "1 minuto") == "win_n_1_minuto"
    assert pg.get_table_name_for_symbol("WIN_N", "1 minuto").startswith("win_n_1_minuto_")
    pg.save_symbol_spec("WIN$N", {"point": 5, "digits": 0, "tick_size": 5, "tick_value": 0.2, "volume_min": 1,
                                  "volume_step": 1, "volume_max": 100, "contract_size": 0.2,
                                  "currency_profit": "BRL", "spread": 1})
    assert pg.get_symbol_spec("WIN$N")["tick_value"] == 0.2
    from mt5_extracao.tick_extractor import TickExtractor
    r = TickExtractor(connector, pg).extract("WIN$N", dt.datetime(2024, 1, 2), dt.datetime(2024, 1, 3))
    assert r["rows"] == 9 * 60 * 3 and r["failed"] == 0
    from mt5_extracao.book_collector import BookCollector
    t = [1_704_196_800.0]
    summary = BookCollector(connector, pg, clock=lambda: t[0], sleep=lambda s: t.__setitem__(0, t[0] + s)) \
        .run(["WIN$N"], max_snapshots=3)
    assert summary["WIN$N"]["rows"] == 30


def test_extraction_with_resume(pg, connector, fake_mt5):
    import calendar
    epoch = lambda d: calendar.timegm(d.timetuple())
    fake_mt5.fail_ranges = [(epoch(dt.datetime(2024, 1, 3, 9)), epoch(dt.datetime(2024, 1, 3, 18)))]
    ext = HistoricalExtractor(connector, pg, IndicatorCalculator(), chunk_config={"m1": 1, "m5_m15": 1, "default": 1})
    run = lambda: ext._process_symbol("WIN$N", 1, "1 minuto", dt.datetime(2024, 1, 2), dt.datetime(2024, 1, 5),
                                      include_indicators=True, overwrite=False)
    assert run() is False
    fake_mt5.fail_ranges = []
    assert run() is True
    with pg.engine.connect() as c:
        assert c.execute(text("SELECT COUNT(*) FROM win_n_1_minuto")).scalar() == 3 * 540


def test_utc_time_basis(pg):
    utc = DatabaseManager(db_type="postgresql", db_path=PG_URL, time_basis="utc", broker_utc_offset=-3)
    utc.save_ohlcv_data("WIN$N", "1 hora", random_walk(5, freq="h", start="2024-01-02 09:00"))
    with utc.engine.connect() as c:
        first = c.execute(text("SELECT MIN(time) FROM win_n_1_hora")).scalar()
    assert first == dt.datetime(2024, 1, 2, 12, 0)
    assert utc.load_ohlcv("WIN$N", "1 hora")["time"].iloc[0] == pd.Timestamp("2024-01-02 09:00")
    utc.engine.dispose()


def test_strategy_validation_storage(pg):
    from mt5_extracao.strategies import validation as v
    from mt5_extracao.strategies.backtester import BacktestConfig, SymbolSpec
    verdict = v.validate(random_walk(1500, seed=2), "donchian", SymbolSpec(point=0.01, tick_value=0.01,
                         volume_min=0.01, volume_step=0.01), BacktestConfig(), v.Criteria(mc_runs=50))
    v.save_validation(pg, "WIN$N", "D1", verdict)
    v.save_validation(pg, "WIN$N", "D1", verdict)
    assert len(v.load_validations(pg, ["WIN$N"], "D1")) == 1


def test_services_builds_url_with_password_from_env(monkeypatch):
    from sqlalchemy.engine import make_url
    real = make_url(PG_URL)
    monkeypatch.setenv("DB_PASSWORD", real.password or "")
    cfg = configparser.ConfigParser()
    no_password = real.set(password=None).set(drivername="postgresql").render_as_string(hide_password=False)
    cfg.read_string(f"[DATABASE]\ntype = postgresql\nurl = {no_password}\n")
    db = services.create_db_manager(cfg)
    assert db.is_connected() and db.dialect == "postgresql"
    assert "***" in db._safe_location() or real.password is None
    db.engine.dispose()
