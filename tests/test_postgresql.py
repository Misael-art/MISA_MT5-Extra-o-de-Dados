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


def test_trajectory_preserves_double_observations_and_legacy_real_ledger(pg):
    from mt5_extracao.strategies.backtester import BacktestConfig, SymbolSpec, run_backtest
    from mt5_extracao.trade_analytics import ledger
    from mt5_extracao.trade_analytics.trajectories import store
    from mt5_extracao.trade_analytics.trajectories.clocks import Clock
    from tests.test_backtester import Manual, bars
    data = bars([[100, 101, 99, 100], [100.123456789, 101, 99, 100.4],
                 [101.1, 104, 100, 102.456789123]], start="2026-01-05 10:00", freq="min")
    result = run_backtest(data, Manual(long=[0], stop={0: 90}),
                          SymbolSpec(point=1, tick_size=1, tick_value=1),
                          BacktestConfig(initial_equity=10000), observation_timeframe="M1")
    ledger.save_trades(pg, "precise", result.trades, symbol="X", timeframe="M1", source="python")
    before = ledger.load(pg, run_id="precise")
    assert before.iloc[0].entry_price != result.trades[0].entry_price  # REAL legado é float32
    identity = store.save(pg, "precise", result.trajectory, source="python", clock=Clock("utc"))
    loaded = store.load(pg, "precise")
    assert loaded["trajectory_id"] == identity
    assert loaded["observations"]["records"][-1]["net_pnl"] == result.trades[0].pnl
    assert loaded["ledger_precision"] == "postgresql_storage_projection"
    assert store.save(pg, "precise", result.trajectory, source="python", clock=Clock("utc")) == identity
    pd.testing.assert_frame_equal(before, ledger.load(pg, run_id="precise"))


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
    # T20.4: capturar e reler uma revisão não segue as correções da tabela atual.
    from mt5_extracao.analytics import datasets
    snapshot = datasets.capture(pg, "WIN$N", "M1")
    with pg.engine.begin() as conn:
        conn.execute(text("UPDATE win_n_1_minuto SET close=9"))
    assert datasets.load(pg, snapshot.manifest_id).bars.close.iloc[0] == 1
    assert datasets.capture(pg, "WIN$N", "M1").manifest_id != snapshot.manifest_id
    # O isolamento declarado precisa existir no backend, não só numa configuração Python.
    with pg.consistent_read() as conn:
        assert conn.exec_driver_sql("SHOW transaction_isolation").scalar() == "repeatable read"
        assert conn.exec_driver_sql("SHOW transaction_read_only").scalar() == "on"
        assert pg.load_ohlcv("WIN$N", "1 minuto", connection=conn).close.iloc[0] == 9
        # Outra conexão publica uma correção enquanto a imagem anterior permanece aberta.
        with pg.engine.begin() as writer:
            writer.execute(text("UPDATE win_n_1_minuto SET close=10"))
        assert pg.load_ohlcv("WIN$N", "1 minuto", connection=conn).close.iloc[0] == 9
    assert pg.load_ohlcv("WIN$N", "1 minuto").close.iloc[0] == 10
    # T20.5: fonte/vigência separadas do cache atual, também no PostgreSQL real.
    from types import SimpleNamespace
    from mt5_extracao.analytics import instruments
    from mt5_extracao.strategies.specs import spec_from_symbol_info
    payload = spec_from_symbol_info(SimpleNamespace(point=2, trade_tick_size=2, trade_tick_value=1))
    revision = instruments.record(pg, "WIN$N", "specification", payload, source="pg_fixture",
                                  valid_from="2020-01-01T00:00:00Z", known_at="2019-12-31T00:00:00Z")
    historical = datasets.capture(pg, "WIN$N", "M1")
    assert historical.spec.point == 2
    assert historical.spec.provenance["specification_revisions"] == [revision]
    assert historical.spec.provenance["specification_status"] == "known"
    # T20.6: famílias cruas e prova de origem são capturadas juntas, sem promover completude.
    from mt5_extracao.analytics import data_eligibility
    start = instruments.broker_utc(pg.load_ohlcv("WIN$N", "1 minuto").time.iloc[0], pg.broker_utc_offset)
    epoch = int(start.value // 1_000_000)
    raw = pd.DataFrame(dict(time_msc=[epoch + 1000, epoch + 2000], bid=[100., 100.], ask=[101., 101.],
                            last=[100., 101.], volume=[1, 1], volume_real=[1., 1.], flags=[56, 88]))
    table = pg.get_table_name_for_symbol("WIN$N", "ticks")
    pg.save_ticks(table, raw)
    proof = data_eligibility.acquisition(raw, "ticks", origin="observed", symbol="WIN$N",
                                       received_at="2024-01-03T00:00:00Z")
    pg.record_block(table, start.to_pydatetime(), (start + pd.Timedelta(minutes=1)).to_pydatetime(), 2, "ok", "fixture", quality=proof)
    qualified = datasets.capture(pg, "WIN$N", "M1", families=["ticks"])
    assert qualified.quality["capabilities"]["ticks"]["observed_rows"] == 2
    assert qualified.quality["capabilities"]["complete_real_ticks"]["coverage_fraction"] is None


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


def test_batch_checkpoint_publication_and_resume(pg):
    from mt5_extracao.robots import backtest, budget, catalog, store
    from mt5_extracao.timeframes import Timeframe
    from tests.market_data import trend
    for symbol in ("QA", "QB"):
        assert pg.save_ohlcv_data(symbol, "1 hora", trend(350, freq="h"))
    robots = [r for r in catalog.python_robots() if r.key == "python:ema_atr"]
    first = budget.Budget(pg, limits=budget.Limits(max_jobs=1))
    results = backtest.run_batch(pg, None, robots, ["QA", "QB"], [Timeframe.H1], budget=first)
    assert results[0]["run"] and results[1]["status"] == "interrupted"
    again = budget.Budget(pg, batch_id=first.batch_id, limits=budget.Limits(max_jobs=2))
    results = backtest.run_batch(pg, None, robots, ["QA", "QB"], [Timeframe.H1], budget=again, batch_id=first.batch_id)
    assert results[0]["skipped"] and results[1]["run"]
    assert len(store.load_runs(pg)) == 2
    saved = budget.checkpoint(pg, first.batch_id)
    assert saved["status"] == "completed" and all(x["status"] == "completed" for x in saved["jobs"])


def test_feature_cache_identity_and_additive_legacy_schema(pg, monkeypatch):
    """T20.25: precisão SQL, hit verificado, corrupção e migração aditiva reais."""
    from mt5_extracao.trade_analytics import cache, enrich, ledger
    bars = random_walk(90, freq="5min", start="2025-01-06 09:00")
    pg.save_ohlcv_data("CACHE", "5 minutos", bars)
    ledger.save_trades(pg, "cache-run", [dict(direction=1, entry_time=bars.time.iloc[40], exit_time=bars.time.iloc[45],
        entry_price=100., exit_price=101., stop_price=99., target_price=102., volume=1., pnl=1.,
        mae=-1., mfe=2., bars=5)], robot_key="python:cache", symbol="CACHE", timeframe="M5")
    with pg.engine.begin() as conn:
        conn.execute(text("CREATE TABLE _trade_features_runs (run_id TEXT PRIMARY KEY, signature TEXT NOT NULL, feature_version INTEGER, computed_at TEXT)"))
        conn.execute(text("CREATE TABLE _trade_features (run_id TEXT, seq INTEGER, legacy_note TEXT)"))
        conn.execute(text("INSERT INTO _trade_features VALUES ('foreign', 0, 'preservar')"))
    trades = ledger.load(pg)
    expected = cache.features(pg, None, trades)
    calls = []
    real = enrich.enrich
    monkeypatch.setattr(enrich, "enrich", lambda *args: calls.append(1) or real(*args))
    actual = cache.features(pg, None, trades)
    assert calls == []
    pd.testing.assert_frame_equal(actual, expected, check_dtype=False)
    with pg.engine.begin() as conn:
        assert conn.execute(text("SELECT legacy_note FROM _trade_features WHERE run_id='foreign'")).scalar() == 'preservar'
        assert len(conn.execute(text("SELECT signature FROM _trade_features_runs WHERE run_id='cache-run'")).scalar()) == 64
        conn.execute(text("UPDATE _trade_features SET atr=123456 WHERE run_id='cache-run'"))
    repaired = cache.features(pg, None, trades)
    assert calls == [1]
    pd.testing.assert_frame_equal(repaired, expected, check_dtype=False)
    cache.features(pg, None, trades)
    assert calls == [1]
