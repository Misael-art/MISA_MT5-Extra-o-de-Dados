"""T16.5: cache do enriquecimento e desempenho das agregações."""
import configparser
import time

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import inspect

from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.trade_analytics import aggregate, cache, enrich, ledger
from mt5_extracao.trade_analytics import calendar as cal


@pytest.fixture
def db(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "base.db"))
    yield db
    db.close()


def _trades(n, start="2025-01-06 09:05", seed=0):
    rng = np.random.default_rng(seed)
    entries = pd.date_range(start, periods=n, freq="37min")
    return [{"direction": 1, "entry_time": e, "exit_time": e + pd.Timedelta(minutes=20), "entry_price": 100.0,
             "exit_price": 101.0, "volume": 1.0, "pnl": float(rng.normal(1, 10))} for e in entries]


@pytest.fixture
def counter(monkeypatch):
    calls = []
    real = enrich.enrich
    monkeypatch.setattr(enrich, "enrich", lambda db, cfg, t: calls.append(len(t)) or real(db, cfg, t))
    return calls


def test_second_read_comes_from_the_cache(db, counter):
    ledger.save_trades(db, "a", _trades(50), robot_key="r", symbol="WIN$N", timeframe="M5")
    ledger.save_trades(db, "b", _trades(30, seed=1), robot_key="r", symbol="WDO$N", timeframe="M5")
    first = aggregate.frame(db, None, robot_key="r")
    second = aggregate.frame(db, None, robot_key="r")
    assert counter == [50, 30]                        # só a primeira leitura calculou
    cols = ["run_id", "seq", "day_of_week", "time_bucket", "win_flag"]
    pd.testing.assert_frame_equal(first[cols].sort_values(["run_id", "seq"]).reset_index(drop=True),
                                  second[cols].sort_values(["run_id", "seq"]).reset_index(drop=True),
                                  check_dtype=False)


def test_changes_invalidate_only_what_changed(db, counter, tmp_path):
    ledger.save_trades(db, "a", _trades(20), robot_key="r", symbol="WIN$N", timeframe="M5")
    ledger.save_trades(db, "b", _trades(10, seed=1), robot_key="r", symbol="WIN$N", timeframe="M5")
    aggregate.frame(db, None)
    ledger.save_trades(db, "b", _trades(12, seed=2), robot_key="r", symbol="WIN$N", timeframe="M5")  # run regravado
    aggregate.frame(db, None)
    assert counter == [20, 10, 12]
    cfg = configparser.ConfigParser()
    cfg["TRADE_ANALYTICS"] = {"time_bucket_min": "15"}                 # configuração que muda o cálculo
    lab = aggregate.frame(db, cfg)
    assert counter[-2:] == [20, 12] and set(lab["time_bucket"].str[-2:]) <= {"00", "15", "30", "45"}
    path = tmp_path / "e.csv"
    path.write_text("time,symbol,impact\n2025-01-06 09:10,WIN$N,alto\n", encoding="utf-8")
    cal.import_source(db, None, "csv", path)                           # calendário novo
    aggregate.frame(db, cfg)
    assert len(counter) == 7
    cache.invalidate(db, "a")
    aggregate.frame(db, cfg)
    assert counter[-1] == 20


def test_feature_cache_adds_new_t16_3b_columns_to_existing_table(db):
    ledger.save_trades(db, "legacy", _trades(1), robot_key="r", symbol="WIN$N", timeframe="M5")
    trades = ledger.load(db)
    old_schema = enrich.enrich(db, None, trades).drop(columns=["volume_source", "mae_mfe_resolution"])
    old_schema.to_sql(cache.TABLE, db.engine, if_exists="append", index=False)

    result = cache.features(db, None, trades)

    columns = {column["name"] for column in inspect(db.engine).get_columns(cache.TABLE)}
    assert {"volume_source", "mae_mfe_resolution"} <= columns
    assert {"volume_source", "mae_mfe_resolution"} <= set(result.columns)
    assert result["feature_version"].iloc[0] == 2


def test_aggregation_speed_on_50k_cached_trades(db):
    trades = _trades(50_000)
    ledger.save_trades(db, "big", trades, robot_key="r", symbol="WIN$N", timeframe="M5")
    lab = aggregate.frame(db, None)                    # primeira vez: calcula e guarda
    started = time.perf_counter()
    aggregate.crosstab(lab, "time_bucket", "day_of_week")
    aggregate.by_dimension(lab, ["day_of_week"], ci=False)
    elapsed = time.perf_counter() - started
    assert elapsed < 1.5, f"agregar 50 mil operações levou {elapsed:.2f}s (meta da tela: < 0,3 s numa máquina comum)"
    started = time.perf_counter()
    again = aggregate.frame(db, None)                  # do cache
    assert len(again) == 50_000 and time.perf_counter() - started < 15
