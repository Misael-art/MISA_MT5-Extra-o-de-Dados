"""BD04: ingestão idempotente, revisão preservada, falha sem sucesso falso e ausência sem zero."""
from types import SimpleNamespace

import pandas as pd
from sqlalchemy import text

from mt5_extracao.analytics import datasets, resampling
from mt5_extracao.database_manager import DatabaseManager
from tests.test_analytics_calendar import m1_bars

SYMBOL = "EURUSD"
INFO = SimpleNamespace(name=SYMBOL)


def _db(tmp_path):
    tmp_path.mkdir(parents=True, exist_ok=True)
    return DatabaseManager(db_type="sqlite", db_path=str(tmp_path / "bd04.sqlite"), broker_utc_offset=-3)


def _raw_count(db):
    table = db.get_table_name_for_symbol(SYMBOL, "1 minuto")
    with db.engine.connect() as conn:
        return conn.execute(text(f'SELECT COUNT(*) FROM "{table}"')).scalar_one()


def _derived(db):
    with db.engine.connect() as conn:
        return pd.read_sql(text("SELECT bar_start_utc, revision, close FROM _bars_derived WHERE tf='M3' "
                                "ORDER BY bar_start_utc, revision"), conn)


def test_repeat_correction_and_snapshot_preserve_history(tmp_path):
    db = _db(tmp_path)
    bars = m1_bars(pd.date_range("2026-09-14 09:00", periods=6, freq="min"))
    assert db.save_ohlcv_data(SYMBOL, "1 minuto", bars)
    assert db.save_ohlcv_data(SYMBOL, "1 minuto", bars)
    assert _raw_count(db) == 6
    resampling.resample_symbol(db, SYMBOL, ["M3"], symbol_info=INFO)
    second = resampling.resample_symbol(db, SYMBOL, ["M3"], symbol_info=INFO)[0]
    assert (second["inserted"], second["corrected"]) == (0, 0)
    before = datasets.capture(db, SYMBOL, "M1")

    corrected = bars.copy()
    corrected.loc[2, "close"] = 250.0
    corrected.loc[2, "high"] = 251.0
    assert db.save_ohlcv_data(SYMBOL, "1 minuto", corrected.iloc[[2]])
    assert _raw_count(db) == 6
    summary = resampling.resample_symbol(db, SYMBOL, ["M3"], symbol_info=INFO)[0]
    assert summary["corrected"] == 1

    derived = _derived(db)
    first_bar = derived[derived.bar_start_utc == derived.bar_start_utc.min()]
    assert first_bar["revision"].tolist() == [1, 2]
    assert first_bar["close"].tolist() == [102.0, 250.0]  # revisão 1 preservada
    assert datasets.load(db, before.manifest_id).bars["close"].iloc[2] == 102.0  # snapshot imutável


def test_failed_save_reports_failure_and_writes_nothing(tmp_path):
    db = _db(tmp_path)
    bad = m1_bars(pd.date_range("2026-09-14 09:00", periods=3, freq="min")).drop(columns=["time"])
    assert db.save_ohlcv_data(SYMBOL, "1 minuto", bad) is False
    table = db.get_table_name_for_symbol(SYMBOL, "1 minuto")
    with db.engine.connect() as conn:
        exists = conn.execute(text("SELECT name FROM sqlite_master WHERE name=:t"), {"t": table}).first()
    assert exists is None or _raw_count(db) == 0


def test_unknown_real_volume_stays_null_not_zero(tmp_path):
    db = _db(tmp_path)
    bars = m1_bars(pd.date_range("2026-09-14 09:00", periods=3, freq="min"), real_volume=[0, None, 7])
    assert db.save_ohlcv_data(SYMBOL, "1 minuto", bars)
    stored = db.load_ohlcv(SYMBOL, "1 minuto")["real_volume"].tolist()
    assert stored[0] == 0 and pd.isna(stored[1]) and stored[2] == 7


def test_out_of_order_ingestion_equals_ordered(tmp_path):
    bars = m1_bars(pd.date_range("2026-09-14 09:00", periods=6, freq="min"))
    ordered, shuffled = _db(tmp_path / "a"), _db(tmp_path / "b")
    assert ordered.save_ohlcv_data(SYMBOL, "1 minuto", bars)
    assert shuffled.save_ohlcv_data(SYMBOL, "1 minuto", bars.iloc[[3, 0, 5, 1, 4, 2]])
    pd.testing.assert_frame_equal(ordered.load_ohlcv(SYMBOL, "1 minuto"), shuffled.load_ohlcv(SYMBOL, "1 minuto"))
    for db in (ordered, shuffled):
        resampling.resample_symbol(db, SYMBOL, ["M3"], symbol_info=INFO)
    pd.testing.assert_frame_equal(_derived(ordered), _derived(shuffled))
