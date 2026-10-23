"""BD08a: família de retorno qualificada pela cadeia real (provedor falso → extrator → derivado → store)."""
import datetime as dt
import shutil
from types import SimpleNamespace

import pandas as pd
import pytest

import bench_reference as ref
from mt5_extracao.analytics import feature_store, indicator_materializer, resampling
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.historical_extractor import HistoricalExtractor
from mt5_extracao.indicator_calculator import IndicatorCalculator
from tests.test_extraction_resume import no_sleep  # noqa: F401  (fixture)

SYMBOL = "WIN$N"
RETURNS = ["price.return_1", "price.return_5", "price.return_20"]


def _chain(connector, path, end=dt.datetime(2024, 1, 4)):
    db = DatabaseManager(db_path=str(path), broker_utc_offset=0)
    ext = HistoricalExtractor(connector, db, IndicatorCalculator(), chunk_config={"m1": 1, "m5_m15": 1, "default": 1})
    assert ext._process_symbol(SYMBOL, 1, "1 minuto", dt.datetime(2024, 1, 2), end, include_indicators=False,
                               overwrite=False)
    resampling.resample_symbol(db, SYMBOL, ["M5"], symbol_info=SimpleNamespace(name=SYMBOL))
    return db


def _materialize(db, **kw):
    return indicator_materializer.materialize(db, SYMBOL, "M5", feature_ids=RETURNS, **kw)


def _latest(db, feature_id):
    history = feature_store.query_history(db, symbol=SYMBOL, timeframe="M5", feature_id=feature_id)
    return history.sort_values("revision").drop_duplicates("observation_time", keep="last") \
        .set_index("observation_time")["numeric_value"]


def _closes(db):
    bars = resampling.latest_bars(db, SYMBOL, "M5", closed_only=True)
    return bars.set_index("bar_start_utc")["close"]


def test_chain_values_match_independent_reference(connector, fake_mt5, tmp_path):
    db = _chain(connector, tmp_path / "chain.sqlite")
    _materialize(db)
    closes = _closes(db)
    assert len(closes) > 40
    for horizon, feature_id in zip((1, 5, 20), RETURNS):
        expected = dict(zip(closes.index.astype(str), ref.returns(closes.tolist(), horizon)))
        stored = _latest(db, feature_id)
        assert len(stored) > 0
        for when, value in stored.items():
            want = expected[str(pd.Timestamp(when))]
            assert (want is None and pd.isna(value)) or value == pytest.approx(want, rel=1e-12)


def test_appending_future_keeps_known_prefix(connector, fake_mt5, tmp_path):
    db = _chain(connector, tmp_path / "prefix.sqlite", end=dt.datetime(2024, 1, 3))
    _materialize(db)
    before = _latest(db, "price.return_5")
    _chain(connector, tmp_path / "prefix.sqlite", end=dt.datetime(2024, 1, 4))
    _materialize(db)
    after = _latest(db, "price.return_5")
    pd.testing.assert_series_equal(after.loc[before.index], before)
    assert len(after) > len(before)


def test_batch_size_does_not_change_values(connector, fake_mt5, tmp_path):
    small = _chain(connector, tmp_path / "s.sqlite")
    small.engine.dispose()
    shutil.copy(small.db_path, tmp_path / "l.sqlite")  # mesma fonte, sem extrair de novo
    large = DatabaseManager(db_path=str(tmp_path / "l.sqlite"), broker_utc_offset=0)
    _materialize(small, batch_size=7)
    _materialize(large, batch_size=5000)
    for feature_id in RETURNS:
        pd.testing.assert_series_equal(_latest(small, feature_id), _latest(large, feature_id))


def test_zero_or_missing_denominator_is_absent_not_infinite():
    closes = [100.0, 0.0, 50.0, None, 10.0]
    out = ref.returns(closes, 1)
    assert out[2] is None and out[4] is None  # denominador 0 ou ausente → ausência
    frame = pd.DataFrame({"close": closes, "high": closes, "low": closes})
    from mt5_extracao.analytics import indicator_registry
    rows = {row["bar_index"]: row for row in indicator_registry.iter_core(frame, feature_ids=["price.return_1"])}
    assert rows[2]["value"] is None and rows[2]["state"] != "VALID"
    assert rows[4]["value"] is None


def test_late_correction_revises_descendants_and_as_of_keeps_the_past(connector, fake_mt5, tmp_path):
    db = _chain(connector, tmp_path / "rev.sqlite")
    _materialize(db)
    known_before = pd.Timestamp.now(tz="UTC")
    raw = db.load_ohlcv(SYMBOL, "1 minuto")
    closing = raw[pd.to_datetime(raw["time"]).dt.minute % 5 == 4]  # última M1 de uma barra M5: muda o close
    target = closing.iloc[[40]].copy()
    target["close"] = target["close"] * 1.5
    target["high"] = target[["high", "close"]].max(axis=1)
    assert db.save_ohlcv_data(SYMBOL, "1 minuto", target, received_at=pd.Timestamp.now(tz="UTC"))
    resampling.resample_symbol(db, SYMBOL, ["M5"], symbol_info=SimpleNamespace(name=SYMBOL))
    counts = _materialize(db)
    assert counts["revised"] > 0
    history = feature_store.query_history(db, symbol=SYMBOL, timeframe="M5", feature_id="price.return_1")
    spread = history.groupby("observation_time")["numeric_value"].nunique()
    when = spread[spread > 1].index[0]  # observação cujo VALOR mudou com a correção
    old = feature_store.query_as_of(db, known_before, symbol=SYMBOL, timeframe="M5", feature_id="price.return_1")
    new = feature_store.query_as_of(db, pd.Timestamp.now(tz="UTC"), symbol=SYMBOL, timeframe="M5",
                                    feature_id="price.return_1")
    old_value = old.set_index("observation_time").loc[when, "numeric_value"]
    new_value = new.set_index("observation_time").loc[when, "numeric_value"]
    assert old_value != new_value
