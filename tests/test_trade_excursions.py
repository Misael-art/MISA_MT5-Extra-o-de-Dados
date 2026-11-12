"""B1/B2: excursões nas lentes, preços ask em vendas e ausência explícita."""
import pandas as pd
import pytest
from sqlalchemy import text

from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.trade_analytics import aggregate, enrich, ledger


@pytest.fixture
def db(tmp_path):
    database = DatabaseManager(db_path=str(tmp_path / "qa.sqlite"))
    yield database
    database.close()


def source(db, *, spec=True):
    bars = pd.DataFrame({"time": pd.date_range("2026-01-05 09:00", periods=3, freq="5min"),
                         "open": [100., 100., 100.], "high": [100., 103., 102.],
                         "low": [100., 97., 98.], "close": [100., 100., 100.],
                         "spread": [1., 2., 4.], "tick_volume": 10, "real_volume": 0})
    db.save_ohlcv_data("QA", "5 minutos", bars)
    if spec:
        db.save_symbol_spec("QA", {"point": .5, "digits": 1, "tick_size": .5, "tick_value": 1.,
                                    "volume_min": 1., "volume_max": 10., "volume_step": 1.,
                                    "contract_size": 1., "currency_profit": "USD", "spread": 1.})
    return bars


def trade(bars, direction=1, **extra):
    return dict(direction=direction, entry_time=bars.time.iloc[1], exit_time=bars.time.iloc[2],
                entry_price=100., exit_price=100., pnl=0., **extra)


def test_lenses_use_computed_excursions_when_ledger_is_empty_without_rewriting_it(db):
    bars = source(db)
    ledger.save_trades(db, "r", [trade(bars), trade(bars, mae=9., mfe=8.)],
                       robot_key="python:qa", symbol="QA", timeframe="M5")
    lab = aggregate.frame(db, None)
    assert lab.mae.tolist() == [3., 9.]
    assert lab.mfe.tolist() == [3., 8.]
    assert lab.excursion_basis.tolist() == ["bid_bar_envelope", "ledger"]
    saved = ledger.load(db)
    assert pd.isna(saved.mae.iloc[0]) and saved.mae.iloc[1] == 9.
    pd.testing.assert_series_equal(aggregate.frame(db, None).mae, lab.mae)


def test_short_excursions_use_ask_on_each_bar(db):
    bars = source(db)
    ledger.save_trades(db, "r", [trade(bars, -1)], symbol="QA", timeframe="M5")
    result = enrich.enrich(db, None, ledger.load(db)).iloc[0]
    # Ask high = max(103 + 2*.5, 102 + 4*.5); ask low = min(97 + 2*.5, 98 + 4*.5).
    assert result.mae == pytest.approx(4.)
    assert result.mfe == pytest.approx(2.)
    assert result.excursion_basis == "ask_bar_envelope"


def test_short_without_point_preserves_missing_outcome(db):
    bars = source(db, spec=False)
    ledger.save_trades(db, "r", [trade(bars, -1)], symbol="QA", timeframe="M5")
    result = enrich.enrich(db, None, ledger.load(db)).iloc[0]
    assert pd.isna(result.mae) and pd.isna(result.mfe)
    assert result.excursion_basis == "ask_unavailable"


def test_excursions_do_not_require_a_prior_bar_for_entry_context(db):
    bars = source(db)
    record = trade(bars)
    record["entry_time"] = bars.time.iloc[0]
    ledger.save_trades(db, "r", [record], symbol="QA", timeframe="M5")
    result = enrich.enrich(db, None, ledger.load(db)).iloc[0]
    assert result.mae == result.mfe == 3.
    assert pd.isna(result.atr)


@pytest.mark.parametrize("point", [None, 0., -1., float("nan"), float("inf")])
def test_invalid_point_never_creates_an_ask_quote(db, monkeypatch, point):
    bars = source(db)
    raw = db.get_symbol_spec("QA")
    raw["point"] = point
    monkeypatch.setattr(db, "get_symbol_spec", lambda *a, **k: raw)
    ledger.save_trades(db, "r", [trade(bars, -1)], symbol="QA", timeframe="M5")
    result = enrich.enrich(db, None, ledger.load(db)).iloc[0]
    assert pd.isna(result.mae) and result.excursion_basis == "ask_unavailable"


def test_short_uses_calibrated_spread_floor_from_the_existing_execution_rule(db):
    bars = source(db)
    db.save_spread_profile("QA", {"hours": [6.] * 24, "open": [6.] * 24})
    ledger.save_trades(db, "r", [trade(bars, -1)], symbol="QA", timeframe="M5")
    result = enrich.enrich(db, None, ledger.load(db)).iloc[0]
    assert result.mae == pytest.approx(6.) and result.mfe == pytest.approx(0.)


def test_old_cache_gains_basis_and_recomputes_without_changing_the_ledger(db):
    bars = source(db)
    ledger.save_trades(db, "r", [trade(bars, -1)], symbol="QA", timeframe="M5")
    aggregate.frame(db, None)
    # Reconstitui somente nesta base descartável o schema/cache anterior à versão 2.
    with db.engine.begin() as connection:
        connection.execute(text("ALTER TABLE _trade_features DROP COLUMN excursion_basis"))
        connection.execute(text("UPDATE _trade_features SET mae=999, mfe=999, feature_version=1"))
        connection.execute(text("UPDATE _trade_features_runs SET signature='legacy', feature_version=1"))
    result = aggregate.frame(db, None).iloc[0]
    assert result.mae == 4. and result.mfe == 2.
    assert result.excursion_basis == "ask_bar_envelope" and result.feature_version == 2
    assert pd.isna(ledger.load(db).mae.iloc[0])
