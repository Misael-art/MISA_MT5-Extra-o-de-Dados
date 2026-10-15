"""T16.3: enriquecimento das operações (calendário, notícia, volume, volatilidade), sem olhar o futuro."""
import configparser

import numpy as np
import pandas as pd
import pytest

from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.trade_analytics import calendar as cal
from mt5_extracao.trade_analytics import enrich, ledger


def _bars(n=600, start="2026-01-05 09:00", freq="5min", jump_at=None):
    times = pd.date_range(start, periods=n, freq=freq)
    close = 100 + np.cumsum(np.full(n, 0.01))
    high, low = close + 0.2, close - 0.2
    if jump_at is not None:                  # armadilha: só a barra da entrada (e depois) é violenta
        high[jump_at:] += 50
        low[jump_at:] -= 50
    return pd.DataFrame({"time": times, "open": close, "high": high, "low": low, "close": close,
                         "tick_volume": np.arange(n) + 1, "spread": 1, "real_volume": 0})


@pytest.fixture
def db(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "base.db"))
    yield db
    db.close()


def _trade(entry, exit_=None, direction=1, pnl=5.0, price=100.0):
    return {"direction": direction, "entry_time": pd.Timestamp(entry), "exit_time": pd.Timestamp(exit_ or entry) +
            pd.Timedelta(minutes=0 if exit_ else 30), "entry_price": price, "exit_price": price + 1, "pnl": pnl}


def _cfg(**extra):
    cfg = configparser.ConfigParser()
    cfg["TRADE_ANALYTICS"] = {k: str(v) for k, v in extra.items()}
    return cfg


def test_calendar_columns_on_known_dates():
    cols = enrich.calendar_columns(pd.Series(pd.to_datetime(["2026-01-09 10:47", "2026-01-30 15:05"])), 30)
    assert cols["day_of_week"].tolist() == [4, 4]                     # sextas
    assert cols["week_of_month"].tolist() == [2, 5] and cols["is_month_end"].tolist() == [False, True]
    assert cols["time_bucket"].tolist() == ["10:30", "15:00"] and cols["hour"].tolist() == [10, 15]


def test_no_lookahead_in_market_features(db):
    """O salto de volatilidade começa na barra da entrada: o ATR da operação não pode vê-lo."""
    bars = _bars(jump_at=400)
    db.save_ohlcv_data("WIN$N", "5 minutos", bars)
    entry = bars["time"].iloc[400]
    ledger.save_trades(db, "r", [_trade(entry)], robot_key="a", symbol="WIN$N", timeframe="M5")
    f = enrich.enrich(db, None, ledger.load(db))
    calm = enrich.enrich(db, None, ledger.load(db).assign(entry_time=bars["time"].iloc[399]))
    assert f["atr"].iloc[0] == pytest.approx(calm["atr"].iloc[0])   # mesma barra anterior usada
    assert f["atr"].iloc[0] < 1                                       # nada dos ±50 da armadilha
    assert f["entry_volume"].iloc[0] == 0                             # real_volume zero é dado, não ausência
    assert f["volume_source"].iloc[0] == "real_volume"
    assert f["mfe"].iloc[0] > 40                                      # MAE/MFE (resultado) veem o caminho
    assert f["mae_mfe_resolution"].iloc[0] == "M5 OHLC (intrabar aproximado)"


def test_midbar_entry_uses_only_the_last_fully_closed_bar(db):
    bars = _bars(jump_at=400)
    bars["real_volume"] = bars["tick_volume"] * 10
    db.save_ohlcv_data("WIN$N", "5 minutos", bars)
    entry = bars["time"].iloc[400] + pd.Timedelta(minutes=2)
    ledger.save_trades(db, "midbar", [_trade(entry, exit_=entry + pd.Timedelta(minutes=5))],
                       robot_key="a", symbol="WIN$N", timeframe="M5")

    features = enrich.enrich(db, None, ledger.load(db))

    assert features["entry_volume"].iloc[0] == 4000             # barra 399; 400 ainda estava aberta
    assert features["volume_source"].iloc[0] == "real_volume"
    assert features["atr"].iloc[0] < 1                          # o salto da barra 400 não entrou no contexto
    assert features["mfe"].iloc[0] > 40                         # mas a barra de entrada integra o resultado aproximado
    assert features["mae_mfe_resolution"].iloc[0] == "M5 OHLC (intrabar aproximado)"


def test_mae_mfe_approximates_exit_bar_but_excludes_bars_after_exit(db):
    bars = _bars()
    bars.loc[401, "high"] += 50       # barra parcial da saída, ainda deve entrar na aproximação OHLC
    bars.loc[402, "high"] += 500      # começa depois da saída e não pode contaminar o resultado
    db.save_ohlcv_data("WIN$N", "5 minutos", bars)
    entry = bars["time"].iloc[400] + pd.Timedelta(minutes=2)
    exit_ = bars["time"].iloc[401] + pd.Timedelta(minutes=2)
    price = float(bars["close"].iloc[400])
    ledger.save_trades(db, "exit-boundary", [_trade(entry, exit_=exit_, price=price)],
                       robot_key="a", symbol="WIN$N", timeframe="M5")

    features = enrich.enrich(db, None, ledger.load(db))

    assert 49 < features["mfe"].iloc[0] < 100
    assert features["mae_mfe_resolution"].iloc[0] == "M5 OHLC (intrabar aproximado)"


def test_volume_source_is_selected_for_the_whole_series(db):
    bars = _bars()
    bars.loc[0, "real_volume"] = np.nan
    db.save_ohlcv_data("WIN$N", "5 minutos", bars)
    entry = bars["time"].iloc[400]
    ledger.save_trades(db, "ticks", [_trade(entry)], robot_key="a", symbol="WIN$N", timeframe="M5")

    features = enrich.enrich(db, None, ledger.load(db))

    assert features["volume_source"].iloc[0] == "tick_volume"
    assert features["entry_volume"].iloc[0] == 400


def test_volatility_bucket_is_point_in_time(db):
    bars = _bars(n=500)
    bars.loc[300:, ["high"]] += 5
    bars.loc[300:, ["low"]] -= 5
    db.save_ohlcv_data("WIN$N", "5 minutos", bars)
    trades = [_trade(bars["time"].iloc[250]), _trade(bars["time"].iloc[320])]
    ledger.save_trades(db, "r", trades, robot_key="a", symbol="WIN$N", timeframe="M5")
    f = enrich.enrich(db, None, ledger.load(db))
    assert f["volatility_bucket"].tolist()[1] == "alta"
    assert f["volatility_rank"].iloc[1] > f["volatility_rank"].iloc[0]


def test_events_window_and_partial_trades(db, tmp_path):
    db.save_ohlcv_data("WIN$N", "5 minutos", _bars())
    path = tmp_path / "e.csv"
    path.write_text("time,symbol,impact,name\n2026-01-05 10:20,WIN$N,alto,Copom\n", encoding="utf-8")
    cal.import_source(db, None, "csv", path)
    ledger.save_trades(db, "r", [_trade("2026-01-05 10:00"), _trade("2026-01-05 13:00"),
                                 {"exit_time": "2026-01-05 10:30", "pnl": -1.0}],
                       robot_key="a", symbol="WIN$N", timeframe="M5")
    f = enrich.enrich(db, _cfg(event_window_before_min=30, event_window_after_min=30), ledger.load(db))
    by = f.set_index("seq")
    assert by.loc[0, "event_impact"] == cal.HIGH and by.loc[0, "minutes_to_event"] == 20 and by.loc[0, "event_name"] == "Copom"
    assert by.loc[1, "event_impact"] == 0
    assert by.loc[2, "time_ref"] == "saida" and pd.isna(by.loc[2, "atr"]) and by.loc[2, "event_impact"] == cal.HIGH
    assert by.loc[0, "minutes_from_open"] == 60 and by.loc[2, "win_flag"] == 0


def test_surprise_is_withheld_until_event_and_values_are_available(db, tmp_path):
    db.save_ohlcv_data("WIN$N", "5 minutos", _bars())
    path = tmp_path / "release.csv"
    path.write_text("time,symbol,impact,name,actual,forecast,source_received_at\n"
                    "2026-01-05 10:05,WIN$N,alto,Release,12,10,2026-01-05T10:06:00-03:00\n", encoding="utf-8")
    cal.import_source(db, None, "csv", path)
    ledger.save_trades(db, "news", [_trade("2026-01-05 10:00"), _trade("2026-01-05 10:05:30"),
                                     _trade("2026-01-05 10:10")],
                       robot_key="a", symbol="WIN$N", timeframe="M5")

    cfg = _cfg(event_window_before_min=30, event_window_after_min=30)
    cfg["APP"] = {"time_basis": "broker", "broker_utc_offset": "-3"}
    features = enrich.enrich(db, cfg, ledger.load(db))

    assert pd.isna(features["event_surprise"].iloc[0])      # evento futuro para a primeira decisão
    assert pd.isna(features["event_surprise"].iloc[1])      # release ocorreu, mas os valores ainda não foram importados
    assert features["event_surprise"].iloc[2] == pytest.approx(2.0)


def test_historical_surprise_without_availability_is_not_backfilled_from_import(db, tmp_path):
    db.save_ohlcv_data("WIN$N", "5 minutos", _bars())
    path = tmp_path / "historical-release.csv"
    path.write_text("time,symbol,impact,name,actual,forecast\n"
                    "2026-01-05 10:05,WIN$N,alto,Historical release,12,10\n", encoding="utf-8")
    cal.import_source(db, None, "csv", path)
    ledger.save_trades(db, "historical-news", [_trade("2026-01-05 10:10")],
                       robot_key="a", symbol="WIN$N", timeframe="M5")

    cfg = _cfg(event_window_before_min=30, event_window_after_min=30)
    cfg["APP"] = {"time_basis": "broker", "broker_utc_offset": "-3"}
    features = enrich.enrich(db, cfg, ledger.load(db))

    assert features["event_impact"].iloc[0] == cal.HIGH
    assert pd.isna(features["event_surprise"].iloc[0])


def test_without_bars_or_calendar_nothing_breaks(db):
    ledger.save_trades(db, "r", [_trade("2026-01-05 10:00")], robot_key="a", symbol="XYZ", timeframe="M5")
    f = enrich.enrich(db, None, ledger.load(db))
    assert len(f) == 1 and pd.isna(f["atr"].iloc[0]) and f["day_of_week"].iloc[0] == 0
    assert enrich.enrich(db, None, ledger.load(db, symbol="nada")).empty
