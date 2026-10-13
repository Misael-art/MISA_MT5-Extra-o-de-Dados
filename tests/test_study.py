"""T7.1: números de estudo (mt5_extracao.study) e inventário da base usados pela interface."""
import pandas as pd
import pytest

from mt5_extracao import study
from mt5_extracao.database_manager import DatabaseManager
from tests.market_data import random_walk


def bars(closes, start="2024-01-02 09:00", freq="h"):
    times = pd.date_range(start, periods=len(closes), freq=freq)
    opens = [closes[0]] + list(closes[:-1])
    return pd.DataFrame({"time": times, "open": opens, "close": closes,
                         "high": [max(o, c) + 1 for o, c in zip(opens, closes)],
                         "low": [min(o, c) - 1 for o, c in zip(opens, closes)],
                         "tick_volume": 10, "spread": 2})


def test_summarize_period_numbers():
    s = study.summarize(bars([100, 110, 99, 121]))
    assert s["bars"] == 4
    assert s["last"] == 121
    assert s["return_pct"] == pytest.approx(21.0)
    assert s["max_drawdown_pct"] == pytest.approx(-10.0)      # de 110 para 99
    assert s["up_bars_pct"] == pytest.approx(50.0)            # a 1ª barra abre e fecha em 100
    assert s["mean_volume"] == 10 and s["mean_spread"] == 2
    assert s["volatility_pct"] > 0
    assert study.summarize(pd.DataFrame()) == {}


def test_seasonality_by_hour_and_weekday():
    season = study.seasonality(bars([100, 101, 102, 103, 104, 105], start="2024-01-01 10:00"))  # segunda-feira
    assert list(season["hour"].index) == [10, 11, 12, 13, 14, 15]
    assert list(season["weekday"].index) == [0]
    assert season["weekday"]["bars"].iloc[0] == 6
    assert season["weekday"]["return_pct"].iloc[0] > 0
    daily = study.seasonality(bars([100, 101, 102], freq="D"))
    assert daily["hour"].empty and len(daily["weekday"]) == 3


def test_compress_keeps_ohlc_meaning():
    df = bars(list(range(100, 200)))
    small, factor = study.compress(df, max_candles=25)
    assert factor == 4 and len(small) == 25
    assert small["open"].iloc[0] == df["open"].iloc[0]
    assert small["close"].iloc[0] == df["close"].iloc[3]
    assert small["high"].iloc[0] == df["high"].iloc[:4].max()
    assert small["tick_volume"].iloc[0] == 40
    same, factor = study.compress(df, max_candles=500)
    assert factor == 1 and len(same) == 100


def test_add_indicators_and_period_start():
    df = study.add_indicators(random_walk(120, freq="min", start="2024-01-02 09:00"))
    assert {"ma20", "ma50", "bb_upper", "bb_lower", "rsi"} <= set(df.columns)
    assert df["ma50"].iloc[:49].isna().all() and df["ma50"].iloc[49:].notna().all()
    assert (df["bb_upper"].dropna() >= df["bb_lower"].dropna()).all()
    assert study.period_start(pd.Timestamp("2024-03-31"), 30) == pd.Timestamp("2024-03-01")
    assert study.period_start(pd.Timestamp("2024-03-31"), None) is None


def test_freshness_text():
    now = pd.Timestamp("2024-06-10 12:00")
    assert study.freshness(None, 1, now) == "sem dados"
    assert study.freshness(pd.Timestamp("2024-06-08 18:00"), 1, now) == "em dia"   # fim de semana não é atraso
    assert study.freshness(pd.Timestamp("2024-06-01 18:00"), 1, now) == "há 8 dias"
    assert study.freshness(pd.Timestamp("2024-01-01"), 1440, now) == "há 5 meses"


def test_every_tile_has_glossary_entry():
    assert all(text.strip() for text in study.GLOSSARY.values())
    for term in ("Retorno no período", "Volatilidade anualizada", "Queda máxima", "IFR (RSI)"):
        assert term in study.GLOSSARY


def test_data_inventory_lists_bar_series_only(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "inv.db"))
    assert db.data_inventory() == []
    assert db.save_ohlcv_data("WIN$N", "1 minuto", random_walk(120, freq="min", start="2024-01-02 09:00"))
    assert db.save_ohlcv_data("PETR4", "1 dia", random_walk(30, freq="D", start="2024-01-02"))
    ticks = pd.DataFrame({"time": pd.date_range("2024-01-02 09:00", periods=3, freq="s"), "time_msc": [1, 2, 3],
                          "seq": 0, "bid": 1.0, "ask": 1.1, "last": 1.0, "volume": 1, "volume_real": 1.0, "flags": 0})
    db.save_ticks(db.get_table_name_for_symbol("WIN$N", "ticks"), ticks)
    # Tabela de uma base antiga, sem registro em _symbol_tables: o símbolo sai do nome
    random_walk(10, freq="h", start="2024-01-02 09:00").to_sql("vale3_1_hora", db.engine, index=False)

    inventory = {(i["symbol"], i["timeframe"]): i for i in db.data_inventory()}
    assert set(inventory) == {("WIN$N", "1 minuto"), ("PETR4", "1 dia"), ("VALE3", "1 hora")}
    win = inventory[("WIN$N", "1 minuto")]
    assert win["rows"] == 120 and win["table"] == "win_n_1_minuto"
    assert win["start"] == pd.Timestamp("2024-01-02 09:00") and win["end"] == pd.Timestamp("2024-01-02 10:59")
