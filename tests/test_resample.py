"""T11.1: montar um timeframe maior a partir de uma base menor, com as regras conferidas contra o MT5."""
import pandas as pd
import pytest

from mt5_extracao import cli, resample
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.timeframes import Timeframe


def m1_bars(n=30, start="2026-09-14 09:00:00"):
    times = pd.date_range(start, periods=n, freq="1min")
    close = [100.0 + i for i in range(n)]
    return pd.DataFrame({"time": times, "open": [c - 0.5 for c in close], "high": [c + 1 for c in close],
                         "low": [c - 1 for c in close], "close": close, "tick_volume": [10 + i for i in range(n)],
                         "spread": [5 + (i % 4) for i in range(n)], "real_volume": [i for i in range(n)]})


@pytest.fixture
def db(tmp_path):
    return DatabaseManager(db_type="sqlite", db_path=str(tmp_path / "db.sqlite"))


def test_bars_follow_the_rules_measured_on_metatrader():
    built = resample.resample_bars(m1_bars(), Timeframe.M1, Timeframe.M5)
    assert len(built) == 6 and list(built["time"].dt.strftime("%H:%M")) == ["09:00", "09:05", "09:10", "09:15", "09:20",
                                                                            "09:25"]
    first = built.iloc[0]
    assert (first["open"], first["high"], first["low"], first["close"]) == (99.5, 105.0, 99.0, 104.0)
    assert first["tick_volume"] == 10 + 11 + 12 + 13 + 14 and first["real_volume"] == 0 + 1 + 2 + 3 + 4
    assert first["spread"] == 5 and built.iloc[1]["spread"] == 5        # o MENOR spread das barras que a formam
    assert list(built.columns) == list(resample.COLUMNS)


def test_gaps_and_partial_bars_are_kept_like_metatrader_does():
    bars = m1_bars(12).drop(index=[2, 3, 4, 5, 6, 7])         # mercado parado de 09:02 a 09:07
    built = resample.resample_bars(bars, Timeframe.M1, Timeframe.M5)
    assert list(built["time"].dt.strftime("%H:%M")) == ["09:00", "09:05", "09:10"]
    assert built.iloc[0]["close"] == 101.0 and built.iloc[1]["open"] == 107.5       # barras com menos minutos
    unordered = m1_bars(10).sample(frac=1, random_state=3)
    assert resample.resample_bars(unordered, Timeframe.M1, Timeframe.M5).equals(
        resample.resample_bars(m1_bars(10), Timeframe.M1, Timeframe.M5))
    hourly = resample.resample_bars(m1_bars(150, "2026-09-14 22:30:00"), Timeframe.M1, Timeframe.H1)
    assert list(hourly["time"].dt.strftime("%d %H:%M")) == ["14 22:00", "14 23:00", "15 00:00"]
    daily = resample.resample_bars(m1_bars(150, "2026-09-14 22:30:00"), Timeframe.M1, Timeframe.D1)
    assert list(daily["time"].dt.strftime("%Y-%m-%d %H:%M")) == ["2026-09-14 00:00", "2026-09-15 00:00"]
    assert resample.resample_bars(m1_bars(0), Timeframe.M1, Timeframe.M5).empty


@pytest.mark.parametrize("source, target, text", [
    (Timeframe.M5, Timeframe.M1, "só dá para montar um timeframe maior"),
    (Timeframe.M5, Timeframe.M5, "não é maior"),
    (Timeframe.M2, Timeframe.M5, "não é múltiplo"),
    (Timeframe.H1, Timeframe.W1, "calendário da corretora"),
    (Timeframe.D1, Timeframe.MN1, "calendário da corretora"),
])
def test_impossible_pairs_are_refused_with_the_reason(source, target, text):
    with pytest.raises(ValueError, match=text):
        resample.check_pair(source, target)


def test_resample_symbol_saves_marks_the_origin_and_is_repeatable(db):
    db.save_ohlcv_data("EURUSD", Timeframe.M1.label, m1_bars(120))
    summary = resample.resample_symbol(db, "EURUSD", Timeframe.M1, [Timeframe.M5, Timeframe.H1])
    assert [(r["timeframe"], r["bars"], r["source_bars"]) for r in summary] == [("M5", 24, 120), ("H1", 2, 120)]
    saved = db.load_ohlcv("EURUSD", Timeframe.M5.label)
    assert len(saved) == 24 and saved.iloc[0]["spread"] == 5 and float(saved.iloc[-1]["close"]) == 219.0
    assert resample.origins(db) == {("EURUSD", "M5"): "M1", ("EURUSD", "H1"): "M1"}
    resample.resample_symbol(db, "EURUSD", Timeframe.M1, [Timeframe.M5])           # rodar de novo não duplica
    assert len(db.load_ohlcv("EURUSD", Timeframe.M5.label)) == 24
    db.save_ohlcv_data("EURUSD", Timeframe.M1.label, m1_bars(5, "2026-09-14 11:00:00"))
    resample.resample_symbol(db, "EURUSD", Timeframe.M1, [Timeframe.M5])           # base cresceu: a série acompanha
    assert len(db.load_ohlcv("EURUSD", Timeframe.M5.label)) == 25
    with pytest.raises(ValueError, match="mt5x extract --symbols PETR4 --tf M1"):
        resample.resample_symbol(db, "PETR4", Timeframe.M1, [Timeframe.M5])
    assert resample.origins(DatabaseManager(db_type="sqlite", db_path=db.db_path + ".novo")) == {}


def test_cli_resample_and_tables(tmp_path, capsys, db):
    db.save_ohlcv_data("EURUSD", Timeframe.M1.label, m1_bars(120))
    cfg = tmp_path / "config.ini"
    cfg.write_text(f"[DATABASE]\ntype = sqlite\npath = {tmp_path / 'db.sqlite'}\n", encoding="utf-8")
    args = ["--config", str(cfg)]
    assert cli.main(args + ["resample", "--symbols", "EURUSD", "--base", "M1", "--tf", "M5,M15"]) == cli.EXIT_OK
    out = capsys.readouterr().out
    assert "EURUSD M5: 24 barras montadas de 120 barras M1" in out and "EURUSD M15: 8 barras" in out
    assert cli.main(args + ["tables"]) == cli.EXIT_OK
    out = capsys.readouterr().out
    assert out.count("(montada a partir de M1)") == 2
    assert cli.main(args + ["resample", "--symbols", "EURUSD,XPTO", "--base", "M1", "--tf", "H1"]) == cli.EXIT_FAIL
    assert "XPTO: Não há barras M1" in capsys.readouterr().out
    assert cli.main(args + ["resample", "--symbols", "EURUSD", "--base", "M5", "--tf", "M1"]) == cli.EXIT_USAGE
