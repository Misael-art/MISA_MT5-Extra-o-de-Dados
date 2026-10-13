"""T7.2: dados das tabelas de ativos (favoritos e benchmark no banco, volume, robô lucrativo, ordenação)."""
from datetime import datetime, timedelta

import pandas as pd

from mt5_extracao import asset_table as at
from mt5_extracao.database_manager import DatabaseManager
from tests.market_data import random_walk


def _item(symbol, timeframe="1 dia", rows=100, end=None):
    end = end or datetime.now()
    return {"symbol": symbol, "timeframe": timeframe, "table": f"{symbol}_{timeframe}".lower().replace(" ", "_"),
            "rows": rows, "start": end - timedelta(days=200), "end": end}


def _run(robot, symbol, tf, net, ret, source="backtest"):
    return {"robot_key": robot, "symbol": symbol, "timeframe": tf, "source": source,
            "metrics": {"net_profit": net, "return_pct": ret}}


def test_favorites_and_benchmark_live_in_the_database(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "prefs.db"))
    assert at.load_prefs(db) is None                       # tabela criada só no primeiro uso
    at.save_favorites(db, ["PETR4", "VALE3"])
    at.set_benchmark(db, "PETR4", "IBOV")
    at.set_benchmark(db, "WIN$N", "IBOV")
    at.save_favorites(db, ["VALE3"])                       # PETR4 saiu dos favoritos e manteve o benchmark
    assert at.load_prefs(db) == {"favorites": ["VALE3"], "benchmarks": {"PETR4": "IBOV", "WIN$N": "IBOV"}}
    at.set_benchmark(db, "PETR4", "  ")
    assert at.load_prefs(db)["benchmarks"] == {"WIN$N": "IBOV"}
    assert at.load_prefs(None) is None


def test_daily_volume_prefers_real_volume_and_reads_the_base(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "vol.db"))
    bars = random_walk(120, freq="h", start="2024-01-02 00:00", volume=50)      # 24 barras por dia, 50 ticks cada
    assert db.save_ohlcv_data("EURUSD", "1 hora", bars)
    volumes = at.volumes(db, db.data_inventory())
    assert volumes == {"EURUSD": 1200.0}
    real = bars.assign(real_volume=10.0)
    assert at.daily_volume(real, 60) == 240.0
    assert at.daily_volume(bars.assign(tick_volume=0), 60) is None
    assert at.daily_volume(pd.DataFrame(), 60) is None
    assert (at.fmt_volume(None), at.fmt_volume(950), at.fmt_volume(12_400), at.fmt_volume(3_500_000)) == \
        ("—", "950", "12,4 mil", "3,5 mi")


def test_profit_uses_the_latest_run_of_each_robot():
    runs = [_run("python:a", "PETR4", "D1", -50.0, -0.05),            # mais recente primeiro, como em load_runs
            _run("python:a", "PETR4", "D1", 900.0, 0.90),             # execução antiga do mesmo robô: não conta
            _run("python:b", "PETR4", "D1", 124.0, 0.124),
            _run("python:b", "PETR4", "H1", 300.0, 0.30),
            _run("python:a", "VALE3", "D1", -10.0, -0.01),
            _run("mql5:x", "WIN$N", "M5", 999.0, 0.99, source="strategy tester (bateria)")]
    cells = at.profit_cells(runs)
    assert at.fmt_profit(at.profit_of(cells, "PETR4", "D1")) == "🟢 +12,4%"
    assert at.fmt_profit(at.profit_of(cells, "PETR4")) == "🟢 +30,0%"         # o ativo inteiro: o melhor timeframe
    assert at.fmt_profit(at.profit_of(cells, "VALE3")) == "🔴 Sem Lucro"
    assert at.fmt_profit(at.profit_of(cells, "WIN$N")) == "⚪ N/A"           # execução auxiliar da bateria não entra
    at.add_run(cells, _run("python:a", "VALE3", "D1", 20.0, 0.02))            # um teste novo substitui o anterior
    assert at.fmt_profit(at.profit_of(cells, "VALE3")) == "🟢 +2,0%"


def _rows(now):
    stale = now - timedelta(days=40)
    series = {"AAA": [_item("AAA", end=now)], "BBB": [_item("BBB", end=stale)], "CCC": [_item("CCC", end=now)],
              "DDD": [], "EEE": [], "FAV": []}
    cells = at.profit_cells([_run("python:a", "BBB", "D1", 10.0, 0.10), _run("python:a", "CCC", "D1", -5.0, -0.05)])
    return at.build_rows(((s, s, items) for s, items in series.items()), favorites=["FAV", "DDD"],
                         volume_map={"AAA": 100.0, "BBB": 5000.0, "CCC": 700.0, "DDD": 1.0}, cells=cells,
                         benchmarks={"CCC": "IBOV"}, selected=["EEE"], now=now)


def test_default_order_is_favorites_then_volume_then_alphabetical():
    rows = _rows(datetime(2024, 6, 3, 12, 0))
    assert list(at.sort_rows(rows)["symbol"]) == ["DDD", "FAV", "BBB", "CCC", "AAA", "EEE"]
    top = at.display(next(at.sort_rows(rows).itertuples()))
    assert top == ("★", "DDD", "1", "⚪ N/A", "⚪ Sem dados", "❌ Não", "", "☐")


def test_header_click_sorts_and_keeps_favorites_on_top():
    rows = _rows(datetime(2024, 6, 3, 12, 0))

    def order(column, reverse=False):
        return list(at.sort_rows(rows, column, reverse)["symbol"])
    assert order(at.COL_PROFIT) == ["DDD", "FAV", "BBB", "CCC", "AAA", "EEE"]         # com lucro, sem lucro, sem teste
    assert order(at.COL_STATUS)[:3] == ["DDD", "FAV", "BBB"]                           # precisa de atualização primeiro
    assert order(at.COL_VOLUME) == ["DDD", "FAV", "BBB", "CCC", "AAA", "EEE"]
    assert order(at.COL_VOLUME, reverse=True) == ["DDD", "FAV", "AAA", "CCC", "BBB", "EEE"]   # sem volume por último
    assert order(at.COL_BENCHMARK)[:3] == ["DDD", "FAV", "CCC"]
    assert order(at.COL_SELECTED)[:3] == ["DDD", "FAV", "EEE"]
    assert order(at.COL_SYMBOL, reverse=True) == ["FAV", "DDD", "EEE", "CCC", "BBB", "AAA"]
    assert at.sort_rows(rows.iloc[0:0]).empty


def test_series_rows_show_status_details_and_profit_of_that_timeframe():
    now = datetime(2024, 6, 3, 12, 0)
    d1, h1 = _item("PETR4", "1 dia", 6489, end=now), _item("PETR4", "1 hora", 120, end=now - timedelta(days=30))
    cells = at.profit_cells([_run("python:a", "PETR4", "D1", 10.0, 0.10)])
    rows = at.sort_rows(at.build_rows([(d1["table"], "PETR4", [d1]), (h1["table"], "PETR4", [h1])], [], {}, cells,
                                      {"PETR4": "IBOV"}, [h1["table"]], per_series=True, now=now))
    shown = [at.display(row) for row in rows.itertuples()]
    assert shown == [("☆", "PETR4", "—", "⚪ N/A", "🔴 Precisa de atualização", "🎯 Sim", "H1 (120 barras)", "☑"),
                     ("☆", "PETR4", "—", "🟢 +10,0%", "🟢 Atualizada", "🎯 Sim", "D1 (6.489 barras)", "☐")]
