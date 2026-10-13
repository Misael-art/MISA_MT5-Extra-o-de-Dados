"""T10.12: custo real no backtester Python: swap por noite e spread medido nos ticks."""
import dataclasses

import pandas as pd
import pytest
from sqlalchemy import text

from mt5_extracao import cli
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.strategies import specs
from mt5_extracao.strategies.backtester import BacktestConfig, SymbolSpec, run_backtest
from tests.test_backtester import Manual

FX = dict(point=0.00001, tick_size=0.00001, tick_value=1.0, volume_min=0.01, volume_step=0.01)
CFG = BacktestConfig(initial_equity=100_000, risk_per_trade=0.01)
SPEC_ROW = {"point": 0.00001, "digits": 5, "tick_size": 0.00001, "tick_value": 1.0, "volume_min": 0.01,
            "volume_step": 0.01, "volume_max": 100, "contract_size": 100000, "currency_profit": "USD", "spread": 1,
            "swap_long": -0.7, "swap_short": -1.0, "swap_mode": 1, "swap_rollover3days": 3}


def hourly(rows, start):
    df = pd.DataFrame(rows, columns=["open", "high", "low", "close"], dtype=float)
    df.insert(0, "time", pd.date_range(start, periods=len(df), freq="h"))
    df["spread"] = 0
    return df


def ticks(rows):
    """rows: (horário, bid, spread em pontos)."""
    stamps = [pd.Timestamp(r[0]) for r in rows]
    return pd.DataFrame({"time_msc": [int(s.value // 1_000_000) for s in stamps], "bid": [r[1] for r in rows],
                         "ask": [r[1] + r[2] * 0.00001 for r in rows]})


def test_swap_counts_weekday_rollovers_and_the_triple_night():
    spec = SymbolSpec(**FX, swap_long=-0.7, swap_short=-1.0, swap_mode=1, swap_triple_day=3)
    assert spec.swap_nights("2026-07-20 10:00", "2026-07-20 22:00") == 0          # mesmo dia
    assert spec.swap_nights("2026-07-20 10:00", "2026-07-21 01:00") == 1          # segunda para terça
    assert spec.swap_nights("2026-07-22 10:00", "2026-07-23 01:00") == 3          # quarta: noite tripla
    assert spec.swap_nights("2026-07-24 10:00", "2026-07-27 01:00") == 1          # sexta para segunda: só a de sexta
    assert spec.swap_nights("2026-07-20 10:00", "2026-07-27 01:00") == 7          # a semana inteira
    assert spec.swap_money(-1, 5.46, "2026-07-23 17:00", "2026-07-24 10:00") == pytest.approx(-5.46)
    assert spec.swap_money(+1, 2.0, "2026-07-20 10:00", "2026-07-21 01:00") == pytest.approx(-1.4)
    assert dataclasses.replace(spec, swap_mode=2).swap_money(+1, 2.0, "2026-07-20 10:00", "2026-07-21 01:00") == 0.0


def test_backtester_charges_swap_on_positions_held_overnight():
    # compra na abertura das 23h de segunda e sai pelo alvo às 01h de terça: uma noite
    df = hourly([[1.1000, 1.1005, 1.0995, 1.1000], [1.1000, 1.1005, 1.0995, 1.1000], [1.1000, 1.1005, 1.0995, 1.1000],
                 [1.1000, 1.1030, 1.0995, 1.1020], [1.1020, 1.1025, 1.1015, 1.1020]], "2026-07-20 22:00")
    strategy = Manual(long=[0], stop={0: 1.0990}, target={0: 1.1020})
    spec = SymbolSpec(**FX, swap_long=-0.7, swap_mode=1)
    with_swap = run_backtest(df, strategy, spec, CFG).trades[0]
    without = run_backtest(df, strategy, spec, dataclasses.replace(CFG, swap=False)).trades[0]
    assert with_swap.exit_reason == "alvo" and with_swap.volume == without.volume
    assert with_swap.pnl == pytest.approx(without.pnl - 0.7 * with_swap.volume)


def test_spread_profile_weights_by_time_and_measures_the_first_tick_of_each_hour():
    profile = specs.spread_profile(ticks([
        ("2026-07-23 00:00:00.050", 1.14108, 21), ("2026-07-23 00:00:00.150", 1.14108, 5),      # 21 vale 0,1 s
        ("2026-07-23 00:00:30", 1.14100, 5), ("2026-07-23 10:00:01", 1.14200, 1), ("2026-07-23 10:30:00", 1.14210, 1),
        ("2026-07-24 00:00:01", 1.14300, 7), ("2026-07-24 00:00:31", 1.14300, 5)]), 0.00001)
    assert profile["ticks"] == 7 and profile["days"] == 2
    assert profile["open"][0] == pytest.approx(14.0) and profile["open"][10] == pytest.approx(1.0)   # (21 + 7) / 2
    assert 5.0 < profile["hours"][0] < 6.0 and profile["hours"][10] == pytest.approx(1.0)
    assert profile["hours"][5] is None and profile["open"][5] is None
    assert specs.spread_profile(ticks([]), 0.00001)["ticks"] == 0


def test_execution_pays_the_measured_spread_and_the_flag_restores_the_bar_spread():
    # sinal na barra das 23h, entrada na abertura da 00h: paga o spread do primeiro tick da hora (20 pontos)
    df = hourly([[1.1000, 1.1005, 1.0995, 1.1000], [1.1000, 1.1005, 1.0995, 1.1000], [1.1000, 1.1005, 1.0995, 1.1000],
                 [1.1000, 1.1005, 1.0995, 1.1000]], "2026-07-22 23:00")
    strategy = Manual(long=[0], stop={0: 1.0950})
    by_hour, at_open = [1.0] * 24, [2.0] * 24
    at_open[0] = 20.0
    spec = SymbolSpec(**FX, spread_by_hour=tuple(by_hour), spread_at_open=tuple(at_open))
    measured = run_backtest(df, strategy, spec, CFG).trades[0]
    bar = run_backtest(df, strategy, spec, dataclasses.replace(CFG, real_spread=False)).trades[0]
    assert bar.entry_price == pytest.approx(1.1000) and measured.entry_price == pytest.approx(1.1000 + 20e-5)
    assert measured.volume < bar.volume                       # risco maior por lote: lote menor
    assert run_backtest(df, strategy, SymbolSpec(**FX), CFG).trades[0].entry_price == pytest.approx(1.1000)
    # barra que não abre na hora cheia paga a média da hora, não o primeiro tick
    quarter = df.assign(time=pd.date_range("2026-07-22 23:45", periods=4, freq="15min"))
    assert run_backtest(quarter, strategy, spec, CFG).trades[0].entry_price == pytest.approx(1.1000 + 20e-5)
    late = df.assign(time=pd.date_range("2026-07-22 23:15", periods=4, freq="15min"))
    assert run_backtest(late, strategy, spec, CFG).trades[0].entry_price == pytest.approx(1.1000 + 1e-5)


def test_old_database_gets_the_new_columns_and_keeps_its_rows(tmp_path):
    path = str(tmp_path / "antiga.db")
    db = DatabaseManager(db_path=path)
    with db.engine.begin() as conn:                            # base criada antes da T10.12
        conn.execute(text("DROP TABLE _symbol_specs"))
        conn.execute(text("CREATE TABLE _symbol_specs (symbol TEXT PRIMARY KEY, point REAL, digits INTEGER, "
                          "tick_size REAL, tick_value REAL, volume_min REAL, volume_step REAL, volume_max REAL, "
                          "contract_size REAL, currency_profit TEXT, spread REAL, updated_at TEXT)"))
        conn.execute(text("INSERT INTO _symbol_specs (symbol, point, digits, tick_size, tick_value, volume_min, "
                          "volume_step, volume_max, contract_size, currency_profit, spread) VALUES "
                          "('WIN$N', 1, 0, 5, 0.2, 1, 1, 100, 1, 'BRL', 5)"))
    db.engine.dispose()
    db = DatabaseManager(db_path=path)
    spec = specs.load_spec(db, "WIN$N")
    assert spec.known and spec.tick_size == 5 and spec.swap_long == 0.0 and spec.spread_by_hour is None
    assert spec.swap_money(1, 1, "2026-07-20 10:00", "2026-07-21 10:00") == 0.0


def test_calibrate_needs_spec_and_ticks_and_survives_a_spec_refresh(tmp_path, capsys):
    path = str(tmp_path / "t.db")
    db = DatabaseManager(db_path=path)
    with pytest.raises(ValueError, match="mt5x specs --symbols EURUSD"):
        specs.calibrate_spread(db, "EURUSD")
    db.save_symbol_spec("EURUSD", SPEC_ROW)
    with pytest.raises(ValueError, match="mt5x ticks --symbols EURUSD"):
        specs.calibrate_spread(db, "EURUSD")
    data = ticks([("2026-07-23 00:00:00.050", 1.14108, 21), ("2026-07-23 00:00:30", 1.14100, 5),
                  ("2026-07-23 10:00:01", 1.14200, 1)])
    data["last"], data["volume"], data["volume_real"], data["flags"] = 0.0, 0, 0.0, 6
    db.save_ticks(db.get_table_name_for_symbol("EURUSD", "ticks"), data)
    profile = specs.calibrate_spread(db, "EURUSD")
    assert profile["open"][0] == pytest.approx(21.0)
    db.save_symbol_spec("EURUSD", {**SPEC_ROW, "spread": 2})   # reler a especificação não apaga a calibração
    spec = specs.load_spec(db, "EURUSD")
    assert spec.spread_points == 2 and spec.spread_at_open[0] == pytest.approx(21.0) and spec.swap_short == -1.0
    db.engine.dispose()

    cfg = tmp_path / "config.ini"
    cfg.write_text(f"[DATABASE]\ntype = sqlite\npath = {path}\n", encoding="utf-8")
    assert cli.main(["--config", str(cfg), "specs", "--symbols", "EURUSD", "--calibrate"]) == cli.EXIT_OK
    out = capsys.readouterr().out
    assert "3 ticks de 1 dia(s)" in out and "Só 1 dia(s) de ticks" in out and "às 00h" in out
    assert cli.main(["--config", str(cfg), "specs", "--symbols", "GBPUSD", "--calibrate"]) == cli.EXIT_FAIL
    assert "mt5x specs --symbols GBPUSD" in capsys.readouterr().out
