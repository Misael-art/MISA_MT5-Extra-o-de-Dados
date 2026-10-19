"""T20.2: valores manuais, sem usar o motor para produzir o esperado.

Os cenários e unidades estão em docs/analytics/T20.2_REFERENCIAS_FINANCEIRAS.md.
"""
from dataclasses import replace

import pandas as pd
import pytest

from mt5_extracao.strategies import ExitRules
from mt5_extracao.strategies.backtester import BacktestConfig, SymbolSpec, compute_metrics, run_backtest
from mt5_extracao.trade_analytics import stats
from mt5_extracao.trade_analytics.execution.engine import ExitPolicy, simulate
from tests.test_backtester import Manual, bars
from tests.test_profit_factor import trades


@pytest.mark.parametrize("risk,distance,expected", [
    (100, 3, 0.8), (5, 3, 0), (10000, 3, 1.2), (0, 3, 0), (100, 0, 0),
])
def test_reference_sizing_tick_units_and_non_lattice_cap(risk, distance, expected):
    # Tick 0,25 vale 10 BRL/lote: stop 3 = 12 ticks = 120 BRL/lote.
    # Passo 0,2; teto 1,3 permite no máximo 1,2, nunca 1,3.
    spec = SymbolSpec(point=0.01, tick_size=0.25, tick_value=10,
                      volume_min=0.2, volume_step=0.2, volume_max=1.3, currency_profit="BRL")
    assert spec.lots_for_risk(risk, distance) == pytest.approx(expected, abs=1e-12, rel=0)


def test_reference_sizing_does_not_round_risk_up_with_epsilon():
    spec = SymbolSpec(point=1, tick_value=1)
    assert spec.lots_for_risk(9.9999999999, 1) == 9


@pytest.mark.parametrize("changes", [{"tick_value": 0}, {"volume_step": 0},
                                    {"volume_max": float("inf")}, {"volume_min": -1}])
def test_invalid_contract_does_not_produce_a_volume(changes):
    with pytest.raises(ValueError, match="Especificação"):
        replace(SymbolSpec(), **changes).lots_for_risk(100, 1)


@pytest.mark.parametrize("risk,distance", [(float("nan"), 1), (1, float("inf"))])
def test_invalid_risk_does_not_produce_a_volume(risk, distance):
    with pytest.raises(ValueError, match="finitos"):
        SymbolSpec().lots_for_risk(risk, distance)


@pytest.mark.parametrize("direction,last_high,last_close,expected_entry,expected_exit,expected_volume,expected_pnl", [
    (1, 105, 104, 100.75, 103.75, 8, 184),
    (-1, 103, 102, 99.75, 102.75, 11, -275),
])
def test_reference_bid_ask_slippage_commission_and_cycle(
        direction, last_high, last_close, expected_entry, expected_exit, expected_volume, expected_pnl):
    data = bars([[100, 101, 99, 100], [100, 102, 99, 101], [101, last_high, 100, last_close]],
                start="2026-01-05 10:00", freq="15min", spread=2)
    spec = SymbolSpec(point=0.25, tick_size=0.25, tick_value=2, currency_profit="BRL")
    cfg = BacktestConfig(initial_equity=10000, risk_per_trade=0.04,
                         slippage_points=1, commission_per_lot=0.5)
    strategy = Manual(long=[0], stop={0: 95}) if direction > 0 else Manual(short=[0], stop={0: 104})
    result = run_backtest(data, strategy, spec, cfg)
    trade = result.trades[0]
    assert trade.entry_price == expected_entry
    assert trade.exit_price == expected_exit
    assert trade.volume == expected_volume
    assert trade.pnl == pytest.approx(expected_pnl, abs=1e-9, rel=0)
    assert result.equity.iloc[-1] == pytest.approx(10000 + expected_pnl)
    assert result.metrics["net_profit"] == pytest.approx(expected_pnl)


def test_reference_swap_before_and_after_partial():
    # Terça entra 10 lotes; quarta realiza 5 a +10; quinta fecha 5 a +5.
    # PnL bruto 75; comissão entrada 5 + saída 5 = 10.
    # Swap terça 10*(-1) e quarta tripla 5*(-3) = -25; líquido = 40.
    data = bars([[100, 101, 99, 100], [100, 101, 99, 100],
                 [100, 111, 100, 110], [105, 106, 104, 105]], start="2026-01-05")
    spec = SymbolSpec(point=1, tick_size=1, tick_value=1, swap_mode=1, swap_long=-1)
    cfg = BacktestConfig(initial_equity=10000, risk_per_trade=0.01, commission_per_lot=0.5)
    rules = ExitRules(partial_r=1, partial_fraction=0.5, breakeven_after_partial=False)
    trade = run_backtest(data, Manual(long=[0], stop={0: 90}, rules=rules), spec, cfg).trades[0]
    assert trade.volume == 10
    assert trade.pnl == pytest.approx(40, abs=1e-9, rel=0)
    assert trade.r_multiple == pytest.approx(0.4)
    source = pd.DataFrame([dict(direction=1, entry_time=data.time[1], exit_time=data.time[3],
                                entry_price=100, stop_price=90, target_price=None, volume=10,
                                exit_reason="fim dos dados")])
    replay = simulate(source, data, spec, cfg, ExitPolicy.from_rules(rules))
    assert replay.trades[0].pnl == pytest.approx(40, abs=1e-9, rel=0)


def test_reference_swap_of_added_lot_starts_at_its_own_entry():
    # Terça 10@100, quarta mais 10@110, quinta todos saem @115.
    # Bruto 150+50=200; swap inicial -40, acréscimo -30; líquido 130.
    data = bars([[100, 101, 99, 100], [100, 101, 99, 100],
                 [110, 111, 109, 110], [115, 116, 114, 115]], start="2026-01-05")
    source = pd.DataFrame([dict(direction=1, entry_time=data.time[1], exit_time=data.time[3],
                                entry_price=100, stop_price=90, target_price=None, volume=10,
                                exit_reason="fim dos dados")])
    spec = SymbolSpec(point=1, tick_size=1, tick_value=1, swap_mode=1, swap_long=-1)
    policy = ExitPolicy(grid_levels=1, grid_direction="favor", grid_spacing_points=10)
    replay = simulate(source, data, spec, BacktestConfig(), policy)
    assert replay.adds == 1
    assert replay.trades[0].pnl == pytest.approx(130, abs=1e-9, rel=0)


def test_reference_swap_sign_weekend_and_disabled():
    spec = SymbolSpec(point=0.01, tick_size=0.25, tick_value=10,
                      swap_mode=1, swap_long=-2, swap_short=3)
    # Qua->seg = 3+1+1 = 5 noites ponderadas; 2 lotes; ponto vale 0,4.
    assert spec.swap_money(1, 2, "2026-01-07", "2026-01-12") == pytest.approx(-8)
    assert spec.swap_money(-1, 2, "2026-01-07", "2026-01-12") == pytest.approx(12)
    assert replace(spec, swap_mode=0).swap_money(1, 2, "2026-01-07", "2026-01-12") == 0


def test_reference_metrics_closed_balance_and_canonical_pf():
    values = [120, -60, 0, 30, -15]
    core = compute_metrics(trades(values), 1000)
    lab = stats.metrics(values, 1000)
    # Ganhos 150; perdas 75; líquido 75; pico 1120 -> fundo 1060.
    for result in (core, lab):
        assert result["profit_factor"] == 2
        assert result["gross_profit"] == 150
        assert result["gross_loss"] == 75
        assert result["win_rate"] == 0.4
        assert result["max_drawdown_pct"] == pytest.approx(60 / 1120)
    assert core["net_profit"] == lab["net"] == 75


@pytest.mark.parametrize("values", [[None], [float("nan")], [float("inf")], [True], [1, None, -1]])
def test_invalid_financial_observation_is_rejected(values):
    # Dado inválido não pode ser descartado, virar zero ou produzir ranking parcial.
    with pytest.raises(ValueError, match="PnL"):
        stats.metrics(values)
    with pytest.raises(ValueError, match="PnL"):
        compute_metrics(trades(values), 1000)
