"""T19.40: referência Decimal independente, estado/igualdade, prefixos e contrato."""
from decimal import Decimal, localcontext

import numpy as np
import pandas as pd
import pytest

from mt5_extracao.composition import Composition, base_composition, compatibility
from mt5_extracao.composition.registry import STRATEGY_PARAMETERS
from mt5_extracao.strategies import SIGNAL_COLUMNS, get_strategy
from mt5_extracao.strategies.ha_hilo import ha_hilo_context

EMPTY = np.finfo(float).max
FLAGS = ["entry_long", "entry_short", "exit_long", "exit_short"]


def market(n=800):
    close = 100+4*np.sin((np.arange(n)-299)/30)
    op = np.r_[close[0], close[:-1]] if n else close
    return pd.DataFrame(dict(time=pd.date_range("2026-10-19", periods=n, freq="5min"),
        open=op, high=np.maximum(op, close)+.03, low=np.minimum(op, close)-.03,
        close=close, spread=0))


def decimal_reference(data, period):
    with localcontext() as ctx:
        ctx.prec = 60
        rows = [[Decimal(str(v)) for v in row] for row in data[["open", "high", "low", "close"]].to_numpy()]
        result, state = [], 0
        for i, (op, high, low, close) in enumerate(rows):
            hc = (op+high+low+close)/4
            ho = (op+close)/2 if not i else (result[-1][0]+result[-1][1])/2
            line = None
            if i >= period:
                previous = rows[i-period:i]
                if close > sum(r[1] for r in previous)/period:
                    state = 1
                elif close < sum(r[2] for r in previous)/period:
                    state = -1
                current = rows[i-period+1:i+1]
                if state:
                    line = sum(r[2 if state > 0 else 1] for r in current)/period
            result.append((ho, hc, line, state))
        return result


@pytest.mark.parametrize("period", [2, 10, 500])
def test_ha_seed_hilo_windows_and_level_flags_match_independent_decimal(period):
    data = market(850)
    expected = decimal_reference(data, period)
    actual = ha_hilo_context(data, period)
    numeric = np.array([[float(v) if v is not None else np.nan for v in row[:3]] for row in expected])
    np.testing.assert_allclose(actual.iloc[:, :3], numeric, rtol=1e-13, equal_nan=True)
    assert actual.state.tolist() == [r[3] for r in expected]
    assert actual.ha_open.iloc[0] == (data.open.iloc[0]+data.close.iloc[0])/2
    strategy = get_strategy("ha_hilo", hilo_period=period)
    signals = strategy.gerar_sinais(data)
    assert list(signals) == list(SIGNAL_COLUMNS) and strategy.min_bars == 300
    for i, (ho, hc, line, _) in enumerate(expected):
        ready = i >= 299 and line is not None
        buy = ready and hc > line and hc > ho
        sell = ready and hc < line and hc < ho
        close = Decimal(str(data.close.iloc[i]))
        assert signals.exit_short.iloc[i] == bool(buy)
        assert signals.exit_long.iloc[i] == bool(sell)
        assert signals.entry_long.iloc[i] == bool(buy and line < close)
        assert signals.entry_short.iloc[i] == bool(sell and line > close)
        if buy or sell:
            assert signals.stop_long.iloc[i] == pytest.approx(float(line))
            assert signals.stop_short.iloc[i] == pytest.approx(float(line))
        else:
            assert pd.isna(signals.stop_long.iloc[i]) and pd.isna(signals.stop_short.iloc[i])
    assert not signals[FLAGS].iloc[:299].any().any()
    assert actual.line.iloc[:period].isna().all()
    assert signals[["target_long", "target_short"]].isna().all().all()
    if period == 10:
        assert signals.entry_long.any() and signals.entry_short.any()
        assert (signals.entry_long & signals.entry_long.shift(1, fill_value=False)).any()  # condição, não cross


def test_hilo_initial_undefined_strict_breakout_equality_updates_line_and_state_retained():
    data = pd.DataFrame([[8, 10, 6, 8], [8, 10, 6, 8], [10, 12, 10, 12],
                         [12, 12, 8, 8], [8, 11, 8, 9], [9, 11, 7, 7]],
                        columns=["open", "high", "low", "close"])
    context = ha_hilo_context(data, 2)
    assert context.state.tolist() == [0, 0, 1, 1, 1, -1]
    assert context.line.iloc[2:].tolist() == [8, 9, 8, 11]
    assert data.close.iloc[3] == context.line.iloc[2] == 8
    assert context.line.iloc[3] == 9  # não congela a linha anterior como o indicador gráfico
    flat = market(800).assign(open=100., high=101., low=99., close=100.)
    assert ha_hilo_context(flat, 500).line.isna().all()  # sem virada, mesmo após 501 barras
    assert not get_strategy("ha_hilo", hilo_period=500).gerar_sinais(flat)[FLAGS].any().any()
    equal = data.iloc[:3].copy();equal.loc[2, "close"] = 10.
    assert ha_hilo_context(equal, 2).state.eq(0).all()  # igualdade com SMA high não inicia alta


def test_ha_doji_is_neutral_and_preserves_the_contract_of_exit_flags():
    data = market(301)
    previous = ha_hilo_context(data.iloc[:300]).iloc[-1]
    doji = (previous.ha_open+previous.ha_close)/2
    data.loc[300, ["open", "high", "low", "close"]] = [doji, doji+.5, doji-.5, doji]
    context = ha_hilo_context(data)
    assert context.ha_close.iloc[-1] == context.ha_open.iloc[-1]
    assert not get_strategy("ha_hilo").gerar_sinais(data)[FLAGS].iloc[-1].any()


def test_fixed_prefix_causality_custom_index_empty_frames_and_future_mutation():
    data = market(650);data.index = pd.Index(np.arange(len(data))*3+7, name="source")
    strategy = get_strategy("ha_hilo")
    full = strategy.gerar_sinais(data)
    for n in (0, 1, 2, 10, 11, 299, 300, 501, 650):
        pd.testing.assert_frame_equal(strategy.gerar_sinais(data.iloc[:n]), full.iloc[:n])
        pd.testing.assert_frame_equal(ha_hilo_context(data.iloc[:n]), ha_hilo_context(data).iloc[:n])
    changed = data.copy();changed.loc[data.index[500:], ["open", "high", "low", "close"]] *= 3
    pd.testing.assert_frame_equal(strategy.gerar_sinais(changed).iloc[:500], full.iloc[:500])


@pytest.mark.parametrize("column", ["open", "high", "low", "close"])
@pytest.mark.parametrize("bad", [np.nan, np.inf, -np.inf, EMPTY, 0., -1.])
def test_invalid_ohlc_poisons_remaining_prefix_without_reseed(column, bad):
    data = market(420);clean = data.copy();data.loc[307, column] = bad
    context = ha_hilo_context(data)
    pd.testing.assert_frame_equal(context.iloc[:307], ha_hilo_context(clean.iloc[:307]))
    assert context.iloc[307:][["ha_open", "ha_close", "line"]].isna().all().all()
    assert context.state.iloc[307:].eq(0).all()
    signals = get_strategy("ha_hilo").gerar_sinais(data)
    assert not signals[FLAGS].iloc[307:].any().any()
    assert signals.iloc[307:, 4:].isna().all().all()


@pytest.mark.parametrize("component", ["ha", "hilo"])
def test_overflow_in_real_sums_poisons_the_context(component):
    data = market(40)
    if component == "ha":
        data.loc[15, ["open", "high", "low", "close"]] = 1e308
        first_bad = 15
    else:
        data[["open", "high", "low", "close"]] = [2e307, 5e307, 1e307, 2e307]
        first_bad = 10
    context = ha_hilo_context(data)
    assert context.ha_open.iloc[:first_bad].notna().all()
    assert context.iloc[first_bad:, :3].isna().all().all()
    assert context.state.iloc[first_bad:].eq(0).all()


@pytest.mark.parametrize("name,bad", [("hilo_period", v) for v in (1, 501, 2., True, "10", np.nan, np.inf, 10**400)]
    + [("hilo_offset", v) for v in (-1, 1, 0., False, "0")]
    + [("stop_buffer_points", v) for v in (-1, 2147483648, 1., True, np.nan, np.inf, "0", 10**400)])
def test_closed_typed_parameter_validation(name, bad):
    with pytest.raises(ValueError, match="HA/HiLo requer"):
        get_strategy("ha_hilo", **{name: bad})
    assert not STRATEGY_PARAMETERS["ha_hilo"][name].accepts(bad)


def test_registration_default_rules_and_typed_roundtrip_do_not_grant_native_support():
    strategy = get_strategy("ha_hilo", stop_buffer_points=2147483647)
    rules = strategy.exit_rules()
    assert rules.signal_stop_offset_points == 2147483647 and not rules.move_guard_from_signal_stop
    assert rules.target_r is rules.target_points is rules.signal_target_points is rules.stop_points is None
    assert not rules.absolute_signal_target and rules.signal_reference_distance_points is None
    assert rules.partial_r is rules.trailing_atr is rules.max_bars is None and not rules.eod_exit
    value = base_composition("ha_hilo", symbol="TEST", timeframe="M5")
    assert Composition(value.to_json()).config_hash == value.config_hash
    assert not compatibility(value) and compatibility(value, backend="mql5")
    assert set(STRATEGY_PARAMETERS["ha_hilo"]) == set(strategy.params)
    assert STRATEGY_PARAMETERS["ha_hilo"]["stop_buffer_points"].unit == "point"
    with pytest.raises(ValueError, match="Faltam colunas"):
        strategy.gerar_sinais(market().drop(columns="open"))
