"""T19.36: referência Decimal independente e prefixos causais da variante HLC3."""
from decimal import Decimal, localcontext

import numpy as np
import pandas as pd
import pytest

from mt5_extracao.composition import BaseAdapter, Composition, base_composition, compatibility
from mt5_extracao.composition.registry import STRATEGY_PARAMETERS
from mt5_extracao.strategies import SIGNAL_COLUMNS, get_strategy
from mt5_extracao.strategies.vwap_bands import daily_vwap

EMPTY = np.finfo(float).max


def market(n=650):
    rng = np.random.default_rng(36)
    close = 100 + np.sin(np.arange(n)/13)*3
    return pd.DataFrame(dict(time=pd.date_range("2026-10-19", periods=n, freq="5min"),
        open=close, high=close+rng.uniform(.1, 2, n), low=close-rng.uniform(.1, 2, n),
        close=close, tick_volume=rng.integers(1, 70, n).astype(float),
        real_volume=np.where(np.arange(n) >= 361, rng.integers(0, 15, n), 0), spread=0))


def decimal_reference(data, source):
    """Recalcula cada prefixo/dia desde zero; não usa os acumuladores do produto."""
    expected, sources = [], []
    with localcontext() as ctx:
        ctx.prec = 60
        for i in range(len(data)):
            prefix = data.iloc[:i+1]
            has_real = source == 0 and any(
                np.isfinite(v) and 0 < v < EMPTY for v in prefix.real_volume)
            chosen = 2 if source == 2 or has_real else 1
            sources.append(chosen)
            current = prefix.time.iloc[-1].normalize()
            daily = prefix[prefix.time.dt.normalize() == current]
            if daily.index[0] == data.index[0] and daily.time.iloc[0] != current:
                expected.append(np.nan)
                continue
            weighted, volume = Decimal(0), Decimal(0)
            for row in daily.itertuples():
                typical = sum(Decimal(str(v)) for v in (row.high, row.low, row.close)) / Decimal(3)
                v = Decimal(str(row.real_volume if chosen == 2 else row.tick_volume))
                weighted += typical*v
                volume += v
            expected.append(float(weighted/volume) if volume else np.nan)
    return np.array(expected), np.array(sources)


@pytest.mark.parametrize("source", [0, 1, 2])
def test_hlc3_weighted_daily_bands_and_all_signals_match_decimal(source):
    data = market()
    expected, sources = decimal_reference(data, source)
    actual = daily_vwap(data, source)
    np.testing.assert_allclose(actual.vwap, expected, rtol=2e-14, atol=1e-12, equal_nan=True)
    np.testing.assert_array_equal(actual.volume_source, sources)
    strategy = get_strategy("vwap_bands", volume_source=source)
    signals = strategy.gerar_sinais(data)
    for i, v in enumerate(expected):
        lower, upper = v*.99, v*1.01
        buy, sell = data.low.iloc[i] <= lower, data.high.iloc[i] >= upper
        active = i >= 299 and np.isfinite(v) and buy != sell
        assert signals.entry_long.iloc[i] == (active and buy)
        assert signals.entry_short.iloc[i] == (active and sell)
        if active:
            assert signals.stop_long.iloc[i] == pytest.approx(lower, abs=2e-12)
            assert signals.stop_short.iloc[i] == pytest.approx(upper, abs=2e-12)
            assert signals.target_long.iloc[i] == pytest.approx(v*.995, abs=2e-12)
            assert signals.target_short.iloc[i] == pytest.approx(v*1.005, abs=2e-12)
    assert signals.entry_long.any() and signals.entry_short.any()
    assert list(signals) == list(SIGNAL_COLUMNS)
    assert not signals.exit_long.any() and not signals.exit_short.any()


def test_hlc3_is_not_close_weighted_and_zero_volume_preserves_last_mean():
    data = market(4).assign(high=[106., 115., 100., 120.], low=[98., 98., 98., 100.],
        close=[99., 102., 99., 110.], tick_volume=[0., 2., 0., 3.])
    actual = daily_vwap(data, 1).vwap
    assert np.isnan(actual.iloc[0])
    assert actual.iloc[1] == 105 and actual.iloc[2] == 105
    assert actual.iloc[3] == 108
    assert actual.iloc[3] != (102*2+110*3)/5


def test_auto_selects_whole_known_real_series_and_retains_source_across_dates():
    data = market(6).assign(time=pd.to_datetime(["2026-10-19 00:00", "2026-10-19 12:00",
        "2026-10-19 12:05", "2026-10-20 00:00", "2026-10-20 00:05", "2026-10-20 00:10"]),
        high=[102., 108., 120., 132., 135., 138.], low=[98., 104., 116., 128., 131., 134.],
        close=[100., 106., 118., 130., 133., 136.], tick_volume=[5., 50., 5., 50., 5., 1.],
        real_volume=[0., 2., 1., 0., 3., 0.])
    actual = daily_vwap(data)
    np.testing.assert_allclose(actual.vwap, [100., 106., 110., np.nan, 133., 133.], equal_nan=True)
    assert actual.volume_source.tolist() == [1, 2, 2, 2, 2, 2]
    # Fenômeno adicional da janela deslizante: ao sair todo real positivo antigo,
    # uma janela nova pode selecionar ticks. O prefixo fixo anterior conserva real.
    fixed = daily_vwap(data.iloc[:4]).iloc[-1]
    rolled = daily_vwap(data.iloc[3:4]).iloc[-1]
    assert fixed.volume_source == 2 and np.isnan(fixed.vwap)
    assert rolled.volume_source == 1 and rolled.vwap == 130.


def test_prefix_causality_including_future_source_switch_and_nondefault_index():
    data = market()
    data.index = pd.Index(np.arange(len(data))*3+17, name="source_bar")
    strategy = get_strategy("vwap_bands")
    full, full_vwap = strategy.gerar_sinais(data), daily_vwap(data)
    for n in (0, 1, 7, 287, 288, 299, 300, 361, 362, 577, 650):
        pd.testing.assert_frame_equal(strategy.gerar_sinais(data.iloc[:n]), full.iloc[:n])
        pd.testing.assert_frame_equal(daily_vwap(data.iloc[:n]), full_vwap.iloc[:n])
    changed = data.copy()
    changed.loc[data.index[361:], ["real_volume", "tick_volume"]] = 999.
    changed.loc[data.index[361:], ["high", "low", "close"]] *= 10
    pd.testing.assert_frame_equal(strategy.gerar_sinais(changed).iloc[:361], full.iloc[:361])


def test_first_partial_day_is_unavailable_and_observed_transition_starts_approximation():
    data = market(4).assign(time=pd.to_datetime(["2026-10-19 09:00", "2026-10-19 23:55",
        "2026-10-21 09:00", "2026-10-21 09:05"]))
    values = daily_vwap(data, 1).vwap
    assert values.iloc[:2].isna().all() and values.iloc[2:].notna().all()
    assert values.iloc[2] == pytest.approx((data.high.iloc[2]+data.low.iloc[2]+data.close.iloc[2])/3)
    assert daily_vwap(data.iloc[:1].assign(time=pd.to_datetime(["2026-10-19"])), 1).vwap.notna().all()


@pytest.mark.parametrize("column", ["high", "low", "close", "tick_volume", "real_volume"])
@pytest.mark.parametrize("bad", [np.nan, np.inf, -np.inf, EMPTY, -1.])
def test_invalid_selected_data_poisons_rest_of_date_then_recovers(column, bad):
    clean = market(6).assign(time=pd.to_datetime(["2026-10-19 00:00", "2026-10-19 00:05",
        "2026-10-19 00:10", "2026-10-19 00:15", "2026-10-20 09:00", "2026-10-20 09:05"]), real_volume=1.)
    data = clean.copy()
    data.loc[1, column] = bad
    source = 2 if column == "real_volume" else 1
    values = daily_vwap(data, source).vwap
    assert values.iloc[0] == daily_vwap(clean, source).vwap.iloc[0]
    assert values.iloc[1:4].isna().all() and values.iloc[4:].notna().all()


@pytest.mark.parametrize("column", ["high", "low", "close"])
def test_zero_hlc_is_invalid_even_when_selected_volume_is_zero(column):
    data = market(3).assign(tick_volume=0.)
    data.loc[1, column] = 0.
    data.loc[2, "tick_volume"] = 1.
    assert daily_vwap(data, 1).vwap.isna().all()


def test_unselected_volume_does_not_poison_selected_but_switch_uses_real_day_history():
    data = market(4).assign(real_volume=[0., -1., 2., 2.])
    assert daily_vwap(data, 1).vwap.notna().all()
    auto = daily_vwap(data)
    assert auto.vwap.iloc[:2].notna().all() and auto.vwap.iloc[2:].isna().all()
    assert auto.volume_source.tolist() == [1, 1, 2, 2]
    real = data.assign(real_volume=1., tick_volume=np.nan)
    assert daily_vwap(real, 2).vwap.notna().all()
    assert daily_vwap(real).vwap.notna().all()
    assert daily_vwap(data.assign(real_volume=0.), 2).vwap.isna().all()


@pytest.mark.parametrize("high,volume", [(1e308, 1.), (1e200, 1e200), (1., 1e308)])
def test_overflow_poison_is_not_repaired_by_later_finite_bar(high, volume):
    data = market(4).assign(high=high, low=high, close=high, tick_volume=volume)
    # Primeiro caso: soma H+L+C. Segundo: produto. Terceiro: volume acumulado.
    data.loc[3, ["high", "low", "close", "tick_volume"]] = 1.
    values = daily_vwap(data, 1).vwap
    assert values.iloc[1:].isna().all()


@pytest.mark.parametrize("source,required,irrelevant", [(0, "real_volume", None),
    (0, "tick_volume", None), (1, "tick_volume", "real_volume"), (2, "real_volume", "tick_volume")])
def test_required_volume_columns_are_explicit_and_no_volume_is_invented(source, required, irrelevant):
    data = market(5)
    strategy = get_strategy("vwap_bands", volume_source=source)
    with pytest.raises(ValueError, match=required):
        strategy.gerar_sinais(data.drop(columns=required))
    with pytest.raises(ValueError, match=required):
        daily_vwap(data.drop(columns=required), source)
    if irrelevant:
        pd.testing.assert_frame_equal(strategy.gerar_sinais(data.drop(columns=irrelevant)), strategy.gerar_sinais(data))


@pytest.mark.parametrize("times", [[1, 2], ["2026-10-19", None], ["2026-10-19", "2026-10-19"],
    ["2026-10-20", "2026-10-19"], ["2026-10-19T00:00:00Z", "2026-10-19T00:05:00Z"]])
def test_ambiguous_missing_or_noncausal_times_are_rejected(times):
    with pytest.raises(ValueError, match="horários naive"):
        daily_vwap(market(2).assign(time=times), 1)


@pytest.mark.parametrize("low,high,buy,sell", [(99., 100., True, False), (100., 101., False, True),
    (99., 101., False, False), (99.001, 100.999, False, False)])
def test_exact_touch_both_bands_neutral_and_first_eligible_bar(low, high, buy, sell):
    data = market(300).assign(high=100., low=100., close=100., tick_volume=1., real_volume=0.)
    # Volume zero preserva a VWAP100 enquanto a barra de sinal toca uma/both bandas.
    data.loc[299, ["high", "low", "tick_volume"]] = [high, low, 0.]
    strategy = get_strategy("vwap_bands", volume_source=1, target_band_pct=0.)
    signals = strategy.gerar_sinais(data)
    assert not signals.entry_long.iloc[:299].any() and not signals.entry_short.iloc[:299].any()
    assert signals.entry_long.iloc[299] == buy and signals.entry_short.iloc[299] == sell
    if buy or sell:
        assert signals.target_long.iloc[299] == signals.target_short.iloc[299] == 100.
    assert strategy.min_bars == 300
    assert not strategy.exit_rules().move_guard_from_signal_stop


@pytest.mark.parametrize("name,value", [("entry_band_pct", 0), ("entry_band_pct", 10.1),
    ("entry_band_pct", True), ("entry_band_pct", np.nan), ("entry_band_pct", np.inf),
    ("entry_band_pct", 10**400), ("target_band_pct", -.1), ("target_band_pct", 1),
    ("target_band_pct", np.nan), ("target_band_pct", "0"), ("stop_offset_points", 0),
    ("stop_offset_points", 1.), ("stop_offset_points", True), ("stop_offset_points", 2147483648),
    ("volume_source", -1), ("volume_source", 3), ("volume_source", True), ("volume_source", 1.)])
def test_typed_limits_and_cross_constraints(name, value):
    with pytest.raises(ValueError, match="VWAP requer"):
        get_strategy("vwap_bands", **{name: value})


def test_composition_units_registration_roundtrip_and_no_native_capability():
    value = base_composition("vwap_bands", symbol="TEST", timeframe="M5")
    assert Composition(value.to_json()).config_hash == value.config_hash
    assert not compatibility(value) and compatibility(value, backend="mql5")
    params = STRATEGY_PARAMETERS["vwap_bands"]
    assert set(params) == set(get_strategy("vwap_bands").params)
    assert params["stop_offset_points"].unit == "point"
    assert params["entry_band_pct"].unit == params["target_band_pct"].unit == "percent"
    assert not params["volume_source"].accepts(True) and not params["stop_offset_points"].accepts(1.)
    bad = value.to_dict()
    bad["strategy"]["params"]["target_band_pct"] = 1.
    with pytest.raises(ValueError):
        BaseAdapter(Composition(bad)).strategy()
