"""BD07: dependência opcional não produz sucesso falso; variantes de retorno nomeadas."""
import types

import pandas as pd

import mt5_extracao.indicator_calculator as legacy
from mt5_extracao.analytics import indicator_contracts
from tests.market_data import random_walk


def test_without_pandas_ta_missing_outputs_are_declared(monkeypatch):
    monkeypatch.setattr(legacy, "ta", None)
    calc = legacy.IndicatorCalculator()
    out = calc.calculate_technical_indicators(random_walk(120, freq="min"))
    assert set(calc.unavailable_outputs) == set(legacy.IndicatorCalculator.UNAVAILABLE_WITHOUT_PANDAS_TA)
    assert out.attrs["unavailable_outputs"]["macd_line"] == "pandas-ta ausente"
    assert out["macd_line"].isna().all() and out["rsi"].notna().any()


def test_basic_indicators_preserve_warmup_missing_values_and_causality(monkeypatch):
    monkeypatch.setattr(legacy, "ta", None)
    close = pd.Series(range(100, 144), index=pd.date_range("2025-01-02", periods=44, freq="min"), dtype=float)
    data = pd.DataFrame({"open": close, "close": close, "high": close + 1, "low": close - 1})
    calc = legacy.IndicatorCalculator()
    result = calc.calculate_technical_indicators(data.copy())
    assert result.attrs["indicator_backend"]["name"] == "BasicIndicators"
    assert result.index.equals(data.index)
    assert result["rsi"].iloc[:14].isna().all() and result["rsi"].iloc[14] == 100
    assert result["atr"].iloc[:14].isna().all() and result["atr"].iloc[14] == 2
    assert result["ma_20"].iloc[:19].isna().all() and result["ma_20"].iloc[19] == 109.5

    prefix = calc.calculate_technical_indicators(data.iloc[:30].copy())
    pd.testing.assert_frame_equal(result.iloc[:30], prefix)
    data.loc[data.index[20], "close"] = float("nan")
    with_gap = calc.calculate_technical_indicators(data.copy())
    assert with_gap["rsi"].iloc[20:35].isna().all()
    assert with_gap["rsi"].iloc[35] == 100


def test_with_pandas_ta_nothing_is_declared_missing(monkeypatch):
    def frame(df, **names):
        return pd.DataFrame({name: df * 0 + 1.0 for name in names})

    stub = types.SimpleNamespace(
        rsi=lambda close, length=14: close * 0 + 50.0, sma=lambda close, length=20: close,
        macd=lambda close: frame(close, MACD_12_26_9=1, MACDs_12_26_9=1, MACDh_12_26_9=1),
        bbands=lambda close: frame(close, **{"BBU_20_2.0": 1, "BBL_20_2.0": 1}),
        atr=lambda h, l, c, length=14: c * 0 + 1.0, true_range=lambda h, l, c: c * 0 + 1.0)
    monkeypatch.setattr(legacy, "ta", stub)
    calc = legacy.IndicatorCalculator()
    out = calc.calculate_technical_indicators(random_walk(60, freq="min"))
    assert calc.unavailable_outputs == {} and "unavailable_outputs" not in out.attrs
    assert out["macd_line"].notna().all()


def test_return_variants_are_distinct_contracts():
    """price.return_N (barras) ≠ var_pct_* legado (janelas de relógio) ≠ session.return (sessão)."""
    pilot = indicator_contracts.contract("price.return_5")
    assert pilot["parameters"] == {"horizon_bars": 5, "price_basis": "close"}
    assert pilot["unit"] == "fraction" and pilot["output_kind"] == "number"
