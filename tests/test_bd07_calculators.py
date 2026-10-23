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
