"""T19.37: S09 mínima, referência independente e motor existente, sem MT5 real."""
from decimal import Decimal, localcontext
import io

import numpy as np
import pandas as pd
import pytest

from mt5_extracao.composition import BaseAdapter, Composition, base_composition, compatibility
from mt5_extracao.strategies import get_strategy
from mt5_extracao.strategies.backtester import BacktestConfig, SymbolSpec, run_backtest

VALUES=[10,10,10,9,8,7,8,9,10,10,10,9,8,7,7,8,9,10,11,12,11,10,9,8,7,6,7,8,9,10]
PARAMS=dict(fast=2,slow=3,warmup_bars=4,stop_points=2,target_points=4)


def frame(values=VALUES):
    return pd.DataFrame(dict(time=pd.date_range("2026-10-14",periods=len(values),freq="5min"),
        open=values,high=np.array(values)+1,low=np.array(values)-1,close=np.asarray(values,dtype=float),spread=0))


def reference(values,period):
    with localcontext() as ctx:
        ctx.prec=40;alpha=Decimal(2)/Decimal(period+1);out=[]
        for value in values:
            value=Decimal(str(value));out.append(value if not out else alpha*value+(1-alpha)*out[-1])
        return out


def test_decimal_reference_closed_cross_equalities_warmup_and_prefix_causality():
    strategy=get_strategy("s09_reference",**PARAMS);data=frame();signals=strategy.gerar_sinais(data)
    fast,slow=reference(VALUES,2),reference(VALUES,3)
    for i in range(len(data)):
        assert signals.entry_long.iloc[i]==(i>=3 and fast[i-1]<=slow[i-1] and fast[i]>slow[i])
        assert signals.entry_short.iloc[i]==(i>=3 and fast[i-1]>=slow[i-1] and fast[i]<slow[i])
        pd.testing.assert_frame_equal(strategy.gerar_sinais(data.iloc[:i+1]),signals.iloc[:i+1])
    assert signals.entry_long.any() and signals.entry_short.any()
    assert not signals.exit_long.any() and not signals.exit_short.any()
    assert signals.stop_long.isna().all() and signals.stop_short.isna().all()
    assert signals.target_long.isna().all() and signals.target_short.isna().all()


@pytest.mark.parametrize("value",[np.nan,np.inf,-np.inf,0,-1])
def test_absence_invalidates_prefix_instead_of_filling_missing_history(value):
    data=frame();data.loc[4,"close"]=value
    signals=get_strategy("s09_reference",**PARAMS).gerar_sinais(data)
    assert not signals.entry_long.iloc[4:].any() and not signals.entry_short.iloc[4:].any()


@pytest.mark.parametrize("params",[dict(fast=0),dict(slow=2),dict(fast=3.5),dict(warmup_bars=3),
                                   dict(stop_points=True),dict(target_points=.5),dict(warmup_bars=100001)])
def test_ambiguous_or_invalid_parameters_are_rejected(params):
    with pytest.raises(ValueError,match="S09 requer"):
        get_strategy("s09_reference",**dict(PARAMS,**params))


def test_composition_default_equals_strategy_and_preserves_materialized_trade_export():
    data=base_composition("s09_reference",symbol="TEST",timeframe="M5").to_dict()
    data["strategy"]["params"]=PARAMS
    composition=Composition(data)
    strategy=get_strategy("s09_reference",**PARAMS)
    spec=SymbolSpec(symbol="TEST",point=1,tick_size=1,tick_value=1,volume_min=.01,volume_step=.01,volume_max=100)
    config=BacktestConfig(initial_equity=1000,risk_per_trade=.01,slippage_points=0,swap=False)
    direct=run_backtest(frame(),strategy,spec,config)
    adapted=BaseAdapter(composition).run(frame(),spec,config,timeframe="M5")
    assert direct.trades==adapted.trades and len(adapted.trades)>0
    for trade in adapted.trades:
        assert abs(trade.entry_price-trade.stop_price)==2
        assert abs(trade.entry_price-trade.target_price)==4
    stored=pd.read_csv(io.StringIO(adapted.trades_frame().to_csv(index=False)))
    assert stored.stop_price.tolist()==[trade.stop_price for trade in adapted.trades]
    assert stored.target_price.tolist()==[trade.target_price for trade in adapted.trades]
    assert Composition(composition.to_json()).config_hash==composition.config_hash
    assert compatibility(composition, backend="native")


def test_default_is_not_ema_atr_or_a_promise_of_original_video_rules():
    s09=get_strategy("s09_reference")
    old=get_strategy("ema_atr")
    assert s09.params==dict(fast=20,slow=50,warmup_bars=250,stop_points=100,target_points=200)
    assert "trend" not in s09.params and s09.exit_rules().trailing_atr is None
    assert old.params==dict(fast=20,slow=50,trend=200,stop_atr=2.0,trail_atr=3.0)
