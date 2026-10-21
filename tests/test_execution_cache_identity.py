"""T20.25: identidade e integridade do cache canônico existente."""
import configparser
from pathlib import Path
import numpy as np
import pandas as pd
import pytest
from sqlalchemy import text

from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.trade_analytics import cache, enrich, ledger
from mt5_extracao.trade_analytics import calendar as cal
from mt5_extracao.robots import store
from tests.market_data import trend


@pytest.fixture
def source(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "identity.sqlite"))
    bars = trend(80, freq="5min", start="2025-01-06 09:00")
    db.save_ohlcv_data("QA", "5 minutos", bars)
    ledger.save_trades(db, "r", [dict(direction=1, entry_time=bars.time.iloc[40], exit_time=bars.time.iloc[45],
                                     entry_price=100., exit_price=101., stop_price=99., target_price=102.,
                                     volume=1., pnl=1., mae=-1., mfe=2., bars=5)],
                       robot_key="python:qa", symbol="QA", timeframe="M5")
    yield db, bars
    db.close()


@pytest.mark.parametrize("column,value", [("volume",2.), ("exit_price",102.), ("stop_price",98.),
    ("target_price",103.), ("mae",-2.), ("mfe",3.), ("exit_reason","novo"), ("params_hash","novo"),
    ("source","novo"), ("robot_key","novo"), ("r_multiple",3.), ("bars",6)])
def test_all_trade_fields_participate_in_signature(source, column, value):
    db, _ = source
    trades = ledger.load(db)
    before = cache.signature(db,None,trades)
    trades.loc[0,column] = value
    assert cache.signature(db,None,trades) != before


def test_small_float_changes_are_not_rounded_away(source):
    db, _ = source
    trades = ledger.load(db)
    before = cache.signature(db,None,trades)
    trades.loc[0,"volume"] = np.nextafter(1., 2.)
    assert cache.signature(db,None,trades) != before


def test_code_change_invalidates_without_manually_bumping_feature_version(source,tmp_path,monkeypatch):
    db, _ = source
    marker = tmp_path / "dependency.py"; marker.write_text("version=1")
    monkeypatch.setattr(cache,"_CODE_FILES",(*cache._CODE_FILES,marker))
    trades = ledger.load(db)
    before = cache.signature(db,None,trades)
    marker.write_text("version=2")
    assert cache.signature(db,None,trades) != before


def test_cost_and_declared_protocol_change_invalidate(source):
    db, _ = source
    trades = ledger.load(db)
    config = configparser.ConfigParser()
    before = cache.signature(db,config,trades)
    config["STRATEGY"] = {"commission_per_lot":"1"}
    assert cache.signature(db,config,trades) != before
    store.save_run(db,dict(run_id="r",robot_key="python:qa",source="backtest",symbol="QA",timeframe="M5",
                          data_start="",data_end="",bars=80,params={},equity=[],trades=[],created_at="2025-01-01",
                          metrics=dict(protocol=dict(version=1,seed=7))))
    before = cache.signature(db,config,trades)
    with db.engine.begin() as conn:
        conn.execute(text("UPDATE _robot_runs SET metrics_json=:m WHERE run_id='r'"),
                     dict(m='{"protocol":{"version":2,"seed":7}}'))
    assert cache.signature(db,config,trades) != before


def test_calendar_content_change_not_hidden_by_same_count_and_timestamp(source):
    db,bars = source
    cal.save(db,"qa",[dict(event_time=bars.time.iloc[40],symbol="QA",name="anterior",impact=3)])
    trades = ledger.load(db)
    first = cache.features(db,None,trades)
    with db.engine.begin() as conn:
        conn.execute(text("UPDATE _economic_events SET name='corrigido'"))
    second = cache.features(db,None,trades)
    assert first.event_name.iloc[0] == "anterior" and second.event_name.iloc[0] == "corrigido"


@pytest.mark.parametrize("correction",["value","invalid","delete"])
def test_cached_payload_is_verified_before_reuse(source,monkeypatch,correction):
    db,_ = source
    trades = ledger.load(db)
    expected = cache.features(db,None,trades)
    with db.engine.begin() as conn:
        if correction == "delete":
            conn.execute(text("DELETE FROM _trade_features WHERE run_id='r'"))
        else:
            conn.execute(text("UPDATE _trade_features SET atr=:v WHERE run_id='r'"),dict(v="bad" if correction=="invalid" else 123456.))
    calls=[]; real=enrich.enrich
    monkeypatch.setattr(enrich,"enrich",lambda *a: calls.append(1) or real(*a))
    actual=cache.features(db,None,trades)
    assert calls == [1]
    pd.testing.assert_frame_equal(actual,expected,check_dtype=False)
    assert len(cache.signature(db,None,trades)) == 64


def test_frozen_input_does_not_follow_mid_computation_correction(source,monkeypatch):
    db,bars = source
    trades = ledger.load(db)
    original=enrich.enrich
    changed=[]
    def correcting(view,config,part):
        if not changed:
            changed.append(True)
            new=bars.copy();new.loc[39,"real_volume"]=98765
            db.save_ohlcv_data("QA","5 minutos",new)
        return original(view,config,part)
    monkeypatch.setattr(enrich,"enrich",correcting)
    first=cache.features(db,None,trades)
    second=cache.features(db,None,trades)
    assert first.entry_volume.iloc[0] != 98765 and second.entry_volume.iloc[0] == 98765


def test_float_payload_hash_survives_sql_object_to_numeric_roundtrip():
    left=pd.DataFrame(dict(atr=pd.Series([1.,None],dtype=object)))
    right=pd.DataFrame(dict(atr=[1.,float("nan")]))
    assert cache._frame_digest(left)==cache._frame_digest(right)


@pytest.fixture
def execution(tmp_path):
    from mt5_extracao.robots import catalog
    db = DatabaseManager(db_path=str(tmp_path / "execution.sqlite"))
    bars = trend(360, freq="h", start="2025-01-06 09:00")
    db.save_ohlcv_data("QA", "1 hora", bars)
    robot = next(r for r in catalog.python_robots() if r.key == "python:ema_atr")
    yield db, bars, robot
    db.close()


def test_producer_reuse_requires_frozen_identity_and_legacy_is_not_rewritten(execution):
    from mt5_extracao.robots import backtest
    db,bars,robot=execution
    record=backtest.run_python(db,None,robot,"QA","H1")
    assert record["metrics"]["execution_identity"]["cache_eligible"]
    hit=backtest.already_tested(db,robot,"QA","H1",None,"")
    assert hit["run_id"]==record["run_id"]
    with db.engine.begin() as conn:
        conn.execute(text("UPDATE _robot_runs SET metrics_json='{}' WHERE run_id=:r"),dict(r=record["run_id"]))
    assert backtest.already_tested(db,robot,"QA","H1",None,"") is None
    assert len(store.load_runs(db))==1  # o legado continua abrindo, sem prova retroativa


@pytest.mark.parametrize("key,value",[("commission_per_lot","1"),("slippage_points","2"),
                                      ("risk_per_trade","0.02"),("initial_equity","200000")])
def test_producer_cost_and_risk_changes_do_not_hit_previous_run(execution,key,value):
    from mt5_extracao.robots import backtest
    db,_,robot=execution
    backtest.run_python(db,None,robot,"QA","H1")
    config=configparser.ConfigParser();config["STRATEGY"]={key:value}
    assert backtest.already_tested(db,robot,"QA","H1",None,"",config=config) is None


def test_loaded_code_change_invalidates_producer_reuse_without_editing_source(execution,monkeypatch):
    from mt5_extracao.robots import backtest
    from mt5_extracao.strategies import indicators
    db,_,robot=execution
    backtest.run_python(db,None,robot,"QA","H1")
    original=indicators.ema
    def changed(*args,**kwargs):
        return original(*args,**kwargs)
    monkeypatch.setattr(indicators,"ema",changed)
    assert backtest.already_tested(db,robot,"QA","H1",None,"") is None


def test_parameter_and_spread_mode_changes_do_not_hit_default_run(execution):
    from mt5_extracao.robots import backtest
    from mt5_extracao.strategies import get_strategy
    db,_,robot=execution
    params=dict(get_strategy("ema_atr").params)
    key=next(iter(params));params[key]+=1
    backtest.run_python(db,None,robot,"QA","H1",params=params)
    assert backtest.already_tested(db,robot,"QA","H1",None,"") is None
    backtest.run_python(db,None,robot,"QA","H1",real_spread=False)
    assert backtest.already_tested(db,robot,"QA","H1",None,"") is None


def test_data_correction_and_period_change_do_not_hit_previous_run(execution):
    from mt5_extracao.robots import backtest
    db,bars,robot=execution
    backtest.run_python(db,None,robot,"QA","H1")
    assert backtest.already_tested(db,robot,"QA","H1",None,"",start=bars.time.iloc[10]) is None
    changed=bars.copy()
    changed.loc[39,["open","high","low","close"]]+=.01
    db.save_ohlcv_data("QA","1 hora",changed)
    assert backtest.already_tested(db,robot,"QA","H1",None,"") is None


def test_implicit_resume_resolves_checkpoint_but_new_data_starts_another_plan(execution):
    from mt5_extracao.robots import backtest,budget
    from mt5_extracao.timeframes import Timeframe
    db,bars,robot=execution
    first=budget.Budget(db)
    initial=backtest.run_batch(db,None,[robot],["QA"],[Timeframe.H1],budget=first)
    second=budget.Budget(db)
    recovered=backtest.run_batch(db,None,[robot],["QA"],[Timeframe.H1],resume=True,budget=second)
    assert second.batch_id==first.batch_id and recovered[0]["reuse_basis"]=="checkpoint"
    assert recovered[0]["run"]["run_id"]==initial[0]["run"]["run_id"]
    changed=bars.copy();changed.loc[39,["open","high","low","close"]]+=.01
    db.save_ohlcv_data("QA","1 hora",changed)
    third=budget.Budget(db)
    fresh=backtest.run_batch(db,None,[robot],["QA"],[Timeframe.H1],resume=True,budget=third)
    assert third.batch_id!=first.batch_id and not fresh[0]["skipped"]
    assert len(store.load_runs(db))==2


def test_checkpoint_cannot_point_to_another_combination(execution):
    from mt5_extracao.robots import backtest,budget
    from mt5_extracao.timeframes import Timeframe
    db,bars,robot=execution
    db.save_ohlcv_data("QB","1 hora",bars)
    first=budget.Budget(db)
    rows=backtest.run_batch(db,None,[robot],["QA","QB"],[Timeframe.H1],budget=first)
    saved=budget.checkpoint(db,first.batch_id)
    qa=next(entry for entry in saved["plan"]["jobs"] if entry["symbol"]=="QA")
    with db.engine.begin() as conn:
        conn.execute(text("UPDATE _robot_batch_jobs SET run_id=:r WHERE batch_id=:b AND job_key=:k"),
                     dict(r=rows[1]["run"]["run_id"],b=first.batch_id,k=qa["key"]))
    second=budget.Budget(db,batch_id=first.batch_id)
    result=backtest.run_batch(db,None,[robot],["QA","QB"],[Timeframe.H1],budget=second,batch_id=first.batch_id)
    assert result[0]["run"] is None and "não corresponde" in result[0]["error"]
    assert result[1]["skipped"] and len(store.load_runs(db))==2
