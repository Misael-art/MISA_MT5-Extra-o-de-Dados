"""I10: referência de listas independente; canal anterior, revisões e serviço real."""
import json
from decimal import Decimal
import numpy as np
import pandas as pd
import pytest
from mt5_extracao.strategies import indicators
from mt5_extracao.analytics import indicator_registry as registry, indicator_materializer as materializer, feature_store, indicator_context, resampling
from mt5_extracao.data_exporter import DataExporter
from mt5_extracao.database_manager import DatabaseManager
from tests.test_indicator_materialization import db, source, seed, facts
IDS=[d['feature_id'] for d in registry.definitions() if d['feature_id'].startswith('channel.donchian_')]

@pytest.mark.parametrize('reference,shift',[('current',0),('previous',1)])
def test_independent_windows_strict_previous_reference_and_prefix(reference,shift):
    frame=pd.DataFrame(dict(high=[100+i%7+i//7 for i in range(70)],low=[90+i%5 for i in range(70)],close=[95+i%8 for i in range(70)]))
    result=indicators.donchian_context(frame,20,reference=reference)
    for i in range(19+shift,len(frame)):
        first,last=i-19-shift,i+1-shift
        upper=max(Decimal(str(v)) for v in frame.high.iloc[first:last]);lower=min(Decimal(str(v)) for v in frame.low.iloc[first:last]);close=Decimal(str(frame.close.iloc[i]))
        expected=dict(upper=upper,lower=lower,mid=(upper+lower)/2,width=upper-lower,position=(close-lower)/(upper-lower),distance_upper=close-upper,distance_lower=close-lower)
        for key,value in expected.items():assert result[key].iloc[i]==float(value)
    pd.testing.assert_frame_equal(result.iloc[:40],indicators.donchian_context(frame.iloc[:40],20,reference=reference))
    trap=frame.copy();trap.loc[20,'high']=10000;prior=indicators.donchian_context(trap)
    assert prior.upper.iloc[20]==max(frame.high.iloc[:20]) and trap.high.iloc[20]>prior.upper.iloc[20]
    assert indicators.donchian_context(trap,reference='current').upper.iloc[20]==10000

@pytest.mark.parametrize('bad',[np.nan,np.inf,'range'])
def test_missing_and_known_ineligible_bar_require_new_consecutive_window(bad):
    frame=pd.DataFrame(dict(high=[101.]*70,low=[99.]*70,close=[100.]*70))
    frame.loc[30,'high']=98 if bad=='range' else bad
    result=indicators.donchian_context(frame)
    assert result.upper.iloc[30:51].isna().all() and result.upper.iloc[51]==101
    rows=registry.compute_core(frame,feature_ids=['channel.donchian_20_previous_upper'])
    assert rows[30]['state']=='SOURCE_VALUE_MISSING' and rows[31]['state']=='DEPENDENCY_UNAVAILABLE'
    frame.loc[30,'high']=101;frame['_channel_reset_before']=False;frame.loc[30,'_channel_reset_before']=True
    reset=indicators.donchian_context(frame);assert reset.upper.iloc[30:50].isna().all() and reset.upper.iloc[50]==101


def test_flat_channel_and_missing_columns_do_not_fill_normalized_position():
    frame=pd.DataFrame(dict(high=[100.]*25,low=[100.]*25,close=[100.]*25))
    rows=registry.compute_core(frame,feature_ids=IDS)
    for row in rows:
        if row['bar_index']<21:continue
        assert row['value'] is None and row['state']=='INVALID_DENOMINATOR' if row['feature_id'].endswith('_position') else row['state']=='AVAILABLE'
    assert all(r['state']=='SOURCE_UNAVAILABLE' for r in registry.compute_core(frame.drop(columns='low'),feature_ids=IDS))
    with pytest.raises(ValueError):indicators.donchian_context(frame,reference='future')


def test_selection_isolated_from_legacy_and_definitions_preserve_existing(monkeypatch):
    monkeypatch.setattr(indicators,'alligator',lambda *_:pytest.fail('unrelated Alligator'))
    monkeypatch.setattr(indicators,'ao_ac',lambda *_:pytest.fail('unrelated AO'))
    rows=registry.compute_core(pd.DataFrame(dict(high=[101.]*25,low=[99.]*25,close=[100.]*25)),feature_ids=IDS)
    assert len(rows)==25*14 and len(IDS)==14
    spec=registry.get_definition('channel.donchian_20_previous_upper')
    assert spec['warmup_count']==21 and spec['parameters']['reference_shift_bars']==1
    assert registry.get_definition('trend.efficiency_ratio_20')['warmup_count']==21


def test_materialize_repeat_append_reopen_context_and_csv(db,tmp_path):
    seed(db,source(30));materializer.materialize(db,'TEST','M5',feature_ids=IDS,batch_size=53);prefix=facts(db)
    seed(db,source(60));added=materializer.materialize(db,'TEST','M5',feature_ids=IDS,batch_size=53)
    assert added['inserted']==30*len(IDS) and added['write_from']==30 and all(r in facts(db) for r in prefix)
    assert materializer.materialize(db,'TEST','M5',feature_ids=IDS)['skipped']
    path=tmp_path/'Donchian anterior.csv';DataExporter(db).export_indicator_values(path,symbol='TEST',timeframe='M5',as_of='2026-10-20T00:00Z',feature_ids=IDS)
    csv=pd.read_csv(path,keep_default_na=False,float_precision='round_trip');bars=resampling.latest_bars(db,'TEST','M5');calculated=registry.compute_core(bars,feature_ids=IDS)
    for ident in IDS:
        pure=np.array([r['value'] if r['value'] is not None else np.nan for r in calculated if r['feature_id']==ident])
        stored=feature_store.query_as_of(db,'2026-10-20T00:00Z',symbol='TEST',timeframe='M5',feature_id=ident,algorithm_version=materializer.algorithm_version(ident))
        exported=np.array([float(v) if v!='' else np.nan for v in csv[ident+'::value']])
        np.testing.assert_array_equal(pure,stored.numeric_value.to_numpy());np.testing.assert_array_equal(pure,exported)
        assert json.loads(csv[ident+'::parameters'].iloc[-1])['reference'] in ('current','previous')
    close=pd.Timestamp('2026-09-01T09:00Z')+pd.Timedelta(minutes=5*21)
    ctx=indicator_context.context(db,pd.DataFrame(dict(decision_time=[close-pd.Timedelta(nanoseconds=1),close])),symbol='TEST',timeframe='M5',feature_ids=['channel.donchian_20_previous_upper'])
    assert ctx.iloc[0]['channel.donchian_20_previous_upper::state']!='AVAILABLE' and ctx.iloc[1]['channel.donchian_20_previous_upper::state']=='AVAILABLE'
    before=facts(db);db.close();reopened=DatabaseManager(db_path=db.db_path)
    try:assert materializer.materialize(reopened,'TEST','M5',feature_ids=IDS)['skipped'] and facts(reopened)==before
    finally:reopened.close()


def test_late_correction_preserves_asof_and_invalid_bar_does_not_compress_window(db):
    frame=source(70);seed(db,frame);materializer.materialize(db,'TEST','M5',feature_ids=IDS)
    old=feature_store.query_as_of(db,'2026-09-02T00:00Z',symbol='TEST')
    changed=frame.iloc[[30]].copy();changed['high']+=100;seed(db,changed)
    receipt=resampling.latest_bars(db,'TEST','M5').computed_at_utc.max();materializer.materialize(db,'TEST','M5',feature_ids=IDS)
    pd.testing.assert_frame_equal(old,feature_store.query_as_of(db,receipt-pd.Timedelta(nanoseconds=1),symbol='TEST'))
    after=feature_store.query_as_of(db,receipt,symbol='TEST');assert not after[after.revision>1].empty


def test_known_ineligible_bar_resets_materialized_channel_and_preserves_asof(db,monkeypatch):
    seed(db,source(70));materializer.materialize(db,'TEST','M5',feature_ids=IDS)
    before=feature_store.query_as_of(db,'2026-09-02T00:00Z',symbol='TEST')
    raw=resampling.latest_bars(db,'TEST','M5',closed_only=False);cutoff=pd.Timestamp('2026-10-20T12:00Z')
    raw.loc[30,'quality_status']='PARTIAL';raw.loc[30,'revision']=2;raw.loc[30,'computed_at_utc']=cutoff
    closed=raw.drop(index=30).reset_index(drop=True)
    monkeypatch.setattr(resampling,'latest_source_sequence',lambda *a,**k:2)
    monkeypatch.setattr(resampling,'latest_bars',lambda *a,closed_only=True,**k:closed.copy() if closed_only else raw.copy())
    result=materializer.materialize(db,'TEST','M5',feature_ids=IDS)
    assert result['write_from']==0 and result['revised']>0
    pd.testing.assert_frame_equal(before,feature_store.query_as_of(db,cutoff-pd.Timedelta(nanoseconds=1),symbol='TEST'))
    after=feature_store.query_as_of(db,cutoff,symbol='TEST',feature_id='channel.donchian_20_previous_upper')
    for offset in range(1,21):
        row=after[after.observation_time==raw.iloc[30+offset].bar_start_utc].iloc[0]
        assert row.quality=='UNAVAILABLE' and row.available_at_utc>=cutoff
    assert after[after.observation_time==raw.iloc[51].bar_start_utc].iloc[0].quality=='VALID'


def test_finite_inputs_with_overflowing_width_do_not_create_zero_position():
    frame=pd.DataFrame(dict(high=[1e308]*25,low=[-1e308]*25,close=[0.]*25))
    with np.errstate(over='ignore'):
        result=indicators.donchian_context(frame)
        assert result['position'].iloc[21:].isna().all()
        rows=registry.compute_core(frame,feature_ids=['channel.donchian_20_previous_position'])
    assert rows[-1]['state']=='INVALID_RESULT' and rows[-1]['value'] is None
