"""Store/revisões com adaptador controlado declarado, sem alegação de paridade MQL."""
import json

import pandas as pd
import pytest
from sqlalchemy import text

from mt5_extracao import sdk,cli
from mt5_extracao.analytics import super_atr,datasets,feature_store,producer_jobs
from tests.test_mentions_contract import db


@pytest.fixture
def adapter(monkeypatch):
    state=dict(closed=0,cancel=False,trigger_cancel=False,available=True)
    class Model:
        def calculate(self,bars,period,mean,deviation):
            if state['trigger_cancel']:state['cancel']=True
            return [dict(atr=float('nan'),level=float('nan'),side=0) if i<period-1 else dict(atr=0.,level=row['close'],side=1) for i,row in enumerate(bars)]
        def close(self):state['closed']+=1
    def source(*a,**k):
        return dict(framework_commit='1'*40,header_sha256='2'*64,adapter_sha256='3'*64,compiler='CONTROLLED TEST PROVIDER',available=state['available'],
            qualification='CONTROLLED_TEST_ADAPTER_NOT_MQL_PARITY',synthetic_fixture=True,native_mt5_qualified=False),Model
    monkeypatch.setattr(super_atr,'source',source);return state


def prices(db,count=6):
    values=[100.+i for i in range(count)]
    db.save_ohlcv_data('QA','1 minuto',pd.DataFrame(dict(time=pd.date_range('2025-01-01',periods=count,freq='min'),open=values,high=[v+1 for v in values],low=[v-1 for v in values],close=values,tick_volume=10,real_volume=0,spread=1)))
    return datasets.capture(db,'QA','M1')


def materialize(db,snapshot,**kwargs):return sdk.materialize_super_atr_snapshot(db,snapshot.manifest_id,framework_root='CONTROLLED_TEST_SOURCE',period=3,**kwargs)


def test_super_atr_store_zero_warmup_receipt_and_idempotence(db,adapter):
    snapshot=prices(db);before=datasets.identity(db);result=materialize(db,snapshot)
    assert result['counts']==dict(inserted=24,revised=0,unchanged=0)
    frame=feature_store.query_history(db);assert len(frame)==24 and frame.available_at_utc.min()>pd.Timestamp('2025-01-01T00:06Z')
    atr=frame.loc[frame.feature_id=='trend.super_atr.atr'];assert list(atr.quality)==['UNAVAILABLE']*2+['VALID']*4
    assert list(atr.numeric_value.dropna())==[0.]*4
    meta=json.loads(atr.metadata_json.iloc[-1]);assert meta['synthetic_fixture'] and meta['source_manifest_id']==snapshot.manifest_id
    assert not meta['historical_receipt_proven'] and datasets.identity(db)==before
    assert feature_store.query_as_of(db,'2025-01-01T00:06Z').empty
    assert materialize(db,snapshot)['counts']==dict(inserted=0,revised=0,unchanged=24)
    assert adapter['closed']==2 and datasets.for_run(db,result['job']['run_id']).manifest_id==snapshot.manifest_id


def test_append_preserves_first_provenance_and_correction_preserves_old_cut(db,adapter):
    first=prices(db);result=materialize(db,first)
    extended=prices(db,7);appended=materialize(db,extended)
    assert appended['algorithm_version']==result['algorithm_version'] and appended['counts']==dict(inserted=4,revised=0,unchanged=24)
    cut=pd.Timestamp.now(tz='UTC');old=feature_store.query_as_of(db,cut)
    source=db.load_ohlcv('QA','1 minuto');source.loc[3,'close']+=.25;db.save_ohlcv_data('QA','1 minuto',source)
    corrected=datasets.capture(db,'QA','M1');changed=materialize(db,corrected)
    assert changed['counts']==dict(inserted=0,revised=16,unchanged=12)
    pd.testing.assert_frame_equal(feature_store.query_as_of(db,cut),old)
    now=feature_store.query_as_of(db,pd.Timestamp.now(tz='UTC'));level=now[now.feature_id=='trend.super_atr.level'].sort_values('observation_time')
    assert json.loads(level.metadata_json.iloc[0])['source_manifest_id']==first.manifest_id
    assert json.loads(level.metadata_json.iloc[3])['source_manifest_id']==corrected.manifest_id


def test_bad_hlc_invalidates_suffix_and_cancel_has_no_values(db,adapter):
    snapshot=prices(db);adapter['trigger_cancel']=True
    with pytest.raises(super_atr.indicator_materializer.Cancelled):materialize(db,snapshot,cancel_check=lambda:adapter['cancel'])
    assert feature_store.query_history(db).empty and adapter['closed']==1
    adapter.update(cancel=False,trigger_cancel=False)
    data=db.load_ohlcv('QA','1 minuto');data.loc[3,'close']=500;db.save_ohlcv_data('QA','1 minuto',data)
    result=materialize(db,datasets.capture(db,'QA','M1'))
    frame=feature_store.query_as_of(db,pd.Timestamp.now(tz='UTC'));level=frame[frame.feature_id=='trend.super_atr.level'].sort_values('observation_time')
    assert list(level.quality)==['UNAVAILABLE','UNAVAILABLE','VALID','UNAVAILABLE','UNAVAILABLE','UNAVAILABLE']
    assert json.loads(level.metadata_json.iloc[-1])['reason']=='INVALID_INPUT_OR_DEPENDENT_SUFFIX'


def test_checkpoint_failure_rolls_back_values_and_source_binding(db,adapter,monkeypatch):
    snapshot=prices(db)
    def fail(*a,**k):raise RuntimeError('QA checkpoint failure')
    monkeypatch.setattr(producer_jobs,'advance_checkpoint',fail)
    with pytest.raises(RuntimeError,match='checkpoint'):materialize(db,snapshot)
    assert feature_store.query_history(db).empty
    with db.engine.connect() as conn:
        run=conn.execute(text("SELECT run_id,status FROM _analytics_producer_runs ORDER BY started_at_utc DESC")).first()
        assert run.status=='FAILED'
    assert datasets.for_run(db,run.run_id) is None


def test_cli_and_sdk_share_materialization_contract_and_seed_version(db,adapter,tmp_path,capsys,monkeypatch):
    snapshot=prices(db);monkeypatch.setattr(cli.Context,'db',property(lambda _:db))
    args=['--json','analytics','features','super-atr-materialize','--manifest',snapshot.manifest_id,'--framework','CONTROLLED_TEST_SOURCE','--period','3']
    assert cli.main(args)==0;r=json.loads(capsys.readouterr().out)['data']['materialization']
    shifted=datasets.capture(db,'QA','M1',start='2025-01-01T00:01:00')
    assert materialize(db,shifted)['algorithm_version']!=r['algorithm_version']
    adapter['available']=False
    with pytest.raises(ValueError,match='Compilador'):materialize(db,snapshot)


def test_missing_framework_is_an_actionable_precondition(tmp_path):
    with pytest.raises(ValueError,match='Framework ausente'):super_atr.capabilities(tmp_path/'missing')
