"""Tipos/valores exatos e integridade na reconstrução de blocos diários (T20.4-PERF)."""
from copy import deepcopy
import json
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import inspect

from mt5_extracao.analytics import datasets
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.strategies.specs import spec_from_symbol_info
from tests.market_data import trend


def payloads_for(frame):
    blocks = {}
    ids = datasets._blocks(frame, 'time', blocks)
    return [json.loads(blocks[ident]) for ident in ids]


PRIMITIVES = ['int8','int16','int32','int64','uint8','uint16','uint32','uint64',
              'float32','float64','bool','datetime64[s]','datetime64[ms]','datetime64[us]','datetime64[ns]']


@pytest.mark.parametrize('dtype', PRIMITIVES)
def test_primitive_daily_blocks_keep_exact_values_dtypes_order_and_missing(dtype):
    if dtype.startswith(('int','uint')):
        info=np.iinfo(dtype);values=[int(info.min),0,int(info.max)]
    elif dtype.startswith('float'):
        info=np.finfo(dtype);values=[-0.,1/3,info.tiny,info.max,np.nan,np.inf,-np.inf]
    elif dtype=='bool':values=[True,False,True]
    else:values=[pd.Timestamp('2025-01-01T00:00:00.123456789'),pd.NaT,pd.Timestamp('2025-02-01')]
    frame=pd.DataFrame(dict(time=pd.date_range('2025-01-01',periods=len(values),freq='D'),value=pd.Series(values,dtype=dtype)))
    payloads=payloads_for(frame);assert len(payloads)==len(frame)
    before=deepcopy(payloads);actual=datasets._decode_frames(payloads)
    pd.testing.assert_frame_equal(actual,frame,check_exact=True)
    assert payloads==before
    if dtype.startswith('float'):assert np.signbit(actual.value.iloc[0])


def test_mixed_primitive_columns_do_not_round_signed_or_unsigned_64_bit_values():
    frame=pd.DataFrame(dict(time=pd.date_range('2025-01-01',periods=3,freq='D'),
        price=pd.Series([1.25,0.,-0.],dtype='float64'),
        signed=pd.Series([2**53+1,2**63-1,-2**63],dtype='int64'),
        unsigned=pd.Series([2**63+1,2**64-1,0],dtype='uint64'),flag=[False,True,False]))
    actual=datasets._decode_frames(payloads_for(frame))
    pd.testing.assert_frame_equal(actual,frame,check_exact=True)
    assert actual.signed.tolist()==[2**53+1,2**63-1,-2**63]
    assert actual.unsigned.tolist()==[2**63+1,2**64-1,0]
    assert np.signbit(actual.price.iloc[-1])


@pytest.mark.parametrize('dtype,values',[
    ('object',[2**53+1,None,3.5]),
    ('Int64',[2**53+1,pd.NA,-(2**53+1)]),
    ('UInt64',[2**64-1,pd.NA,2**63+1]),
    ('Float64',[0.,pd.NA,-1.25]),
    ('Float32',[0.,pd.NA,-1.25]),
    ('boolean',[True,pd.NA,False]),
    ('string',['','ação',pd.NA]),
    ('datetime64[ns, Europe/Berlin]',[pd.NaT,pd.NaT,pd.NaT]),
])
def test_complex_columns_keep_per_block_missing_and_precision_semantics(dtype,values):
    frame=pd.DataFrame(dict(time=pd.date_range('2025-01-01',periods=3,freq='D'),value=pd.Series(values,dtype=dtype)))
    actual=datasets._decode_frames(payloads_for(frame))
    pd.testing.assert_frame_equal(actual,frame,check_exact=True)
    if dtype=='object':assert type(actual.value.iloc[0]) is int and actual.value.iloc[0]==2**53+1


def test_timezone_offsets_changing_at_dst_keep_valid_per_block_decoding():
    frame=pd.DataFrame(dict(time=pd.date_range('2025-03-29T12:00',periods=3,freq='D',tz='Europe/Berlin'),value=[1,2,3]))
    payloads=payloads_for(frame)
    assert len(payloads)==3 and payloads[0]['rows'][0][0]['timestamp'].endswith('+01:00')
    assert payloads[-1]['rows'][0][0]['timestamp'].endswith('+02:00')
    pd.testing.assert_frame_equal(datasets._decode_frames(payloads),frame,check_exact=True)


def test_complex_payloads_are_decoded_before_the_next_block_is_materialized(monkeypatch):
    frame=pd.DataFrame(dict(value=pd.Series([2**53+1,None],dtype='object')))
    payloads=[datasets._frame_payload(frame.iloc[i:i+1]) for i in range(2)]
    decode=datasets._decode_frame;decoded=[]
    def spy(payload):
        result=decode(payload);decoded.append(True);return result
    def stream():
        yield payloads[0]
        assert decoded==[True],'Complex JSON blocks were buffered before decoding.'
        yield payloads[1]
    monkeypatch.setattr(datasets,'_decode_frame',spy)
    pd.testing.assert_frame_equal(datasets._decode_frames(stream()),frame,check_exact=True)


def test_different_schemas_dtypes_and_empty_chunks_keep_canonical_concat_semantics():
    frames=[pd.DataFrame(dict(value=pd.Series([2**63-1],dtype='int64'))),
            pd.DataFrame(dict(other=pd.Series([],dtype='float32'),value=pd.Series([],dtype='int64'))),
            pd.DataFrame(dict(value=pd.Series([2**64-1],dtype='uint64'),other=pd.Series([3.5],dtype='float32')))]
    payloads=[datasets._frame_payload(frame) for frame in frames]
    expected=pd.concat([datasets._decode_frame(p) for p in payloads],ignore_index=True)
    pd.testing.assert_frame_equal(datasets._decode_frames(payloads),expected,check_exact=True)


@pytest.mark.parametrize('values',[[],[1.,2.],[np.nan]])
def test_equivalent_empty_blocks_keep_index_and_dtype_when_combined(values):
    frames=[pd.DataFrame(dict(value=pd.Series(v,dtype='float64'))) for v in ([],values,[])]
    payloads=[datasets._frame_payload(frame) for frame in frames]
    expected=pd.concat([datasets._decode_frame(p) for p in payloads],ignore_index=True)
    pd.testing.assert_frame_equal(datasets._decode_frames(payloads),expected,check_exact=True)


@pytest.mark.parametrize('change',[
    lambda p:p['dtypes'].__setitem__(0,'arbitrary-type'),
    lambda p:p['rows'][0].__setitem__(0,{'unknown':'marker'}),
    lambda p:p['rows'][0].append(99),
])
def test_malformed_payload_is_not_admitted_by_batching(change):
    first=datasets._frame_payload(pd.DataFrame(dict(value=[1.])))
    second=deepcopy(first);change(second)
    with pytest.raises((ValueError,TypeError,KeyError)):
        datasets._decode_frames([first,second])


def prepare(db):
    db.save_ohlcv_data('QA','1 dia',trend(31,freq='D'))
    db.save_symbol_spec('QA',spec_from_symbol_info(SimpleNamespace(point=.0001,trade_tick_size=.0001,trade_tick_value=1)))
    return datasets.capture(db,'QA','D1')


@pytest.fixture(scope='module')
def frozen(tmp_path_factory):
    db=DatabaseManager(db_path=str(tmp_path_factory.mktemp('snapshot-block-decoding')/'own.sqlite'),broker_utc_offset=0)
    snapshot=prepare(db)
    yield db,snapshot
    db.close()


def test_daily_snapshot_uses_one_bar_conversion_and_keeps_bundle_bytes(frozen,monkeypatch,tmp_path):
    db,snapshot=frozen;before=datasets._bundle(db,snapshot.manifest_id)
    assert len(before['manifest']['frames']['bars'])==31
    calls=[];decode=datasets._decode_frame
    def spy(payload):
        if payload['columns']==before_columns:calls.append(len(payload['rows']))
        return decode(payload)
    before_columns=json.loads(before['blocks'][before['manifest']['frames']['bars'][0]])['columns']
    monkeypatch.setattr(datasets,'_decode_frame',spy)
    loaded=datasets.load(db,snapshot.manifest_id)
    pd.testing.assert_frame_equal(loaded.bars,snapshot.bars,check_exact=True)
    assert calls==[31]
    assert datasets._bundle(db,snapshot.manifest_id)==before
    first,second=tmp_path/'first.json',tmp_path/'second.json'
    datasets.export_snapshot(db,snapshot.manifest_id,first);datasets.export_snapshot(db,snapshot.manifest_id,second)
    assert first.read_bytes()==second.read_bytes()


def tamper(bundle,kind):
    if kind in ('raw_hash','quality','dtype'):
        old=bundle['manifest']['frames']['bars'][0];payload=json.loads(bundle['blocks'].pop(old))
        if kind=='dtype':payload['dtypes'][1]='arbitrary-type'
        else:payload['rows'][0][payload['columns'].index('close')]=-1000.
        raw=datasets._json(payload);new=old if kind=='raw_hash' else datasets._hash(raw)
        bundle['blocks'][new]=raw;bundle['manifest']['frames']['bars'][0]=new
    elif kind=='missing':bundle['blocks'].pop(bundle['manifest']['frames']['bars'][0])
    elif kind=='extra':bundle['blocks'][datasets._hash('{}')]='{}'
    elif kind=='period':bundle['manifest']['period']['rows']+=1
    elif kind=='instrument':bundle['manifest']['derivation']['instrument_context']['specification_status']='unproven'
    else:bundle['manifest_id']='0'*64;return
    bundle['manifest_id']=datasets._hash(datasets._json(bundle['manifest']))


@pytest.mark.parametrize('kind',['raw_hash','quality','dtype','missing','extra','period','instrument','manifest_hash'])
def test_each_integrity_gate_still_rejects_before_import_writes(frozen,tmp_path,kind):
    db,snapshot=frozen;bundle=datasets._bundle(db,snapshot.manifest_id);tamper(bundle,kind)
    path=tmp_path/'bad.json';path.write_text(datasets._json(bundle))
    target=DatabaseManager(db_path=str(tmp_path/'target.sqlite'),broker_utc_offset=0)
    try:
        with pytest.raises(ValueError,match='integridade'):datasets.import_snapshot(target,path)
        assert not inspect(target.engine).has_table('_dataset_blocks')
        assert datasets.load(db,snapshot.manifest_id).manifest_id==snapshot.manifest_id
    finally:target.close()
