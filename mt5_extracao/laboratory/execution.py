"""Recibo observacional de execução na mesma grade/revisão, sem inferir fills."""
import json
import time
import pandas as pd

from mt5_extracao.experiments.schema import digest,encoded
from mt5_extracao.robots import store
from mt5_extracao.trade_analytics.trajectories import store as trajectories
from mt5_extracao.trade_analytics import ledger
from mt5_extracao.trade_analytics.trajectories.clocks import Clock,bar_end_in_source
from . import service


def link(db,report,run_id,*,guard=None):
    if guard is None:
        from mt5_extracao.analytics import indicator_materializer
        started=dict(started_monotonic=time.monotonic())
        guard=lambda:indicator_materializer._guard(started,None,None,120,1024)
    guard()
    report=service.load_report(encoded(report));runs=store.load_runs(db,run_id=run_id)
    if len(runs)!=1:raise ValueError('Execução não encontrada na base atual.')
    run=runs[0];primary=report['populations'][0]
    if (run['symbol'],run['timeframe'],run.get('data_manifest_id'))!=(report['symbol'],report['grid_timeframe'],primary['manifest_id']):
        raise ValueError('Execução exige o mesmo ativo, grade principal e snapshot da análise. Não projetar um run de outra revisão.')
    trajectory=trajectories.load(db,run_id)
    guard()
    if trajectory is None:raise ValueError('Execução sem observações conciliadas; fills/SL/TP não serão inferidos de intenções ou preços.')
    payload=trajectory['observations'];events=payload['records']
    book=ledger.load(db,run_id=run_id)
    protection={int(row.seq):dict(initial_stop_price=None if pd.isna(row.stop_price) else float(row.stop_price),
        initial_target_price=None if pd.isna(row.target_price) else float(row.target_price)) for row in book.itertuples()}
    if len(events)>200000:raise ValueError('Execução excede200 mil observações; escolha janela/run menor explicitamente.')
    clock=Clock(**trajectory['clock']);times=pd.DatetimeIndex(pd.to_datetime(report['times'],utc=True));lookup={t:i for i,t in enumerate(times)}
    cutoff=service._utc(report['selection']['as_of']);mapped=[[] for _ in times];outside=0
    for ordinal,event in enumerate(events):
        if ordinal%128==0:guard()
        close=clock.timestamp(bar_end_in_source(event['bar_time'],run['timeframe']))
        index=lookup.get(close)
        if index is None or clock.timestamp(event['available_at'])>cutoff:outside+=1;continue
        mapped[index].append(dict(event,**protection.get(event['cycle_seq'],{}),bar_close_utc=close.isoformat(),available_at_utc=clock.timestamp(event['available_at']).isoformat()))
    capital=[{} for _ in times]
    for key in ('account_balance','floating_equity','performance_equity'):
        for ordinal,(at,value) in enumerate(run.get(key) or []):
            if ordinal%128==0:guard()
            index=lookup.get(clock.timestamp(bar_end_in_source(at,run['timeframe'])))
            if index is not None and times[index]<=cutoff:capital[index][key]=value
    value=dict(schema_version=1,analysis_id=report['analysis_id'],analysis_sha256=digest(report),run_id=run_id,
        data_manifest_id=run['data_manifest_id'],trajectory_id=trajectory['trajectory_id'],source=trajectory['source'],
        producer_identity=trajectory.get('producer_identity',{}),ledger_precision=trajectory.get('ledger_precision'),
        observation_basis=payload.get('sequence_basis'),clock=trajectory['clock'],currency=run['metrics'].get('currency'),
        events_by_index=mapped,capital_by_index=capital,outside_or_after_cut=outside,valuation=run.get('valuation'),
        operational_acceptance=False,historical_receipt_proven=False,
        limitations=['Observações conciliadas com ledger/snapshot; a sequência declara a base do produtor.',
            'Disponibilidade do motor/modelo não comprova recebimento histórico de uma execução real.',
            'SL/TP são níveis iniciais registrados no ledger, não confirmação de modificação/aceite no broker.',
            'SL/TP/fills ausentes no recibo permanecem ausentes; intenções não são convertidas em operações.'])
    value['receipt_sha256']=digest(value);return value


def validate(report,value):
    if not isinstance(value,dict) or value.get('analysis_sha256')!=digest(report) or value.get('receipt_sha256')!=digest({k:v for k,v in value.items() if k!='receipt_sha256'}):raise ValueError('Recibo de execução alterado ou de outra análise.')
    return json.loads(encoded(value))


def candle(report,value,index):
    value=validate(report,value)
    if type(index) is not int or not 0<=index<len(report['times']):raise ValueError('Candle fora do recorte.')
    return dict(run_id=value['run_id'],source=value['source'],observation_basis=value['observation_basis'],
        trajectory_id=value['trajectory_id'],events=value['events_by_index'][index],capital=value['capital_by_index'][index],
        currency=value['currency'],valuation=value['valuation'],operational_acceptance=False,historical_receipt_proven=False)
