"""Família registrada de associações, usando inferência e ledger canônicos.

O relatório já é exploratório/exposto. Registro local nunca certifica reserva
intocada, recebimento histórico dos preços ou capacidade de negociação.
"""
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import time

import numpy as np
import pandas as pd
from sqlalchemy import text

from ..analytics import associations,indicator_materializer
from .. import timeframes
from ..laboratory import service
from . import store
from .schema import digest,encoded,token,closed

ROLES=('train','validation','holdout')


def _implementation():
    return digest(dict(adapter=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),inference=associations._pair_signature()))


@dataclass(frozen=True)
class Revision:
    payload:str

    @property
    def protocol_id(self):return digest(self.to_dict())

    def to_dict(self):return json.loads(self.payload)


def register(db,report,spec):
    report=service.load_report(encoded(report));spec=json.loads(encoded(spec))
    tf=timeframes.parse(report['grid_timeframe'])
    if tf is None or tf.name in ('W1','MN1'):raise ValueError('Protocolo exige grade nominal fixa até D1.')
    closed(spec,('name','revision','channels','horizons','windows','session_basis','method','bootstrap_replicates','seed'),'Protocolo de relações')
    token(spec['name'],'Nome')
    if type(spec['revision']) is not int or not 1<=spec['revision']<=2147483647:raise ValueError('Revisão exige inteiro positivo.')
    channels={c['id']:c for c in report['channels']}
    ids=spec['channels'];horizons=spec['horizons']
    if not isinstance(ids,list) or not ids or any(not isinstance(i,str) or i not in channels for i in ids) or len(set(ids))!=len(ids):raise ValueError('Declare canais distintos da análise congelada.')
    if not isinstance(horizons,list) or not horizons or any(type(h) is not int or h not in (1,3,6) for h in horizons) or len(set(horizons))!=len(horizons):raise ValueError('Horizontes distintos:1,3,6 barras nominais.')
    if len(ids)*len(horizons)>24:raise ValueError('Família excede24 hipóteses; reduza explicitamente antes de registrar.')
    if spec['method'] not in ('spearman','pearson'):raise ValueError('Método exige spearman ou pearson.')
    if type(spec['bootstrap_replicates']) is not int or not 99<=spec['bootstrap_replicates']<=1999:raise ValueError('Bootstrap exige99 a1999 réplicas.')
    if type(spec['seed']) is not int or not 0<=spec['seed']<=2**32-1:raise ValueError('Declare seed inteiro entre0 e2^32−1.')
    if spec['session_basis'] not in ('frozen_calendar','utc_day_exploratory'):raise ValueError('Declare frozen_calendar ou utc_day_exploratory.')
    if spec['session_basis']=='frozen_calendar' and (not report.get('calendar_receipts') or any(not s for s in report.get('session_ids',[])) or len(report.get('session_ids',[]))!=len(report['times'])):
        raise ValueError('Sem calendário/sessões congelados completos; UTC diário exige opção exploratória explícita.')
    closed(spec['windows'],ROLES,'Janelas')
    times=pd.to_datetime(report['times'],utc=True);previous=None;scopes={}
    origin=digest(dict(symbol=report['symbol'],timeframe=report['grid_timeframe'],sources=[p['source'] for p in report['populations']]))
    for role in ROLES:
        window=spec['windows'][role];closed(window,('start','end'),'Janela')
        a,b=service._utc(window['start']),service._utc(window['end'])
        if not a<b or previous is not None and a<previous or not ((times>=a)&(times<b)).any():raise ValueError('Janelas train/validation/holdout precisam de candles, ordem cronológica e ausência de sobreposição.')
        if a<times[0] or b>times[-1]+pd.Timedelta(minutes=1):raise ValueError('Janelas devem ficar dentro dos fechamentos congelados; fim exclusivo.')
        window.update(start=a.isoformat(),end=b.isoformat());previous=b
        scopes[role]=dict(origin_key=origin,first=a.isoformat(),last=(b-pd.Timedelta(nanoseconds=1)).isoformat())
    payload=dict(type='laboratory_associations_v1',analysis_id=report['analysis_id'],analysis_sha256=digest(report),implementation=_implementation(),**spec)
    # Um nome/revisão não pode colidir silenciosamente com protocolos de estratégias.
    return store.put_protocol(db,Revision(encoded(payload)),dict(roles=scopes,report_already_exposed=True,prior_external_exposure='unknown'))


def receipt(db,protocol_id,key):
    event_id='relations-result:'+digest([protocol_id,key])
    with db.engine.connect() as conn:
        raw=conn.execute(text('SELECT payload_json FROM _experiment_events WHERE event_id=:i'),dict(i=event_id)).scalar()
    return json.loads(raw) if raw else None


def evaluate(db,report,protocol_id,*,key,cancel_check=None,timeout_seconds=120,max_memory_mb=1024):
    token(key,'Chave de avaliação');report=service.load_report(encoded(report));p=store.protocol(db,protocol_id);spec=p['definition']
    if spec.get('type')!='laboratory_associations_v1' or spec.get('analysis_sha256')!=digest(report):raise ValueError('Protocolo pertence a outra análise/conteúdo.')
    if spec['implementation']!=_implementation():raise ValueError('Implementação mudou; preserve recibo anterior ou registre outra revisão.')
    for value,maximum in [(timeout_seconds,3600),(max_memory_mb,8192)]:
        if type(value) not in (int,float) or not np.isfinite(value) or not 0<value<=maximum:raise ValueError('Orçamento positivo e finito: até3600s/8192MiB.')
    started=dict(started_monotonic=time.monotonic())
    def guard():indicator_materializer._guard(started,None,cancel_check,timeout_seconds,max_memory_mb)
    guard();old=receipt(db,protocol_id,key)
    if old:return old
    # Admissão e exposição antes de qualquer resultado, mesmo se cancelar/falhar.
    admission='relations-admission:'+digest([protocol_id,key])
    with store.transaction(db) as conn:
        if conn.execute(text('SELECT event_id FROM _experiment_events WHERE event_id=:i'),dict(i=admission)).first():raise ValueError('Esta avaliação já foi admitida sem resultado; consulte o ledger e use nova chave explícita.')
        store.event(conn,admission,protocol_id,'','relations_evaluation_admitted',dict(key=key))
        for role in ROLES:store.access(conn,protocol_id,role,p['context']['roles'][role],key+':'+role,'evaluation')
    try:
        result=_evaluate(report,spec,guard)
        result.update(protocol_id=protocol_id,key=key,analysis_id=report['analysis_id'],analysis_sha256=digest(report),implementation=spec['implementation'])
        result['receipt_sha256']=digest(result)
        guard()
        with store.transaction(db) as conn:store.event(conn,'relations-result:'+digest([protocol_id,key]),protocol_id,'','relations_evaluated',result)
        return result
    except BaseException as exc:
        with store.transaction(db) as conn:store.event(conn,'relations-failure:'+digest([protocol_id,key]),protocol_id,'','relations_evaluation_failed',dict(key=key,cancelled=isinstance(exc,indicator_materializer.Cancelled),error_type=type(exc).__name__))
        raise


def _evaluate(report,spec,guard):
    times=pd.DatetimeIndex(pd.to_datetime(report['times'],utc=True));step=pd.Timedelta(minutes=timeframes.parse(report['grid_timeframe']).minutes)
    sessions=report.get('session_ids') if spec['session_basis']=='frozen_calendar' else times.strftime('%Y-%m-%d').tolist()
    bars=pd.DataFrame(dict(symbol=report['symbol'],tf=report['grid_timeframe'],session_id=sessions,
        bar_start_utc=times-step,bar_end_utc=times,computed_at_utc=times,close=[p[3] for p in report['prices']]))
    targets=associations.build_forward_close_targets(bars,horizons=spec['horizons'],as_of=report['selection']['as_of'])
    channels={c['id']:c for c in report['channels']};results=[]
    for ident in spec['channels']:
        channel=channels[ident];blocked=service._channel_block(channel)
        for horizon in spec['horizons']:
            target=targets[targets.horizon_bars==horizon].reset_index(drop=True)
            x=channel['values'] if not blocked else [None]*len(times)
            frame=pd.DataFrame(dict(symbol=report['symbol'],timeframe=report['grid_timeframe'],session_id=sessions,decision_time=times,
                x_value=x,y_value=target.value,x_available_at_utc=channel['source_ends'],y_available_at_utc=target.available_at_utc,
                y_state=target.state,label_end_utc=target.label_end_utc))
            nominal=service._nominal_mask(report,horizon).shift(-horizon,fill_value=False)
            frame.loc[~nominal,'y_state']='UNAVAILABLE'
            for role in ROLES:
                guard();a,b=(pd.Timestamp(spec['windows'][role][k]) for k in ('start','end'))
                selected=frame.loc[(times>=a)&(times<b)].copy()
                purge=selected.label_end_utc.ge(b);selected=selected.loc[~purge]
                r=associations.analyze_association(selected,kind=associations.FEATURE_TARGET,method=spec['method'],
                    as_of=min(service._utc(report['selection']['as_of']),b),bootstrap_replicates=spec['bootstrap_replicates'],seed=spec['seed'],guard=guard)
                r.update(channel_id=ident,horizon=horizon,role=role,purged_boundary=int(purge.sum()),blocked_reason=blocked)
                results.append(r)
    count=len(spec['channels'])*len(spec['horizons'])*len(ROLES)
    results=associations.adjust_bonferroni(results,tests_counted=count)
    stability=[]
    for ident in spec['channels']:
        for horizon in spec['horizons']:
            rows=[r for r in results if r['channel_id']==ident and r['horizon']==horizon]
            eligible=all(r['status']=='OK' and r['p_adjusted']<=.05 and r['ci_low']*r['ci_high']>0 for r in rows)
            consistent=eligible and len({np.sign(r['effect']) for r in rows})==1
            stability.append(dict(channel_id=ident,horizon=horizon,consistent_across_registered_windows=bool(consistent)))
    return dict(schema_version=1,results=results,tests_counted=count,stability=stability,session_basis=spec['session_basis'],
        report_already_exposed=True,prior_external_exposure='unknown',qualified_final_holdout=False,predictive_qualification=False,
        limitations=['Família e janelas registradas antes desta avaliação; relatório de origem já exposto.',
            'Bootstrap temporal e Bonferroni não certificam independência, previsão, lucro ou recebimento histórico dos preços.',
            'A correção cobre esta família registrada; novas revisões/tentativas não recebem controle global de busca por inferência.',
            'Rótulos que atravessam o fim de cada janela são expurgados; lacunas nominais não são comprimidas.',
            'UTC diário exploratório não equivale a sessão comprovada da corretora.'])


def validate_receipt(report,value):
    if not isinstance(value,dict) or value.get('analysis_sha256')!=digest(report) or value.get('receipt_sha256')!=digest({k:v for k,v in value.items() if k!='receipt_sha256'}):raise ValueError('Recibo estatístico alterado ou de outra análise.')
    return json.loads(encoded(value))
