"""Observações econômicas públicas no store/journal atuais, sem OHLCV fictício.

Datas da série são rótulos de período; disponibilidade é o recebimento local.
USREC é a convenção retrospectiva do FRED, não uma previsão ou anúncio em tempo real.
"""
import csv
from datetime import date,datetime,timedelta,timezone
import io
import json
import math
import time

import pandas as pd
import requests

from . import datasets,feature_store,features,indicator_materializer,mentions,producer_jobs

MAX_BYTES=512*1024
SOURCES={
 'bcb.sgs.11':dict(name='Selic diária · BCB SGS11',feature_id='macro.bcb.sgs.11',value_type='NUMBER',
    unit='percent/day',frequency='daily',maximum_days=366,
    endpoint='https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados',
    reference='https://dadosabertos.bcb.gov.br/dataset/11-taxa-de-juros---selic',
    attribution='Banco Central do Brasil, SGS11, Taxa de juros Selic; Open Data Commons ODbL',
    period_convention='Source calendar-date label; UTC midnight is a reference coordinate, not publication time'),
 'fred.usrec':dict(name='Fase econômica EUA · FRED USREC · mensal',feature_id='macro.fred.usrec.phase',value_type='CATEGORY',
    unit='phase',frequency='monthly',maximum_days=3660,
    endpoint='https://fred.stlouisfed.org/graph/fredgraph.csv',reference='https://fred.stlouisfed.org/series/USREC',
    attribution='Federal Reserve Bank of St. Louis, NBER based Recession Indicators [USREC], retrieved from FRED. Citation required.',
    period_convention='FRED trough method: recession from month following peak through trough; retrospective and revisable'),
}


def catalog():
    return [dict(id=key,**value,historical_receipt_proven=False) for key,value in SOURCES.items()]


def _bounds(start,end,maximum):
    try:a,b=date.fromisoformat(start),date.fromisoformat(end)
    except (ValueError,TypeError):raise ValueError('Use datas YYYY-MM-DD; fim exclusivo.') from None
    if not a<b or (b-a).days>maximum or b>datetime.now(timezone.utc).date():
        raise ValueError(f'Janela deve estar encerrada e ter até{maximum} dias; fim exclusivo.')
    return a,b


def _read(spec,a,b,guard,remaining):
    params=(dict(formato='json',dataInicial=a.strftime('%d/%m/%Y'),dataFinal=(b-timedelta(days=1)).strftime('%d/%m/%Y'))
            if spec['frequency']=='daily' else dict(id='USREC',cosd=a.isoformat(),coed=(b-timedelta(days=1)).isoformat()))
    guard()
    with requests.get(spec['endpoint'],params=params,timeout=min(10,remaining),stream=True) as response:
        if response.status_code!=200:
            raise RuntimeError(f'Fonte econômica recusou a consulta (HTTP{response.status_code}); nenhum valor zero foi presumido. Confira acesso/quota e retome explicitamente.')
        raw=bytearray()
        for chunk in response.iter_content(16384):
            guard();raw.extend(chunk)
            if len(raw)>MAX_BYTES:raise ValueError('Resposta econômica excede512KiB; reduza a janela sem truncamento.')
    return bytes(raw)


def parse(source,raw,a,b):
    if len(raw)>MAX_BYTES:raise ValueError('Resposta econômica excede512KiB.')
    try:text=raw.decode('utf-8-sig')
    except UnicodeError:raise ValueError('Fonte econômica sem UTF-8 válido.') from None
    if source=='bcb.sgs.11':
        try:items=json.loads(text)
        except ValueError:raise ValueError('SGS não retornou JSON válido.') from None
        if not isinstance(items,list):raise ValueError('SGS retornou erro/objeto, não observações.')
    else:
        reader=csv.DictReader(io.StringIO(text))
        if reader.fieldnames not in (['observation_date','USREC'],['DATE','USREC']):raise ValueError('CSV USREC sem as colunas oficiais esperadas.')
        items=list(reader)
    if len(items)>4000:raise ValueError('Mais de4000 observações; reduza a janela.')
    result=[];seen=set()
    for item in items:
        if not isinstance(item,dict):raise ValueError('Observação econômica inválida.')
        try:
            d=datetime.strptime(item['data'],'%d/%m/%Y').date() if source=='bcb.sgs.11' else date.fromisoformat(item.get('observation_date',item.get('DATE','')))
        except (ValueError,TypeError,KeyError):raise ValueError('Data de referência econômica inválida.') from None
        if d in seen or not a<=d<b:raise ValueError('Data econômica duplicada ou fora do recorte solicitado.')
        seen.add(d);value=item.get('valor') if source=='bcb.sgs.11' else item.get('USREC')
        if value in ('','. ','.',None):value=None
        elif source=='fred.usrec':
            if value not in ('0','1'):raise ValueError('USREC aceita somente0/1 ou ausência; categoria não é estimada do preço.')
            value='recession' if value=='1' else 'expansion'
            if d.day!=1:raise ValueError('USREC mensal precisa de rótulo no primeiro dia do mês.')
        else:
            try:value=float(value.replace(',','.'))
            except (ValueError,TypeError,AttributeError):raise ValueError('Valor SGS não numérico; não substituir por zero.') from None
            if not math.isfinite(value):raise ValueError('Valor SGS não finito.')
        result.append((d,value))
    return sorted(result)


def collect(db,*,source_id,start,end,cancel_check=None,write_guard=None,timeout_seconds=120,max_memory_mb=1024):
    if source_id not in SOURCES:raise ValueError('Fonte econômica desconhecida; consulte macro-sources.')
    for value,name,maximum in [(timeout_seconds,'timeout_seconds',3600),(max_memory_mb,'max_memory_mb',8192)]:
        if isinstance(value,bool) or not isinstance(value,(float,int)) or not math.isfinite(value) or not 0<value<=maximum:
            raise ValueError(f'{name} exige valor positivo até{maximum}.')
    spec=SOURCES[source_id];a,b=_bounds(start,end,spec['maximum_days']);started=dict(started_monotonic=time.monotonic())
    def guard():indicator_materializer._guard(started,write_guard,cancel_check,timeout_seconds,max_memory_mb)
    guard();identity=datasets.identity(db);symbol='MACRO:'+source_id.upper()
    parameters={k:spec[k] for k in ['reference','attribution','frequency','period_convention']}
    parameters.update(adapter_version='1',storage_timeframe_role='UTC period reference coordinate, not a daily source frequency')
    if source_id=='fred.usrec':parameters['value_mapping']='0=expansion;1=recession; missing=unavailable'
    version='1+'+mentions._hash(dict(source_id=source_id,parameters=parameters,value_type=spec['value_type'],unit=spec['unit']))
    definition=dict(feature_id=spec['feature_id'],name=spec['name'],algorithm_version=version,parameters=parameters,
        dependencies=['external:'+source_id],family='macro',formula='Source observation, without inference or interpolation',
        value_type=spec['value_type'],unit=spec['unit'],source_id=source_id,timeframe='D1',availability_rule='first_local_capture; retrospective_source_period_label')
    family='macro:'+source_id
    previous=producer_jobs.get_checkpoint(db,symbol,'D1',family);sequence=previous['source_sequence'] if previous else 0
    run=producer_jobs.start_run(db,symbol,'D1',family,version,mentions._hash(parameters),sequence,
        configuration=dict(source=source_id,start=start,end=end,input_basis='public_source_observations',limits=dict(timeout_seconds=timeout_seconds,max_memory_mb=max_memory_mb)))
    run['started_monotonic']=started['started_monotonic']
    try:
        try:raw=_read(spec,a,b,guard,timeout_seconds)
        except requests.RequestException:
            raise RuntimeError('Fonte econômica indisponível ou prazo de rede excedido; nenhum valor foi publicado. Confira a conexão e retome explicitamente.') from None
        observations=parse(source_id,raw,a,b)
        captured=datetime.now(timezone.utc);records=[]
        for d,value in observations:
            guard();at=pd.Timestamp(d,tz='UTC');finish=at+pd.Timedelta(days=1) if spec['frequency']=='daily' else at+pd.offsets.MonthBegin(1)
            metadata=dict(macro=True,reference=spec['reference'],attribution=spec['attribution'],period_convention=spec['period_convention'],
                source_period_start=at.isoformat(),window_end=finish.isoformat(),capture_clock='stored_observed_at',
                historical_publication_available_at=None,retrospective=True,scope='source observations; not price cause or forecast')
            records.append(dict(symbol=symbol,timeframe='D1',feature_id=spec['feature_id'],algorithm_version=version,
                observation_time=at,value=value,available_at=captured,observed_at=captured,
                source_revision=mentions._hash(dict(date=d.isoformat(),value=value,source=source_id)),
                quality='UNAVAILABLE' if value is None else 'PROVISIONAL' if finish>pd.Timestamp(captured) else 'VALID',metadata=metadata))
        guard()
        if datasets.identity(db)!=identity:raise indicator_materializer.SourceChanged('Base alterada durante a consulta econômica; fatos não publicados.')
        features.register(db,**definition)
        def before(conn):
            guard()
            if conn.dialect.name=='postgresql':
                from sqlalchemy import text
                conn.execute(text('LOCK TABLE _dataset_origin IN SHARE MODE'))
            if datasets.identity(db,connection=conn)!=identity:raise indicator_materializer.SourceChanged('Origem mudou antes da publicação econômica.')
        def checkpoint(conn,counts):
            guard();producer_jobs.advance_checkpoint(conn,symbol=symbol,timeframe='D1',family=family,algorithm_version=version,
                config_hash=mentions._hash(parameters),source_sequence=sequence+1,last_bar_start=records[-1]['observation_time'].isoformat() if records else None,
                status='COMPLETE',state=dict(source_id=source_id,start=start,end=end,observations=len(records),response_sha256=__import__('hashlib').sha256(raw).hexdigest()))
        counts=feature_store.save_values(db,records,transaction_guard=before,transaction_hook=checkpoint)
        job=producer_jobs.finish_run(db,run,source_sequence=sequence+1,status='COMPLETE',counts=counts);run['_closed']=True
        return dict(source_id=source_id,symbol=symbol,timeframe='D1',features=[[spec['feature_id'],version]],
            observations=len(records),captured_at=captured.isoformat(),reference=spec['reference'],attribution=spec['attribution'],
            historical_receipt_proven=False,retrospective=True,counts=counts,job=job)
    except BaseException as exc:
        if not run.get('_closed'):
            producer_jobs.abort_run(db,run,source_sequence=sequence,error=exc,cancelled=isinstance(exc,indicator_materializer.Cancelled));run['_closed']=True
        raise
