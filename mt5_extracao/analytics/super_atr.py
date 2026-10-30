"""SA-02: produtor do header HADOUKEN fixado, no store/journal existentes.

Não há outra fórmula Python: o adaptador C++ já mantido no framework executa
o header original. Disponibilidade padrão é o recebimento local do cálculo.
"""
from datetime import datetime,timezone
import hashlib
import importlib.util
import json
import math
from pathlib import Path
import shutil
import subprocess
import sys
import time

import pandas as pd
from sqlalchemy import text

from . import datasets,feature_store,features,indicator_materializer,producers,producer_jobs
from mt5_extracao import timeframes
from mt5_extracao.experiments.schema import digest

OUTPUTS={'level':('trend.super_atr.level','Linha','NUMBER','price_units'),
         'atr':('trend.super_atr.atr','ATR','NUMBER','price_units'),
         'side':('trend.super_atr.side','Lado','CATEGORY','side'),
         'turn':('trend.super_atr.turn','Virada','CATEGORY','turn')}
HEADER='Include/Framework/HADOUKEN/Indicators/HDK_SuperATR.mqh'
ADAPTER='Tools/research_super_atr/model.py'
MAX_BARS=20000


def _normalized(raw):return raw.decode('utf-8-sig').replace('\r\n','\n').encode('utf-8')


def source(framework_root,manifest_path=None):
    """Inspeção passiva da composição oficial; não clona/repina/compila."""
    from scripts.setup_development import inspect_checkout,load_manifest,MANIFEST,SetupError
    root=Path(framework_root).expanduser().resolve()
    manifest=Path(manifest_path).expanduser() if manifest_path is not None else MANIFEST
    try:
        spec=load_manifest(manifest)['repositories']['hadouken']
        if inspect_checkout(root,spec['url'],spec['commit']) is None:raise ValueError('Framework ausente; informe a raiz canônica existente.')
        hashes={}
        for relative in (HEADER,ADAPTER):
            expected=subprocess.run(['git','-C',str(root),'show',spec['commit']+':'+relative],capture_output=True,timeout=10,check=True).stdout
            raw=(root/relative).read_bytes()
            if _normalized(raw)!=_normalized(expected):raise ValueError('Fonte Super ATR difere do commit fixado; reconcilie a composição existente.')
            hashes[relative]=hashlib.sha256(_normalized(raw)).hexdigest()
        compiler=shutil.which('g++') or shutil.which('clang++')
        compiler_version=(subprocess.run([compiler,'--version'],capture_output=True,text=True,timeout=5,check=True).stdout.splitlines()[0] if compiler else None)
        module_spec=importlib.util.spec_from_file_location('_mt5x_pinned_super_atr',root/ADAPTER)
        module=importlib.util.module_from_spec(module_spec);module_spec.loader.exec_module(module)
    except (OSError,subprocess.SubprocessError,SetupError) as exc:
        message=str(exc).replace('preserve-o e use outra pasta.','preserve-o e reconcilie a composição nesta raiz.').replace('Escolha outra pasta.','Informe a raiz canônica existente.')
        raise ValueError('Composição Super ATR indisponível: '+message) from None
    return dict(framework_commit=spec['commit'],header_sha256=hashes[HEADER],adapter_sha256=hashes[ADAPTER],
        compiler=compiler_version,available=bool(compiler),qualification='PINNED_HEADER_CPP_ADAPTER',synthetic_fixture=False,
        native_mt5_qualified=False),module.RealModel


def capabilities(framework_root,manifest_path=None):
    metadata,_factory=source(framework_root,manifest_path)
    return dict(metadata,outputs=[dict(feature_id=x[0],name=x[1],value_type=x[2],unit=x[3]) for x in OUTPUTS.values()],
        initial_side='up',means={'0':'SMA','1':'EMA seeded by first complete SMA'},availability='local_materialization_receipt',
        max_bars=MAX_BARS,notes=['Compilador C++ do ambiente existente necessário; não instala dependências.',
            'Header/adaptador fixados; compilação C++ não certifica MT5, Expert, conta ou desempenho.'])


def _same_value(old,record):
    value=old['numeric_value'] if pd.notna(old['numeric_value']) else old['categorical_value']
    if pd.isna(value):value=None
    old_meta=json.loads(old['metadata_json'])
    return (value==record['value'] and old['quality']==record['quality'] and old['source_revision']==record['source_revision'] and
        {k:v for k,v in old_meta.items() if k!='source_manifest_id'}==record['metadata'])


def materialize(db,manifest_id,*,framework_root,manifest_path=None,period=20,mean=0,deviation=1.,
                as_of=None,write_guard=None,cancel_check=None,timeout_seconds=120,max_memory_mb=1024):
    if type(period) is not int or not 1<=period<=10000 or type(mean) is not int or mean not in (0,1):raise ValueError('Período1…10000; média0=SMA ou1=EMA.')
    if type(deviation) not in (int,float) or not math.isfinite(deviation) or deviation<=0:raise ValueError('Desvio exige número positivo finito.')
    for value,maximum in [(timeout_seconds,3600),(max_memory_mb,8192)]:
        if type(value) not in (int,float) or not math.isfinite(value) or not 0<value<=maximum:raise ValueError('Orçamento exige valor positivo até3600s/8192MiB.')
    started=dict(started_monotonic=time.monotonic())
    def guard():indicator_materializer._guard(started,write_guard,cancel_check,timeout_seconds,max_memory_mb)
    guard();snapshot=datasets.load(db,manifest_id);manifest=snapshot.manifest
    if manifest['schema_version']==datasets.FEATURE_ONLY_VERSION:raise ValueError('Super ATR exige snapshot OHLCV de mercado, não fonte sem candles.')
    identity=datasets.identity(db)
    if manifest['source']['namespace']!=identity['namespace']:raise ValueError('Snapshot de outra origem; materialize na base canônica da fonte.')
    tf=timeframes.parse(manifest['timeframe']);offset=manifest['time'].get('broker_utc_offset_hours')
    if tf is None:raise ValueError('Super ATR exige timeframe nativo reconhecido; não há agregação implícita.')
    if manifest['time'].get('offset_state')!='declared' or offset is None:raise ValueError('Fuso broker precisa estar declarado no snapshot.')
    from mt5_extracao.laboratory.service import _bounds,_utc
    now=pd.Timestamp.now(tz='UTC');cut=now if as_of is None else _utc(as_of)
    if cut>now:raise ValueError('Corte futuro não é permitido; use barras já fechadas.')
    bars=snapshot.bars.reset_index(drop=True);starts,ends=_bounds(bars,tf,offset)
    keep=ends<=cut;bars=bars.loc[keep].reset_index(drop=True);starts,ends=starts[keep],ends[keep]
    if not len(bars) or len(bars)>MAX_BARS:raise ValueError('Escolha1…20000 barras fechadas; a primeira barra fixa a semente.')
    artifact,factory=source(framework_root,manifest_path);guard()
    if not artifact['available']:raise ValueError('Compilador C++ ausente no ambiente existente; prepare a toolchain pelo fluxo oficial antes de materializar.')
    parameters=dict(period=period,mean=mean,deviation=float(deviation),initial_side='up',seed_start_utc=starts[0].isoformat(),
        input_columns=['high','low','close'],
        seed_basis='first_bar_of_frozen_input; full_prefix',timeframe=tf.name,availability_basis='local_receipt',
        turn_rule='change_between_consecutive_valid_states; first_valid_is_initial_side',**artifact)
    implementation=dict(adapter=hashlib.sha256(_normalized(Path(__file__).read_bytes())).hexdigest(),
        source_tokens=indicator_materializer._source_components(producers.__file__,('_source_tokens','_prefix_source_revisions','_extend_source_chain','_json_safe')))
    version='1+'+digest(dict(parameters=parameters,implementation=implementation));family='super_atr:'+version
    previous=producer_jobs.get_checkpoint(db,manifest['symbol'],tf.name,family);sequence=previous['source_sequence'] if previous else 0
    run=producer_jobs.start_run(db,manifest['symbol'],tf.name,family,version,digest(parameters),sequence,
        configuration=dict(source_manifest_id=manifest_id,parameters=parameters,calculation_cutoff=cut.isoformat(),historical_receipt_proven=False))
    run['started_monotonic']=started['started_monotonic'];model=None
    try:
        guard();model=factory();guard()
        inputs=bars[['high','low','close']].to_dict('records');valid_count=len(inputs)
        for i,row in enumerate(inputs):
            guard();h,l,c=(float(row[k]) for k in ('high','low','close'))
            if not all(math.isfinite(v) and v!=sys.float_info.max for v in (h,l,c)) or l<=0 or h<l or not l<=c<=h:
                valid_count=i;break
        computed=model.calculate(inputs[:valid_count],period=period,mean=mean,deviation=float(deviation)) if valid_count else []
        computed += [None]*(len(inputs)-valid_count);guard();captured=datetime.now(timezone.utc)
        tokens=bars[['high','low','close']].copy();tokens['bar_start_utc']=starts
        revisions=producers._prefix_source_revisions(producers._source_tokens(tokens));records=[];last_side=None
        history={}
        for ident,_name,_type,_unit in OUTPUTS.values():
            old=feature_store.query_as_of(db,captured,symbol=manifest['symbol'],timeframe=tf.name,feature_id=ident,algorithm_version=version)
            history[ident]={pd.Timestamp(r['observation_time']):r for r in old.to_dict('records')}
        for i,row in enumerate(computed):
            guard();valid=row is not None and row['side'] in (-1,1) and row['atr']>=0 and all(math.isfinite(row[k]) and row[k]!=sys.float_info.max for k in ('level','atr'))
            side=('up' if row['side']==1 else 'down') if valid else None
            values=dict(level=row['level'] if valid else None,atr=row['atr'] if valid else None,side=side,
                turn=side if valid and last_side is not None and side!=last_side else 'none' if valid else None)
            reason=None if valid else 'WARMUP' if i<period-1 and i<valid_count else 'INVALID_INPUT_OR_DEPENDENT_SUFFIX'
            for key,(ident,_name,_type,_unit) in OUTPUTS.items():
                metadata=dict(state='AVAILABLE' if valid else 'UNAVAILABLE',reason=reason,window_end=ends[i].isoformat(),
                    calculation_bar_start_utc=starts[i].isoformat(),calculation_bar_end_utc=ends[i].isoformat(),
                    seed_start_utc=starts[0].isoformat(),source=manifest['source'],source_clock=manifest['time'],
                    framework=artifact,synthetic_fixture=artifact['synthetic_fixture'],historical_receipt_proven=False)
                record=dict(symbol=manifest['symbol'],timeframe=tf.name,feature_id=ident,algorithm_version=version,
                    observation_time=starts[i],value=values[key],quality='VALID' if valid else 'UNAVAILABLE',
                    available_at=captured,observed_at=captured,source_revision=revisions[i],metadata=metadata)
                old=history[ident].get(starts[i])
                origin=json.loads(old['metadata_json']).get('source_manifest_id') if old is not None and _same_value(old,record) else manifest_id
                metadata['source_manifest_id']=origin or manifest_id;records.append(record)
            last_side=side
        guard()
        for key,(ident,name,value_type,unit) in OUTPUTS.items():
            features.register(db,ident,f'Super ATR · {name} · p{period} · '+('SMA' if mean==0 else 'EMA')+f' · d{deviation:g}',version,
                parameters=parameters,dependencies=['bars:{timeframe}'],family='super_atr',
                formula='Pinned HADOUKEN header via its original C++ platform adapter; no parallel formula',
                unit=unit,warmup_count=period-1,warmup_unit='bars',source_id='hadouken.super_atr',timeframe=tf.name,
                availability_rule='first_local_materialization_receipt; nominal_close_is_not_historical_receipt',value_type=value_type)
        def before(conn):
            guard()
            if conn.dialect.name=='postgresql':conn.execute(text('LOCK TABLE _dataset_origin IN SHARE MODE'))
            if datasets.identity(db,connection=conn)!=identity:raise indicator_materializer.SourceChanged('Origem mudou durante o cálculo; fatos não publicados.')
        def checkpoint(conn,counts):
            guard();datasets.bind(db,run['run_id'],manifest_id,connection=conn)
            producer_jobs.advance_checkpoint(conn,symbol=manifest['symbol'],timeframe=tf.name,family=family,algorithm_version=version,
                config_hash=digest(parameters),source_sequence=sequence+1,last_bar_start=starts[-1].isoformat(),status='COMPLETE',
                state=dict(source_manifest_id=manifest_id,seed_start_utc=starts[0].isoformat(),bar_count=len(bars),prefix_chain=revisions[-1]))
        counts=feature_store.save_values(db,records,transaction_guard=before,transaction_hook=checkpoint)
        job=producer_jobs.finish_run(db,run,source_sequence=sequence+1,status='COMPLETE',counts=counts);run['_closed']=True
        return dict(symbol=manifest['symbol'],timeframe=tf.name,source_manifest_id=manifest_id,parameters=parameters,
            features=[[v[0],version] for v in OUTPUTS.values()],algorithm_version=version,bars=len(bars),
            window_start_utc=starts[0].isoformat(),window_end_utc=ends[-1].isoformat(),
            available_from=captured.isoformat(),historical_receipt_proven=False,native_mt5_qualified=False,counts=counts,job=job)
    except BaseException as exc:
        if not run.get('_closed'):producer_jobs.abort_run(db,run,source_sequence=sequence,error=exc,cancelled=isinstance(exc,indicator_materializer.Cancelled));run['_closed']=True
        raise
    finally:
        if model is not None:model.close()
