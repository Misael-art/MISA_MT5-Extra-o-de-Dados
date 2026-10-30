"""MEN: classificação tardia explícita no store/journal existentes, sem rede."""
from collections import Counter
from copy import deepcopy
from datetime import datetime, timezone
import json
import math
import time

import pandas as pd
from sqlalchemy import text

from mt5_extracao.analytics import datasets, feature_store, indicator_materializer, mention_classifier, mention_ingestion, mention_qualification, mentions, producer_jobs


def capabilities(classifier_id=None):
    """Inspeção do artefato escolhido; não classifica texto nem consulta rede."""
    from . import mention_classifier_pt,mention_qualification_pt
    chosen=classifier_id or mention_classifier.CLASSIFIER_ID
    models={mention_classifier.CLASSIFIER_ID:(mention_classifier,mention_qualification.REPORT),
            mention_classifier_pt.CLASSIFIER_ID:(mention_classifier_pt,mention_qualification_pt.REPORT)}
    if chosen not in models:raise ValueError('Classificador não qualificado neste artefato; consulte mention-classifiers.')
    model,frozen=models[chosen];report=deepcopy(frozen)
    try:descriptor=model.descriptor()
    except RuntimeError as exc:return dict(available=False,reason=str(exc),qualification=report)
    return dict(available=True,classifier=descriptor,qualification=report,
                benchmark_matches=descriptor==report['classifier'] and report['criteria_met'],
                models=[dict(id=key,languages=value[1]['classifier']['languages'],qualification=value[1]['qualification']) for key,value in models.items()])


def current(db, contract, start, *, connection=None):
    frame=feature_store.query_history(db,symbol=contract['scope']['symbol'],timeframe=contract['scope']['timeframe'],
        algorithm_version=contract['algorithm_version'],start=start,end=mentions._utc(start,'start')+
        pd.Timedelta(microseconds=1),connection=connection)
    result={}
    for row in frame.sort_values('revision').to_dict('records'):
        result[row['feature_id']]=row
    return result


def generation(rows):
    return {key:(int(row['revision']),row['source_revision']) for key,row in rows.items()}


def classify(db, *, selection, batch, synthetic_fixture=False, write_guard=None, cancel_check=None,
             timeout_seconds=120,max_memory_mb=1024):
    """Exige Q já recebido da mesma população; anexa somente P/N e saídas por item."""
    for value,name,maximum in [(timeout_seconds,'timeout_seconds',3600),(max_memory_mb,'max_memory_mb',8192)]:
        if isinstance(value,bool) or not isinstance(value,(int,float)) or not math.isfinite(value) or not 0<value<=maximum:
            raise ValueError(f'{name} precisa ser positivo e até{maximum}.')
    started=dict(started_monotonic=time.monotonic())
    def guard():indicator_materializer._guard(started,write_guard,cancel_check,timeout_seconds,max_memory_mb)
    guard();contract=mentions.describe(db,**selection);scope=contract['scope'];identity=datasets.identity(db)
    capability=capabilities(scope['classifier']['id'])
    if not capability.get('benchmark_matches') or scope['classifier']!=capability.get('classifier'):
        raise ValueError('Classificador ausente, não qualificado neste artefato ou versão/idioma divergente; consulte mention-classifiers e recapture com o contrato explícito.')
    if scope['source']['mode']!='documents' or batch.get('annotations') is not None:
        raise ValueError('Classificação exige documentos originais do lote, sem anotação/score fornecido pelo chamador.')
    prepared=mention_ingestion.prepare(contract,batch,synthetic_fixture=synthetic_fixture,guard=guard)
    receipt=prepared['receipt'];before=current(db,contract,batch['start']);q=before.get('mentions.quantity')
    if receipt['coverage']!='complete' or q is None or q['quality']!='VALID' or q['source_revision']!=receipt['capture_id']:
        raise ValueError('Classificação exige Quantidade completa já recebida da mesma população/revisão; ingira/capture primeiro.')
    family='mentions:classify:'+contract['scope_hash'];previous=producer_jobs.get_checkpoint(db,scope['symbol'],scope['timeframe'],family)
    sequence=previous['source_sequence'] if previous else 0
    run=producer_jobs.start_run(db,scope['symbol'],scope['timeframe'],family,contract['algorithm_version'],contract['scope_hash'],sequence,
        configuration=dict(input_basis='bounded_object_classification',capture_id=receipt['capture_id'],classifier=scope['classifier']))
    run['started_monotonic']=started['started_monotonic'];counts=dict(inserted=0,revised=0,unchanged=0)
    try:
        eligible,_,_,_=mention_ingestion._documents(contract,batch['documents'],mentions._utc(batch['start'],'start'),
            mentions._utc(batch['end'],'end'),guard)
        from . import mention_classifier_pt
        model=mention_classifier_pt if scope['classifier']['id']==mention_classifier_pt.CLASSIFIER_ID else mention_classifier
        qualification=capability['qualification']['qualification']
        outputs=[]
        for doc in eligible:
            guard();label=model.classify_document(doc['text'],scope['subject']['aliases'],language=doc['language'])
            outputs.append(dict(id_sha256=mentions._hash(doc['id']),revision_sha256=mentions._hash(doc),subject_id=scope['subject']['id'],**label))
        guard();labels=dict(Counter(o['label'] for o in outputs));classified=datetime.now(timezone.utc).isoformat()
        unsupported=bool(outputs) and all(o['reason'] in ('UNSUPPORTED_LANGUAGE','LANGUAGE_UNKNOWN','LANGUAGE_NOT_CONFIRMED') for o in outputs)
        records=[];q_metadata=json.loads(q['metadata_json'])
        for feature in ['mentions.positive','mentions.negative']:
            record=next(deepcopy(r) for r in prepared['records'] if r['feature_id']==feature)
            measure=feature.split('.')[1]
            record.update(value=None if unsupported else labels.get(measure,0),quality='UNAVAILABLE' if unsupported else 'VALID',
                available_at=classified,observed_at=classified,metadata=dict(q_metadata,classification='unavailable' if unsupported else 'complete',
                    classifier=scope['classifier'],classification_qualified=True,qualification=qualification,
                    classification_outputs=outputs,label_counts=labels,reason='NO_SUPPORTED_LANGUAGE' if unsupported else None))
            records.append(record)
        def lock(conn):
            guard()
            if conn.dialect.name=='postgresql':
                conn.execute(text('LOCK TABLE _dataset_origin IN SHARE MODE'))
                conn.execute(text('SELECT feature_id FROM _feature_definitions WHERE algorithm_version=:version ORDER BY feature_id FOR UPDATE'),{'version':contract['algorithm_version']}).all()
            if datasets.identity(db,connection=conn)!=identity or generation(current(db,contract,batch['start'],connection=conn))!=generation(before):
                raise indicator_materializer.SourceChanged('População/base/classificação mudou durante o cálculo; reexecute sem misturar gerações.')
        def checkpoint(conn,changes):
            guard();producer_jobs.advance_checkpoint(conn,symbol=scope['symbol'],timeframe=scope['timeframe'],family=family,
                algorithm_version=contract['algorithm_version'],config_hash=contract['scope_hash'],source_sequence=sequence+int(bool(changes['inserted'] or changes['revised'])),
                last_bar_start=receipt['window_start'],status='INCOMPLETE' if unsupported else 'COMPLETE',
                state=dict(capture_id=receipt['capture_id'],classifier=scope['classifier'],outputs_sha256=mentions._hash(outputs)))
            guard()
        counts=feature_store.save_values(db,records,transaction_guard=lock,transaction_hook=checkpoint)
        job=producer_jobs.finish_run(db,run,source_sequence=sequence+int(bool(counts['inserted'] or counts['revised'])),
            status='INCOMPLETE' if unsupported else 'COMPLETE',counts=counts)
        return dict(receipt=dict(q_metadata['capture'],classification='unavailable' if unsupported else 'complete',label_counts=labels,
            classifier=scope['classifier'],qualification=qualification),counts=counts,job=job,
            algorithm_version=contract['algorithm_version'],job_family=family)
    except BaseException as exc:
        producer_jobs.abort_run(db,run,source_sequence=sequence,error=exc,counts=counts,cancelled=isinstance(exc,indicator_materializer.Cancelled));raise
