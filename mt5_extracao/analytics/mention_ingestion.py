"""MEN: documentos/agregados limitados, no store e journal canônicos.

Texto só existe durante a preparação; valores conservam fingerprints e contagens.
Este serviço não abre rede. Importar um lote não certifica o acesso à sua fonte.
"""
from collections import Counter
from datetime import datetime, timezone
import json
import math
import re
import time
import unicodedata

from sqlalchemy import text

from mt5_extracao.analytics import datasets, feature_store, features, indicator_materializer, mentions, producer_jobs

MAX_DOCUMENTS = 1000
MAX_INPUT_BYTES = 4*1024*1024
LABELS = {"positive", "negative", "neutral", "mixed", "abstained"}
SOURCES = (
    ("F01", "X", "access_required", "https://docs.x.com/x-api/posts/counts/introduction"),
    ("F02", "Bluesky", "access_pending", "https://bsky.network/docs/jetstream/"),
    ("F03", "YouTube", "access_required", "https://developers.google.com/youtube/v3/docs/commentThreads/list"),
    ("F04", "Reddit", "agreement_required", "https://redditinc.com/policies/data-api-terms"),
    ("F05", "Publicadores/sites/fóruns", "publisher_specific", "https://github.com/HackerNews/API"),
    ("F06", "Brandwatch", "contract_required", "https://developers.brandwatch.com/docs/data-retrieval"),
    ("F07", "MarketPsych", "contract_and_schema_required", "https://www.marketpsych.com/home"),
    ("F08", "Google Trends", "not_mention_counts", "https://support.google.com/trends/answer/4365533"),
    ("F09", "GDELT", "news_family_not_sentiment_counts", "https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/"),
)


def source_capabilities():
    """Catálogo documental; não sonda rede nem afirma integração de conectores."""
    items=[dict(id=ident, name=name, access=access, reference=url, connector_implemented=False,
                 qualification="DOCUMENTED_ONLY", requires_explicit_population=True)
            for ident, name, access, url in SOURCES]
    publisher=next(item for item in items if item['id']=='F05')
    publisher.update(connector_implemented=True,qualification='IMPLEMENTED_REQUIRES_CAPTURE_RECEIPT',
        connectors=[dict(provider='hackernews',corpus='hackernews.newstories.panel.v1',mode='documents',
                         universe='sample',access='public_api',coverage='explicit_item_panel_only')])
    return items


def _normalized(value):
    return " ".join(unicodedata.normalize("NFKC", value).casefold().split())


def _contains(body, phrase):
    return re.search(r"(?<!\w)"+re.escape(_normalized(phrase))+r"(?!\w)", body) is not None


def _documents(contract, documents, start, end, guard):
    if not isinstance(documents, list) or len(documents) > MAX_DOCUMENTS:
        raise ValueError(f"Lote exige lista de até{MAX_DOCUMENTS} publicações, sem truncamento.")
    scope = contract['scope'];subject, query = scope['subject'], scope['query']
    found, excluded, eligible, duplicates = {}, Counter(), [], 0
    for raw in documents:
        guard()
        doc=mentions._object(raw, {'id','revision','published_at','text','kind'}, {'language','author','channel','community','deleted'}, 'document')
        for key in ('id','revision'):
            doc[key]=mentions._text(doc[key], 'document.'+key)
        if not isinstance(doc['text'],str) or len(doc['text'].encode()) > 64*1024:
            raise ValueError('Texto precisa ser string de até64KiB; conteúdo maior não é truncado silenciosamente.')
        if not isinstance(doc['kind'],str):raise ValueError('Tipo de publicação inválido.')
        if type(doc.get('deleted',False)) is not bool:raise ValueError('deleted precisa ser booleano.')
        if doc['published_at'] is not None or not doc.get('deleted'):
            doc['published_at']=mentions._utc(doc['published_at'],'published_at').isoformat()
        for key in ('language','author','channel','community'):
            doc[key]=mentions._text(doc[key], 'document.'+key) if doc.get(key) is not None else None
        fingerprint=mentions._hash(doc)
        if doc['id'] in found:
            if found[doc['id']][0] != fingerprint:
                raise ValueError('Uma publicação tem revisões conflitantes no mesmo lote; reconcilie a origem antes de contar.')
            duplicates+=1;continue
        found[doc['id']]=(fingerprint,doc)
    for fingerprint,doc in sorted(found.values(),key=lambda item:item[1]['id']):
        guard();published=mentions._utc(doc['published_at'],'published_at') if doc['published_at'] is not None else None;body=_normalized(doc['text'])
        reason=None
        if doc.get('deleted'):reason='DELETED'
        elif not start <= published < end:reason='OUTSIDE_WINDOW'
        elif doc['kind'] not in scope['source']['publication_types']:reason='EXCLUDED_PUBLICATION_TYPE'
        elif not any(_contains(body,a) for a in subject['aliases']):reason='OBJECT_NOT_MATCHED'
        elif subject['include_terms'] and not any(_contains(body,t) for t in subject['include_terms']):reason='INCLUSION_NOT_MATCHED'
        elif any(_contains(body,t) for t in subject['exclude_terms']):reason='EXCLUSION_MATCHED'
        elif not _contains(body,query['expression']):reason='QUERY_LITERAL_NOT_MATCHED'
        for dimension,field in [('languages','language'),('authors','author'),('channels','channel'),('communities','community')]:
            if reason is not None:break
            if query[dimension] and doc[field] is None:
                raise ValueError(f'Fonte sem {field}; não pode certificar o filtro {dimension} nem transformar desconhecido em zero.')
            if query[dimension] and doc[field] not in query[dimension]:reason='FILTER_'+dimension.upper()
        if reason:excluded[reason]+=1
        else:eligible.append(doc)
    return eligible,dict(excluded),duplicates,[dict(id_sha256=mentions._hash(doc['id']),revision_sha256=fingerprint)
        for fingerprint,doc in sorted(found.values(),key=lambda item:item[1]['id'])]


def prepare(contract, batch, *, synthetic_fixture=False, guard=lambda:None):
    """Valida um lote inteiro antes da primeira escrita; não interpreta texto como sentimento."""
    if type(synthetic_fixture) is not bool:raise ValueError('synthetic_fixture precisa ser booleano.')
    raw=feature_store._canonical_json(batch,'Lote MEN')
    if len(raw.encode()) > MAX_INPUT_BYTES:raise ValueError('Lote MEN excede4MiB; divida pelo produtor, sem truncar.')
    batch=mentions._object(batch, {'capture_schema_version','scope_hash','start','end','captured_at','coverage','documents'},
        {'reason','cursor','aggregate','annotations','classified_at'},'capture')
    if type(batch['capture_schema_version']) is not int or batch['capture_schema_version']!=1:
        raise ValueError('capture_schema_version não suportado.')
    if batch['scope_hash']!=contract['scope_hash']:raise ValueError('Lote pertence a outro objeto/fonte/consulta/base.')
    scope=contract['scope'];guard()
    if synthetic_fixture and not scope['source']['provider'].startswith('qa.'):
        raise ValueError('Fixture precisa de fonte qa. explícita; não se apresenta como coletor real.')
    start,end,captured=[mentions._utc(batch[k],k) for k in ['start','end','captured_at']]
    now=mentions._utc(datetime.now(timezone.utc),'received_at')
    if captured>now:raise ValueError('Captura não pode declarar recebimento futuro.')
    processed=captured if synthetic_fixture else now
    cursor=mentions._text(batch['cursor'],'cursor',limit=2048) if batch.get('cursor') is not None else None
    eligible,excluded,duplicates,roster=_documents(contract,batch['documents'],start,end,guard)
    if any(mentions._utc(d['published_at'],'published_at')>captured for d in eligible):
        raise ValueError('Publicação não pode anteceder sua própria existência na captura.')
    quantity=len(eligible);positive=negative=None;classification='pending';classified_at=None;label_counts=None
    if scope['source']['mode']=='aggregate':
        if batch['documents'] or batch.get('annotations'):
            raise ValueError('Agregado e documentos/amostra têm populações distintas; importe em escopos separados.')
        aggregate=mentions._object(batch.get('aggregate'),{'quantity'},{'positive','negative'},'aggregate')
        for key,value in aggregate.items():mentions._count(value,key)
        quantity=aggregate['quantity']
        if any(aggregate.get(k) is not None for k in ['positive','negative']):
            raise ValueError('P/N de agregado exigem qualificação do provedor/classificador; não se extrapola amostra.')
        classification='unavailable'
    elif batch.get('aggregate') is not None:
        raise ValueError('Contagem total não é o universo de documentos recuperados; use outro escopo.')
    annotations=batch.get('annotations')
    if annotations is not None:
        if not synthetic_fixture or not scope['classifier']['id'].startswith('qa.'):
            raise ValueError('Anotações manuais deste incremento são somente de fixture; classificação real exige MEN-04.')
        if not isinstance(annotations,dict):raise ValueError('annotations precisa mapear publicação para rótulos por objeto.')
        labels=[]
        for doc in eligible:
            by_subject=annotations.get(doc['id'])
            if (not isinstance(by_subject,dict) or not isinstance(by_subject.get(scope['subject']['id']),str) or
                    by_subject[scope['subject']['id']] not in LABELS):
                raise ValueError('Cada publicação elegível precisa de rótulo explícito para o objeto; ausência não é neutro.')
            labels.append(by_subject[scope['subject']['id']])
        label_counts=dict(Counter(labels));positive=label_counts.get('positive',0);negative=label_counts.get('negative',0)
        classification='complete';classified_at=batch.get('classified_at')
    coverage=batch['coverage']
    if coverage!='complete':
        quantity=positive=negative=None;classification='unavailable';classified_at=None;label_counts=None
    population=dict(scope_hash=contract['scope_hash'],start=start.isoformat(),end=end.isoformat(),roster=roster,
                    coverage=coverage,reason=batch.get('reason'),aggregate=batch.get('aggregate'),synthetic_fixture=synthetic_fixture)
    revision=mentions._hash(population)
    records=mentions.value_records(contract,start=start,end=end,captured_at=captured,processed_at=processed,
        population_revision=revision,quantity=quantity,positive=positive,negative=negative,
        coverage=coverage,classification=classification,classified_at=classified_at,reason=batch.get('reason'))
    receipt=dict(capture_schema_version=1,capture_id=revision,scope_hash=contract['scope_hash'],
        provider=scope['source']['provider'],source_mode=scope['source']['mode'],universe=scope['universe'],
        window_start=start.isoformat(),window_end=end.isoformat(),coverage=coverage,reason=batch.get('reason'),
        unique_documents=len(roster),eligible_documents=len(eligible),quantity=quantity,exclusions=excluded,roster=roster,
        synthetic_fixture=synthetic_fixture,source_verified=False,text_retained=False,
        cursor_sha256=mentions._hash(cursor) if cursor is not None else None)
    for record in records:
        # Colunas observed_at/available_at conservam o primeiro recebimento. Um
        # retry idêntico com relógio novo não muda o hash semântico da contagem.
        record['metadata'].pop('captured_at')
        record['metadata'].update(capture={k:v for k,v in receipt.items() if k!='cursor_sha256'},synthetic_fixture=synthetic_fixture)
        if record['feature_id']!='mentions.quantity' and label_counts is not None:
            record['metadata'].update(label_counts=label_counts,classification_qualified=False)
    return dict(receipt=receipt,records=records,cursor=cursor,duplicates=duplicates)


def ingest(db, *, selection, batch, synthetic_fixture=False, write_guard=None, cancel_check=None,
           timeout_seconds=120, max_memory_mb=1024, _run=None, _acquisition=None):
    """Publica Q/P/N + checkpoint juntos, com os recursos/cancelamento existentes."""
    if not isinstance(selection,dict):raise ValueError('selection precisa ser objeto JSON.')
    for value,name,limit in [(timeout_seconds,'timeout_seconds',3600),(max_memory_mb,'max_memory_mb',8192)]:
        if isinstance(value,bool) or not isinstance(value,(int,float)) or not math.isfinite(value) or not 0<value<=limit:
            raise ValueError(f'{name} precisa ser positivo e até{limit}.')
    started=_run if _run is not None else dict(started_monotonic=time.monotonic())
    def guard():indicator_materializer._guard(started,write_guard,cancel_check,timeout_seconds,max_memory_mb)
    guard();contract=mentions.describe(db,**selection);identity=datasets.identity(db)
    if identity['namespace']!=contract['scope']['base_namespace']:
        raise indicator_materializer.SourceChanged('Base alterada durante resolução do contrato MEN; reexecute.')
    prepared=prepare(contract,batch,synthetic_fixture=synthetic_fixture,guard=guard);guard()
    scope=contract['scope'];receipt=prepared['receipt']
    if _acquisition is not None:
        receipt.update(acquisition=_acquisition,source_verified=True)
        for record in prepared['records']:
            record['metadata']['capture']={k:v for k,v in receipt.items() if k!='cursor_sha256'}
    classification_before=None
    if scope['classifier']['id']=='object-vader-en':
        from mt5_extracao.analytics import mention_classification
        classification_before=mention_classification.current(db,contract,batch['start'])
        # Repetir a mesma captura não apaga P/N calculados posteriormente.
        # Uma população nova continua invalidando os rótulos anteriores.
        prepared['records']=[record for record in prepared['records'] if not (
            record['feature_id']!='mentions.quantity' and
            (old:=classification_before.get(record['feature_id'])) is not None and
            old['source_revision']==receipt['capture_id'] and
            json.loads(old['metadata_json']).get('classification_qualified') is True)]
    for definition in contract['definitions']:guard();features.register(db,**definition)
    previous=producer_jobs.get_checkpoint(db,scope['symbol'],scope['timeframe'],contract['job_family'])
    start_sequence=previous['source_sequence'] if previous else 0
    run=_run if _run is not None else producer_jobs.start_run(db,scope['symbol'],scope['timeframe'],contract['job_family'],
        contract['algorithm_version'],contract['scope_hash'],start_sequence,
        configuration=dict(capture_id=receipt['capture_id'],scope_hash=contract['scope_hash'],input_basis='bounded_mention_batch'))
    run['started_monotonic']=started['started_monotonic']
    counts=dict(inserted=0,revised=0,unchanged=0);end_sequence=start_sequence
    try:
        def before(conn):
            guard()
            if conn.dialect.name=='postgresql':conn.execute(text('LOCK TABLE _dataset_origin IN SHARE MODE'))
            if datasets.identity(db,connection=conn)!=identity:
                raise indicator_materializer.SourceChanged('Base alterada durante ingestão MEN; nenhuma contagem parcial publicada.')
            if classification_before is not None:
                if conn.dialect.name=='postgresql':
                    conn.execute(text('SELECT feature_id FROM _feature_definitions WHERE algorithm_version=:version ORDER BY feature_id FOR UPDATE'),{'version':contract['algorithm_version']}).all()
                if mention_classification.generation(mention_classification.current(db,contract,batch['start'],connection=conn))!=mention_classification.generation(classification_before):
                    raise indicator_materializer.SourceChanged('Geração MEN mudou durante a preparação; retome sem apagar classificação tardia.')
        def checkpoint(conn,counts):
            nonlocal end_sequence
            guard()
            prior=conn.execute(text('SELECT source_sequence,state_json FROM _analytics_producer_checkpoints '
                'WHERE symbol=:symbol AND tf=:tf AND family=:family'),
                dict(symbol=scope['symbol'],tf=scope['timeframe'],family=contract['job_family'])).first()
            complete=receipt['coverage']=='complete'
            sequence=(int(prior[0]) if prior else 0)+int(complete and bool(counts['inserted'] or counts['revised']))
            prior_cursor=json.loads(prior[1]).get('cursor') if prior else None
            producer_jobs.advance_checkpoint(conn,symbol=scope['symbol'],timeframe=scope['timeframe'],
                family=contract['job_family'],algorithm_version=contract['algorithm_version'],config_hash=contract['scope_hash'],
                source_sequence=sequence,last_bar_start=receipt['window_start'],status='COMPLETE' if complete else 'INCOMPLETE',
                state=dict(capture_id=receipt['capture_id'],cursor=prepared['cursor'] if complete else prior_cursor,
                           coverage=receipt['coverage'],input_basis='bounded_mention_batch'))
            guard()
            end_sequence=sequence
        counts=feature_store.save_values(db,prepared['records'],transaction_guard=before,transaction_hook=checkpoint)
        status='COMPLETE' if receipt['coverage']=='complete' else 'INCOMPLETE'
        measurement=producer_jobs.finish_run(db,run,source_sequence=end_sequence,status=status,counts=counts)
        run['_closed']=True
        return dict(receipt=receipt,counts=counts,duplicates=prepared['duplicates'],job=measurement,
                    algorithm_version=contract['algorithm_version'],job_family=contract['job_family'])
    except BaseException as exc:
        producer_jobs.abort_run(db,run,source_sequence=start_sequence,error=exc,counts=counts,
            cancelled=isinstance(exc,indicator_materializer.Cancelled))
        run['_closed']=True
        raise
