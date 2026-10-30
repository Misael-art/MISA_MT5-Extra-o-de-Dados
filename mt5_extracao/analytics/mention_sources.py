"""MEN: captura explícita por APIs públicas, sem transação SQL durante rede."""
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
import json
import math
import time

import requests
from sqlalchemy import text

from mt5_extracao.analytics import datasets, feature_store, features, indicator_materializer, mention_ingestion, mentions, producer_jobs

HN_API = 'https://hacker-news.firebaseio.com/v0'
HN_CORPUS = 'hackernews.newstories.panel.v1'
MAX_RESPONSE_BYTES = 512*1024


class _PlainText(HTMLParser):
    def __init__(self):super().__init__(convert_charrefs=True);self.parts=[];self.hidden=0
    def handle_starttag(self,tag,attrs):
        if tag in ('script','style'):self.hidden+=1
        if tag in ('p','br','div','li'):self.parts.append(' ')
    def handle_endtag(self,tag):
        if tag in ('script','style'):self.hidden=max(0,self.hidden-1)
        if tag in ('p','div','li'):self.parts.append(' ')
    def handle_data(self,data):
        if not self.hidden:self.parts.append(data)


def _plain(value):
    if value is None:return ''
    if not isinstance(value,str):raise ValueError('Texto da API precisa ser string.')
    parser=_PlainText();parser.feed(value);parser.close();return ' '.join(''.join(parser.parts).split())


def _wait(seconds,guard):
    until=time.monotonic()+seconds
    while time.monotonic()<until:
        guard();time.sleep(min(.1,max(0,until-time.monotonic())))


def _get(session,url,guard,started,timeout_seconds,metrics,*,request_gate=None,response_gate=None):
    for attempt in range(3):
        if request_gate is not None:request_gate()
        guard();remaining=timeout_seconds-(time.monotonic()-started['started_monotonic'])
        if remaining<=0:raise TimeoutError('Tempo da captura esgotado; retome pelo mesmo journal.')
        metrics['requests']+=1
        try:
            with session.get(url,timeout=min(5.,remaining),stream=True) as response:
                if response_gate is not None:response_gate(response)
                if response.status_code in (429,500,502,503,504):
                    if attempt==2:raise RuntimeError(f'API pública retornou HTTP{response.status_code}; captura incompleta, retome depois.')
                    retry=response.headers.get('Retry-After')
                    try:
                        delay=float(retry) if retry else .25*2**attempt
                    except ValueError:
                        try:delay=(parsedate_to_datetime(retry).astimezone(timezone.utc)-datetime.now(timezone.utc)).total_seconds()
                        except (ValueError,TypeError,OverflowError):delay=.25*2**attempt
                    if not math.isfinite(delay) or delay>30 or delay>remaining:
                        raise RuntimeError('Retry-After excede o orçamento; aguarde e retome a captura explicitamente.')
                    metrics['retries']+=1;_wait(max(0,delay),guard);continue
                if response.status_code!=200:
                    raise RuntimeError(f'API pública retornou HTTP{response.status_code}; acesso/cobertura não confirmados.')
                payload=bytearray()
                for chunk in response.iter_content(16384):
                    guard();payload.extend(chunk)
                    if len(payload)>MAX_RESPONSE_BYTES:raise ValueError('Resposta excede512KiB; nenhum conteúdo é truncado ou publicado parcialmente.')
                metrics['bytes']+=len(payload)
                try:return json.loads(payload)
                except ValueError as exc:raise ValueError('API não devolveu JSON válido; nenhuma contagem foi publicada.') from exc
        except requests.RequestException as exc:
            if attempt==2:raise RuntimeError('Falha de rede após três tentativas; retome a mesma captura.') from exc
            metrics['retries']+=1;_wait(.25*2**attempt,guard)


def _document(item,ident):
    if not isinstance(item,dict) or type(item.get('id')) is not int or item['id']!=ident:
        raise ValueError('Publicação ausente/incompatível na API; não pode ser tratada como zero ou removida sem prova.')
    if any(type(item[k]) is not bool for k in ('deleted','dead') if k in item):
        raise ValueError('Estados deleted/dead inválidos; cobertura não confirmada.')
    deleted=bool(item.get('deleted') or item.get('dead'))
    kind={'story':'original','job':'original','poll':'original','comment':'reply'}.get(item.get('type'))
    if kind is None and not deleted:raise ValueError('Tipo de publicação desconhecido na API; reconcilie a cobertura.')
    epoch=item.get('time')
    if epoch is None and deleted:published=None
    elif type(epoch) is not int or epoch<0:raise ValueError('Publicação sem horário Unix válido; cobertura não confirmada.')
    else:published=datetime.fromtimestamp(epoch,timezone.utc).isoformat()
    body='' if deleted else '\n'.join(filter(None,[_plain(item.get('title')),_plain(item.get('text'))]))
    stable=dict(id=str(ident),published_at=published,text=body,kind=kind or 'original',deleted=deleted)
    return dict(stable,revision=mentions._hash(stable),author=item.get('by'))


def _origin_guard(conn,db,identity,contract):
    if conn.dialect.name=='postgresql':
        conn.execute(text('LOCK TABLE _dataset_origin IN SHARE MODE'))
        conn.execute(text('SELECT feature_id FROM _feature_definitions WHERE algorithm_version=:version '
            'AND family=\'mentions\' ORDER BY feature_id FOR UPDATE'),{'version':contract['algorithm_version']}).all()
    if datasets.identity(db,connection=conn)!=identity or identity['namespace']!=contract['scope']['base_namespace']:
        raise indicator_materializer.SourceChanged('Base mudou durante a captura; nenhuma janela foi confirmada na nova origem.')


def _pending(db,contract,identity,run,request,guard,*,resume,ids=None):
    scope=contract['scope'];selected={}
    def before(conn):guard();_origin_guard(conn,db,identity,contract)
    def save(conn,_counts):
        guard();row=conn.execute(text('SELECT source_sequence,last_bar_start_utc,state_json FROM _analytics_producer_checkpoints '
            'WHERE symbol=:symbol AND tf=:tf AND family=:family'),
            dict(symbol=scope['symbol'],tf=scope['timeframe'],family=contract['job_family'])).first()
        previous=json.loads(row[2]) if row else {};old=previous.get('pending') or {}
        if old.get('owner_run_id') not in (None,run['run_id']):
            status=conn.execute(text('SELECT status FROM _analytics_producer_runs WHERE run_id=:id'),{'id':old['owner_run_id']}).scalar()
            if status=='RUNNING':raise RuntimeError('Já existe captura ativa deste escopo; aguarde ou cancele a operação proprietária.')
        cohort=old.get('ids') if resume and old.get('request')==request else None
        if cohort is not None and old.get('ids_sha256')!=mentions._hash(cohort):
            raise ValueError('Roster pendente diverge do hash; reconcilie o recibo, sem buscar outra população silenciosamente.')
        if ids is not None:cohort=ids
        pending=dict(owner_run_id=run['run_id'],request=request,ids=cohort,
                     ids_sha256=mentions._hash(cohort) if cohort is not None else None)
        state=dict(previous,pending=pending,input_basis='public_api_panel')
        producer_jobs.advance_checkpoint(conn,symbol=scope['symbol'],timeframe=scope['timeframe'],family=contract['job_family'],
            algorithm_version=contract['algorithm_version'],config_hash=contract['scope_hash'],source_sequence=int(row[0]) if row else 0,
            last_bar_start=row[1] if row else None,state=state,status='RUNNING')
        guard();selected['ids']=cohort
    feature_store.save_values(db,[],transaction_guard=before,transaction_hook=save)
    return selected['ids']


def collect(db, *, selection, start, end, resume=True, classify=False, write_guard=None, cancel_check=None,
            timeout_seconds=120,max_memory_mb=1024):
    """Um painel explicitamente selecionado, uma janela fechada; não é censo do HN."""
    if type(resume) is not bool:raise ValueError('resume precisa ser booleano.')
    if type(classify) is not bool:raise ValueError('classify precisa ser booleano.')
    for value,name,maximum in [(timeout_seconds,'timeout_seconds',3600),(max_memory_mb,'max_memory_mb',8192)]:
        if isinstance(value,bool) or not isinstance(value,(int,float)) or not math.isfinite(value) or not 0<value<=maximum:
            raise ValueError(f'{name} precisa ser positivo e até{maximum}.')
    started=dict(started_monotonic=time.monotonic())
    def guard():indicator_materializer._guard(started,write_guard,cancel_check,timeout_seconds,max_memory_mb)
    guard();contract=mentions.describe(db,**selection);scope=contract['scope'];source=scope['source'];identity=datasets.identity(db)
    from mt5_extracao.analytics import mention_stackexchange, mention_social
    provider=source['provider']
    social=provider in mention_social.CORPORA
    corpus={'hackernews':HN_CORPUS,'stackexchange':mention_stackexchange.CORPUS,**mention_social.CORPORA}.get(provider)
    if corpus is None or source['corpus']!=corpus or source['mode']!='documents' or scope['universe']!='sample':
        raise ValueError('Conector exige provedor/corpus implementado, documents e sample; consulte mention-sources.')
    parameters=source.get('parameters',{})
    if not social and set(parameters)-{'panel_size'}:raise ValueError('Parâmetros da fonte desconhecidos; endpoint/credenciais não são argumentos deste conector.')
    count=mention_social.validate(scope) if social else parameters.get('panel_size',24)
    if type(count) is not int or not 1<=count<=64:raise ValueError('panel_size precisa ser inteiro entre1 e64, explicitamente parte da população.')
    if not social and (any(scope['query'][k] for k in ('languages','channels','communities')) or scope['query']['region'] is not None):
        raise ValueError('Este painel não certifica idioma/região/canal/comunidade; remova filtros não suportados. Autores públicos podem ser selecionados.')
    start,end=mentions._utc(start,'start'),mentions._utc(end,'end');now=datetime.now(timezone.utc)
    mentions.value_records(contract,start=start,end=end,captured_at=now,processed_at=now,population_revision='preflight',quantity=0)
    adapter=provider+'-composio-panel-v1' if social else 'hackernews-v1' if provider=='hackernews' else 'stackexchange-questions-v1'
    request=dict(scope_hash=contract['scope_hash'],start=start.isoformat(),end=end.isoformat(),panel_size=count,adapter=adapter)
    for definition in contract['definitions']:guard();features.register(db,**definition)
    previous=producer_jobs.get_checkpoint(db,scope['symbol'],scope['timeframe'],contract['job_family'])
    run=producer_jobs.start_run(db,scope['symbol'],scope['timeframe'],contract['job_family'],contract['algorithm_version'],
        contract['scope_hash'],previous['source_sequence'] if previous else 0,configuration=dict(request=request,input_basis='public_api_panel'))
    run['started_monotonic']=started['started_monotonic'];metrics=dict(requests=0,retries=0,bytes=0)
    try:
        ids=_pending(db,contract,identity,run,request,guard,resume=resume);resumed=ids is not None
        with requests.Session() as session:
            session.headers.update({'User-Agent':'MT5X-MEN/1 bounded-public-api-research'})
            if ids is None:
                roster=(mention_social.roster(db,scope,count,start,end,guard,run,timeout_seconds,metrics) if social else _get(session,HN_API+'/newstories.json',guard,run,timeout_seconds,metrics) if provider=='hackernews' else
                    mention_stackexchange.roster(db,session,count,start,end,guard,run,timeout_seconds,metrics))
                if not isinstance(roster,list) or any((not isinstance(i,str) or not i) if social else (type(i) is not int or i<=0) for i in roster):
                    raise ValueError('Roster da API inválido; nenhum painel foi contado.')
                ids=roster if social else list(dict.fromkeys(roster))[:count]
                _pending(db,contract,identity,run,request,guard,resume=True,ids=ids)
            if (not isinstance(ids,list) or len(ids)>count or any((not isinstance(i,str) or not i) if social else (type(i) is not int or i<=0) for i in ids) or len(set(ids))!=len(ids)):
                raise ValueError('Roster pendente inválido; não será reinterpretado como outra seleção.')
            producer_jobs.attach_run_inputs(db,run,dict(request=request,ids=ids,roster_sha256=mentions._hash(ids)))
            if provider=='hackernews':
                documents=[_document(_get(session,HN_API+f'/item/{ident}.json',guard,run,timeout_seconds,metrics),ident) for ident in ids];licenses=[]
            elif social:documents=mention_social.documents(db,scope,ids,guard,run,timeout_seconds,metrics);licenses=[]
            else:documents,licenses=mention_stackexchange.documents(db,session,ids,guard,run,timeout_seconds,metrics)
        captured=datetime.now(timezone.utc).isoformat();guard()
        if datasets.identity(db)!=identity:
            raise indicator_materializer.SourceChanged('Base mudou durante a requisição; reabra a captura na origem correta.')
        batch=dict(capture_schema_version=1,scope_hash=contract['scope_hash'],start=start.isoformat(),end=end.isoformat(),
            captured_at=captured,coverage='complete',documents=documents,cursor=mentions._hash(dict(request=request,ids=ids)))
        acquisition=dict(adapter='hackernews-v1',provider='hackernews',api=HN_API,panel_size=count,selected_items=len(ids),
            roster_sha256=mentions._hash(ids),text_scope='HN title and local text; linked articles not fetched',
            excluded_source_states=['deleted','dead'],author_content_redistributed=False)
        if provider=='stackexchange':
            acquisition.update(adapter=adapter,provider=provider,api=mention_stackexchange.API,
                text_scope='Stack Overflow question title and full local body; answers/comments not fetched',
                excluded_source_states=['unavailable_id_fails_capture'],licenses_observed=licenses['api_observed'],
                licensing=licenses,api_filter=mention_stackexchange.QUESTION_FILTER,
                license_basis='API field where present; missing only after2018-05-02 uses explicit publisher-policy inference; no raw redistribution',
                selection='first N questions created in the UTC window; sort creation descending; not census',
                quota_and_backoff='canonical journal checkpoints; identical URL at least60s; API method backoff; remaining quota explicit')
        if social:
            acquisition.update(adapter=adapter,provider=provider,api=mention_social.ROOTS[provider],
                text_scope='Explicit source panel: local post body or first-level video comments only',
                selection='first N current/recent API IDs, pinned for resume; observations outside window excluded; not census',
                excluded_source_states=['missing_id_fails_capture','reposts_excluded'],
                transport='Composio CLI; existing authorized account; GET only; no token in store',
                access_profile=source['access_profile'],terms_declared=source['terms_version'],
                qualification='CAPTURE_RECEIPT_ONLY_NOT_SOURCE_ACCURACY',
                quota_and_backoff='canonical checkpoint; no automatic retry; unavailable headers/quota remain unknown')
        result=mention_ingestion.ingest(db,selection=selection,batch=batch,write_guard=write_guard,cancel_check=cancel_check,
            timeout_seconds=timeout_seconds,max_memory_mb=max_memory_mb,_run=run,_acquisition=acquisition)
        result['acquisition']=dict(metrics,resumed_panel=resumed)
        if classify:
            from mt5_extracao.analytics import mention_classification
            guard()
            remaining=timeout_seconds-(time.monotonic()-started['started_monotonic'])
            result['classification']=mention_classification.classify(db,selection=selection,batch=batch,
                write_guard=write_guard,cancel_check=cancel_check,timeout_seconds=remaining,max_memory_mb=max_memory_mb)
        return result
    except BaseException as exc:
        if not run.get('_closed'):
            producer_jobs.abort_run(db,run,source_sequence=previous['source_sequence'] if previous else 0,error=exc,
                cancelled=isinstance(exc,indicator_materializer.Cancelled))
            run['_closed']=True
        raise
