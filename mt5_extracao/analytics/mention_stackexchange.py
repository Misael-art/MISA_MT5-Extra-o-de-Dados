"""Publicador independente MEN: perguntas públicas, com quota/backoff no journal atual."""
from datetime import datetime,timedelta,timezone
from email.utils import parsedate_to_datetime
import json
import math
import time
from urllib.parse import urlencode

from sqlalchemy import text

from mt5_extracao.analytics import feature_store,mentions,producer_jobs

API='https://api.stackexchange.com/2.3'
CORPUS='stackexchange.stackoverflow.questions.panel.v1'
# Identificador público de filtro (não é chave de acesso). Pré-calculado pela
# API filters/create: base=withbody, include=question.content_license.
QUESTION_FILTER='!X-cWn5YrCQCchzB5B4*yqi7.FnN7_ijJ-pWqLgm'
LICENSE_POLICY='https://stackoverflow.com/help/licensing'
LICENSE_POLICY_START=int(datetime(2018,5,2,tzinfo=timezone.utc).timestamp())
SYMBOL='@mentions.http';FAMILY='mentions:http:stackexchange:'


def _budget(db,guard,*,method,url=None,claim=False,delay=0,quota=None,provider="stackexchange"):
    """Recibo de acesso no checkpoint canônico; nenhuma espera/rede dentro de SQL."""
    if provider not in {"stackexchange","reddit","twitter","youtube"}:raise ValueError("Provedor HTTP desconhecido.")
    family=FAMILY if provider=="stackexchange" else "mentions:http:"+provider+":"
    waiting={}
    keys=['global','method:'+method]+(['url:'+mentions._hash(url)] if url else [])
    def update(conn,_counts):
        guard()
        if conn.dialect.name=='postgresql':conn.execute(text('SELECT pg_advisory_xact_lock(187071)'))
        now=datetime.now(timezone.utc)
        previous={}
        for key in keys:
            row=conn.execute(text('SELECT source_sequence,state_json FROM _analytics_producer_checkpoints '
                'WHERE symbol=:symbol AND tf=:tf AND family=:family'),dict(symbol=SYMBOL,tf='M1',family=family+key)).first()
            previous[key]=(int(row[0]),json.loads(row[1])) if row else (0,{})
        if claim:
            wait=max([0.]+[(datetime.fromisoformat(state['not_before'])-now).total_seconds()
                           for _,state in previous.values() if state.get('not_before')])
            waiting['seconds']=wait
            if wait>0:return
        for key,(sequence,state) in previous.items():
            until=now+timedelta(seconds=60 if claim and key.startswith('url:') else .1 if claim and key=='global' else 0)
            if not claim and key=='method:'+method:until=now+timedelta(seconds=delay)
            if quota==0 and key=='global':until=(now+timedelta(days=1)).replace(hour=0,minute=0,second=0,microsecond=0)
            if state.get('not_before'):until=max(until,datetime.fromisoformat(state['not_before']))
            state=dict(state,not_before=until.isoformat())
            if quota is not None and key=='global':state['quota_remaining']=quota
            producer_jobs.advance_checkpoint(conn,symbol=SYMBOL,timeframe='M1',family=family+key,algorithm_version='1',
                config_hash=mentions._hash(key),source_sequence=sequence+int(claim),last_bar_start=None,status='COMPLETE',state=state)
        guard()
    feature_store.save_values(db,[],transaction_hook=update)
    return waiting.get('seconds',0.)


def _read(db,session,path,parameters,method,guard,run,timeout,metrics):
    from mt5_extracao.analytics import mention_sources as transport
    url=API+path+'?'+urlencode(parameters)
    def claim():
        while True:
            guard();delay=_budget(db,guard,method=method,url=url,claim=True)
            if delay<=0:return
            remaining=timeout-(time.monotonic()-run['started_monotonic'])
            if delay>=remaining:raise RuntimeError('Quota/backoff da API excede o tempo restante; aguarde o prazo do journal e retome o mesmo painel.')
            transport._wait(delay,guard)
    def response_gate(response):
        retry=response.headers.get('Retry-After')
        if retry:
            try:delay=float(retry)
            except ValueError:delay=(parsedate_to_datetime(retry).astimezone(timezone.utc)-datetime.now(timezone.utc)).total_seconds()
            if not math.isfinite(delay) or delay>7*86400:raise ValueError('Retry-After inválido; acesso não confirmado.')
            _budget(db,guard,method=method,delay=max(0,delay))
        if response.status_code!=200:
            # Mesmo um HTTP de erro pode trazer quota/backoff. Não ignorar esse
            # recibo antes do transporte decidir retry; nenhum corpo é retido.
            raw=bytearray()
            for chunk in response.iter_content(16384):
                guard();raw.extend(chunk)
                if len(raw)>transport.MAX_RESPONSE_BYTES:raise ValueError('Erro da API excede512KiB; acesso não confirmado.')
            try:error=json.loads(raw)
            except ValueError:return
            if isinstance(error,dict):
                quota=error.get('quota_remaining');delay=error.get('backoff',0)
                if type(delay) is int and 0<=delay<=7*86400 and type(quota) is int and quota>=0:
                    _budget(db,guard,method=method,delay=delay,quota=quota)
    payload=transport._get(session,url,guard,run,timeout,metrics,request_gate=claim,response_gate=response_gate)
    if not isinstance(payload,dict):raise ValueError('Wrapper da API de perguntas inválido.')
    delay=payload.get('backoff',0);quota=payload.get('quota_remaining')
    if type(delay) is not int or not 0<=delay<=7*86400 or type(quota) is not int or quota<0:
        raise ValueError('Quota/backoff ausentes ou inválidos; não é possível confirmar a cobertura.')
    _budget(db,guard,method=method,delay=delay,quota=quota)
    if payload.get('error_id') is not None:
        raise RuntimeError('API de perguntas recusou o acesso; confira quota/backoff e retome explicitamente, sem contagem zero.')
    if not isinstance(payload.get('items'),list) or type(payload.get('has_more')) is not bool:
        raise ValueError('Página da API sem items/has_more válidos; não pode ser tratada como vazia.')
    return payload


def roster(db,session,count,start,end,guard,run,timeout,metrics):
    payload=_read(db,session,'/questions',dict(site='stackoverflow',page=1,pagesize=count,sort='creation',order='desc',
        fromdate=int(start.timestamp()),todate=int(end.timestamp())-1), 'questions',guard,run,timeout,metrics)
    ids=[item.get('question_id') if isinstance(item,dict) else None for item in payload['items']]
    if len(ids)>count or any(type(i) is not int or i<=0 for i in ids) or len(set(ids))!=len(ids):
        raise ValueError('Painel de perguntas inválido; IDs não são truncados ou deduplicados silenciosamente.')
    # has_more descreve o corpus além do painel explícito, não perda de uma
    # página prometida. Um painel curto com mais resultados é incompleto.
    if payload['has_more'] and len(ids)!=count:raise ValueError('Página truncada antes do tamanho do painel; cobertura incompleta.')
    return ids


def documents(db,session,ids,guard,run,timeout,metrics):
    from mt5_extracao.analytics import mention_sources as transport
    found={};licenses=set();inferred=0
    for offset in range(0,len(ids),8):
        selected=ids[offset:offset+8]
        payload=_read(db,session,'/questions/'+';'.join(map(str,selected)),
            dict(site='stackoverflow',pagesize=100,filter=QUESTION_FILTER),'questions-by-ids',guard,run,timeout,metrics)
        if payload['has_more']:raise ValueError('API paginou um lote de até8 IDs; cobertura não confirmada.')
        returned=[]
        for item in payload['items']:
            guard();ident=item.get('question_id') if isinstance(item,dict) else None
            if type(ident) is not int or ident not in selected or ident in found:
                raise ValueError('Pergunta duplicada/fora do painel; nenhuma contagem publicada.')
            epoch=item.get('creation_date');license=item.get('content_license')
            if type(epoch) is not int or epoch<0:raise ValueError('Pergunta sem horário reconhecido; cobertura não confirmada.')
            if license is None and epoch>=LICENSE_POLICY_START:
                # Contribuição e suas revisões não podem anteceder a criação.
                # Após esta data a política oficial é CC4.0. Não inferir a
                # licença de revisões de perguntas antigas nem substituir um
                # campo explicitamente conflitante da API.
                inferred+=1
            elif license not in {'CC BY-SA 2.5','CC BY-SA 3.0','CC BY-SA 4.0'}:
                raise ValueError('Pergunta sem licença reconhecida; revisão antiga/conflitante exige prova própria.')
            if not isinstance(item.get('title'),str) or not isinstance(item.get('body'),str):
                raise ValueError('Pergunta sem título/corpo completo; snippet não substitui conteúdo.')
            body=transport._plain(item['title'])+'\n'+transport._plain(item['body'])
            if len(body.encode())>64*1024:raise ValueError('Pergunta excede64KiB; não é truncada silenciosamente.')
            owner=item.get('owner') or {};author=owner.get('user_id') if isinstance(owner,dict) else None
            if author is not None and type(author) is not int:raise ValueError('Identidade pública de autor inválida.')
            stable=dict(id=str(ident),published_at=datetime.fromtimestamp(epoch,timezone.utc).isoformat(),text=body,kind='original',deleted=False)
            found[ident]=dict(stable,revision=mentions._hash(stable),author=str(author) if author is not None else None)
            returned.append(ident)
            if license is not None:licenses.add(license)
        if set(returned)!=set(selected):raise ValueError('API omitiu ID do painel; remoção/indisponibilidade não certifica zero.')
    return [found[i] for i in ids],dict(api_observed=sorted(licenses),policy_inferred_items=inferred,
        policy_inferred_license='CC BY-SA 4.0' if inferred else None,policy_reference=LICENSE_POLICY,
        policy_effective_from='2018-05-02T00:00:00Z',policy_version='so-public-contribution-license-2018-v1')
