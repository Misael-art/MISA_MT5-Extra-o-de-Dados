"""Painéis sociais explícitos por Composio CLI; credenciais pertencem ao conector.

Somente GET em endpoints fixos. Texto permanece em memória; o journal conserva
IDs, revisão e orçamento, nunca respostas brutas ou material de autenticação.
"""
from datetime import datetime, timezone
import json
import re
import shutil
import subprocess
import threading
import time
from urllib.parse import urlencode

from . import mentions

CORPORA = {'reddit':'reddit.community.originals.panel.v1',
           'twitter':'x.recent-search.posts.panel.v1',
           'youtube':'youtube.video.top-level-comments.panel.v1'}
ROOTS = {'reddit':'https://oauth.reddit.com', 'twitter':'https://api.x.com/2',
         'youtube':'https://www.googleapis.com/youtube/v3'}
MAX_BYTES = 512*1024


def validate(scope):
    source=scope['source'];provider=source['provider'];p=source.get('parameters',{})
    if provider not in CORPORA or source['corpus']!=CORPORA[provider] or source['mode']!='documents' or scope['universe']!='sample':
        raise ValueError('Fonte social exige corpus versionado, documents e sample.')
    if source['access_profile']!='composio-read-approved':
        raise ValueError('Autorize a fonte no Composio e declare access_profile=composio-read-approved conforme o acesso permitido.')
    if set(p)-{'panel_size','account','video_id'}:
        raise ValueError('Parâmetros sociais desconhecidos; não informe endpoint ou credenciais.')
    if p.get('account') is not None:mentions._text(p['account'],'account',limit=128)
    count=p.get('panel_size',24)
    if type(count) is not int or not 1<=count<=64:raise ValueError('panel_size deve estar entre1 e64.')
    query=scope['query']
    if query['region'] is not None or query['languages'] or query['channels']:
        raise ValueError('Este painel não certifica filtros de região, idioma ou canal; não os ignore silenciosamente.')
    if provider=='reddit':
        if len(query['communities'])!=1 or not re.fullmatch(r'[A-Za-z0-9_]{2,50}',query['communities'][0]):
            raise ValueError('Reddit exige uma comunidade explícita, sem URL ou curingas.')
        if source['publication_types']!=['original']:raise ValueError('O painel Reddit implementado cobre publicações originais; comentários são outro painel.')
    elif query['communities']:raise ValueError('Comunidades não são suportadas neste provedor.')
    if provider=='youtube':
        if not re.fullmatch(r'[A-Za-z0-9_-]{11}',str(p.get('video_id',''))):raise ValueError('YouTube exige video_id explícito de11 caracteres.')
        if source['publication_types']!=['comment']:raise ValueError('O painel YouTube cobre comentários de primeiro nível, sem respostas implícitas.')
    elif 'video_id' in p:raise ValueError('video_id pertence somente ao YouTube.')
    return count


def proxy(provider,path,parameters,guard,timeout,account=None):
    """Leitor limitado do processo próprio; nunca registra stdout/stderr da CLI."""
    binary=shutil.which('composio')
    if not binary:raise RuntimeError('Composio CLI ausente; use o fluxo oficial de instalação/conexão da fonte.')
    url=ROOTS[provider]+path+'?'+urlencode(parameters)
    command=[binary,'proxy',url,'--toolkit',provider,'--method','GET']
    if account is not None:command+=['--account',account]
    guard();process=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,stdin=subprocess.DEVNULL)
    chunks=[];size=0;overflow=threading.Event();finished=threading.Event()
    def read():
        nonlocal size
        try:
            while chunk:=process.stdout.read(16384):
                size+=len(chunk)
                if size>MAX_BYTES:overflow.set();break
                chunks.append(chunk)
        finally:finished.set()
    reader=threading.Thread(target=read,name='mentions-composio-read',daemon=True);reader.start()
    started=time.monotonic()
    try:
        while process.poll() is None or not finished.is_set():
            guard()
            if overflow.is_set():raise ValueError('Resposta social excede512KiB; reduza o painel, sem truncamento.')
            if time.monotonic()-started>timeout:raise TimeoutError('Consulta social excedeu o prazo; retome pelo journal.')
            time.sleep(.05)
        if overflow.is_set():raise ValueError('Resposta social excede512KiB; reduza o painel.')
        if process.returncode:
            raise RuntimeError(f'Acesso {provider} não confirmado. Confira a conexão/conta, aprovação, quota e prazo no fluxo oficial; nenhuma contagem foi publicada.')
        try:payload=json.loads(b''.join(chunks).decode('utf-8-sig'))
        except (ValueError,UnicodeError) as exc:raise ValueError('Conector social retornou JSON inválido; não equivale a um painel vazio.') from exc
        if not isinstance(payload,dict) or payload.get('error') or payload.get('errors'):
            raise ValueError('API social retornou erro ou resposta parcial; não publicar zero.')
        return payload
    finally:
        if process.poll() is None:
            process.terminate()
            try:process.wait(timeout=3)
            except subprocess.TimeoutExpired:process.kill();process.wait(timeout=3)
        reader.join(timeout=2);process.stdout.close()


def _read(db,scope,path,parameters,guard,run,timeout,metrics):
    from . import mention_stackexchange as budget, mention_sources
    provider=scope['source']['provider'];url=ROOTS[provider]+path+'?'+urlencode(parameters)
    while True:
        guard();delay=budget._budget(db,guard,method=path,url=url,claim=True,provider=provider)
        if delay<=0:break
        if delay>=timeout-(time.monotonic()-run['started_monotonic']):
            raise RuntimeError('Prazo de acesso persistido excede o orçamento; aguarde e retome o mesmo painel.')
        mention_sources._wait(delay,guard)
    metrics['requests']+=1
    try:
        data=proxy(provider,path,parameters,guard,max(.001,timeout-(time.monotonic()-run['started_monotonic'])),
                   account=scope['source'].get('parameters',{}).get('account'))
    except BaseException:
        # A CLI não expõe todos os headers HTTP: não inventar quota restante ou
        # Retry-After, nem repetir automaticamente uma resposta incerta.
        budget._budget(db,lambda:None,method=path,delay=60,provider=provider)
        raise
    metrics['bytes']+=len(json.dumps(data,ensure_ascii=False).encode())
    return data


def roster(db,scope,count,start,end,guard,run,timeout,metrics):
    provider=scope['source']['provider']
    if provider=='reddit':
        community=scope['query']['communities'][0]
        data=_read(db,scope,f'/r/{community}/new',dict(limit=count,raw_json=1),guard,run,timeout,metrics)
        if data.get('kind')!='Listing' or not isinstance(data.get('data',{}).get('children'),list):raise ValueError('Listing Reddit inválido.')
        items=data['data']['children'];ids=[i.get('data',{}).get('name') for i in items]
        if data['data'].get('after') and len(ids)<count:raise ValueError('Página Reddit incompleta para o painel prometido.')
    elif provider=='twitter':
        if (datetime.now(timezone.utc)-start).total_seconds()>7*86400:
            raise ValueError('Busca recente X não certifica janela com mais de7 dias; não substituir por histórico presumido.')
        data=_read(db,scope,'/tweets/search/recent',dict(query=scope['query']['expression']+' -is:retweet',
            start_time=start.isoformat().replace('+00:00','Z'),end_time=end.isoformat().replace('+00:00','Z'),
            max_results=max(10,count)),guard,run,timeout,metrics)
        items=_posts(data);ids=[i.get('id') for i in items[:count]]
    else:
        data=_read(db,scope,'/commentThreads',dict(part='snippet',videoId=scope['source']['parameters']['video_id'],
            order='time',maxResults=count,textFormat='plainText'),guard,run,timeout,metrics)
        if not isinstance(data.get('items'),list):raise ValueError('Página YouTube sem items; não é zero.')
        ids=[i.get('snippet',{}).get('topLevelComment',{}).get('id') for i in data['items']]
        if data.get('nextPageToken') and len(ids)<count:raise ValueError('Página YouTube incompleta para o painel prometido.')
    if len(ids)>count or len(ids)!=len(set(ids)) or any(not isinstance(i,str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,256}',i) for i in ids):
        raise ValueError('Roster social inválido ou duplicado; população não é truncada silenciosamente.')
    return ids


def _posts(data):
    if 'data' not in data and data.get('meta',{}).get('result_count')==0:return []
    if not isinstance(data.get('data'),list):raise ValueError('Página X sem data/result_count válido.')
    return data['data']


def documents(db,scope,ids,guard,run,timeout,metrics):
    if not ids:return []
    provider=scope['source']['provider']
    if provider=='reddit':
        data=_read(db,scope,'/api/info',dict(id=','.join(ids),raw_json=1),guard,run,timeout,metrics)
        if data.get('kind')!='Listing' or not isinstance(data.get('data',{}).get('children'),list):raise ValueError('Hidratação Reddit inválida.')
        found={i.get('data',{}).get('name'):i.get('data',{}) for i in data['data']['children']}
    elif provider=='twitter':
        data=_read(db,scope,'/tweets',{'ids':','.join(ids),'tweet.fields':'created_at,lang,author_id,referenced_tweets,note_tweet'},guard,run,timeout,metrics)
        found={i.get('id'):i for i in _posts(data)}
    else:
        data=_read(db,scope,'/comments',dict(part='snippet',id=','.join(ids),textFormat='plainText'),guard,run,timeout,metrics)
        if not isinstance(data.get('items'),list):raise ValueError('Hidratação YouTube inválida.')
        found={i.get('id'):i.get('snippet',{}) for i in data['items']}
    items=data['data']['children'] if provider=='reddit' else _posts(data) if provider=='twitter' else data['items']
    if len(items)!=len(ids) or set(found)!=set(ids):raise ValueError('Publicações ausentes/duplicadas na hidratação; preserve o roster e retome sem publicar zero.')
    result=[]
    for ident in ids:
        item=found[ident]
        if provider=='reddit':
            body=' '.join([item.get('title') or '',item.get('selftext') or ''])
            stamp=item.get('created_utc')
            if isinstance(stamp,bool) or not isinstance(stamp,(int,float)):raise ValueError('Reddit sem created_utc válido.')
            deleted=bool(item.get('removed_by_category')) or item.get('selftext') in ('[deleted]','[removed]')
            fields=dict(published_at=datetime.fromtimestamp(stamp,timezone.utc).isoformat(),text='' if deleted else body,
                kind='repost' if item.get('crosspost_parent') else 'original',author=item.get('author'),community=item.get('subreddit'),deleted=deleted)
        elif provider=='twitter':
            refs={r.get('type') for r in item.get('referenced_tweets',[])}
            if 'retweeted' in refs:kind='repost'
            else:kind='quote_with_comment' if 'quoted' in refs else 'reply' if 'replied_to' in refs else 'original'
            fields=dict(published_at=item.get('created_at'),text=item.get('note_tweet',{}).get('text',item.get('text')),
                        kind=kind,author=item.get('author_id'),language=item.get('lang'))
        else:
            if item.get('videoId')!=scope['source']['parameters']['video_id']:raise ValueError('Comentário pertence a outro vídeo.')
            fields=dict(published_at=item.get('publishedAt'),text=item.get('textOriginal'),kind='comment',
                author=item.get('authorChannelId',{}).get('value'),channel=item.get('channelId'))
        stable=dict(id=provider+':'+ident,**fields)
        result.append(dict(stable,revision=mentions._hash(stable)))
    return result
