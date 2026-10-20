"""Journal TM06: referência independente e cadeia persistente do relógio observado."""
from decimal import Decimal
from pathlib import Path
import re
from ..robots import mql
FIELDS=set('event_at position at_msc bar previous prior_bar current_open before anchor seen_before seen renewed direction upper lower close times highs lows'.split())
INTS=set('event_at position at_msc bar previous prior_bar current_open before anchor seen_before seen renewed direction'.split())

def observations(directory,by_origin,directions):
    text='\n'.join(mql.read_text(p) for p in (Path(directory)/'Tester').glob('Agent-*/logs/*.log'))
    if 'HDK_DC_GAP ' in text:raise ValueError('Cobertura de observações Donchian ausente; SL e outros prazos preservados. Confira os artefatos antes de reiniciar o relógio.')
    matches=list(re.finditer(r'HDK_DC (event_at=[^\r\n]+)',text));result=[];previous={}
    if text.count('HDK_DC event_at=')!=len(matches):raise ValueError('Journal Donchian truncado ou desconhecido; preserve os logs.')
    for match in matches:
        tokens=[part.split('=',1) for part in match.group(1).split()]
        row=dict(tokens)
        if len(tokens)!=len(row) or set(row)!=FIELDS:raise ValueError('Campos do journal Donchian ausentes/desconhecidos.')
        for key in INTS:row[key]=int(row[key])
        for key in ('upper','lower','close'):row[key]=Decimal(row[key])
        times=[int(v) for v in row['times'].split(',')]
        highs=[Decimal(v) for v in row['highs'].split(',')];lows=[Decimal(v) for v in row['lows'].split(',')]
        origin,pid,at=row['event_at'],row['position'],row['at_msc']
        if origin not in by_origin or by_origin[origin][0]!=pid:raise ValueError('Observação Donchian sem posição/entrada próprias.')
        entry=next(f for f in by_origin[origin][1] if f['entry']==0)
        if (len(times)!=21 or len(highs)!=21 or len(lows)!=21 or times[-1]!=row['bar'] or times[-2]!=row['prior_bar']
            or any(t<=0 or t>=row['current_open'] for t in times) or any(a>=b for a,b in zip(times,times[1:]))
            or not all(h.is_finite() and l.is_finite() and h>=l for h,l in zip(highs,lows))
            or not all(row[k].is_finite() for k in ('upper','lower','close'))
            or row['upper']!=max(highs[:20]) or row['lower']!=min(lows[:20])
            or row['current_open']*1000>at or row['current_open']*1000<=entry['at_msc']
            or row['direction']!=directions[origin] or any(row[k] not in (0,1) for k in ('seen_before','seen','renewed'))):
            raise ValueError('Canal anterior/close/disponibilidade Donchian diverge da janela independente.')
        import pandas as pd
        from ..strategies.indicators import donchian_context
        canonical=donchian_context(pd.DataFrame(dict(high=list(map(float,highs)),low=list(map(float,lows)),close=[float('nan')]*20+[float(row['close'])])),20,reference='previous').iloc[-1]
        if canonical.upper!=float(row['upper']) or canonical.lower!=float(row['lower']):
            raise ValueError('Canal nativo diverge do cálculo canônico publicado em T19.7.')
        old=previous.get(pid);anchor=old['anchor'] if old else entry['at_msc'];seen=bool(old['seen']) if old else False
        old_bar=old['bar'] if old else origin
        outside=row['close']>row['upper'] if row['direction']>0 else row['close']<row['lower']
        old_outside=old is not None and (old['close']>old['upper'] if row['direction']>0 else old['close']<old['lower'])
        renewed=outside and (row['bar']!=old_bar or not old_outside or row['direction']*(row['close']-old['close'])>0)
        if (row['previous']!=old_bar or row['previous'] not in (row['bar'],row['prior_bar']) or row['before']!=anchor
            or bool(row['seen_before'])!=seen or bool(row['renewed'])!=renewed or row['anchor']!=(at if renewed else anchor)
            or bool(row['seen'])!=(seen or renewed) or (old is not None and at<old['at_msc'])
            or (old is not None and row['bar']==old_bar and all(row[k]==old[k] for k in ('upper','lower','close')))):
            raise ValueError('Relógio Donchian reiniciado, renovação falsa ou observação repetida; conferir confirmação e cobertura.')
        row.update(times=times,highs=[str(v) for v in highs],lows=[str(v) for v in lows])
        previous[pid]=dict(row)
        for key in ('upper','lower','close'):row[key]=str(row[key])
        result.append(row)
    return result


def context(suffix,decision,pid,rows):
    tokens=[part.split('=',1) for part in suffix.strip().split()];fields=dict(tokens)
    expected=set('dc_known dc_anchor dc_seen dc_bar dc_upper dc_lower dc_close dc_observed dc_context'.split())
    if len(tokens)!=len(fields) or set(fields)!=expected:raise ValueError('Contexto de decisão Donchian ausente/truncado.')
    numeric={k:Decimal(v) for k,v in fields.items()}
    if not all(v.is_finite() for v in numeric.values()):raise ValueError('Contexto Donchian contém número inválido.')
    if numeric['dc_known']==0:
        if any(numeric.values()):raise ValueError('Contexto Donchian desconhecido contém valores presumidos.')
        return dict(channel_idle_ms=None,channel_returned=False,channel_breakout_seen=False),None,False
    available=[r for r in rows if r['position']==pid and r['at_msc']<=decision]
    if not available:raise ValueError('Decisão Donchian sem observação causal anterior.')
    last=available[-1]
    pairs=dict(dc_anchor='anchor',dc_seen='seen',dc_bar='bar',dc_upper='upper',dc_lower='lower',dc_close='close',dc_context='at_msc')
    if (numeric['dc_known']!=1 or numeric['dc_observed']!=decision or any(numeric[k]!=Decimal(str(last[v])) for k,v in pairs.items())
        or not last['before']<=last['anchor']<=decision):raise ValueError('Contexto Donchian difere do journal confirmado/instante conhecido.')
    returned=bool(last['seen'] and Decimal(last['lower'])<=Decimal(last['close'])<=Decimal(last['upper']));idle=decision-last['anchor']
    metadata=dict(channel_idle_ms=idle,channel_returned=returned,channel_breakout_seen=bool(last['seen']),
        channel_anchor_msc=last['anchor'],channel_observed_msc=decision,channel_context_known_msc=last['at_msc'],
        channel_bar=last['bar'],channel_upper=last['upper'],channel_lower=last['lower'],channel_close=last['close'],
        channel_source_times=last['times'],channel_source_highs=last['highs'],channel_source_lows=last['lows'])
    return metadata,idle,returned
