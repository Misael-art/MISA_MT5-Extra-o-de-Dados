"""Conferência independente de redução/saldo e fills nativos, sem novo executor."""
from collections import defaultdict
from datetime import datetime,timezone
from decimal import Decimal
from pathlib import Path
import math,re
from ..robots import mql
from ..composition.partials import stages,reduction

PARTIAL=re.compile(r'S09_PARTIAL event_at=(\d+) at=(\d+) stage=(\d+) state=(\w+) before=([^ ]+) requested=([^ ]+) filled=([^ ]+) remaining=([^ ]+) stop_before=([^ ]+) stop_after=([^ ]+) be_requested=([01]) be_confirmed=([01]) position=(\d+) history_reduction=([^ ]+) price=([^ ]+) net=([^ ]+) costs=([^\s]+)')
DEAL=re.compile(r'S09_DEAL position=(\d+) deal=(\d+) at_msc=(\d+) entry=(\d+) type=(\d+) volume=([^ ]+) price=([^ ]+) profit=([^ ]+) commission=([^ ]+) swap=([^ ]+) fee=([^ ]+) digits=(\d+)')

def _finite(value):
    number=float(value)
    if not math.isfinite(number):raise ValueError('Evento parcial/fill contém número inválido.')
    return number

def _clock(ms):return datetime.fromtimestamp(ms/1000,timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

def read_events(directory,plan,signals,sizing,ledger):
    text='\n'.join(mql.read_text(p) for p in (Path(directory)/'Tester').glob('Agent-*/logs/*.log'))
    if "S09_PARTIAL_ANCHOR missing" in text:raise ValueError("Âncora da parcial ausente; preserve identidade/risco antes de retomar.")
    deals=[];partials=[]
    for m in DEAL.finditer(text):
        pid,ticket,at,entry,side,*values,digits=m.groups()
        volume,price,profit,commission,swap,fee=map(_finite,values)
        if int(entry) not in (0,1) or int(side) not in (0,1) or volume<=0 or price<=0 or not 0<=int(digits)<=8:
            raise ValueError('Fill nativo incompatível com posição única sem adição/reversão.')
        deals.append(dict(position=int(pid),deal=int(ticket),at_msc=int(at),entry=int(entry),type=int(side),
                          volume=volume,price=price,profit=profit,commission=commission,swap=swap,fee=fee,digits=int(digits)))
    if len({d['deal'] for d in deals})!=len(deals):raise ValueError('Fill nativo duplicado; preserve a sessão.')
    grouped=defaultdict(list)
    for row in deals:grouped[row['position']].append(row)
    if len(grouped)!=len(ledger):raise ValueError('Posições/fills não correspondem ao livro fechado.')
    by_origin={};used=set()
    for signal in [s for s in signals if s['opened']]:
        matches=[(pid,rows) for pid,rows in grouped.items() if any(d['deal']==signal['deal_ticket'] and d['entry']==0 for d in rows)]
        if len(matches)!=1:raise ValueError('Entrada não identificada no histórico de fills.')
        pid,rows=matches[0];entries=[d for d in rows if d['entry']==0];exits=[d for d in rows if d['entry']==1]
        if len(entries)!=1 or not exits or any(d['type']==entries[0]['type'] for d in exits):
            raise ValueError('Histórico contém adição/inversão ou saída ausente; não qualificar parcial.')
        opened=entries[0];expected=next((r for r in ledger if r['entry_time']==_clock(opened['at_msc']) and r['direction']==signal['direction']),None)
        if expected is None or expected['entry_time'] in used:raise ValueError('Posição não identificada univocamente no livro.')
        used.add(expected['entry_time'])
        total=sum((Decimal(str(d['volume'])) for d in exits),Decimal(0))
        net=sum((sum((Decimal(str(d[k])) for k in ('profit','commission','swap','fee')),Decimal(0)) for d in rows),Decimal(0))
        price=sum((Decimal(str(d['price']))*Decimal(str(d['volume'])) for d in exits),Decimal(0))/total
        quantum=Decimal(10)**(-opened['digits'])
        if (total!=Decimal(str(opened['volume'])) or opened['volume']!=expected['volume']
                or opened['volume']!=signal['filled_volume'] or Decimal(str(expected['pnl']))!=net
                or opened['price']!=expected['entry_price'] or abs(price-Decimal(str(expected['exit_price'])))>quantum/2):
            raise ValueError('Volume/preço/custos dos fills não reconciliam com a posição/CSV/saldo.')
        by_origin[signal['event_at']]=(pid,opened,exits,expected)
    schedule=stages(plan);state={};last_stage={}
    for m in PARTIAL.finditer(text):
        origin,at,stage,status,before,requested,filled,remaining,old_sl,new_sl,be_req,be_ok,pid,history,price,net,costs=m.groups()
        origin,at,stage,pid=map(int,(origin,at,stage,pid))
        before,requested,filled,remaining,old_sl,new_sl,history,price,net,costs=map(_finite,(before,requested,filled,remaining,old_sl,new_sl,history,price,net,costs))
        if origin not in by_origin or not 1<=stage<=len(schedule) or status not in ('pending','confirmed'):
            raise ValueError('Parcial ausente de contexto, inviável ou pendente/incerta; conferir histórico antes de repetir.')
        position,entry,exits,trade=by_origin[origin];key=(origin,stage)
        spec=next(s for s in sizing if s['event_at']==origin)
        planned=reduction(entry['volume'],before,schedule[stage-1],spec['volume_min'],spec['volume_max'],spec['volume_step'])
        if (position!=pid or requested!=planned or at*1000<entry['at_msc'] or at>int(datetime.strptime(trade['exit_time'],'%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc).timestamp())
                or old_sl<=0 or new_sl<=0 or bool(int(be_req))!=plan['params'].get('breakeven_after_confirmation',False)
                or (new_sl-old_sl)*trade['direction']<0):
            raise ValueError('Agenda/ownership/tempo/proteção da parcial diverge do preset ou livro.')
        previous=state.get(key)
        if status=='pending':
            expected_before=entry['volume'] if stage==1 else state.get((origin,stage-1),{}).get('remaining')
            if previous or last_stage.get(origin,0)!=stage-1 or before!=expected_before or filled!=0 or remaining!=before or old_sl!=new_sl or be_ok!='0':
                raise ValueError('Intenção parcial repetida ou sem confirmação da etapa anterior.')
            last_stage[origin]=stage
        else:
            if not previous or previous['state']!='pending' or previous['before']!=before or previous['requested']!=requested:
                raise ValueError('Confirmação parcial sem intenção própria correspondente.')
            matching=[d for d in exits if d['at_msc']//1000==at and d['volume']==filled and d['price']==price]
            if len(matching)!=1:raise ValueError('Redução não corresponde a um fill histórico inequívoco.')
            deal=matching[0];actual_cost=sum((Decimal(str(deal[k])) for k in ('commission','swap','fee')),Decimal(0))
            if (filled!=requested or history!=filled or Decimal(str(before))-Decimal(str(filled))!=Decimal(str(remaining))
                    or remaining<spec['volume_min'] or (be_ok=='1' and (be_req!='1' or (new_sl-entry['price'])*trade['direction']<0))
                    or Decimal(str(net))!=Decimal(str(deal['profit']))+actual_cost or Decimal(str(costs))!=actual_cost):
                raise ValueError('Redução/saldo/BE/custos não demonstram parcial confirmada.')
        row=dict(event_at=origin,at=at,stage=stage,state=status,before=before,requested=requested,filled=filled,remaining=remaining,
                 stop_before=old_sl,stop_after=new_sl,be_requested=be_req=='1',be_confirmed=be_ok=='1',position=pid,history_reduction=history,price=price,net=net,costs=costs)
        state[key]=row;partials.append(row)
    if any(row['state']!='confirmed' for row in state.values()):raise ValueError('Confirmação parcial ausente; não aprovar resultado.')
    return partials,deals

def semantic(rows):
    # Tickets do Tester variam; vínculo real é conferido antes da comparação semântica.
    return [{k:v for k,v in row.items() if k not in ('position','deal')} for row in rows]
