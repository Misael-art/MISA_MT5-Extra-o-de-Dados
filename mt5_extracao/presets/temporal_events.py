"""Recibo temporal: causalidade, PnL observado, redução total e fills efetivos."""
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
import math,re
from ..robots import mql
from ..strategies.temporal import REASONS
from ..strategies.temporal_er import from_selection,EfficiencyTemporalPolicy
from .efficiency_events import read_context

EVENT=re.compile(r"HDK_TIME event_at=(\d+) at_msc=(\d+) decision_msc=(\d+) reason=([1-8]) state=(\w+) age_ms=(\d+) net=([^ ]+) before=([^ ]+) filled=([^ ]+) remaining=([^ ]+) position=(\d+) history_reduction=([^ ]+) price=([^ ]+) cash=([^ ]+) costs=([^ ]+) realized=([^ ]+) floating=([^ ]+) swap=([^ ]+) history_count=(\d+)")
D=lambda v:Decimal(str(v))

def read_events(directory,selection,signals,sizing,deals,*,channel_observations=None,atr_observations=None):
    text="\n".join(mql.read_text(p) for p in (Path(directory)/"Tester").glob("Agent-*/logs/*.log"))
    if "HDK_TIME_ANCHOR missing" in text:raise ValueError("Âncora temporal ausente; não reiniciar idade nem inferir configuração.")
    matches=list(EVENT.finditer(text))
    if text.count("HDK_TIME event_at=")!=len(matches):raise ValueError("Formato temporal ausente/desconhecido; preserve o diário nativo.")
    if "HDK_ATR event_at=" in text and selection["mode"]!="temporal_atr_v1":raise ValueError("ATR nativo foi observado com a política desabilitada.")
    if selection["mode"]=="disabled":
        if matches:raise ValueError("Saída temporal foi executada apesar da política desabilitada.")
        return []
    policy=from_selection(selection);grouped=defaultdict(list)
    for fill in deals:grouped[fill["position"]].append(fill)
    by_origin={}
    for signal in signals:
        if not signal["opened"]:continue
        found=[(pid,fs) for pid,fs in grouped.items() if any(f['deal']==signal['deal_ticket'] and f['entry']==0 for f in fs)]
        if len(found)!=1:raise ValueError("Entrada temporal não identificada nos fills.")
        by_origin[signal['event_at']]=found[0]
    from ..strategies.temporal_donchian import DonchianTemporalPolicy
    from . import donchian_events
    channel_rows=donchian_events.observations(directory,by_origin,{s['event_at']:s['direction'] for s in signals if s['opened']}) if isinstance(policy,DonchianTemporalPolicy) else []
    if channel_observations is not None:channel_observations.extend(channel_rows)
    from ..strategies.temporal_atr import AtrTemporalPolicy
    from . import atr_events
    atr_rows=atr_events.observations(directory,by_origin,policy) if isinstance(policy,AtrTemporalPolicy) else []
    if atr_observations is not None:atr_observations.extend(atr_rows)
    pending={};confirmed=set();result=[]
    for match in matches:
        origin,at,decision,code,state,age,net,before,filled,remaining,pid,history,price,cash,costs,realized,floating,swap,count=match.groups()
        origin,at,decision,code,age,pid,count=map(int,(origin,at,decision,code,age,pid,count))
        nums=list(map(float,(before,filled,remaining,history,price,cash,costs,realized,floating,swap)))
        if not all(math.isfinite(n) for n in nums):raise ValueError("Evento temporal contém número inválido.")
        before,filled,remaining,history,price,cash,costs,realized,floating,swap=nums
        net=None if net=="unavailable" else float(net)
        if net is not None and not math.isfinite(net):raise ValueError("PnL temporal não finito.")
        if origin not in by_origin or state not in ('pending','confirmed') or pid!=by_origin[origin][0]:
            raise ValueError("Saída temporal pendente/incerta ou sem ownership; conferir histórico antes de repetir.")
        fs=by_origin[origin][1];entry=next(f for f in fs if f['entry']==0);exits=[f for f in fs if f['entry']==1]
        if at<decision or decision<entry['at_msc'] or age!=decision-entry['at_msc'] or before<=0:
            raise ValueError("Idade/relógio temporal diverge do primeiro preenchimento; parcial não reinicia a idade.")
        reason=REASONS[code]
        er_context={}
        if isinstance(policy,EfficiencyTemporalPolicy):
            suffix=match.string[match.end():].partition("\n")[0]
            atr_context={};atr_baseline=atr_current=None
            if isinstance(policy,AtrTemporalPolicy):
                suffix,marker,atr_suffix=suffix.partition(' atr_known=')
                if not marker:raise ValueError('Contexto ATR da decisão ausente.')
                atr_context,atr_baseline,atr_current=atr_events.context('atr_known='+atr_suffix,decision,pid,atr_rows)
            dc_context={};idle=None;returned=False
            if isinstance(policy,DonchianTemporalPolicy) and policy.channel_idle_seconds:
                er_suffix,marker,dc_suffix=suffix.partition(' dc_known=')
                if not marker:raise ValueError('Contexto Donchian da decisão ausente.')
                dc_context,idle,returned=donchian_events.context('dc_known='+dc_suffix,decision,pid,channel_rows)
                suffix=er_suffix
            er_context,reference_er=read_context(suffix,decision)
            extra=dict(idle_ms=idle,returned=returned) if isinstance(policy,DonchianTemporalPolicy) else {}
            if isinstance(policy,AtrTemporalPolicy):extra.update(atr_baseline=atr_baseline,atr_current=atr_current)
            actual_decision=policy.decision(age,net,er=er_context['er_value'],**extra)
            reference_decision=policy.decision(age,net,er=reference_er,**extra)
            er_context.update(dc_context);er_context.update(atr_context)
            if actual_decision!=reason or reference_decision!=reason:raise ValueError("Condição ER/idade/prioridade diverge do contexto conhecido e da referência independente.")
        elif policy.decision(age,net)!=reason:raise ValueError("Motivo/prazo temporal diverge da referência independente ou do PnL conhecido.")
        previous_exits=exits[:-1]
        expected_before=D(entry['volume'])-sum((D(f['volume']) for f in previous_exits),D(0))
        if D(before)!=expected_before:raise ValueError("Saldo antes da saída temporal diverge das reduções anteriores.")
        if net is not None:
            if not 1<=count<len(fs) or any(f['at_msc']>decision for f in fs[:count]):
                raise ValueError("Corte do histórico temporal contém futuro ou está ausente.")
            known=fs[:count]
            if len(known)!=1+len(previous_exits):raise ValueError("Corte do histórico não corresponde aos fills conhecidos antes da saída.")
            actual=sum((sum((D(f[k]) for k in ('profit','commission','swap','fee')),D(0)) for f in known),D(0))
            if abs(actual-D(realized))>D('0.00000002') or abs(D(net)-D(realized)-D(floating)-D(swap))>D('0.00000002'):
                raise ValueError("PnL temporal omite/dobra custos ou reduções observados.")
        row=dict(event_at=origin,at_msc=at,decision_msc=decision,reason=reason,state=state,age_ms=age,net_observed=net,
                 before=before,filled=filled,remaining=remaining,position=pid,history_reduction=history,price=price,cash=cash,costs=costs,
                 realized=realized if net is not None else None,floating=floating if net is not None else None,swap=swap if net is not None else None,history_count=count,**er_context)
        if state=='pending':
            if origin in pending or origin in confirmed or at!=decision or remaining!=before or any(v!=0 for v in (filled,history,price,cash,costs)):
                raise ValueError("Intenção temporal repetida ou contém fill antes da confirmação.")
            pending[origin]=row
        else:
            first=pending.get(origin)
            if first is None or origin in confirmed or any(row[k]!=first[k] for k in ('decision_msc','reason','age_ms','net_observed','before','history_count','realized','floating','swap',*er_context)):
                raise ValueError("Confirmação temporal sem intenção única/imutável; não repetir envio.")
            spec=next(s for s in sizing if s['event_at']==origin);volume_epsilon=D(spec['volume_step'])*D('1e-8')
            fill=exits[-1];profit=sum((D(fill[k]) for k in ('profit','commission','swap','fee')),D(0));fees=sum((D(fill[k]) for k in ('commission','swap','fee')),D(0))
            if (remaining!=0 or abs(D(filled)-D(before))>volume_epsilon or abs(D(history)-D(filled))>volume_epsilon
                or D(fill['volume'])!=D(filled) or not decision<=fill['at_msc']<=at or price!=fill['price']
                or abs(D(cash)-profit)>D('0.00000002') or abs(D(costs)-fees)>D('0.00000002')):
                raise ValueError("Liberação temporal não confirmada por saldo zero e fill/custos reais.")
            confirmed.add(origin)
        result.append(row)
    if set(pending)!=confirmed:raise ValueError("Há encerramento temporal pendente/incerto; preserve a sessão e reconcilie antes de repetir.")
    return result
