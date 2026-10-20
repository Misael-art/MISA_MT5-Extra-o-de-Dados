"""Agenda parcial tipada; prévia Decimal, sem envio nem confirmação presumida."""
from decimal import Decimal,ROUND_FLOOR


def stages(plan):
    mode,p=plan['mode'],plan['params']
    if mode=='strategy_default':return []
    if mode=='fixed_r':
        return [dict(trigger_r=p['trigger_r'],quantity=p['fraction'],mode=0,base=0)]
    out=[dict(trigger_r=p[f'trigger{i}_r'],quantity=p[f'quantity{i}'],
              mode=1 if mode=='staged_lots' else 0,base=p[f'base{i}']) for i in range(1,p['count']+1)]
    if any(s['mode']==0 and s['quantity']>=1 for s in out):
        raise ValueError('Fração parcial deve estar entre 0 e 100%, sem zerar a posição.')
    if any(right['trigger_r']<=left['trigger_r'] for left,right in zip(out,out[1:])):
        raise ValueError('Gatilhos das parciais devem ser estritamente crescentes.')
    if mode=='staged_fraction' and all(s['base']==0 for s in out) and sum(Decimal(str(s['quantity'])) for s in out)>=1:
        raise ValueError('Soma das parciais sobre volume inicial deve preservar saldo positivo.')
    return out


def reduction(initial,remaining,stage,minimum,maximum,step):
    initial,remaining,minimum,maximum,step=map(lambda x:Decimal(str(x)),(initial,remaining,minimum,maximum,step))
    quantity=Decimal(str(stage['quantity']))
    if (not all(v.is_finite() and v>0 for v in (initial,remaining,minimum,maximum,step,quantity))
            or remaining>initial or maximum<minimum):
        raise ValueError('Especificação ou volume parcial inválido; nenhuma redução foi confirmada.')
    requested=quantity if stage['mode']==1 else quantity*(initial if stage['base']==0 else remaining)
    part=(min(requested,maximum)/step).to_integral_value(rounding=ROUND_FLOOR)*step
    if part<minimum or part>=remaining or remaining-part<minimum:
        raise ValueError('Etapa parcial inviável pelo mínimo/passo/saldo; não autoriza break-even.')
    return float(part)


def preview_plan(plan,*,volume,minimum,maximum,step):
    remaining=volume;out=[]
    for stage in stages(plan):
        part=reduction(volume,remaining,stage,minimum,maximum,step)
        remaining=float(Decimal(str(remaining))-Decimal(str(part)))
        out.append(dict(**stage,reduction=part,remaining=remaining))
    return out
