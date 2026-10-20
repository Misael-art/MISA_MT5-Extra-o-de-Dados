from pathlib import Path
from copy import deepcopy
import pytest
from mt5_extracao.presets import temporal_events as events
from mt5_extracao.strategies.temporal import TemporalPolicy

@pytest.fixture
def capture(tmp_path):
    file=tmp_path/'Tester/Agent-private/logs/test.log';file.parent.mkdir(parents=True)
    # Custos de entrada/saída não nulos: PnL atual negativo apesar de flutuante positivo.
    deals=[dict(position=2,deal=2,at_msc=1000,entry=0,volume=1.,price=100.,profit=0.,commission=-2.,swap=0.,fee=0.),
           dict(position=2,deal=3,at_msc=11000,entry=1,volume=1.,price=101.,profit=1.,commission=-2.,swap=0.,fee=0.)]
    base='HDK_TIME event_at=0 at_msc=11000 decision_msc=11000 reason=4 state={state} age_ms=10000 net=-1 before=1 filled={filled} remaining={remaining} position=2 history_reduction={history} price={price} cash={cash} costs={costs} realized=-2 floating=1 swap=0 history_count=1'
    pending=base.format(state='pending',filled=0,remaining=1,history=0,price=0,cash=0,costs=0)
    confirmed=base.format(state='confirmed',filled=1,remaining=0,history=1,price=101,cash=-1,costs=-2)
    selection=dict(mode='temporal_v1',params=TemporalPolicy(loss_seconds=10,gain_seconds=10).to_dict())
    signals=[dict(event_at=0,opened=True,deal_ticket=2)];sizing=[dict(event_at=0,volume_step=.01)]
    file.write_text(pending+'\n'+confirmed,encoding='utf-8')
    return tmp_path,file,selection,signals,sizing,deals,pending,confirmed

def test_actual_ack_and_entry_cost_are_required(capture):
    root,file,selection,signals,sizing,deals,_,_=capture
    out=events.read_events(root,selection,signals,sizing,deals)
    assert out[-1]['reason']=='time_loss' and out[-1]['remaining']==0
    assert out[-1]['net_observed']==-1 and out[-1]['realized']==-2

@pytest.mark.parametrize('fault',['pending','no_pending','repeat','uncertain','age_reset','ignored_cost','wrong_reason','fake_release','missing_history','unknown_format','early_ack'])
def test_unconfirmed_or_noncausal_time_cannot_qualify(capture,fault):
    root,file,selection,signals,sizing,deals,pending,confirmed=capture
    text=pending+'\n'+confirmed
    if fault=='pending':text=pending
    if fault=='no_pending':text=confirmed
    if fault=='repeat':text+='\n'+confirmed
    if fault=='uncertain':text=text.replace('state=confirmed','state=uncertain')
    if fault=='age_reset':text=text.replace('age_ms=10000','age_ms=1000')
    if fault=='ignored_cost':text=text.replace('net=-1','net=1').replace('realized=-2','realized=0').replace('reason=4','reason=3')
    if fault=='wrong_reason':text=text.replace('reason=4','reason=3')
    if fault=='fake_release':text=text.replace('state=confirmed age_ms=10000 net=-1 before=1 filled=1 remaining=0','state=confirmed age_ms=10000 net=-1 before=1 filled=0 remaining=0')
    if fault=='missing_history':text=text.replace('history_reduction=1','history_reduction=0')
    if fault=='unknown_format':text=text.replace('reason=4','reason=6')
    if fault=='early_ack':text= pending+'\n'+confirmed.replace('at_msc=11000','at_msc=10999')
    file.write_text(text,encoding='utf-8')
    with pytest.raises(ValueError):events.read_events(root,selection,signals,sizing,deals)


def test_partial_does_not_reset_fill_age_or_hide_entry_cost(capture):
    root,file,selection,signals,sizing,deals,pending,confirmed=capture
    deals[0].update(volume=2.,commission=-4.)
    deals[-1].update(price=102.,profit=2.,commission=-1.)
    deals.insert(1,dict(position=2,deal=7,at_msc=6000,entry=1,volume=1.,price=110.,profit=10.,commission=-1.,swap=0.,fee=0.))
    pending=pending.replace('reason=4','reason=3').replace('net=-1','net=7').replace('realized=-2','realized=5').replace('floating=1','floating=2').replace('history_count=1','history_count=2')
    confirmed=confirmed.replace('reason=4','reason=3').replace('net=-1','net=7').replace('realized=-2','realized=5').replace('floating=1','floating=2').replace('history_count=1','history_count=2').replace('price=101','price=102').replace('cash=-1 costs=-2','cash=1 costs=-1')
    file.write_text(pending+'\n'+confirmed,encoding='utf-8')
    result=events.read_events(root,selection,signals,sizing,deals)
    assert result[-1]['age_ms']==10000 and result[-1]['before']==1 and result[-1]['reason']=='time_gain'
