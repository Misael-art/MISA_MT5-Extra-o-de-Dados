from datetime import datetime,timezone
from pathlib import Path
import pytest
from mt5_extracao.presets.partial_events import read_events,semantic
from tests.test_partial_plan import plan

def stamp(at):return datetime.fromtimestamp(at,timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

@pytest.fixture
def prepared(tmp_path):
    origin=1791969300;entry=origin+300
    signals=[dict(event_at=origin,opened=True,deal_ticket=2,direction=1,filled_volume=3)]
    sizing=[dict(event_at=origin,volume_min=.01,volume_max=100,volume_step=.01)]
    ledger=[dict(entry_time=stamp(entry),exit_time=stamp(entry+30),direction=1,volume=3,
                 entry_price=1.17,exit_price=1.17117,pnl=344)]
    lines=[]
    for ticket,at,kind,volume,price,profit,cost in [(2,entry,0,3,1.17,0,-3),(3,entry+10,1,1,1.1705,50,-1),(4,entry+20,1,1,1.171,100,-1),(5,entry+30,1,1,1.172,200,-1)]:
        lines.append(f'S09_DEAL position=2 deal={ticket} at_msc={at*1000} entry={kind} type={kind} volume={volume} price={price} profit={profit} commission={cost} swap=0 fee=0 digits=5')
    for stage,before,left,old_sl,price,net in [(1,3,2,1.169,1.1705,49),(2,2,1,1.17,1.171,99)]:
        for state in ('pending','confirmed'):
            confirmed=state=='confirmed'
            lines.append(f'S09_PARTIAL event_at={origin} at={entry+stage*10} stage={stage} state={state} before={before} requested=1 filled={1 if confirmed else 0} remaining={left if confirmed else before} stop_before={old_sl} stop_after={1.17 if confirmed else old_sl} be_requested=1 be_confirmed={int(confirmed)} position=2 history_reduction={1 if confirmed else 0} price={price if confirmed else 0} net={net if confirmed else 0} costs={-1 if confirmed else 0}')
    path=tmp_path/'Tester/Agent-test/logs/test.log';path.parent.mkdir(parents=True);path.write_text('\n'.join(lines))
    return tmp_path,path,signals,sizing,ledger

def read(prepared):
    root,_,signals,sizing,ledger=prepared
    return read_events(root,plan(),signals,sizing,ledger)

def test_fills_costs_volume_and_confirmation_reconcile_as_one_trade(prepared):
    partials,deals=read(prepared)
    assert len(partials)==4 and len(deals)==4
    assert [p['remaining'] for p in partials if p['state']=='confirmed']==[2,1]
    assert sum(d['profit']+d['commission']+d['swap']+d['fee'] for d in deals)==344
    assert 'position' not in semantic(partials)[0] and 'deal' not in semantic(deals)[0]

@pytest.mark.parametrize('fault',['missing_confirmation','duplicate_intention','accepted_only','partial_fill','changed_owner','no_stop','false_be','changed_cost','changed_ledger','extra_entry','missing_deal'])
def test_fault_never_publishes_partial_acceptance(prepared,fault):
    _,path,_,_,ledger=prepared;text=path.read_text()
    if fault=='missing_confirmation':text='\n'.join(l for l in text.splitlines() if 'state=confirmed' not in l)
    if fault=='duplicate_intention':text+='\n'+next(l for l in text.splitlines() if 'state=pending' in l)
    if fault=='accepted_only':text=text.replace('state=confirmed','state=uncertain')
    if fault=='partial_fill':text=text.replace('filled=1','filled=0.5')
    if fault=='changed_owner':text=text.replace('position=2 history_reduction','position=99 history_reduction')
    if fault=='no_stop':text=text.replace('stop_after=1.17','stop_after=0')
    if fault=='false_be':text=text.replace('stop_after=1.17 be_requested=1 be_confirmed=1','stop_after=1.169 be_requested=1 be_confirmed=1')
    if fault=='changed_cost':text=text.replace('net=49','net=50')
    if fault=='changed_ledger':ledger[0]['pnl']=350
    if fault=='extra_entry':text+='\n'+text.splitlines()[0].replace('deal=2','deal=9')
    if fault=='missing_deal':text='\n'.join(l for l in text.splitlines() if 'deal=3 ' not in l)
    path.write_text(text)
    with pytest.raises(ValueError):read(prepared)
