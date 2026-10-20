from copy import deepcopy
import pytest
from mt5_extracao.presets import temporal_events,donchian_events
from mt5_extracao.strategies.temporal_donchian import DonchianTemporalPolicy
from tests.test_temporal_events import capture


def observation(second=False):
    times=list(range(2 if second else 1,23 if second else 22));highs=[101.]*21;lows=[99.]*21
    highs[19 if second else 20]=111
    return ('HDK_DC event_at=20 position=2 at_msc='+str(23000 if second else 22000)+' bar='+str(22 if second else 21)+
        ' previous='+str(21 if second else 20)+' prior_bar='+str(21 if second else 20)+' current_open='+str(23 if second else 22)+
        ' before='+str(22000 if second else 21000)+' anchor=22000 seen_before='+str(int(second))+' seen=1 renewed='+str(int(not second))+
        ' direction=1 upper='+str(111 if second else 101)+' lower=99 close='+str(100 if second else 110)+
        ' times='+','.join(map(str,times))+' highs='+','.join(map(str,highs))+' lows='+','.join(map(str,lows)))

@pytest.fixture
def channel_capture(capture):
    root,file,_,signals,sizing,deals,pending,confirmed=capture
    signals[0].update(event_at=20,direction=1);sizing[0]['event_at']=20;deals[0]['at_msc']=21000;deals[-1]['at_msc']=24000
    suffix=' er_value=unavailable er_observed_msc=0 er_source_count=0 er_bar_open_at=0 er_times=none er_closes=none dc_known=1 dc_anchor=22000 dc_seen=1 dc_bar=22 dc_upper=111 dc_lower=99 dc_close=100 dc_observed=24000 dc_context=23000'
    text='\n'.join([observation(),observation(True)]+[line.replace('event_at=0','event_at=20').replace('reason=4','reason=7').replace('at_msc=11000','at_msc=24000').replace('decision_msc=11000','decision_msc=24000').replace('age_ms=10000','age_ms=3000')+suffix for line in (pending,confirmed)])
    file.write_text(text)
    return root,file,dict(mode='temporal_donchian_v1',params=DonchianTemporalPolicy(channel_idle_seconds=2).to_dict()),signals,sizing,deals,text


def test_independent_window_journal_anchor_and_full_close(channel_capture):
    root,file,selection,signals,sizing,deals,text=channel_capture;observed=[]
    result=temporal_events.read_events(root,selection,signals,sizing,deals,channel_observations=observed)
    assert len(observed)==2 and observed[0]['renewed']==1 and observed[1]['renewed']==0
    assert result[-1]['reason']=='time_donchian' and result[-1]['channel_idle_ms']==2000 and result[-1]['remaining']==0

@pytest.mark.parametrize('fault',['missing','forming','fake_anchor','reset','renew_tie','context','future','repeat','unknown_gap'])
def test_unverified_clock_or_channel_never_qualifies(channel_capture,fault):
    root,file,selection,signals,sizing,deals,text=channel_capture
    if fault=='missing':text=text.replace(observation()+'\n','')
    if fault=='forming':text=text.replace('upper=101 lower=99 close=110','upper=111 lower=99 close=110')
    if fault=='fake_anchor':text=text.replace('dc_anchor=22000','dc_anchor=21000')
    if fault=='reset':text=text.replace('before=22000 anchor=22000','before=21000 anchor=22000')
    if fault=='renew_tie':text=text.replace('seen=1 renewed=0','seen=1 renewed=1')
    if fault=='context':text=text.replace('dc_context=23000','dc_context=24000')
    if fault=='future':text=text.replace('current_open=23','current_open=25')
    if fault=='repeat':text=observation()+'\n'+text
    if fault=='unknown_gap':text+='\nHDK_DC_GAP position=2 reason=closed_observation_coverage_missing'
    file.write_text(text)
    with pytest.raises(ValueError):temporal_events.read_events(root,selection,signals,sizing,deals)
