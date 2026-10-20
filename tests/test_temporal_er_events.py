from copy import deepcopy
from decimal import Decimal
import pytest
from mt5_extracao.presets import temporal_events as events
from mt5_extracao.presets.efficiency_events import read_context
from mt5_extracao.strategies.temporal_er import EfficiencyTemporalPolicy
from tests.test_temporal_events import capture


def context(value='0',now=11000,times=None,closes=None):
    return ' er_value='+value+' er_observed_msc='+str(now)+' er_source_count=21 er_bar_open_at=22 er_times='+(','.join(map(str,range(1,22))) if times is None else times)+' er_closes='+(','.join(['100']*21) if closes is None else closes)


def test_er_context_reference_receipt_and_existing_full_close_confirmation(capture):
    root,file,selection,signals,sizing,deals,pending,confirmed=capture
    # Native decision at22s can only observe closed source bars earlier than22s.
    deals[-1]['at_msc']=22000
    suffix=context(now=22000)
    text='\n'.join(line.replace('reason=4','reason=6').replace('at_msc=11000','at_msc=22000').replace('decision_msc=11000','decision_msc=22000').replace('age_ms=10000','age_ms=21000')+suffix for line in (pending,confirmed))
    file.write_text(text)
    selection=dict(mode='temporal_er_v1',params=EfficiencyTemporalPolicy(er_min_seconds=21,maximum_er=.5).to_dict())
    rows=events.read_events(root,selection,signals,sizing,deals)
    assert rows[-1]['reason']=='time_er' and rows[-1]['er_value']==0 and rows[-1]['er_reference']==0
    assert rows[-1]['remaining']==0 and rows[-1]['er_observed_msc']==22000


@pytest.mark.parametrize('fault',['missing','future','forming','nan','count','formula','late','reorder'])
def test_unverifiable_er_does_not_qualify(fault):
    text=context(now=22000)
    if fault=='missing':text=''
    if fault=='future':text=text.replace('er_bar_open_at=22','er_bar_open_at=23')
    if fault=='forming':text=text.replace('er_times=1,2,','er_times=22,2,')
    if fault=='nan':text=text.replace('er_closes=100,','er_closes=nan,')
    if fault=='count':text=text.replace('er_source_count=21','er_source_count=20')
    if fault=='formula':text=text.replace('er_value=0 ','er_value=0.9 ')
    if fault=='late':text=text.replace('er_observed_msc=22000','er_observed_msc=22001')
    if fault=='reorder':text=text.replace('er_times=1,2,','er_times=2,1,')
    with pytest.raises(ValueError):read_context(text,22000)
