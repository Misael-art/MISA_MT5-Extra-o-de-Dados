from copy import deepcopy
import pytest
from mt5_extracao.presets import temporal_events,execution
from mt5_extracao.strategies.temporal_atr import AtrTemporalPolicy
from tests.test_temporal_events import capture

def observation(initial=True):
 n=250 if initial else 251;highs=[101]*n;lows=[99]*n
 if not initial:highs[-1]=115;lows[-1]=85
 values=dict(event_at=250,position=2,at_msc=251000 if initial else 252000,initial=int(initial),base_known=251000,seed=1,baseline_bar=250,current_open=n+1,baseline=2,current=2 if initial else 4,horizon=10 if initial else 5,
             times=",".join(map(str,range(1,n+1))),highs=",".join(map(str,highs)),lows=",".join(map(str,lows)),closes=",".join(["100"]*n))
 source={k:values.pop(k) for k in ("times","highs","lows","closes")};values["count"]=n
 header="HDK_ATR "+" ".join(str(k)+"="+str(v) for k,v in values.items())
 chunks=[]
 for field,vector in source.items():
  tokens=vector.split(",")
  for offset in range(0,n,32):chunks.append(f"HDK_ATR_SOURCE position=2 at_msc={values['at_msc']} field={field} offset={offset} values="+",".join(tokens[offset:offset+32]))
 return "\n".join([header]+chunks)

@pytest.fixture
def atr_capture(capture):
 root,file,_,signals,sizing,deals,pending,confirmed=capture
 signals[0].update(event_at=250,direction=1);sizing[0]["event_at"]=250;deals[0]["at_msc"]=251000;deals[-1]["at_msc"]=256000
 suffix=" er_value=unavailable er_observed_msc=0 er_source_count=0 er_bar_open_at=0 er_times=none er_closes=none atr_known=1 atr_baseline=2 atr_current=4 atr_horizon=5 atr_context=252000"
 text="\n".join([observation(),observation(False)]+[line.replace("event_at=0","event_at=250").replace("reason=4","reason=8").replace("at_msc=11000","at_msc=256000").replace("decision_msc=11000","decision_msc=256000").replace("age_ms=10000","age_ms=5000")+suffix for line in (pending,confirmed)])
 file.write_text(text)
 return root,file,dict(mode="temporal_atr_v1",params=AtrTemporalPolicy(max_seconds=100,atr_base_seconds=10,atr_min_seconds=2).to_dict()),signals,sizing,deals,text

def test_independent_atr_snapshot_frozen_base_and_confirmed_receipt(atr_capture):
 root,file,selection,signals,sizing,deals,text=atr_capture;observed=[]
 result=temporal_events.read_events(root,selection,signals,sizing,deals,atr_observations=observed)
 assert len(observed)==2 and observed[0]["baseline"]==observed[1]["baseline"]=="2"
 assert result[-1]["reason"]=="time_atr" and result[-1]["atr_horizon_seconds"]==5 and result[-1]["remaining"]==0

@pytest.mark.parametrize("fault",["missing_initial","future","base_reset","horizon","formula","current_nan","old_context","fake_current","new_seed","unconfirmed","missing_chunk","repeated_chunk","other_position","truncated_chunk"])
def test_unqualified_atr_does_not_accept(atr_capture,fault):
 root,file,selection,signals,sizing,deals,text=atr_capture
 if fault=="missing_initial":text=text.replace(observation()+"\n","")
 if fault=="future":text=text.replace("current_open=252","current_open=300")
 if fault=="base_reset":text=text.replace("initial=0 base_known=251000","initial=0 base_known=252000")
 if fault=="horizon":text=text.replace("horizon=5 ","horizon=6 ")
 if fault=="formula":text=text.replace("current=4 ","current=3 ")
 if fault=="current_nan":text=text.replace("current=4 ","current=nan ")
 if fault=="old_context":text=text.replace("atr_context=252000","atr_context=251000")
 if fault=="fake_current":text=text.replace("atr_current=4","atr_current=2")
 if fault=="new_seed":text=text.replace("seed=1 baseline_bar=250 current_open=252","seed=2 baseline_bar=250 current_open=252")
 if fault=="missing_chunk":text=text.replace("field=highs offset=224","field=highs offset=225",1)
 if fault=="repeated_chunk":text=text+"\n"+next(line for line in text.splitlines() if "HDK_ATR_SOURCE" in line)
 if fault=="other_position":text=text.replace("HDK_ATR_SOURCE position=2","HDK_ATR_SOURCE position=3",1)
 if fault=="truncated_chunk":text=text.replace("field=closes offset=224 values=", "field=unknown offset=224 values=",1)
 if fault=="unconfirmed":text=text.replace("state=confirmed","state=uncertain")
 file.write_text(text)
 with pytest.raises(ValueError):temporal_events.read_events(root,selection,signals,sizing,deals)

def test_replay_checks_atr_context_not_only_same_cash():
 old=dict(ledger=[],signals=[],atr_observations=[dict(position=1,baseline="2",current="4",horizon=5,at_msc=252000)])
 same=[dict(old["atr_observations"][0],position=99)]
 assert execution.first_difference(old,[],[],atr=same) is None
 same[0]["at_msc"]+=1
 assert execution.first_difference(old,[],[],atr=same)["section"]=="atr_observations"
