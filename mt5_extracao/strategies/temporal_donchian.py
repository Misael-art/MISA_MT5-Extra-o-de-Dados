"""TM06: rompimento confirmado por close, retorno inclusivo e relógio sem renovação."""
from dataclasses import dataclass
import math
import numpy as np
import pandas as pd
from .temporal_er import EfficiencyTemporalPolicy
from .indicators import donchian_context,efficiency_ratio

@dataclass(frozen=True)
class DonchianTemporalPolicy(EfficiencyTemporalPolicy):
    channel_idle_seconds:int=0
    def __post_init__(self):
        super().__post_init__()
        if type(self.channel_idle_seconds) is not int or not 0<=self.channel_idle_seconds<=2147483647:
            raise ValueError("Tempo sem renovação deve ser segundos inteiros entre0 e2147483647.")
    @property
    def enabled(self):return super().enabled or self.channel_idle_seconds>0
    def decision(self,age_ms,net,er=None,*,idle_ms=None,returned=False):
        prior=super().decision(age_ms,net,er=er)
        if prior.startswith('time_') or type(age_ms) is not int or age_ms<0:return prior
        if (self.channel_idle_seconds and type(idle_ms) is int and idle_ms>=self.channel_idle_seconds*1000
            and returned is True):return 'time_donchian'
        return prior

def observations(policy,data):
    channel=donchian_context(data,20,reference='previous')
    close=pd.to_numeric(data.close,errors='coerce')
    valid=np.isfinite(data.high)&np.isfinite(data.low)&np.isfinite(close)&data.high.ge(data.low)
    index=np.arange(len(data));latest={}
    for direction,condition in ((1,close.gt(channel.upper)),(-1,close.lt(channel.lower))):
        latest[direction]=np.maximum.accumulate(np.where(condition,index,-1))
    return dict(er=efficiency_ratio(close,20),channel=channel,times=pd.to_datetime(data.time),
                closes=close,latest=latest,last_bad=np.maximum.accumulate(np.where(~valid,index,-1)))

def decide(policy,age_ms,net,values,index,*,direction,opened_index):
    er=float(values['er'].iloc[index]);er=er if math.isfinite(er) and 0<=er<=1 else None
    row=values['channel'].iloc[index];last=int(values['latest'][direction][index])
    known=all(math.isfinite(float(row[k])) for k in ('upper','lower')) and int(values['last_bad'][index])<opened_index
    seen=last>=opened_index
    returned=bool(known and seen and row.lower<=float(values['closes'].iloc[index])<=row.upper)
    # close-lower é a distância assinada canônica, evitando outra série/formula.
    idle=int((values['times'].iloc[index]-values['times'].iloc[last]).value//1000000) if known and seen else None
    return policy.decision(age_ms,net,er=er,idle_ms=idle,returned=returned),dict(er_feature='trend.efficiency_ratio_20',er_value=er,
        channel_feature='channel.donchian_20_previous',channel_idle_ms=idle,channel_breakout_seen=bool(known and seen),
        channel_returned=returned,channel_upper=float(row.upper) if known else None,channel_lower=float(row.lower) if known else None)
