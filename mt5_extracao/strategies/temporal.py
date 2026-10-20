"""T19.15: decisão temporal pura; não calcula fills nem substitui o SL protetor."""
from dataclasses import dataclass,asdict
from decimal import Decimal,ROUND_HALF_UP
import math

REASONS=("hold","time_absolute","time_progress","time_gain","time_loss","time_tie","time_er","time_donchian")
CLOCK_FIELDS=("max_seconds","progress_seconds","gain_seconds","loss_seconds","tie_seconds")
@dataclass(frozen=True)
class TemporalPolicy:
    max_seconds:int=0
    progress_seconds:int=0
    minimum_profit:float=0
    gain_seconds:int=0
    loss_seconds:int=0
    tie_seconds:int=0
    flat_band:float=0
    money_digits:int=2
    def __post_init__(self):
        for name in CLOCK_FIELDS:
            v=getattr(self,name)
            if type(v) is not int or not 0<=v<=2147483647:raise ValueError("Prazo temporal deve ser segundos inteiros entre0 e2147483647; zero desabilita.")
        if type(self.money_digits) is not int or not 0<=self.money_digits<=8:raise ValueError("Casas da moeda devem estar entre0 e8 e corresponder ao Tester.")
        for value in (self.minimum_profit,self.flat_band):
            if type(value) not in (int,float) or not math.isfinite(value) or value<0:raise ValueError("Lucro mínimo e faixa de empate devem ser finitos e não negativos.")
            scaled=Decimal(str(value)).scaleb(self.money_digits)
            if scaled!=scaled.to_integral_value() or scaled>9000000000000000:raise ValueError("Limiar deve respeitar as casas da moeda, sem arredondamento implícito.")
    @property
    def enabled(self):return any(getattr(self,n) for n in CLOCK_FIELDS)
    def to_dict(self):return asdict(self)
    def decision(self,age_ms,net):
        if type(age_ms) is not int or age_ms<0:return "unavailable"
        if self.max_seconds and age_ms>=self.max_seconds*1000:return "time_absolute"
        if type(net) not in (int,float) or not math.isfinite(net):return "unavailable"
        value=Decimal(str(net)).scaleb(self.money_digits).to_integral_value(rounding=ROUND_HALF_UP)
        if abs(value)>9000000000000000:return "unavailable"
        minimum=Decimal(str(self.minimum_profit)).scaleb(self.money_digits)
        band=Decimal(str(self.flat_band)).scaleb(self.money_digits)
        if self.progress_seconds and age_ms>=self.progress_seconds*1000 and value<minimum:return "time_progress"
        state="gain" if value>band else "loss" if value<-band else "tie"
        deadline=getattr(self,state+"_seconds")
        return "time_"+state if deadline and age_ms>=deadline*1000 else "hold"

def elapsed_ms(opened,available,clock):
    """Relógio T19.14 declarado, sem inferir UTC ou sessões do host."""
    first,last=clock.timestamp(opened),clock.timestamp(available)
    if last<first:raise ValueError("Relógio temporal anterior ao preenchimento; não reiniciar a idade.")
    return int((last-first).value//1000000)


def source_age_ms(opened,available):
    """Diferença no mesmo relógio de fonte; não atribui fuso do host a valores naive."""
    import pandas as pd
    first,last=pd.Timestamp(opened),pd.Timestamp(available)
    if pd.isna(first) or pd.isna(last) or (first.tzinfo is None)!=(last.tzinfo is None) or last<first:
        raise ValueError("Relógios temporais ausentes/incompatíveis ou anteriores ao preenchimento.")
    return int((last-first).value//1000000)
