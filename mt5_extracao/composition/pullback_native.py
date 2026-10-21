"""Perfil fechado Pullback. Legado e simulação Python mantêm aceites próprios."""
from dataclasses import dataclass
from .adapter import base_composition
from .schema import Composition
from .registry import BASE_MODES,EXECUTION_ORDER
from .native import PARTIALS_INPUT_TYPES,PANEL_STANDARD_INCLUDES,_sizing_parameters,_description
from ..robots import catalog,native_inputs

ROBOT_KEY="mql5:hdk_pullback_nativo"
ROBOT_VERSION="1.00"
APPROVED_FINGERPRINT="93da50a5dc8c952f"
HADOUKEN_REVISION="ae07e7bda3a3dc156563e13918b486a0f1cb4843"
PANEL_ROBOT_VERSION="1.10"
PANEL_APPROVED_FINGERPRINT="e1b14494047477b2"
PANEL_HADOUKEN_REVISION="1024cee0ec49a29d16b48c1c46d80adaa3259013"
STRATEGY_INPUTS={"trend":("InpTrendPeriod","int","bar"),"adx_min":("InpAdxMin","double","index_0_100"),
 "rsi_period":("InpRsiPeriod","int","bar"),"rsi_pullback":("InpRsiPullback","double","index_0_100"),
 "swing_bars":("InpSwingBars","int","bar"),"target_r":("InpTakeProfitR","double","R")}
INPUT_TYPES={k:v for k,v in PARTIALS_INPUT_TYPES.items() if k not in ("InpFastPeriod","InpSlowPeriod","InpWarmupBars","InpStopPoints","InpTargetPoints")}
INPUT_TYPES.update({name:kind for name,kind,_ in STRATEGY_INPUTS.values()})
PARTIAL={"mode":"fixed_r","params":{"trigger_r":1.0,"fraction":.5,"breakeven_after_confirmation":True}}

def require_robot(robot):
    profiles={ROBOT_VERSION:(APPROVED_FINGERPRINT,INPUT_TYPES,3,HADOUKEN_REVISION),
              PANEL_ROBOT_VERSION:(PANEL_APPROVED_FINGERPRINT,INPUT_TYPES,3,PANEL_HADOUKEN_REVISION)}
    profile=profiles.get(robot.version)
    if (robot.key!=ROBOT_KEY or profile is None or robot.kind!=catalog.MQL5 or not robot.has_source
        or set(robot.missing_includes) - (PANEL_STANDARD_INCLUDES if robot.version==PANEL_ROBOT_VERSION else set())
        or catalog.fingerprint(robot)!=profile[0]
        or len(robot.inputs)!=len(INPUT_TYPES) or {i.name:i.type for i in robot.inputs}!=INPUT_TYPES):
        raise ValueError("Pullback bloqueado: exige fonte, includes, versão e inputs do perfil nativo aprovado; legado não é promovido.")
    return profile

def default_composition(robot,*,symbol,timeframe):
    require_robot(robot)
    data=base_composition("pullback",symbol=symbol,timeframe=timeframe,risk_fraction=.005,compound=True).to_dict()
    data["partial_exit_plan"]={"mode":PARTIAL["mode"],"params":dict(PARTIAL["params"])}
    return Composition(data)

@dataclass(frozen=True)
class PullbackNativeAdapter:
    composition:Composition
    robot:catalog.Robot
    def __post_init__(self):
        if not isinstance(self.composition,Composition):object.__setattr__(self,"composition",Composition(self.composition))
        self._validate()
    def _validate(self):
        require_robot(self.robot);data=self.composition.to_dict();p=data["strategy"]["params"]
        if data["strategy"]["key"]!="pullback":raise ValueError("Perfil Pullback não admite outra estratégia.")
        if not (2<=p["trend"]<=1500 and 2<=p["rsi_period"]<=1500 and 1<=p["swing_bars"]<=1500
                and 0<p["rsi_pullback"]<50 and p["target_r"]>1):raise ValueError("Confira períodos até 1500, RSI entre 0 e 50 e alvo maior que 1R.")
        for slot,base in BASE_MODES.items():
            allowed={base,"fixed_volume"} if slot=="sizing" else {"fixed_r"} if slot=="partial_exit_plan" else {base}
            if data[slot]["mode"] not in allowed:raise ValueError(slot+": capacidade não demonstrada no perfil Pullback.")
        if data["partial_exit_plan"]!=PARTIAL:raise ValueError("Pullback qualifica somente metade do volume inicial em 1R, com BE após confirmação.")
        if data["filters"] or data["execution_order"]!=list(EXECUTION_ORDER) or data["bar_policy"]!={"intrabar_priority":"stop_first","position_limit":1}:
            raise ValueError("Pullback exige política base, uma posição e nenhum filtro adicional.")
        sizing=data["sizing"]["params"]
        if not sizing["compound"] or not 0<sizing["risk_fraction"]<=.005:raise ValueError("Risco por saldo até 0,5%, sem ampliar exposição.")
        return data
    def describe(self,preset_id):
        data=self._validate();preset_id=native_inputs.scalar(preset_id,"token")
        if not preset_id or len(preset_id)>80:raise ValueError("Identidade pública deve ter de 1 a 80 caracteres portáteis.")
        values={name:str(data["strategy"]["params"][key]) for key,(name,_,_) in STRATEGY_INPUTS.items()}
        values.update(InpMagic="915202610",InpDeviationPoints="20",InpPresetId=preset_id,InpConfigHash=self.composition.config_hash)
        bindings={"strategy.params."+key:{"input":name,"type":"int" if kind=="int" else "number","unit":unit} for key,(name,kind,unit) in STRATEGY_INPUTS.items()}
        inactive=[]
        from decimal import Decimal
        values["InpRiskPct"]=format(Decimal(str(data["sizing"]["params"]["risk_fraction"]))*100,"f")
        _sizing_parameters(data,3,values,bindings,inactive)
        result=_description(self.composition,self.robot,data,values,bindings,inactive,preset_id,*require_robot(self.robot),"hadouken.pullback")
        result["capabilities"]["modules"].update(profit_exit=["strategy_default"],protection=["strategy_default"],partial_exit_plan=["fixed_r"])
        return result
