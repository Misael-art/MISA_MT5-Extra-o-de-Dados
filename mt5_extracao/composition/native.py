"""T19.23: perfil instalado S09 v1, sem concessão de suporte por sidecar externo.

Mapeia somente capacidades comprovadas do Expert aprovado. Não envia ordem,
instala Expert, escreve preset nem muda limites. Recibo real vem do Session.
"""
from dataclasses import dataclass
from decimal import Decimal
import math

from .adapter import base_composition
from .registry import BASE_MODES, EXECUTION_ORDER
from .schema import Capabilities, Composition
from .partials import stages
from ..robots import catalog, native_inputs
from ..strategies import get_strategy

PROFILE_ID = "hadouken.s09_reference"
PROFILE_VERSION = 1
ROBOT_KEY = "mql5:hdk_s09_referencia"
ROBOT_VERSION = "1.00"
APPROVED_FINGERPRINT = "91ff0ec8a7bba77e"
HADOUKEN_REVISION = "d60e42185471f0e5bc94ec17dcfe8e11f94bbf2b"
LOTS_ROBOT_VERSION = "1.10"
LOTS_APPROVED_FINGERPRINT = "8fcc5dfacc9118fa"
LOTS_HADOUKEN_REVISION = "6120466bc2b23ac20684263f2d607120ee48b85e"
INPUT_TYPES = {"InpFastPeriod": "int", "InpSlowPeriod": "int", "InpWarmupBars": "int",
               "InpStopPoints": "int", "InpTargetPoints": "int", "InpRiskPct": "double",
               "InpMagic": "long", "InpDeviationPoints": "int", "InpPresetId": "string", "InpConfigHash": "string"}
RECEIPT_FIELDS = {name: "int" if kind in ("int", "long") else "number" if kind == "double" else "bool" if kind == "bool" else "token"
                  for name, kind in INPUT_TYPES.items()}
PARTIALS_ROBOT_VERSION = "1.20"
PARTIALS_APPROVED_FINGERPRINT = "60bf18181739612d"
PARTIALS_HADOUKEN_REVISION = "89139c8134fca00461ada8850257a0dbd8d7763d"
LOTS_INPUT_TYPES = {**INPUT_TYPES, "InpLotMode": "int", "InpFixedLot": "double"}

PARTIALS_INPUT_TYPES = {**LOTS_INPUT_TYPES, "InpPartialStages":"int", "InpPartialBreakeven":"bool",
    **{f"InpPartial{name}{i}":kind for i in (1,2) for name,kind in (("Trigger","double"),("Quantity","double"),("Mode","int"),("Base","int"))}}

TEMPORAL_ROBOT_VERSION="1.30"
TEMPORAL_APPROVED_FINGERPRINT="f73f751ee93cc7f2"
TEMPORAL_HADOUKEN_REVISION="09068ad0045e3b8500bb20547f2a413dc1bb7c8c"
TIME_INPUTS={"max_seconds":("InpTimeMaxSeconds","int","second"),
 "progress_seconds":("InpTimeProgressSeconds","int","second"),
 "minimum_profit":("InpTimeMinimumProfit","double","account_money"),
 "gain_seconds":("InpTimeGainSeconds","int","second"),"loss_seconds":("InpTimeLossSeconds","int","second"),
 "tie_seconds":("InpTimeTieSeconds","int","second"),"flat_band":("InpTimeFlatBand","double","account_money"),
 "money_digits":("InpTimeMoneyDigits","int","currency_decimal")}
TEMPORAL_INPUT_TYPES={**PARTIALS_INPUT_TYPES,**{name:kind for name,kind,unit in TIME_INPUTS.values()}}

ER_TEMPORAL_ROBOT_VERSION="1.40"
ER_TEMPORAL_APPROVED_FINGERPRINT="24a4d21ae609d9c7"
ER_TEMPORAL_HADOUKEN_REVISION="994d1bafd20f729daa1a292c1cdd79515c66e39c"
ER_INPUTS={"er_min_seconds":("InpTimeERMinSeconds","int","second"),
           "maximum_er":("InpTimeERMaximum","double","fraction")}
ER_TEMPORAL_INPUT_TYPES={**TEMPORAL_INPUT_TYPES,**{name:kind for name,kind,unit in ER_INPUTS.values()}}

DONCHIAN_TEMPORAL_ROBOT_VERSION="1.50"
DONCHIAN_TEMPORAL_APPROVED_FINGERPRINT="b39a586b622e4713"
DONCHIAN_TEMPORAL_HADOUKEN_REVISION="eaaa059c58802930fb1e6c351a4cc01e6ac78fb6"
DONCHIAN_INPUTS={"channel_idle_seconds":("InpTimeDonchianIdleSeconds","int","second")}
DONCHIAN_TEMPORAL_INPUT_TYPES={**ER_TEMPORAL_INPUT_TYPES,**{name:kind for name,kind,unit in DONCHIAN_INPUTS.values()}}

ATR_TEMPORAL_ROBOT_VERSION="1.60"
ATR_TEMPORAL_APPROVED_FINGERPRINT="4afcfe4bcef17c18"
ATR_TEMPORAL_HADOUKEN_REVISION="52c10013365cf3c73682cc433b82bb82fefa957d"
ATR_INPUTS={"atr_base_seconds":("InpTimeATRBaseSeconds","int","second"),
            "atr_min_seconds":("InpTimeATRMinSeconds","int","second")}
ATR_TEMPORAL_INPUT_TYPES={**DONCHIAN_TEMPORAL_INPUT_TYPES,**{name:kind for name,kind,unit in ATR_INPUTS.values()}}

SESSION_ROBOT_VERSION="1.70"
SESSION_APPROVED_FINGERPRINT="ad7a304b82ff8084"
SESSION_HADOUKEN_REVISION="8a3b9d284b716e4a14a3d1bcaf95670a1544330a"
SESSION_INPUTS={key:(name,"int",unit) for key,name,unit in (
    ("start_minute","InpSessionStartMinute","broker_minute"),
    ("end_minute","InpSessionEndMinute","broker_minute"),
    ("exclude_start_minute","InpSessionExcludeStartMinute","broker_minute"),
    ("exclude_end_minute","InpSessionExcludeEndMinute","broker_minute"),
    ("weekdays","InpSessionWeekdays","session_weekday_mask"),
    ("anchor_minute","InpSessionAnchorMinute","broker_minute"),
    ("max_cycles","InpSessionMaxCycles","first_fill_cycle"))}
SESSION_INPUT_TYPES={**ATR_TEMPORAL_INPUT_TYPES,"InpSessionEnabled":"bool",
                     **{name:kind for name,kind,unit in SESSION_INPUTS.values()}}

def _require_robot(robot):
    actual = {item.name: item.type for item in robot.inputs}
    profiles = {ROBOT_VERSION: (APPROVED_FINGERPRINT, INPUT_TYPES, 1, HADOUKEN_REVISION),
                LOTS_ROBOT_VERSION: (LOTS_APPROVED_FINGERPRINT, LOTS_INPUT_TYPES, 2, LOTS_HADOUKEN_REVISION),
                PARTIALS_ROBOT_VERSION:(PARTIALS_APPROVED_FINGERPRINT,PARTIALS_INPUT_TYPES,3,PARTIALS_HADOUKEN_REVISION),
                TEMPORAL_ROBOT_VERSION:(TEMPORAL_APPROVED_FINGERPRINT,TEMPORAL_INPUT_TYPES,4,TEMPORAL_HADOUKEN_REVISION),
                ER_TEMPORAL_ROBOT_VERSION:(ER_TEMPORAL_APPROVED_FINGERPRINT,ER_TEMPORAL_INPUT_TYPES,5,ER_TEMPORAL_HADOUKEN_REVISION),
                DONCHIAN_TEMPORAL_ROBOT_VERSION:(DONCHIAN_TEMPORAL_APPROVED_FINGERPRINT,DONCHIAN_TEMPORAL_INPUT_TYPES,6,DONCHIAN_TEMPORAL_HADOUKEN_REVISION),
                ATR_TEMPORAL_ROBOT_VERSION:(ATR_TEMPORAL_APPROVED_FINGERPRINT,ATR_TEMPORAL_INPUT_TYPES,7,ATR_TEMPORAL_HADOUKEN_REVISION),
                SESSION_ROBOT_VERSION:(SESSION_APPROVED_FINGERPRINT,SESSION_INPUT_TYPES,8,SESSION_HADOUKEN_REVISION)}
    profile = profiles.get(robot.version)
    if (robot.kind != catalog.MQL5 or not robot.has_source or robot.key != ROBOT_KEY or profile is None
            or catalog.fingerprint(robot) != profile[0] or actual != profile[1]
            or len(robot.inputs) != len(profile[1]) or robot.missing_includes):
        raise ValueError("Perfil S09 exige fonte/versão/fingerprint HADOUKEN aprovados e todos os inputs conhecidos; "
                         "atualize a biblioteca pela revisão indicada, sem substituir arquivos do usuário.")
    return profile


def default_composition(robot, *, symbol, timeframe):
    """Defaults efetivos do perfil nativo; não é persistência do preset Default."""
    if robot.key=="mql5:hdk_pullback_nativo":
        from .pullback_native import default_composition as pullback_default
        return pullback_default(robot,symbol=symbol,timeframe=timeframe)
    _require_robot(robot)
    return base_composition("s09_reference", symbol=symbol, timeframe=timeframe, risk_fraction=.005, compound=True)


def _points(value, label):
    if (type(value) not in (int, float) or not math.isfinite(value) or value != int(value)
            or not 0 < value <= 2147483647):
        raise ValueError(f"S09 nativa exige {label} em pontos inteiros positivos representáveis pelo input int; não truncar valores.")
    return str(int(value))


@dataclass(frozen=True)
class S09NativeAdapter:
    composition: Composition
    robot: catalog.Robot

    def __post_init__(self):
        if not isinstance(self.composition, Composition):
            object.__setattr__(self, "composition", Composition(self.composition))
        self._validate()

    def _validate(self):
        profile = _require_robot(self.robot)
        data = self.composition.to_dict()
        if data["strategy"]["key"] != "s09_reference":
            raise ValueError("Perfil nativo disponível somente para S09 referência; Pullback permanece bloqueado até T19.57.")
        get_strategy("s09_reference", **data["strategy"]["params"])
        for slot, base in BASE_MODES.items():
            allowed = {base, "fixed_points"} if slot in ("profit_exit", "protection") else {base}
            if slot == "sizing" and profile[2] >= 2:
                allowed.add("fixed_volume")
            if slot=="conditional_policy" and profile[2]>=4:allowed.add("temporal_v1")
            if slot=="conditional_policy" and profile[2]>=5:allowed.add("temporal_er_v1")
            if slot=="conditional_policy" and profile[2]>=6:allowed.add("temporal_donchian_v1")
            if slot=="conditional_policy" and profile[2]>=7:allowed.add("temporal_atr_v1")
            if slot=="partial_exit_plan" and profile[2]>=3:
                allowed.update(("fixed_r","staged_lots","staged_fraction"))
            if data[slot]["mode"] not in allowed:
                raise ValueError(f"{slot}: modo sem suporte no perfil S09 nativo; use {', '.join(sorted(allowed))}.")
        if (data["filters"] and profile[2]<8) or data["execution_order"] != list(EXECUTION_ORDER):
            raise ValueError("S09 nativa exige ordem de execução base e nenhum filtro adicional; remova a capacidade sem suporte.")
        if profile[2]>=8:
            from ..strategies.session_entry import from_filters
            from_filters(data["filters"])
        if data["bar_policy"] != {"intrabar_priority": "stop_first", "position_limit": 1}:
            raise ValueError("S09 nativa admite uma posição e política base; não ampliar exposição.")
        sizing = data["sizing"]["params"]
        if not sizing["compound"] or not 0 < sizing["risk_fraction"] <= .005:
            raise ValueError("S09 nativa usa risco por saldo, composto, até 0.5%; confira a fração (0.005 = 0.5%), sem elevar o limite.")
        for slot, name in [("profit_exit", "target_points"), ("protection", "stop_points")]:
            value = data[slot]["params"][name] if data[slot]["mode"] == "fixed_points" else data["strategy"]["params"][name]
            _points(value, name)
        partials=stages(data["partial_exit_plan"])
        stop=data["protection"]["params"]["stop_points"] if data["protection"]["mode"]=="fixed_points" else data["strategy"]["params"]["stop_points"]
        target=data["profit_exit"]["params"]["target_points"] if data["profit_exit"]["mode"]=="fixed_points" else data["strategy"]["params"]["target_points"]
        if any(stage["trigger_r"]>=target/stop for stage in partials):
            raise ValueError("Parcial deve anteceder o alvo total, usando R do SL inicial.")
        return data

    def describe(self, preset_id):
        """Novo snapshot tipado; revalida fonte para detectar edição após construir o adaptador."""
        data = self._validate()
        fingerprint, input_types, profile_version, hadouken_revision = _require_robot(self.robot)
        preset_id = native_inputs.scalar(preset_id, "token")
        if not preset_id or len(preset_id) > 80:
            raise ValueError("Identidade pública do preset deve ter de 1 a 80 caracteres portáteis.")
        p = data["strategy"]["params"]
        values = {"InpFastPeriod": str(p["fast"]), "InpSlowPeriod": str(p["slow"]),
                  "InpWarmupBars": str(p["warmup_bars"]), "InpStopPoints": str(p["stop_points"]),
                  "InpTargetPoints": str(p["target_points"]),
                  "InpRiskPct": format(Decimal(str(data["sizing"]["params"]["risk_fraction"]))*100, "f"),
                  "InpMagic": "909202610", "InpDeviationPoints": "20",
                  "InpPresetId": preset_id, "InpConfigHash": self.composition.config_hash}
        bindings = {"strategy.params."+name: {"input": target, "type": "int", "unit": "bar"}
                    for name, target in [("fast", "InpFastPeriod"), ("slow", "InpSlowPeriod"), ("warmup_bars", "InpWarmupBars")]}
        inactive = []
        for slot, name, target in [("profit_exit", "target_points", "InpTargetPoints"),
                                    ("protection", "stop_points", "InpStopPoints")]:
            explicit = data[slot]["mode"] == "fixed_points"
            path = slot+".params."+name if explicit else "strategy.params."+name
            value = data[slot]["params"][name] if explicit else p[name]
            values[target] = _points(value, name)
            bindings[path] = {"input": target, "type": "number" if explicit else "int", "unit": "point"}
            if explicit:
                inactive.append("strategy.params."+name)
        _sizing_parameters(data, profile_version, values, bindings, inactive)
        if profile_version>=4:
            from ..strategies.temporal import TemporalPolicy
            from ..strategies.temporal_er import from_selection,EfficiencyTemporalPolicy
            enabled=data["conditional_policy"]["mode"] in ("temporal_v1","temporal_er_v1","temporal_donchian_v1","temporal_atr_v1")
            policy=from_selection(data["conditional_policy"]) if enabled else TemporalPolicy()
            for key,(name,kind,unit) in TIME_INPUTS.items():
                values[name]=format(Decimal(str(getattr(policy,key))),"f")
                if enabled:bindings["conditional_policy.params."+key]={"input":name,"type":"int" if kind=="int" else "number","unit":unit}
        if profile_version>=5:
            er_policy=policy if isinstance(policy,EfficiencyTemporalPolicy) else EfficiencyTemporalPolicy()
            for key,(name,kind,unit) in ER_INPUTS.items():
                values[name]=format(Decimal(str(getattr(er_policy,key))),"f")
                if data["conditional_policy"]["mode"] in ("temporal_er_v1","temporal_donchian_v1","temporal_atr_v1"):bindings["conditional_policy.params."+key]={"input":name,"type":"int" if kind=="int" else "number","unit":unit}
        if profile_version>=6:
            values["InpTimeDonchianIdleSeconds"]=str(getattr(policy,"channel_idle_seconds",0))
            if data["conditional_policy"]["mode"] in ("temporal_donchian_v1","temporal_atr_v1"):
                bindings["conditional_policy.params.channel_idle_seconds"]={"input":"InpTimeDonchianIdleSeconds","type":"int","unit":"second"}
        if profile_version>=7:
            for key,(name,kind,unit) in ATR_INPUTS.items():
                values[name]=str(getattr(policy,key,0))
                if data["conditional_policy"]["mode"]=="temporal_atr_v1":bindings["conditional_policy.params."+key]={"input":name,"type":"int","unit":unit}
        if profile_version>=8:
            from ..strategies.session_entry import SessionEntryPolicy,from_filters
            session=from_filters(data["filters"])
            values["InpSessionEnabled"]="true" if session is not None else "false"
            params=(session or SessionEntryPolicy()).to_dict()
            for key,(name,kind,unit) in SESSION_INPUTS.items():
                values[name]=str(params[key])
                if session is not None:bindings["filters.0.params."+key]={"input":name,"type":"int","unit":unit}
        return _description(self.composition,self.robot,data,values,bindings,inactive,preset_id,
                            fingerprint,input_types,profile_version,hadouken_revision,PROFILE_ID)


def _sizing_parameters(data,profile_version,values,bindings,inactive):
    bindings["sizing.params.risk_fraction"] = {"input": "InpRiskPct", "type": "number", "unit": "fraction"}
    if profile_version >= 2:
        fixed = data["sizing"]["mode"] == "fixed_volume"
        values.update(InpLotMode="0" if fixed else "1",
                      InpFixedLot=format(Decimal(str(data["sizing"]["params"]["volume"])), "f") if fixed else "0.01")
        if fixed:
            bindings["sizing.params.volume"] = {"input": "InpFixedLot", "type": "number", "unit": "lot"}
    if profile_version>=3:
        plan=data["partial_exit_plan"];partial_stages=stages(plan)
        values.update(InpPartialStages=str(len(partial_stages)),InpPartialBreakeven="true" if plan["params"].get("breakeven_after_confirmation",False) else "false")
        for i in (1,2):
            stage=partial_stages[i-1] if i<=len(partial_stages) else dict(trigger_r=.5 if i==1 else 1.,quantity=.5,mode=0,base=0 if i==1 else 1)
            for key,name in (("trigger_r","Trigger"),("quantity","Quantity"),("mode","Mode"),("base","Base")):
                values[f"InpPartial{name}{i}"]=format(Decimal(str(stage[key])),"f")
        params=plan["params"]
        if partial_stages:
            bindings["partial_exit_plan.params.breakeven_after_confirmation"]={"input":"InpPartialBreakeven","type":"bool","unit":"boolean"}
            if plan["mode"]=="fixed_r":
                for key,target,unit in (("trigger_r","InpPartialTrigger1","R"),("fraction","InpPartialQuantity1","fraction")):
                    bindings["partial_exit_plan.params."+key]={"input":target,"type":"number","unit":unit}
            else:
                bindings["partial_exit_plan.params.count"]={"input":"InpPartialStages","type":"int","unit":"stage"}
                for i in (1,2):
                    for key,name,kind,unit in ((f"trigger{i}_r","Trigger","number","R"),(f"quantity{i}","Quantity","number","lot" if plan["mode"]=="staged_lots" else "fraction"),(f"base{i}","Base","int","volume_base")):
                        if i<=len(partial_stages):bindings["partial_exit_plan.params."+key]={"input":f"InpPartial{name}{i}","type":kind,"unit":unit}
                        else:inactive.append("partial_exit_plan.params."+key)


def _description(composition,robot,data,values,bindings,inactive,preset_id,fingerprint,input_types,profile_version,hadouken_revision,profile_id):
    modules = {slot: [base] for slot, base in BASE_MODES.items()}
    modules.update(profit_exit=["strategy_default", "fixed_points"], protection=["strategy_default", "fixed_points"], filters=[])
    if profile_version >= 2:
        modules["sizing"] = ["risk_fraction", "fixed_volume"]
    if profile_version>=3:modules["partial_exit_plan"]=["strategy_default","fixed_r","staged_lots","staged_fraction"]
    if profile_version>=4:modules["conditional_policy"]=["disabled","temporal_v1"]
    if profile_version>=5:modules["conditional_policy"].append("temporal_er_v1")
    if profile_version>=6:modules["conditional_policy"].append("temporal_donchian_v1")
    if profile_version>=7:modules["conditional_policy"].append("temporal_atr_v1")
    if profile_version>=8:modules["filters"]=["session_v1"]
    capabilities = Capabilities({"capabilities_schema_version": 1,
        "robot": {"key": robot.key, "version": robot.version, "fingerprint": fingerprint},
        "backend": "mql5", "strategy_key": data["strategy"]["key"], "modules": modules, "bindings": bindings}, composition)
    fields = {name: "int" if kind in ("int", "long") else "number" if kind == "double" else "bool" if kind == "bool" else "token"
              for name, kind in input_types.items()}
    return {"native_profile_schema_version": 1, "profile_id": profile_id, "profile_version": profile_version,
            "hadouken_revision": hadouken_revision, "robot": capabilities.to_dict()["robot"],
            "capabilities": capabilities.to_dict(), "parameters": values, "receipt_fields": fields,
            "expected_native_inputs": native_inputs.expected_values(values, fields),
            "inactive_paths": inactive, "conversions": {"sizing.params.risk_fraction": "fraction × 100 → InpRiskPct percent"},
            "fixed_constraints": {"compound": True, "positions": 1, "daily_loss_pct": 2, "weekly_loss_pct": 4,
                                  "max_dd_pct": 10, "max_open_risk_pct": .5, "margin_buffer_pct": 20,
                                  "block_averaging": True, "partial": profile_version>=3, "breakeven": profile_version>=3, "trailing": False},
            "modality": "native_tester_demo_only", "operational_acceptance": False}


def native_adapter(composition,robot):
    if robot.key=="mql5:hdk_pullback_nativo":
        from .pullback_native import PullbackNativeAdapter
        return PullbackNativeAdapter(composition,robot)
    return S09NativeAdapter(composition,robot)
