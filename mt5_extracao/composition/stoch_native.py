"""T19.38: variante S10 fechada, somente fonte aprovada e Strategy Tester DEMO."""
from dataclasses import dataclass
from decimal import Decimal
from .adapter import base_composition
from .registry import BASE_MODES, EXECUTION_ORDER
from .schema import Capabilities, Composition
from ..robots import catalog, native_inputs
from ..strategies import get_strategy

PROFILE_ID = "hadouken.stoch_ema9"
ROBOT_KEY = "mql5:hdk_stochema9"
ROBOT_VERSION = "1.30"
APPROVED_FINGERPRINT = "26545f544c784c39"
HADOUKEN_REVISION = "9a003dd051f4b40f9c6f1effcf749aecb0122f51"
STRATEGY_INPUTS = {
    "k_period": ("InpKPeriod", "int", "bar"),
    "slowing": ("InpSlowing", "int", "bar"),
    "d_period": ("InpDPeriod", "int", "bar"),
    "ema_period": ("InpEmaPeriod", "int", "bar"),
    "upper_level": ("InpUpperLevel", "double", "index_0_100"),
    "lower_level": ("InpLowerLevel", "double", "index_0_100"),
    "stop_offset_points": ("InpStopOffsetPoints", "int", "point"),
    "target_points": ("InpTargetPoints", "int", "point"),
}
INPUT_TYPES = {"InpMagicNumber": "long", "InpRiskPct": "double", "InpDailyLossPct": "double",
               "InpWeeklyLossPct": "double", "InpMaxDrawdownPct": "double",
               **{name: kind for name, kind, _ in STRATEGY_INPUTS.values()},
               "InpDeviationPoints": "int", "InpExportTrades": "bool"}
STRATEGY_FIELDS = (("k_period", "%K (barras)"), ("slowing", "Desaceleração (barras)"),
                   ("d_period", "%D SMA (barras)"), ("ema_period", "EMA (barras)"),
                   ("upper_level", "Nível de compra (0..100)"), ("lower_level", "Nível de venda (0..100)"),
                   ("stop_offset_points", "Offset além da EMA (pontos)"), ("target_points", "Alvo desde o sinal (pontos; 0 desliga)"))


def require_robot(robot):
    actual = {item.name: item.type for item in robot.inputs}
    if (robot.kind != catalog.MQL5 or not robot.has_source or robot.key != ROBOT_KEY
            or robot.version != ROBOT_VERSION or catalog.fingerprint(robot) != APPROVED_FINGERPRINT
            or actual != INPUT_TYPES or len(robot.inputs) != len(INPUT_TYPES)
            or set(robot.missing_includes) - {"Canvas/Canvas.mqh"}):
        raise ValueError("S10 exige fonte/versão/fingerprint aprovados e os 15 inputs conhecidos; atualize a biblioteca completa pela revisão indicada.")
    return APPROVED_FINGERPRINT, INPUT_TYPES, 1, HADOUKEN_REVISION


def default_composition(robot, *, symbol, timeframe):
    require_robot(robot)
    return base_composition("stoch_ema9", symbol=symbol, timeframe=timeframe, risk_fraction=.005, compound=True)


@dataclass(frozen=True)
class StochNativeAdapter:
    composition: Composition
    robot: catalog.Robot

    def __post_init__(self):
        if not isinstance(self.composition, Composition):
            object.__setattr__(self, "composition", Composition(self.composition))
        self._validate()

    def _validate(self):
        require_robot(self.robot)
        data = self.composition.to_dict()
        if data["strategy"]["key"] != "stoch_ema9":
            raise ValueError("Perfil S10 exige a estratégia stoch_ema9.")
        get_strategy("stoch_ema9", **data["strategy"]["params"])
        for slot, mode in BASE_MODES.items():
            if data[slot]["mode"] != mode:
                raise ValueError(f"{slot}: S10 nativa aceita somente {mode}; a âncora do alvo é o fechamento do sinal e o stop segue a EMA.")
        if data["filters"] or data["execution_order"] != list(EXECUTION_ORDER):
            raise ValueError("S10 nativa não oferece filtros adicionais; preserve a ordem de execução base.")
        if data["bar_policy"] != {"intrabar_priority": "stop_first", "position_limit": 1}:
            raise ValueError("S10 nativa exige uma posição e política base.")
        sizing = data["sizing"]["params"]
        if not sizing["compound"] or not 0 < sizing["risk_fraction"] <= .005:
            raise ValueError("S10 nativa usa saldo composto e orçamento até 0,5%; não aumente o limite para executar.")
        return data

    def describe(self, preset_id):
        data = self._validate()
        preset_id = native_inputs.scalar(preset_id, "token")
        if not preset_id or len(preset_id) > 80:
            raise ValueError("ID do preset deve ter de 1 a 80 caracteres portáteis.")
        values = {"InpMagicNumber": "20261038", "InpRiskPct": format(Decimal(str(data["sizing"]["params"]["risk_fraction"]))*100, "f"),
                  "InpDailyLossPct": "2", "InpWeeklyLossPct": "4", "InpMaxDrawdownPct": "10",
                  "InpDeviationPoints": "20", "InpExportTrades": "false"}
        bindings = {"sizing.params.risk_fraction": {"input": "InpRiskPct", "type": "number", "unit": "fraction"}}
        for key, (name, kind, unit) in STRATEGY_INPUTS.items():
            values[name] = format(Decimal(str(data["strategy"]["params"][key])), "f")
            bindings["strategy.params."+key] = {"input": name, "type": "int" if kind == "int" else "number", "unit": unit}
        capabilities = Capabilities({"capabilities_schema_version": 1,
            "robot": {"key": self.robot.key, "version": self.robot.version, "fingerprint": APPROVED_FINGERPRINT},
            "backend": "mql5", "strategy_key": "stoch_ema9",
            "modules": {**{slot: [mode] for slot, mode in BASE_MODES.items()}, "filters": []}, "bindings": bindings}, self.composition)
        fields = {name: "int" if kind in ("int", "long") else "number" if kind == "double" else "bool" for name, kind in INPUT_TYPES.items()}
        return {"native_profile_schema_version": 1, "profile_id": PROFILE_ID, "profile_version": 1,
                "hadouken_revision": HADOUKEN_REVISION, "robot": capabilities.to_dict()["robot"],
                "capabilities": capabilities.to_dict(), "parameters": values, "receipt_fields": fields,
                "expected_native_inputs": native_inputs.expected_values(values, fields), "inactive_paths": [],
                "conversions": {"sizing.params.risk_fraction": "fraction × 100 → InpRiskPct percent"},
                "fixed_constraints": {"compound": True, "positions": 1, "daily_loss_pct": 2, "weekly_loss_pct": 4,
                    "max_dd_pct": 10, "max_open_risk_pct": data["sizing"]["params"]["risk_fraction"]*100,
                    "margin_buffer_pct": 20, "block_averaging": True, "partial": False, "breakeven": False,
                    "trailing": "ema_move_guard_closed_bar", "target_anchor": "signal_close", "native_window_bars": 1500},
                "modality": "native_tester_demo_only", "operational_acceptance": False}
