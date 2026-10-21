"""T19.39: variante Madrid fechada, somente fonte aprovada e Strategy Tester DEMO."""
from dataclasses import dataclass
from decimal import Decimal
from .adapter import base_composition
from .registry import BASE_MODES, EXECUTION_ORDER
from .schema import Capabilities, Composition
from ..robots import catalog, native_inputs
from ..strategies import get_strategy

PROFILE_ID = "hadouken.madrid"
ROBOT_KEY = "mql5:hdk_madrid"
ROBOT_VERSION = "1.20"
APPROVED_FINGERPRINT = "870c0f8db71c3d5a"
HADOUKEN_REVISION = "6677bce1a4d08f6f92d51ad6caccd078d2e33ef5"
STRATEGY_INPUTS = {
    "first_period": ("InpFirstPeriod", "int", "bar"),
    "period_step": ("InpPeriodStep", "int", "bar"),
    "distance_atr": ("InpDistanceAtr", "double", "ATR"),
    "distance_points": ("InpDistancePoints", "int", "point"),
    "trail_line": ("InpTrailLine", "int", "EMA_line"),
    "offset_atr": ("InpOffsetAtr", "double", "ATR"),
}
INPUT_TYPES = {"InpMagicNumber": "long", "InpRiskPct": "double", "InpDailyLossPct": "double",
               "InpWeeklyLossPct": "double", "InpMaxDrawdownPct": "double",
               **{name: kind for name, kind, _ in STRATEGY_INPUTS.values()},
               "InpDeviationPoints": "int", "InpExportTrades": "bool"}
STRATEGY_FIELDS = (("first_period", "Período da primeira EMA (barras)"), ("period_step", "Passo entre EMAs (barras)"),
                   ("distance_atr", "Afastamento (ATR14)"), ("distance_points", "Afastamento (pontos; 0 usa ATR)"),
                   ("trail_line", "Média do stop e guard (1 a 10)"), ("offset_atr", "Offset do stop (ATR14)"))


def require_robot(robot):
    actual = {item.name: item.type for item in robot.inputs}
    if (robot.kind != catalog.MQL5 or not robot.has_source or robot.key != ROBOT_KEY
            or robot.version != ROBOT_VERSION or catalog.fingerprint(robot) != APPROVED_FINGERPRINT
            or actual != INPUT_TYPES or len(robot.inputs) != len(INPUT_TYPES)
            or set(robot.missing_includes) - {"Canvas/Canvas.mqh"}):
        raise ValueError("Madrid exige fonte/versão/fingerprint aprovados e os 13 inputs conhecidos; atualize a biblioteca completa pela revisão indicada.")
    return APPROVED_FINGERPRINT, INPUT_TYPES, 1, HADOUKEN_REVISION


def default_composition(robot, *, symbol, timeframe):
    require_robot(robot)
    return base_composition("madrid", symbol=symbol, timeframe=timeframe, risk_fraction=.005, compound=True)


@dataclass(frozen=True)
class MadridNativeAdapter:
    composition: Composition
    robot: catalog.Robot

    def __post_init__(self):
        if not isinstance(self.composition, Composition):
            object.__setattr__(self, "composition", Composition(self.composition))
        self._validate()

    def _validate(self):
        require_robot(self.robot)
        data = self.composition.to_dict()
        if data["strategy"]["key"] != "madrid":
            raise ValueError("Perfil Madrid exige a estratégia madrid.")
        get_strategy("madrid", **data["strategy"]["params"])
        for slot, mode in BASE_MODES.items():
            if data[slot]["mode"] != mode:
                raise ValueError(f"{slot}: Madrid nativa aceita somente {mode}; o TP fica em M1 ± afastamento do sinal e o stop segue a média com offset ATR.")
        if data["filters"] or data["execution_order"] != list(EXECUTION_ORDER):
            raise ValueError("Madrid nativa não oferece filtros adicionais; preserve a ordem de execução base.")
        if data["bar_policy"] != {"intrabar_priority": "stop_first", "position_limit": 1}:
            raise ValueError("Madrid nativa exige uma posição e política base.")
        sizing = data["sizing"]["params"]
        if not sizing["compound"] or not 0 < sizing["risk_fraction"] <= .005:
            raise ValueError("Madrid nativa usa saldo composto e orçamento até 0,5%; não aumente o limite para executar.")
        return data

    def describe(self, preset_id):
        data = self._validate()
        preset_id = native_inputs.scalar(preset_id, "token")
        if not preset_id or len(preset_id) > 80:
            raise ValueError("ID do preset deve ter de 1 a 80 caracteres portáteis.")
        values = {"InpMagicNumber": "20261039", "InpRiskPct": format(Decimal(str(data["sizing"]["params"]["risk_fraction"]))*100, "f"),
                  "InpDailyLossPct": "2", "InpWeeklyLossPct": "4", "InpMaxDrawdownPct": "10",
                  "InpDeviationPoints": "20", "InpExportTrades": "false"}
        bindings = {"sizing.params.risk_fraction": {"input": "InpRiskPct", "type": "number", "unit": "fraction"}}
        for key, (name, kind, unit) in STRATEGY_INPUTS.items():
            values[name] = format(Decimal(str(data["strategy"]["params"][key])), "f")
            bindings["strategy.params."+key] = {"input": name, "type": "int" if kind == "int" else "number", "unit": unit}
        capabilities = Capabilities({"capabilities_schema_version": 1,
            "robot": {"key": self.robot.key, "version": self.robot.version, "fingerprint": APPROVED_FINGERPRINT},
            "backend": "mql5", "strategy_key": "madrid",
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
                    "trailing": "shared_ema_move_guard", "target_anchor": "signal_m1_plus_minus_distance",
                    "distance_mode": "points" if data["strategy"]["params"]["distance_points"] else "ATR14",
                    "zero_distance_points": "use_atr_scale_with_required_tp", "persistent_cycle_flags": False,
                    "moving_target": False, "ema_seed": "first_close_in_observed_window",
                    "atr_seed": "first_high_minus_low_then_RMA_1_14", "warmup_bars": 300, "native_window_bars": 1500},
                "modality": "native_tester_demo_only", "operational_acceptance": False}
