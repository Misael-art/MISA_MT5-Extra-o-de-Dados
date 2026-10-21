"""T19.36: variante VWAP fechada, somente fonte aprovada e Strategy Tester DEMO."""
from dataclasses import dataclass
from decimal import Decimal
from .adapter import base_composition
from .registry import BASE_MODES, EXECUTION_ORDER
from .schema import Capabilities, Composition
from ..robots import catalog, native_inputs
from ..strategies import get_strategy

PROFILE_ID = "hadouken.vwap_bands"
ROBOT_KEY = "mql5:hdk_vwapbands"
ROBOT_VERSION = "1.20"
APPROVED_FINGERPRINT = "7139d746ab6b2c1d"
HADOUKEN_REVISION = "dc789710f57a54260c0243ceb1716b66ae523ea9"
STRATEGY_INPUTS = {
    "entry_band_pct": ("InpEntryBandPct", "double", "percent"),
    "target_band_pct": ("InpTargetBandPct", "double", "percent"),
    "stop_offset_points": ("InpStopPoints", "int", "point"),
    "volume_source": ("InpVolumeSource", "int", "volume_source_0_auto_1_ticks_2_real"),
}
INPUT_TYPES = {"InpMagicNumber": "long", "InpRiskPct": "double", "InpDailyLossPct": "double",
               "InpWeeklyLossPct": "double", "InpMaxDrawdownPct": "double",
               **{name: kind for name, kind, _ in STRATEGY_INPUTS.values()},
               "InpDeviationPoints": "int", "InpExportTrades": "bool"}
STRATEGY_FIELDS = (("entry_band_pct", "Banda de entrada (%)"), ("target_band_pct", "Banda de alvo (%; 0 = VWAP central)"),
                   ("stop_offset_points", "Offset além da banda (pontos)"), ("volume_source", "Volume: 0 Auto / 1 ticks / 2 real"))


def require_robot(robot):
    actual = {item.name: item.type for item in robot.inputs}
    if (robot.kind != catalog.MQL5 or not robot.has_source or robot.key != ROBOT_KEY
            or robot.version != ROBOT_VERSION or catalog.fingerprint(robot) != APPROVED_FINGERPRINT
            or actual != INPUT_TYPES or len(robot.inputs) != len(INPUT_TYPES)
            or set(robot.missing_includes) - {"Canvas/Canvas.mqh"}):
        raise ValueError("VWAP exige fonte/versão/fingerprint aprovados e os 11 inputs conhecidos; atualize a biblioteca completa pela revisão indicada.")
    return APPROVED_FINGERPRINT, INPUT_TYPES, 1, HADOUKEN_REVISION


def default_composition(robot, *, symbol, timeframe):
    require_robot(robot)
    return base_composition("vwap_bands", symbol=symbol, timeframe=timeframe, risk_fraction=.005, compound=True)


@dataclass(frozen=True)
class VwapNativeAdapter:
    composition: Composition
    robot: catalog.Robot

    def __post_init__(self):
        if not isinstance(self.composition, Composition):
            object.__setattr__(self, "composition", Composition(self.composition))
        self._validate()

    def _validate(self):
        require_robot(self.robot)
        data = self.composition.to_dict()
        if data["strategy"]["key"] != "vwap_bands":
            raise ValueError("Perfil VWAP exige a estratégia vwap_bands.")
        get_strategy("vwap_bands", **data["strategy"]["params"])
        for slot, mode in BASE_MODES.items():
            if data[slot]["mode"] != mode:
                raise ValueError(f"{slot}: VWAP nativa aceita somente {mode}; o alvo é a banda fixada no sinal e o stop usa offset além da banda.")
        if data["filters"] or data["execution_order"] != list(EXECUTION_ORDER):
            raise ValueError("VWAP nativa não oferece filtros adicionais; preserve a ordem de execução base.")
        if data["bar_policy"] != {"intrabar_priority": "stop_first", "position_limit": 1}:
            raise ValueError("VWAP nativa exige uma posição e política base.")
        sizing = data["sizing"]["params"]
        if not sizing["compound"] or not 0 < sizing["risk_fraction"] <= .005:
            raise ValueError("VWAP nativa usa saldo composto e orçamento até 0,5%; não aumente o limite para executar.")
        return data

    def describe(self, preset_id):
        data = self._validate()
        preset_id = native_inputs.scalar(preset_id, "token")
        if not preset_id or len(preset_id) > 80:
            raise ValueError("ID do preset deve ter de 1 a 80 caracteres portáteis.")
        values = {"InpMagicNumber": "20261036", "InpRiskPct": format(Decimal(str(data["sizing"]["params"]["risk_fraction"]))*100, "f"),
                  "InpDailyLossPct": "2", "InpWeeklyLossPct": "4", "InpMaxDrawdownPct": "10",
                  "InpDeviationPoints": "20", "InpExportTrades": "false"}
        bindings = {"sizing.params.risk_fraction": {"input": "InpRiskPct", "type": "number", "unit": "fraction"}}
        for key, (name, kind, unit) in STRATEGY_INPUTS.items():
            values[name] = format(Decimal(str(data["strategy"]["params"][key])), "f")
            bindings["strategy.params."+key] = {"input": name, "type": "int" if kind == "int" else "number", "unit": unit}
        capabilities = Capabilities({"capabilities_schema_version": 1,
            "robot": {"key": self.robot.key, "version": self.robot.version, "fingerprint": APPROVED_FINGERPRINT},
            "backend": "mql5", "strategy_key": "vwap_bands",
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
                    "trailing": False, "target_anchor": "signal_vwap_band", "zero_target_band": "central_vwap_required_tp",
                    "volume_source": data["strategy"]["params"]["volume_source"], "vwap_anchor": "first_available_server_date_bar_with_initial_partial_day_refused", "native_window_bars": 1500},
                "modality": "native_tester_demo_only", "operational_acceptance": False}
