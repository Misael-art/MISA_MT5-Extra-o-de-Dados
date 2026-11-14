"""SA-01 contrato1: parâmetros/pacote do consumidor nativo, sem fórmula Python."""
import calendar
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from .adapter import base_composition
from .registry import BASE_MODES, EXECUTION_ORDER, STRATEGY_PARAMETERS
from .schema import Capabilities, Composition
from .. import timeframes
from ..robots import catalog, native_inputs

PROFILE_ID = "hadouken.super_atr_triple"
ROBOT_KEY = "mql5:hdk_superatrtriplo"
ROBOT_VERSION = "1.00"
APPROVED_FINGERPRINT = "802c44f481a9bb8a"
HADOUKEN_REVISION = "42793107df40118080936e63196ad152f2bfb930"
UNCONFIGURED_SEED = "1970-01-01T00:00"  # representação explícita do datetime0 do código
STRATEGY_INPUTS = {
    "seed_start_broker": ("InpSeedStart", "datetime"),
    "period": ("InpPeriod", "int"), "mean": ("InpMean", "int"),
    "deviation1": ("InpDeviation1", "double"), "deviation2": ("InpDeviation2", "double"),
    "deviation3": ("InpDeviation3", "double"), "initial_side": ("InpInitialSide", "int"),
    "entries_enabled": ("InpEntriesEnabled", "bool"),
}
INPUT_TYPES = {**{name: kind for name, kind in STRATEGY_INPUTS.values()},
               "InpRiskPct": "double", "InpMagic": "long", "InpDeviationPoints": "int",
               "InpPresetId": "string", "InpConfigHash": "string"}
RECEIPT_FIELDS = {name: "datetime" if kind == "datetime" else "int" if kind in ("int", "long")
                  else "number" if kind == "double" else "bool" if kind == "bool" else "token"
                  for name, kind in INPUT_TYPES.items()}
STRATEGY_FIELDS = (
    ("seed_start_broker", "Primeira barra broker (YYYY-MM-DDTHH:MM)"),
    ("period", "Período comum (barras)"), ("mean", "Média (0 SMA / 1 EMA)"),
    ("deviation1", "Desvio1 · SL inicial"), ("deviation2", "Desvio2"), ("deviation3", "Desvio3"),
    ("initial_side", "Lado da seed (+1 / -1)"), ("entries_enabled", "Armar entradas no Tester"),
)


def coerce_parameters(values):
    """Conversão dos controles existentes; data broker não recebe fuso do host."""
    result = {}
    for key, spec in STRATEGY_PARAMETERS["super_atr_triple"].items():
        value = values[key]
        if spec.kind == "broker_datetime":
            result[key] = str(value).strip() or UNCONFIGURED_SEED
        elif spec.kind == "bool":
            result[key] = native_inputs.scalar(value, "bool")
        else:
            text = str(value).strip().replace(",", ".")
            result[key] = int(text) if spec.kind == "int" else float(text)
    return result


def require_robot(robot):
    actual = {item.name: item.type for item in robot.inputs}
    if (robot.kind != catalog.MQL5 or not robot.has_source or robot.key != ROBOT_KEY
            or robot.version != ROBOT_VERSION or catalog.fingerprint(robot) != APPROVED_FINGERPRINT
            or actual != INPUT_TYPES or len(robot.inputs) != len(INPUT_TYPES)
            or set(robot.missing_includes) - {"Canvas/Canvas.mqh"}):
        raise ValueError("Triplo exige fonte/versão/fingerprint aprovados e os13 inputs conhecidos; reconcilie a biblioteca fixada.")
    return APPROVED_FINGERPRINT, INPUT_TYPES, 1, HADOUKEN_REVISION


def default_composition(robot, *, symbol, timeframe):
    require_robot(robot)
    return base_composition("super_atr_triple", symbol=symbol, timeframe=timeframe, risk_fraction=.005)


@dataclass(frozen=True)
class SuperATRTripleNativeAdapter:
    composition: Composition
    robot: catalog.Robot

    def __post_init__(self):
        if not isinstance(self.composition, Composition):
            object.__setattr__(self, "composition", Composition(self.composition))
        self._validate()

    def _validate(self):
        require_robot(self.robot)
        data = self.composition.to_dict()
        if data["strategy"]["key"] != "super_atr_triple":
            raise ValueError("Triplo exige o contrato super_atr_triple.")
        p = data["strategy"]["params"]
        frame = timeframes.parse(data["context"]["timeframe"])
        seed = datetime.strptime(p["seed_start_broker"], "%Y-%m-%dT%H:%M")
        encoded = calendar.timegm(seed.timetuple())
        if frame is None or frame.minutes >= 1440 or encoded % (frame.minutes*60):
            raise ValueError("Triplo exige TF nativo intradiário e seed alinhada à abertura broker.")
        if not p["deviation1"] < p["deviation2"] < p["deviation3"] or p["initial_side"] not in (-1, 1):
            raise ValueError("Triplo exige três desvios crescentes e lado inicial +1 ou -1.")
        if encoded == 0 and p["entries_enabled"]:
            raise ValueError("Defina a primeira barra real em relógio broker antes de armar entradas.")
        for slot, mode in BASE_MODES.items():
            if data[slot]["mode"] != mode:
                raise ValueError(f"{slot}: Triplo1 usa somente {mode}; SL na curva1, TP ausente e sem gestões concorrentes.")
        if data["filters"] or data["execution_order"] != list(EXECUTION_ORDER):
            raise ValueError("Triplo1 não oferece filtros extras; preserve a ordem de execução.")
        size = data["sizing"]["params"]
        if not size["compound"] or not 0 < size["risk_fraction"] <= .005:
            raise ValueError("Triplo usa saldo composto e risco de até0,5% por entrada.")
        return data, seed, encoded > 0

    def describe(self, preset_id):
        data, seed, ready = self._validate()
        preset_id = native_inputs.scalar(preset_id, "token")
        if not preset_id or len(preset_id) > 80:
            raise ValueError("ID do preset exige1…80 caracteres portáteis.")
        values = {"InpMagic": "909202611", "InpDeviationPoints": "20", "InpPresetId": preset_id,
                  "InpConfigHash": self.composition.config_hash,
                  "InpRiskPct": format(Decimal(str(data["sizing"]["params"]["risk_fraction"]))*100, "f")}
        bindings = {"sizing.params.risk_fraction": {"input": "InpRiskPct", "type": "number", "unit": "fraction"}}
        for key, (name, kind) in STRATEGY_INPUTS.items():
            value = data["strategy"]["params"][key]
            values[name] = seed.strftime("%Y.%m.%d %H:%M") if kind == "datetime" else str(value).lower() if kind == "bool" else str(value)
            spec = STRATEGY_PARAMETERS["super_atr_triple"][key]
            bindings["strategy.params."+key] = {"input": name, "type": spec.kind, "unit": spec.unit}
        capabilities = Capabilities({"capabilities_schema_version": 1,
            "robot": {"key": self.robot.key, "version": self.robot.version, "fingerprint": APPROVED_FINGERPRINT},
            "backend": "mql5", "strategy_key": "super_atr_triple",
            "modules": {**{slot: [mode] for slot, mode in BASE_MODES.items()}, "filters": []}, "bindings": bindings}, self.composition)
        sources = native_inputs.project_sources(self.robot)
        return {"native_profile_schema_version": 1, "profile_id": PROFILE_ID, "profile_version": 1,
            "hadouken_revision": HADOUKEN_REVISION, "robot": capabilities.to_dict()["robot"],
            "capabilities": capabilities.to_dict(), "parameters": values, "receipt_fields": RECEIPT_FIELDS,
            "expected_native_inputs": native_inputs.expected_values(values, RECEIPT_FIELDS), "inactive_paths": [],
            "conversions": {"strategy.params.seed_start_broker": "broker datetime unchanged; datetime0 denotes unconfigured Default; no host timezone conversion",
                            "sizing.params.risk_fraction": "fraction × 100 → InpRiskPct percent"},
            "configuration_ready": ready,
            "configuration_reason": "" if ready else "Defina a primeira barra real em relógio broker em um preset Manual.",
            "runtime_binding_required": True, "project_source_files": sources,
            "project_source_sha256": native_inputs.project_sources_hash(sources), "configuration_transition": "next_session_only",
            "fixed_constraints": {"compound": True, "positions": 1, "daily_loss_pct": 2, "weekly_loss_pct": 4,
                "max_dd_pct": 10, "margin_buffer_pct": 20, "block_averaging": True, "partial": False,
                "breakeven": False, "trailing": False, "target_anchor": "disabled_required_zero_tp",
                "stop_anchor": "absolute_confirmed_curve1_outward_tick", "same_bar_reverse": False,
                "episode": "initial seed consensus is not an entry; same reserved episode never resends",
                "restart": "entry episode must confirm after instance start; old consensus does not rearm",
                "exit": "opposite three-curve closed consensus; existing account risk has priority"},
            "modality": "native_tester_demo_only", "operational_acceptance": False}
