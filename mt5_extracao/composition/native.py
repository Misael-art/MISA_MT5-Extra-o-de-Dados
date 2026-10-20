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
from ..robots import catalog, native_inputs
from ..strategies import get_strategy

PROFILE_ID = "hadouken.s09_reference"
PROFILE_VERSION = 1
ROBOT_KEY = "mql5:hdk_s09_referencia"
ROBOT_VERSION = "1.00"
APPROVED_FINGERPRINT = "91ff0ec8a7bba77e"
HADOUKEN_REVISION = "d60e42185471f0e5bc94ec17dcfe8e11f94bbf2b"
INPUT_TYPES = {"InpFastPeriod": "int", "InpSlowPeriod": "int", "InpWarmupBars": "int",
               "InpStopPoints": "int", "InpTargetPoints": "int", "InpRiskPct": "double",
               "InpMagic": "long", "InpDeviationPoints": "int", "InpPresetId": "string", "InpConfigHash": "string"}
RECEIPT_FIELDS = {name: "int" if kind in ("int", "long") else "number" if kind == "double" else "token"
                  for name, kind in INPUT_TYPES.items()}


def _require_robot(robot):
    actual = {item.name: item.type for item in robot.inputs}
    if (robot.kind != catalog.MQL5 or not robot.has_source or robot.key != ROBOT_KEY
            or robot.version != ROBOT_VERSION or catalog.fingerprint(robot) != APPROVED_FINGERPRINT
            or actual != INPUT_TYPES or len(robot.inputs) != len(INPUT_TYPES) or robot.missing_includes):
        raise ValueError("Perfil S09 exige fonte/versão/fingerprint HADOUKEN aprovados e todos os inputs conhecidos; "
                         "atualize a biblioteca pela revisão indicada, sem substituir arquivos do usuário.")


def default_composition(robot, *, symbol, timeframe):
    """Defaults efetivos do perfil nativo; não é persistência do preset Default."""
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
        _require_robot(self.robot)
        data = self.composition.to_dict()
        if data["strategy"]["key"] != "s09_reference":
            raise ValueError("Perfil nativo disponível somente para S09 referência; Pullback permanece bloqueado até T19.57.")
        get_strategy("s09_reference", **data["strategy"]["params"])
        for slot, base in BASE_MODES.items():
            allowed = {base, "fixed_points"} if slot in ("profit_exit", "protection") else {base}
            if data[slot]["mode"] not in allowed:
                raise ValueError(f"{slot}: modo sem suporte no perfil S09 nativo; use {', '.join(sorted(allowed))}.")
        if data["filters"] or data["execution_order"] != list(EXECUTION_ORDER):
            raise ValueError("S09 nativa exige ordem de execução base e nenhum filtro adicional; remova a capacidade sem suporte.")
        if data["bar_policy"] != {"intrabar_priority": "stop_first", "position_limit": 1}:
            raise ValueError("S09 nativa admite uma posição e política base; não ampliar exposição.")
        sizing = data["sizing"]["params"]
        if not sizing["compound"] or not 0 < sizing["risk_fraction"] <= .005:
            raise ValueError("S09 nativa usa risco por saldo, composto, até 0.5%; confira a fração (0.005 = 0.5%), sem elevar o limite.")
        for slot, name in [("profit_exit", "target_points"), ("protection", "stop_points")]:
            value = data[slot]["params"][name] if data[slot]["mode"] == "fixed_points" else data["strategy"]["params"][name]
            _points(value, name)
        return data

    def describe(self, preset_id):
        """Novo snapshot tipado; revalida fonte para detectar edição após construir o adaptador."""
        data = self._validate()
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
        bindings["sizing.params.risk_fraction"] = {"input": "InpRiskPct", "type": "number", "unit": "fraction"}
        modules = {slot: [base] for slot, base in BASE_MODES.items()}
        modules.update(profit_exit=["strategy_default", "fixed_points"], protection=["strategy_default", "fixed_points"], filters=[])
        capabilities = Capabilities({"capabilities_schema_version": 1,
            "robot": {"key": self.robot.key, "version": self.robot.version, "fingerprint": APPROVED_FINGERPRINT},
            "backend": "mql5", "strategy_key": "s09_reference", "modules": modules, "bindings": bindings}, self.composition)
        return {"native_profile_schema_version": 1, "profile_id": PROFILE_ID, "profile_version": PROFILE_VERSION,
                "hadouken_revision": HADOUKEN_REVISION, "robot": capabilities.to_dict()["robot"],
                "capabilities": capabilities.to_dict(), "parameters": values, "receipt_fields": dict(RECEIPT_FIELDS),
                "expected_native_inputs": native_inputs.expected_values(values, RECEIPT_FIELDS),
                "inactive_paths": inactive, "conversions": {"sizing.params.risk_fraction": "fraction × 100 → InpRiskPct percent"},
                "fixed_constraints": {"compound": True, "positions": 1, "daily_loss_pct": 2, "weekly_loss_pct": 4,
                                      "max_dd_pct": 10, "max_open_risk_pct": .5, "margin_buffer_pct": 20,
                                      "block_averaging": True, "partial": False, "breakeven": False, "trailing": False},
                "modality": "native_tester_demo_only", "operational_acceptance": False}
