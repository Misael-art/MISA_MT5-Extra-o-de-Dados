"""
Contrato do robô (versão 1): o que um robô MQL5 declara no `robot.json` para o SDK conferir sem adivinhar.

    problems = validate(robot)            # lista vazia = contrato válido
    name = input_name(robot, "magic")     # nome do parâmetro que cumpre o papel (None se não declarado)

A especificação está em docs/sdk/contrato.md e o esquema em robot.schema.json (documentação: a validação é feita
aqui, à mão, para não exigir uma dependência nova). Robô sem `"contract"` no `robot.json` continua aceito: as
demais partes do projeto caem na leitura por nomes prováveis (`risk.find_inputs`).
"""
from dataclasses import dataclass
from typing import Dict, List, Optional

CONTRACT_VERSION = 1

# papel -> o que o parâmetro representa
ROLES = {
    "magic": "magic number (identifica as operações do robô)",
    "risk_pct": "risco por operação, em % do capital",
    "daily_loss_pct": "perda máxima diária, em %",
    "max_drawdown_pct": "drawdown máximo desde o pico, em %",
    "stop": "tamanho do stop (maior que zero = stop sempre definido)",
    "export_trades": "liga a exportação de operações (HDK_trades_<magic>.csv)",
}
REQUIRED_ROLES = ("magic", "risk_pct", "daily_loss_pct", "max_drawdown_pct", "stop")
NUMERIC_ROLES = ("risk_pct", "daily_loss_pct", "max_drawdown_pct", "stop")

# campo do robot.json -> tipo aceito
FIELDS = {"contract": int, "name": str, "description": str, "regime": str, "timeframes": list, "markets": str,
          "risk": dict, "inputs": dict, "tunable": list, "safety_lib": str, "parity_with": str}


@dataclass
class Problem:
    field: str
    message: str

    def __str__(self) -> str:
        return f"{self.field}: {self.message}"


def declared(robot) -> bool:
    """O robô declara um contrato no robot.json?"""
    return "contract" in (getattr(robot, "manifest", None) or {})


def input_name(robot, role: str) -> Optional[str]:
    """Nome do parâmetro declarado para o papel (None sem contrato ou sem o papel)."""
    inputs = (getattr(robot, "manifest", None) or {}).get("inputs")
    name = inputs.get(role) if isinstance(inputs, dict) else None
    return name if isinstance(name, str) and name else None


def _number(value) -> Optional[float]:
    try:
        return float(str(value).split("|")[0].replace(",", "."))
    except (TypeError, ValueError):
        return None


def validate(robot) -> List[Problem]:
    """Confere o robot.json contra o contrato e contra os parâmetros do fonte. Mensagens em português."""
    manifest = getattr(robot, "manifest", None) or {}
    if "contract" not in manifest:
        return [Problem("contract", "o robot.json não declara \"contract\": 1 (ou não existe ao lado do robô)")]
    problems = []
    for name, value in manifest.items():
        expected = FIELDS.get(name)
        if expected is None:
            problems.append(Problem(name, "campo desconhecido no contrato 1 (confira a grafia)"))
        elif not isinstance(value, expected) or isinstance(value, bool):
            problems.append(Problem(name, f"deveria ser {_TYPE_NAMES[expected]}"))
    if manifest.get("contract") != CONTRACT_VERSION:
        problems.append(Problem("contract", f"versão {manifest.get('contract')!r} não suportada (use {CONTRACT_VERSION})"))
    if not robot.has_source:
        return problems + [Problem("inputs", "robô sem código-fonte (.mq5): os parâmetros não podem ser conferidos")]
    inputs = manifest.get("inputs")
    if not isinstance(inputs, dict):
        return problems + [Problem("inputs", "obrigatório: o nome do parâmetro de cada papel (" +
                                   ", ".join(REQUIRED_ROLES) + ")")]
    known = {i.name: i for i in robot.inputs}
    for role, name in inputs.items():
        if role not in ROLES:
            problems.append(Problem(f"inputs.{role}", "papel desconhecido; os papéis são: " + ", ".join(ROLES)))
        elif not isinstance(name, str) or name not in known:
            problems.append(Problem(f"inputs.{role}", f"o robô não tem o parâmetro '{name}'"))
        elif role in NUMERIC_ROLES and _number(known[name].default) is None:
            problems.append(Problem(f"inputs.{role}", f"'{name}' deveria ser numérico (padrão: {known[name].default})"))
    for role in REQUIRED_ROLES:
        if role not in inputs:
            problems.append(Problem(f"inputs.{role}", f"obrigatório: falta o parâmetro de {ROLES[role]}"))
    for name in manifest.get("tunable") or []:
        if name not in known:
            problems.append(Problem("tunable", f"o robô não tem o parâmetro '{name}'"))
        elif _number(known[name].default) is None:
            problems.append(Problem("tunable", f"'{name}' não é numérico: não dá para variar ±20%"))
    for name in manifest.get("timeframes") or []:
        if not isinstance(name, str):
            problems.append(Problem("timeframes", "deveria ser uma lista de textos, como [\"M5\", \"M15\"]"))
    return problems


_TYPE_NAMES = {int: "um número inteiro", str: "um texto", list: "uma lista", dict: "um objeto"}


def facts(robot, values: Optional[Dict[str, str]] = None) -> dict:
    """
    O que o contrato permite afirmar sobre risco e limites, com os valores do preset (`values`) sobre os do código.
    Mesmas chaves de RiskRating.facts: risk_pct, daily_loss_pct, max_drawdown_pct, mandatory_stop.
    """
    defaults = {i.name: i.default for i in robot.inputs}
    values = values or {}
    result = {}
    for role in NUMERIC_ROLES:
        name = input_name(robot, role)
        number = _number(values.get(name, defaults.get(name))) if name else None
        if number is None:
            continue
        if role == "stop":
            result["mandatory_stop"] = number > 0
        else:
            result[role] = number
    return result


def validate_composition(robot, composition, capabilities) -> List[Problem]:
    """Valida sidecars explicitamente; não estende SDK1 nem comprova suporte nativo.

    `robot` vem do catálogo: versão, inputs e fingerprint são conferidos contra o
    fonte atual. Gerar/carregar preset continua exigindo o adaptador de T19.23/25.
    """
    from ..composition import Capabilities, Composition, CompositionError
    from .catalog import MQL5, fingerprint

    problems = validate(robot)
    try:
        comp = composition if isinstance(composition, Composition) else Composition(composition)
        # Revalidar contra esta composição mesmo se o chamador entregou objeto já lido.
        caps = Capabilities(capabilities.to_dict() if isinstance(capabilities, Capabilities) else capabilities, comp)
    except CompositionError as exc:
        return problems + [Problem(p.field, p.message) for p in exc.problems]
    data = caps.to_dict()
    actual = {"key": robot.key, "version": robot.version, "fingerprint": fingerprint(robot)}
    if robot.kind != MQL5:
        problems.append(Problem("capabilities.backend", "sidecar de inputs exige robô MQL5 do catálogo."))
    for key, value in actual.items():
        if not value or data["robot"][key] != value:
            problems.append(Problem(f"capabilities.robot.{key}", "difere do robô/fonte atual; atualize a declaração e revalide."))
    comp_data = comp.to_dict()
    for slot, modes in data["modules"].items():
        requested = ([module["mode"] for module in comp_data[slot]] if slot == "filters"
                     else [comp_data[slot]["mode"]])
        if any(mode not in modes for mode in requested):
            problems.append(Problem(f"capabilities.modules.{slot}",
                                    "modo solicitado não declarado; escolha capacidade compatível com este robô."))
    inputs = {item.name: item for item in robot.inputs}
    types = {"int": {"int", "uint", "long", "ulong", "short", "ushort", "char", "uchar"},
             "number": {"double", "float"}, "bool": {"bool"}}
    for path, binding in data["bindings"].items():
        item = inputs.get(binding["input"])
        if item is None:
            problems.append(Problem(f"bindings.{path}.input", "input não existe no fonte; declare o nome exato do catálogo."))
        elif item.type not in types[binding["type"]]:
            problems.append(Problem(f"bindings.{path}.type", "tipo incompatível com o input MQL5; conversão/enum exige adaptador próprio."))
    return problems
