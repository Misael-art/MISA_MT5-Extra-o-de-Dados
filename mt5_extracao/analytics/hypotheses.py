"""Catálogo JSON versionado das hipóteses analíticas (T13.4)."""
import json
import math
from pathlib import Path


STATUSES = ("DRAFT", "EXPLORATORY", "VALIDATING", "APPROVED", "MONITORING", "RETIRED")
_REQUIRED = ("pattern_id", "title", "version", "status", "universe", "session", "condition", "trigger", "entry",
             "exit", "costs", "parameters", "validation", "sample_builder", "target", "outcome")
_CATALOG = Path(__file__).with_name("hypothesis_definitions")


def _validate_condition(condition, label="condition"):
    if not isinstance(condition, dict):
        raise ValueError(f"{label} precisa ser um objeto JSON.")
    if "field" in condition:
        if not isinstance(condition.get("field"), str) or not condition["field"].strip():
            raise ValueError(f"{label}.field precisa ser texto não vazio.")
        if condition.get("operator") not in {"eq", "ne", "gt", "gte", "lt", "lte", "in", "not_in"}:
            raise ValueError(f"{label}.operator não é suportado.")
        has_value, has_parameter = "value" in condition, "parameter" in condition
        if has_value == has_parameter:
            raise ValueError(f"{label} precisa declarar value ou parameter, exatamente um.")
        if has_parameter and (not isinstance(condition["parameter"], str) or not condition["parameter"].strip()):
            raise ValueError(f"{label}.parameter precisa nomear um parâmetro.")
        return
    operator = condition.get("operator")
    children = condition.get("conditions")
    if operator not in {"and", "or", "not"} or not isinstance(children, list) or not children:
        raise ValueError(f"{label} precisa de field/operator/value ou operator/conditions.")
    if operator == "not" and len(children) != 1:
        raise ValueError(f"{label} com operator=not deve ter exatamente uma condição.")
    for index, child in enumerate(children):
        _validate_condition(child, f"{label}.conditions[{index}]")


def validate_definition(definition):
    """Valida o contrato declarativo sem executar expressões Python vindas do arquivo."""
    if not isinstance(definition, dict):
        raise ValueError("A hipótese precisa ser um objeto JSON.")
    missing = [name for name in _REQUIRED if name not in definition]
    if missing:
        raise ValueError("Campos obrigatórios ausentes: " + ", ".join(missing))
    for name in ("pattern_id", "title", "version", "sample_builder"):
        if not isinstance(definition[name], str) or not definition[name].strip():
            raise ValueError(f"{name} precisa ser texto não vazio.")
    if definition["status"] not in STATUSES:
        raise ValueError("status precisa ser um de: " + ", ".join(STATUSES))
    for name in ("universe", "session", "trigger", "entry", "exit", "costs", "parameters", "validation", "target",
                 "outcome"):
        if not isinstance(definition[name], dict):
            raise ValueError(f"{name} precisa ser um objeto JSON.")
    _validate_condition(definition["condition"])
    _validate_condition(definition["target"], "target")
    if not definition["universe"].get("timeframes"):
        raise ValueError("universe.timeframes precisa listar ao menos um timeframe.")
    if not isinstance(definition["validation"].get("horizon_minutes"), (int, float)) or \
            definition["validation"]["horizon_minutes"] <= 0:
        raise ValueError("validation.horizon_minutes precisa ser positivo.")
    if not isinstance(definition["validation"].get("min_samples"), int) or definition["validation"]["min_samples"] < 2:
        raise ValueError("validation.min_samples precisa ser inteiro >= 2.")
    for grid_name in ("parameter_grid", "sensitivity_grid"):
        grid = definition.get(grid_name, {})
        if not isinstance(grid, dict):
            raise ValueError(f"{grid_name} precisa ser um objeto JSON.")
        combinations = 1
        for name, values in grid.items():
            if name not in definition["parameters"] or not isinstance(values, list) or not values:
                raise ValueError(f"{grid_name}.{name} precisa listar valores para um parâmetro declarado.")
            combinations *= len(values)
            if any(isinstance(value, float) and not math.isfinite(value) for value in values):
                raise ValueError(f"{grid_name}.{name} contém número não finito.")
        if combinations > 100:
            raise ValueError(f"{grid_name} excede o limite de 100 combinações por avaliação.")
    parameter_names = set(definition["parameters"])

    def referenced_parameters(condition):
        if "field" in condition:
            return {condition["parameter"]} if "parameter" in condition else set()
        return set().union(*(referenced_parameters(child) for child in condition["conditions"]))

    references = referenced_parameters(definition["condition"])
    references.update(referenced_parameters(definition["target"]))
    unknown = sorted(references - parameter_names)
    if unknown:
        raise ValueError("Condições referenciam parâmetros não declarados: " + ", ".join(unknown))
    return definition


def load_definition(path):
    """Lê e valida um arquivo JSON externo ou do catálogo."""
    with Path(path).open("r", encoding="utf-8") as handle:
        definition = json.load(handle)
    return validate_definition(definition)


def list_definitions():
    """Carrega os arquivos JSON incluídos no pacote, sem alterar o banco."""
    return [load_definition(path) for path in sorted(_CATALOG.glob("*.json"))]


def get_definition(pattern_id, version=None):
    matches = [item for item in list_definitions() if item["pattern_id"] == str(pattern_id)]
    if version is not None:
        matches = [item for item in matches if item["version"] == str(version)]
    if not matches:
        suffix = f"@{version}" if version is not None else ""
        raise ValueError(f"Hipótese não encontrada: {pattern_id}{suffix}.")
    return matches[-1]
