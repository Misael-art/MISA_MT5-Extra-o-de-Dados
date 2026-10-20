"""Artefatos laterais v1 fechados, portáveis e sem autorização operacional."""
from dataclasses import dataclass
import hashlib
import json

from .registry import (EXECUTION_ORDER, FILTER_PARAMETERS, MODULE_PARAMETERS,
                       STRATEGY_PARAMETERS, TIMEFRAMES, parameter_paths)

COMPOSITION_SCHEMA_VERSION = 1
CAPABILITIES_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class Problem:
    field: str
    message: str


class CompositionError(ValueError):
    def __init__(self, field, message):
        self.problems = [Problem(field, message)]
        super().__init__(f"{field}: {message}")


def _fail(field, message):
    raise CompositionError(field, message)


def _object(value, fields, field):
    if type(value) is not dict:
        _fail(field, "informe um objeto JSON.")
    if any(type(key) is not str for key in value):
        _fail(field, "nomes de campos devem ser strings JSON.")
    missing, unknown = set(fields) - value.keys(), value.keys() - set(fields)
    if missing or unknown:
        _fail(field, f"campos ausentes: {', '.join(sorted(missing)) or 'nenhum'}; "
                     f"desconhecidos: {', '.join(sorted(unknown)) or 'nenhum'}. Confira o schema v1.")


def _text(value, field):
    if not isinstance(value, str) or not value.strip() or value != value.strip() or any(ord(c) < 32 for c in value):
        _fail(field, "informe texto não vazio, sem controles nem espaços nas bordas.")


def _version(value, field):
    if type(value) is not int or value != 1:
        _fail(field, "versão desconhecida; use um leitor compatível (v1), sem conversão automática.")


def _params(value, specs, field):
    _object(value, specs, field)
    for key, spec in specs.items():
        if not spec.accepts(value[key]):
            _fail(f"{field}.{key}", f"valor inválido: esperado {spec.kind} finito, unidade {spec.unit}; "
                                   f"mínimo {'>' if spec.exclusive_minimum else '>='}{spec.minimum}, "
                                   f"máximo {spec.maximum if spec.maximum is not None else 'não limitado'}.")


def _module(value, modes, field):
    _object(value, ("mode", "params"), field)
    if type(value["mode"]) is not str or value["mode"] not in modes:
        _fail(f"{field}.mode", f"modo desconhecido; use: {', '.join(modes)}.")
    _params(value["params"], modes[value["mode"]], f"{field}.params")


def _pairs(pairs):
    out = {}
    for key, value in pairs:
        if key in out:
            _fail(key, "campo JSON duplicado; mantenha um único valor explícito.")
        out[key] = value
    return out


def _decode(value):
    if isinstance(value, str):
        try:
            value = json.loads(value, object_pairs_hook=_pairs,
                               parse_constant=lambda x: _fail(x, "número não finito; informe valor finito."))
        except json.JSONDecodeError as exc:
            _fail("json", f"JSON inválido na linha {exc.lineno}; corrija o arquivo.")
    return value


def canonical_json(value):
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (ValueError, TypeError, OverflowError):
        _fail("json", "use somente valores JSON finitos, sem objetos Python ou ciclos.")


@dataclass(frozen=True, init=False)
class Composition:
    # Guardar JSON evita mutação de dict/list depois da validação ou do hash.
    _json: str

    def __init__(self, value):
        value = _decode(value)
        _object(value, ("composition_schema_version", "strategy", "context", *MODULE_PARAMETERS,
                        "filters", "execution_order", "bar_policy"), "composition")
        _version(value["composition_schema_version"], "composition_schema_version")
        _object(value["strategy"], ("key", "params"), "strategy")
        key = value["strategy"]["key"]
        if type(key) is not str or key not in STRATEGY_PARAMETERS:
            _fail("strategy.key", "estratégia desconhecida; selecione uma estratégia registrada.")
        _params(value["strategy"]["params"], STRATEGY_PARAMETERS[key], "strategy.params")
        _object(value["context"], ("symbol", "timeframe"), "context")
        _text(value["context"]["symbol"], "context.symbol")
        if type(value["context"]["timeframe"]) is not str or value["context"]["timeframe"] not in TIMEFRAMES:
            _fail("context.timeframe", "série desconhecida; informe timeframe MT5 explícito. Renko requer contrato próprio.")
        for slot, modes in MODULE_PARAMETERS.items():
            _module(value[slot], modes, slot)
        if type(value["filters"]) is not list:
            _fail("filters", "informe uma lista ordenada de filtros.")
        seen = set()
        for i, module in enumerate(value["filters"]):
            _module(module, FILTER_PARAMETERS, f"filters.{i}")
            if module["mode"] in seen:
                _fail(f"filters.{i}", "filtro duplicado; mantenha uma regra por tipo.")
            seen.add(module["mode"])
        if type(value["execution_order"]) is not list or value["execution_order"] != list(EXECUTION_ORDER):
            _fail("execution_order", "precedência incompatível; use sinal fechado → filtros → dimensionamento → próxima abertura.")
        _object(value["bar_policy"], ("intrabar_priority", "position_limit"), "bar_policy")
        if value["bar_policy"]["intrabar_priority"] != "stop_first":
            _fail("bar_policy.intrabar_priority", "o motor OHLC exige stop_first; não presumir ordem real dos ticks.")
        if type(value["bar_policy"]["position_limit"]) is not int or value["bar_policy"]["position_limit"] != 1:
            _fail("bar_policy.position_limit", "modo base exige uma posição; multiordem depende dos gates próprios.")
        if value["conditional_policy"]["mode"] in ("temporal_v1","temporal_er_v1"):
            from ..strategies.temporal_er import from_selection
            policy=from_selection(value["conditional_policy"])
            if not policy.enabled:_fail("conditional_policy","Ative ao menos um prazo; para nenhum prazo use disabled.")
        partial = value["partial_exit_plan"]
        from .partials import stages
        stages(partial)
        if partial["mode"] == "fixed_r" and partial["params"]["fraction"] >= 1:
            _fail("partial_exit_plan.params.fraction", "parcial deve ser menor que 1; fechamento total não é parcial.")
        if partial["mode"] == "fixed_r" and value["profit_exit"]["mode"] == "fixed_r" and (
                partial["params"]["trigger_r"] >= value["profit_exit"]["params"]["target_r"]):
            _fail("partial_exit_plan", "gatilho parcial deve anteceder o alvo total em R.")
        object.__setattr__(self, "_json", canonical_json(value))

    def to_dict(self):
        return json.loads(self._json)

    def to_json(self):
        return self._json

    @property
    def config_hash(self):
        return hashlib.sha256(self._json.encode("utf-8")).hexdigest()


@dataclass(frozen=True, init=False)
class Capabilities:
    _json: str

    def __init__(self, value, composition):
        value = _decode(value)
        _object(value, ("capabilities_schema_version", "robot", "backend", "strategy_key", "modules", "bindings"), "capabilities")
        _version(value["capabilities_schema_version"], "capabilities_schema_version")
        _object(value["robot"], ("key", "version", "fingerprint"), "capabilities.robot")
        for key, item in value["robot"].items():
            _text(item, f"capabilities.robot.{key}")
        if value["backend"] != "mql5":
            _fail("capabilities.backend", "o sidecar descreve inputs mql5; suporte Python vem do adaptador instalado.")
        data = composition.to_dict()
        if value["strategy_key"] != data["strategy"]["key"]:
            _fail("capabilities.strategy_key", "estratégia difere da composição; selecione o sidecar correspondente.")
        _object(value["modules"], (*MODULE_PARAMETERS, "filters"), "capabilities.modules")
        for slot, modes in {**MODULE_PARAMETERS, "filters": FILTER_PARAMETERS}.items():
            declared = value["modules"][slot]
            if type(declared) is not list or any(type(mode) is not str or mode not in modes for mode in declared):
                _fail(f"capabilities.modules.{slot}", "declare uma lista de modos conhecidos; lista vazia significa indisponível.")
            if len(set(declared)) != len(declared):
                _fail(f"capabilities.modules.{slot}", "modo duplicado; mantenha uma declaração por modo.")
        paths = parameter_paths(data)
        if type(value["bindings"]) is not dict:
            _fail("capabilities.bindings", "informe mapeamento explícito caminho → input/tipo/unidade.")
        used = set()
        for path, binding in value["bindings"].items():
            if path not in paths:
                _fail(f"bindings.{path}", "caminho desconhecido; declare um parâmetro presente na composição.")
            _object(binding, ("input", "type", "unit"), f"bindings.{path}")
            _text(binding["input"], f"bindings.{path}.input")
            if binding["input"] in used:
                _fail(f"bindings.{path}.input", "input duplicado; evite duas origens concorrentes para um input.")
            used.add(binding["input"])
            spec = paths[path]
            if binding["type"] != spec.kind or binding["unit"] != spec.unit:
                _fail(f"bindings.{path}", f"declare tipo {spec.kind} e unidade {spec.unit}; conversões exigem adaptador explícito.")
        object.__setattr__(self, "_json", canonical_json(value))

    def to_dict(self):
        return json.loads(self._json)

    def to_json(self):
        return self._json
