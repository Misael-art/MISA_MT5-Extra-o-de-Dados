"""Seleção congelada do Laboratório para a carteira técnica EURUSD M1."""
import json
from pathlib import Path
import re

from ..experiments.schema import digest
from . import ultra_market
from .ultra_catalog import MARKET_PATTERN
from . import ultra_market_parameters


SCHEMA = "mt5x-ultra-market-preset/1"
PORTABLE = "mt5x-ultra-market-preset/2"
_HASH = re.compile(r"[a-f0-9]{64}")
_FIELDS = {"schema", "version", "parent_sha256", "purpose", "profile", "parameters", "preset_sha256"}
_PROFILE = {"market", "modules", "source_closure_sha256", "canonical_files", "portfolio_cap_pct"}
_RISK = {"daily_loss_cents", "daily_profit_cents", "max_attempts", "max_losses"}
_MANAGEMENT = {"partial_r", "partial_fraction", "breakeven_after_partial", "trailing_atr"}


def _edits(parameters):
    if not isinstance(parameters, dict):
        raise ValueError("parameters: esperado objeto por módulo.")
    result = {}
    for key, fields in parameters.items():
        if key != "orb" or not isinstance(fields, dict) or set(fields) != {"range_minutes"}:
            raise ValueError(f"parameters.{key}: a edição suportada é orb.range_minutes.")
        value = fields["range_minutes"]
        if type(value) is not int or not 1 <= value <= 240:
            raise ValueError("parameters.orb.range_minutes: esperado inteiro entre 1 e 240.")
        result[key] = {"InpRangeMinutes": str(value)}
    return result


def seal(profile, *, parameters, previous):
    value = dict(schema=SCHEMA, version=1 if previous is None else previous["version"] + 1,
                 parent_sha256=None if previous is None else previous["preset_sha256"],
                 purpose="technical_tester", profile=profile, parameters=parameters)
    return dict(value, preset_sha256=digest(value))


def validate_owner_rules(rules, keys):
    if not isinstance(rules, dict):
        raise ValueError("owner_rules: esperado objeto por módulo.")
    for key, rule in rules.items():
        path = f"owner_rules.{key}"
        if key not in keys or not isinstance(rule, dict) or set(rule) - {"risk", "management"}:
            raise ValueError(f"{path}: módulo ausente ou política desconhecida.")
        risk, management = rule.get("risk", {}), rule.get("management", {})
        if not isinstance(risk, dict) or risk.keys() - _RISK:
            raise ValueError(f"{path}.risk: limite não implementado.")
        for name, value in risk.items():
            if type(value) is not int or not 1 <= value <= 2147483647:
                raise ValueError(f"{path}.risk.{name}: use inteiro positivo até 2147483647.")
        if not isinstance(management, dict) or management.keys() - _MANAGEMENT:
            raise ValueError(f"{path}.management: regra não implementada.")
        if "partial_r" in management or "partial_fraction" in management:
            if not {"partial_r", "partial_fraction"} <= management.keys():
                raise ValueError(f"{path}.management: declare o gatilho e a fração da parcial juntos.")
        for name, value in management.items():
            valid = type(value) is bool if name == "breakeven_after_partial" else (
                type(value) in (int, float) and 0 < value <= (100 if name != "partial_fraction" else 1)
                and (name != "partial_fraction" or value < 1))
            if not valid:
                raise ValueError(f"{path}.management.{name}: valor fora do contrato de gestão.")


def portable(config, manifest, *, strategies, pattern=None, parameters=None, owner_rules=None, research=None,
             previous=None):
    from ..robots import ultra_market_portfolio

    parameters = {} if parameters is None else parameters
    owner_rules = {} if owner_rules is None else owner_rules
    keys = tuple(strategies)
    validate_owner_rules(owner_rules, keys)
    if previous is not None:
        previous = read(previous)
        if previous["schema"] != PORTABLE or previous["version"] >= 2147483647:
            raise ValueError("preset.previous: esperado preset portátil com versão incrementável.")
    profile, _, _ = ultra_market_portfolio.resolve(
        config, manifest, strategies=keys, pattern=pattern, edits=ultra_market_parameters.edits(parameters),
        portable=True, owner_rules=owner_rules)
    if previous is not None and profile["market"] != previous["profile"]["market"]:
        raise ValueError("preset.previous.profile.market: requisitos incompatíveis com a linhagem.")
    value = dict(schema=PORTABLE, version=1 if previous is None else previous["version"] + 1,
                 parent_sha256=None if previous is None else previous["preset_sha256"], purpose="technical_tester",
                 profile=profile, parameters=parameters, owner_rules=owner_rules, research=research)
    return dict(value, preset_sha256=digest(value))


def freeze_portable(config, snapshot, output, *, strategies=None, pattern=None, parameters=None, owner_rules=None,
                    previous=None):
    from .ultra_catalog import MARKET_NATIVE

    research = None
    if previous is not None:
        previous = read(previous)
        if previous["schema"] != PORTABLE:
            raise ValueError("preset.previous.schema: use uma política portátil.")
        modules = previous["profile"]["modules"]
        if strategies is None:
            strategies = [module["key"] for module in modules]
        if pattern is None and MARKET_PATTERN in strategies:
            pattern = next((module["pattern"] for module in modules if module["key"] == MARKET_PATTERN), None)
        if parameters is None:
            parameters = previous["parameters"]
        if owner_rules is None:
            owner_rules = previous["owner_rules"]
        research = previous["research"]
    if strategies is None:
        strategies = (*MARKET_NATIVE, *((MARKET_PATTERN,) if pattern is not None else ()))
    value = portable(config, ultra_market.verify(snapshot), strategies=strategies, pattern=pattern,
                     parameters=parameters, owner_rules=owner_rules, research=research, previous=previous)
    write(output, value)
    return value


def read(source):
    value = json.loads(Path(source).read_text(encoding="utf-8")) if isinstance(source, (str, Path)) else source
    schema = value.get("schema") if isinstance(value, dict) else None
    fields = _FIELDS | {"owner_rules", "research"} if schema == PORTABLE else _FIELDS
    if schema not in (SCHEMA, PORTABLE) or set(value) != fields:
        raise ValueError("preset.schema: contrato de carteira desconhecido.")
    body = {key: item for key, item in value.items() if key != "preset_sha256"}
    if digest(body) != value["preset_sha256"]:
        raise ValueError("preset.preset_sha256: conteúdo alterado após o congelamento.")
    version, parent = value["version"], value["parent_sha256"]
    if type(version) is not int or not 1 <= version <= 2147483647:
        raise ValueError("preset.version: esperado inteiro positivo até 2147483647.")
    valid_parent = parent is None if version == 1 else isinstance(parent, str) and _HASH.fullmatch(parent)
    if not valid_parent:
        raise ValueError("preset.parent_sha256: linhagem de revisão inválida.")
    if value["purpose"] != "technical_tester":
        raise ValueError("preset.purpose: a carteira aceita somente controles técnicos no Tester.")
    profile = value["profile"]
    if not isinstance(profile, dict) or set(profile) != _PROFILE:
        raise ValueError("preset.profile: contrato do executor desconhecido.")
    modules = profile["modules"]
    if not isinstance(modules, list) or not modules:
        raise ValueError("preset.profile.modules: selecione ao menos um módulo.")
    for index, item in enumerate(modules):
        if (not isinstance(item, dict) or not isinstance(item.get("key"), str)
                or not isinstance(item.get("inputs"), dict) or item.get("priority") != index
                or (item["key"] == MARKET_PATTERN and not isinstance(item.get("pattern"), dict))):
            raise ValueError(f"preset.profile.modules[{index}]: identidade, parâmetros ou prioridade inválida.")
    edits(value)
    if schema == PORTABLE:
        validate_owner_rules(value["owner_rules"], [module["key"] for module in modules])
        research = value["research"]
        if research is not None and (not isinstance(research, dict)
                or type(research.get("training_end_msc")) is not int):
            raise ValueError("preset.research.training_end_msc: fronteira de treino inválida.")
    return value


def freeze(config, snapshot, output, *, strategies=None, pattern=None, parameters=None, previous=None):
    """Emite a seleção ou deriva uma revisão preservando o restante da seleção."""
    from ..robots import ultra_market_portfolio

    manifest = ultra_market.verify(snapshot)
    if previous is not None:
        previous = read(previous)
        if previous["schema"] != SCHEMA:
            raise ValueError("preset.previous.schema: use o congelamento portátil nesta linhagem.")
        if previous["version"] == 2147483647:
            raise ValueError("preset.version: limite de revisões alcançado.")
        prior = previous["profile"]
        if prior["market"] != ultra_market_portfolio.market_contract(manifest):
            raise ValueError("preset.profile.market: use a mesma fita para revisar esta seleção.")
        if strategies is not None:
            raise ValueError("strategies: uma revisão conserva a ordem e os módulos do preset anterior.")
        strategies = [module["key"] for module in prior["modules"]]
        previous_pattern = next((module["pattern"] for module in prior["modules"] if module["key"] == MARKET_PATTERN),
                                None)
        reference, _, _ = ultra_market_portfolio.resolve(
            config, manifest, strategies=strategies, pattern=previous_pattern,
            edits=_edits(previous["parameters"]), frozen=prior["modules"])
        if reference != prior:
            raise ValueError("preset.profile: a referência anterior diverge das fontes fixadas.")
        if pattern is None:
            pattern = previous_pattern
        elif previous_pattern is None:
            raise ValueError("pattern: uma revisão não pode adicionar um novo módulo.")
        if parameters is None:
            parameters = previous["parameters"]
    parameters = {} if parameters is None else parameters
    profile, _, _ = ultra_market_portfolio.resolve(config, manifest, strategies=strategies, pattern=pattern,
                                                  edits=_edits(parameters))
    value = seal(profile, parameters=parameters, previous=previous)
    write(output, value)
    return value


def write(output, value):
    with Path(output).open("x", encoding="utf-8", newline="\n") as stream:
        json.dump(value, stream, indent=2, ensure_ascii=False, allow_nan=False)
        stream.write("\n")


def edits(preset):
    return (ultra_market_parameters.edits if preset["schema"] == PORTABLE else _edits)(preset["parameters"])
