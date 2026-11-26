"""Seleção congelada do Laboratório para a carteira técnica EURUSD M1."""
import json
from pathlib import Path
import re

from ..experiments.schema import digest
from . import ultra_market
from .ultra_catalog import MARKET_PATTERN


SCHEMA = "mt5x-ultra-market-preset/1"
_HASH = re.compile(r"[a-f0-9]{64}")
_FIELDS = {"schema", "version", "parent_sha256", "purpose", "profile", "parameters", "preset_sha256"}
_PROFILE = {"market", "modules", "source_closure_sha256", "canonical_files", "portfolio_cap_pct"}


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


def read(source):
    value = json.loads(Path(source).read_text(encoding="utf-8")) if isinstance(source, (str, Path)) else source
    if not isinstance(value, dict) or set(value) != _FIELDS or value["schema"] != SCHEMA:
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
    _edits(value["parameters"])
    return value


def freeze(config, snapshot, output, *, strategies=None, pattern=None, parameters=None, previous=None):
    """Emite a seleção ou deriva uma revisão preservando o restante da seleção."""
    from ..robots import ultra_market_portfolio

    manifest = ultra_market.verify(snapshot)
    if previous is not None:
        previous = read(previous)
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
    return _edits(preset["parameters"])
