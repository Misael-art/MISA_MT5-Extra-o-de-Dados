"""Registro versionado de características e seleção por dependências.

Dependências de fontes externas usam chaves estáveis como ``bars:M1`` ou
``market_calendar``. ``{timeframe}`` declara dependência aplicada ao timeframe
do valor (por exemplo, ``bars_derived:{timeframe}``). Dependências de outras
características usam ``feature:<feature_id>``; assim uma alteração de origem
alcança somente os cálculos que dependem dela, direta ou indiretamente.
"""
from fnmatch import fnmatchcase

from mt5_extracao.analytics import feature_store


def _feature_dependency(dependency):
    return dependency[len("feature:"):] if dependency.startswith("feature:") else None


def _dependency_matches(dependency, changed):
    if dependency == changed:
        return True
    return "{timeframe}" in dependency and fnmatchcase(changed, dependency.replace("{timeframe}", "*"))


def _latest_by_id(definitions):
    latest = {}
    for definition in definitions:
        current = latest.get(definition["feature_id"])
        if current is None or (definition["registered_at_utc"], definition["algorithm_version"]) > (
                current["registered_at_utc"], current["algorithm_version"]):
            latest[definition["feature_id"]] = definition
    return latest


def _validate_dependencies(feature_id, dependencies, definitions):
    latest = _latest_by_id(definitions)
    for dependency in dependencies:
        target = _feature_dependency(dependency)
        if target is not None and target not in latest and target != feature_id:
            raise ValueError(f"Dependência não registrada: {dependency}.")
    graph = {key: [target for item in item_def["dependencies"]
                   if (target := _feature_dependency(item)) is not None]
             for key, item_def in latest.items()}
    graph[feature_id] = [target for item in dependencies
                         if (target := _feature_dependency(item)) is not None]
    visiting, visited = set(), set()

    def visit(node):
        if node in visiting:
            raise ValueError("Dependências de características não podem formar ciclos.")
        if node in visited:
            return
        visiting.add(node)
        for target in graph.get(node, ()):
            visit(target)
        visiting.remove(node)
        visited.add(node)

    for node in graph:
        visit(node)


def register(db, feature_id, name, algorithm_version, *, parameters=None, dependencies=(), family=None, formula=None,
             unit=None, warmup_count=None, warmup_unit=None, source_id=None, timeframe=None,
             availability_rule=None, value_type=None):
    """Registra uma versão imutável após validar referências e ciclos."""
    if feature_id is None or name is None or algorithm_version is None:
        raise ValueError("feature_id, name e algorithm_version são obrigatórios.")
    feature_id = str(feature_id).strip()
    name = str(name).strip()
    algorithm_version = str(algorithm_version).strip()
    if not feature_id or not name or not algorithm_version:
        raise ValueError("feature_id, name e algorithm_version são obrigatórios.")
    if parameters is None:
        parameters = {}
    if not isinstance(parameters, dict):
        raise ValueError("parameters precisa ser um objeto JSON.")
    if isinstance(dependencies, str):
        dependencies = [dependencies]
    normalized = []
    for dependency in dependencies or ():
        if not isinstance(dependency, str) or not dependency.strip():
            raise ValueError("Cada dependência precisa ser texto não vazio.")
        value = dependency.strip()
        if value not in normalized:
            normalized.append(value)
    _validate_dependencies(feature_id, normalized, feature_store.list_definitions(db))
    return feature_store.register_definition(
        db, feature_id, name, algorithm_version, parameters, normalized, family=family, formula=formula, unit=unit,
        warmup_count=warmup_count, warmup_unit=warmup_unit, source_id=source_id, timeframe=timeframe,
        availability_rule=availability_rule, value_type=value_type,
    )


def list_features(db, include_versions=False):
    definitions = feature_store.list_definitions(db)
    if include_versions:
        return definitions
    latest = _latest_by_id(definitions)
    return sorted(latest.values(), key=lambda item: item["feature_id"])


def get_feature(db, feature_id, algorithm_version=None):
    matches = [item for item in feature_store.list_definitions(db) if item["feature_id"] == feature_id]
    if algorithm_version is not None:
        matches = [item for item in matches if item["algorithm_version"] == algorithm_version]
    if not matches:
        suffix = f"@{algorithm_version}" if algorithm_version else ""
        raise ValueError(f"Característica não registrada: {feature_id}{suffix}.")
    if algorithm_version is None:
        matches = [_latest_by_id(matches)[feature_id]]
    return matches[0]


def affected_by(db, changed_dependencies):
    """Retorna as definições atuais que precisam ser recalculadas após uma mudança.

    Os itens de ``changed_dependencies`` podem ser fontes como ``bars:M1`` ou
    chaves ``feature:<id>``. O retorno é topológico e não inclui características
    sem relação com as fontes alteradas.
    """
    if isinstance(changed_dependencies, str):
        changed_dependencies = [changed_dependencies]
    changed = {str(item).strip() for item in changed_dependencies or () if str(item).strip()}
    if not changed:
        return []
    latest = _latest_by_id(feature_store.list_definitions(db))
    remaining = dict(latest)
    affected = []
    while True:
        found = [feature_id for feature_id, definition in sorted(remaining.items())
                 if any(_dependency_matches(dependency, item)
                        for dependency in definition["dependencies"] for item in changed)]
        if not found:
            break
        for feature_id in found:
            definition = remaining.pop(feature_id)
            affected.append(definition)
            changed.add(f"feature:{feature_id}")
    return affected


def recalculate_affected(db, changed_dependencies, recalculators):
    """Executa somente os recálculos afetados, em ordem de dependência.

    ``recalculators`` mapeia ``feature_id`` para uma função ``(db, definition)``.
    A lista completa é validada antes da primeira chamada, para evitar um recálculo
    parcial causado por um handler ausente.
    """
    affected = affected_by(db, changed_dependencies)
    missing = [item["feature_id"] for item in affected if item["feature_id"] not in recalculators]
    if missing:
        raise ValueError("Faltam recálculos registrados para: " + ", ".join(missing))
    return [
        {"feature_id": item["feature_id"], "algorithm_version": item["algorithm_version"],
         "result": recalculators[item["feature_id"]](db, item)}
        for item in affected
    ]
