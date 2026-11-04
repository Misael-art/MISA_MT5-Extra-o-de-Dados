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


def affected_by(db, changed_dependencies, *, definition_filter=None, definitions=None):
    """Retorna as definições atuais que precisam ser recalculadas após uma mudança.

    Os itens de ``changed_dependencies`` podem ser fontes como ``bars:M1`` ou
    chaves ``feature:<id>``. O retorno é topológico e não inclui características
    sem relação com as fontes alteradas.
    ``definition_filter`` restringe as versões registradas antes de escolher a
    atual por ID; sem filtro, a seleção global anterior permanece igual.
    """
    if isinstance(changed_dependencies, str):
        changed_dependencies = [changed_dependencies]
    changed = {str(item).strip() for item in changed_dependencies or () if str(item).strip()}
    if not changed:
        return []
    definitions = feature_store.list_definitions(db) if definitions is None else list(definitions)
    if definition_filter is not None:
        definitions = [item for item in definitions if definition_filter(item)]
    latest = _latest_by_id(definitions)
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
    # A fonte pode atingir pai e filho diretamente na mesma rodada. A seleção
    # transitiva acima não garante que o pai tenha sido calculado primeiro.
    pending = {item["feature_id"]: item for item in affected}
    ordered = []
    while pending:
        ready = [feature_id for feature_id, definition in sorted(pending.items())
                 if not any(_feature_dependency(item) in pending
                            for item in definition["dependencies"])]
        if not ready:
            raise ValueError("Dependências de características não podem formar ciclos.")
        for feature_id in ready:
            ordered.append(pending.pop(feature_id))
    return ordered


def recalculate_affected(db, changed_dependencies, recalculators, *, definition_filter=None, definitions=None):
    """Executa somente os recálculos afetados, em ordem de dependência.

    ``recalculators`` mapeia ``feature_id`` para uma função ``(db, definition)``.
    A lista completa é validada antes da primeira chamada, para evitar um recálculo
    parcial causado por um handler ausente.
    """
    affected = affected_by(db, changed_dependencies, definition_filter=definition_filter, definitions=definitions)
    missing = [item["feature_id"] for item in affected if item["feature_id"] not in recalculators]
    if missing:
        raise ValueError("Faltam recálculos registrados para: " + ", ".join(missing))
    return [
        {"feature_id": item["feature_id"], "algorithm_version": item["algorithm_version"],
         "result": recalculators[item["feature_id"]](db, item)}
        for item in affected
    ]


# Operações fechadas e tipadas, despachadas pelo grafo existente; nenhuma expressão é executada.
COMPOSITION_VERSION = 2
RECIPE_VERSION = 1
MAX_COMPOSITION_NODES = 32


def _difference(values):
    return values[0] - values[1]


def _absolute(values):
    return abs(values[0])


_COMPOSITE_OPERATORS = {
    "difference":dict(arity=2, formula="left - right", evaluate=_difference),
    "absolute":dict(arity=1, formula="abs(input)", evaluate=_absolute),
}
_COMPOSITE_UNITS = frozenset({"fraction", "price_units"})
_COMPOSITE_AVAILABILITY = "max_declared_parent_availability_at_same_observation"


def composition_operations():
    """Descoberta dos operadores fixos, também usada pelas escolhas da CLI."""
    return tuple(_COMPOSITE_OPERATORS)


def _composite_inputs(inputs, operation):
    if not isinstance(operation, str) or operation not in _COMPOSITE_OPERATORS:
        raise ValueError("Operação composta desconhecida; use difference ou absolute.")
    arity = _COMPOSITE_OPERATORS[operation]["arity"]
    if not isinstance(inputs, (list, tuple)) or len(inputs) != arity:
        raise ValueError(f"{operation} exige {arity} entradas ID/versão em ordem explícita.")
    result = []
    for pair in inputs:
        if (not isinstance(pair, (list, tuple)) or len(pair) != 2 or
                not all(isinstance(value, str) and value.strip() for value in pair)):
            raise ValueError("Cada entrada exige ID/versão explícitos (feature_id e algorithm_version).")
        result.append(tuple(value.strip() for value in pair))
    return result


def _composite_unit(specs):
    if any(spec.get("output_kind") != "number" for spec in specs):
        raise ValueError("Compostos aritméticos exigem entradas numéricas; categorias/booleanos não são números.")
    if any(spec.get("family") == "structure" or spec["feature_id"].startswith("structure.") for spec in specs):
        raise ValueError("Pivôs armazenados exigem qualificação de identidade/confirmação de eventos antes de compor.")
    raw_units = [spec.get("unit") for spec in specs]
    if any(not isinstance(unit, str) or not unit for unit in raw_units):
        raise ValueError("Unidade ausente; a aritmética exige fraction ou price_units declarados.")
    units = set(raw_units)
    if len(units) != 1:
        raise ValueError("Unidades incompatíveis; não há conversão implícita entre entradas.")
    unit = next(iter(units))
    if unit not in _COMPOSITE_UNITS:
        raise ValueError("A aritmética inicial aceita somente fraction ou price_units declarados; unidade ausente "
                         "ou escala restrita exige outro contrato, sem inferir domínio.")
    return unit


def register_composite(db, feature_id, name, algorithm_version, *, operation, inputs):
    """Registra uma receita imutável sobre versões armazenadas, sem calcular/materializar valores."""
    from mt5_extracao.analytics import query_service

    pairs = _composite_inputs(inputs, operation)
    known = {(s["feature_id"], s["algorithm_version"]):s for s in query_service.definition_catalog(db)}
    if any(pair not in known or known[pair]["registered_definition"] is None for pair in pairs):
        raise ValueError("Pai/versão não registrado na base; registre/materialize essa versão antes de compor.")
    for pair in pairs:
        _frozen_recipe(known[pair])
    unit = _composite_unit([known[pair] for pair in pairs])
    recipe = dict(schema_version=RECIPE_VERSION, operation=operation, operator_version=1,
                  inputs=[dict(feature_id=ident, algorithm_version=version) for ident, version in pairs])
    return register(db, feature_id, name, algorithm_version, parameters=dict(composition=recipe),
        dependencies=["feature:" + ident for ident, _version in pairs], family="composite",
        formula=_COMPOSITE_OPERATORS[operation]["formula"], unit=unit, value_type="NUMBER", warmup_count=0,
        warmup_unit="bars", source_id="frozen_feature_context",
        availability_rule=_COMPOSITE_AVAILABILITY)


def _frozen_recipe(spec):
    if spec.get("family") != "composite":
        return None
    parameters = spec.get("parameters")
    if (not isinstance(parameters, dict) or set(parameters) != {"composition"}
            or spec.get("availability_rule") != _COMPOSITE_AVAILABILITY
            or spec.get("source_id") != "frozen_feature_context" or type(spec.get("warmup_count")) is not int
            or spec.get("warmup_count") != 0
            or spec.get("warmup_unit") != "bars"):
        raise ValueError("Metadados divergem do contrato composto congelado.")
    recipe = parameters["composition"]
    if (not isinstance(recipe, dict) or set(recipe) != {"schema_version", "operation", "operator_version", "inputs"}
            or type(recipe["schema_version"]) is not int or recipe["schema_version"] != RECIPE_VERSION
            or type(recipe["operator_version"]) is not int or recipe["operator_version"] != 1):
        raise ValueError(f"Contrato composto não suportado: {spec['feature_id']}@{spec['algorithm_version']}.")
    raw = recipe["inputs"]
    if (not isinstance(raw, list) or not all(isinstance(item, dict) and
            set(item) == {"feature_id", "algorithm_version"} for item in raw)):
        raise ValueError("Entradas compostas exigem IDs e versões explícitos.")
    pairs = _composite_inputs([(item["feature_id"], item["algorithm_version"]) for item in raw], recipe["operation"])
    if (spec["output_kind"] != "number" or spec["formula"] != _COMPOSITE_OPERATORS[recipe["operation"]]["formula"]
            or set(spec["dependencies"]) != {"feature:" + ident for ident, _version in pairs}):
        raise ValueError("Receita, tipo ou dependências divergem do contrato composto congelado.")
    return recipe["operation"], pairs


def _composition_components(operations):
    """Uma imagem dos componentes usados; operadores não selecionados ficam fora."""
    import hashlib
    import platform
    import numpy as np
    import pandas as pd
    from mt5_extracao import timeframes
    from mt5_extracao.analytics import datasets, indicator_context, indicator_materializer, instruments

    functions = {}
    for operation in operations:
        name = getattr(_COMPOSITE_OPERATORS[operation]["evaluate"], "__name__", None)
        if not isinstance(name, str) or not name.isidentifier():
            raise ValueError("Operador exige função Python nomeada e componente de implementação verificável.")
        functions[operation] = name
    source_components = indicator_materializer._source_components
    own = source_components(__file__, (
        "_composition_components", "_composition_contract", "compose_snapshot", "_frozen_recipe", "_composite_unit",
        "_composite_inputs", "affected_by", "recalculate_affected", "_latest_by_id", "_dependency_matches",
        "_feature_dependency", *sorted(set(functions.values()))))
    operator_hashes = {operation: own[name] for operation, name in functions.items()}
    common = dict(scheme="frozen_node_components_v1", recipe_version=RECIPE_VERSION,
        sources=dict(graph={name: digest for name, digest in own.items() if name not in functions.values()},
            context=source_components(indicator_context.__file__, (
                "from_snapshot", "_context_rows", "_ids", "_version", "_state", "_value", "records")),
            store=source_components(feature_store.__file__, ("asof_join", "_utc_timestamp", "_canonical_json")),
            snapshot=source_components(datasets.__file__, ("_feature_selection", "_window_utc")),
            snapshot_access=indicator_materializer._object_source_component(datasets.Snapshot),
            broker_time=source_components(instruments.__file__, ("broker_utc",)),
            timeframe=source_components(timeframes.__file__, ("parse", "_norm")),
            source_reader=source_components(indicator_materializer.__file__, (
                "_source_components", "_object_source_component"))),
        python=platform.python_version(), pandas=pd.__version__, numpy=np.__version__)
    encoded = feature_store._canonical_json(common, "composition implementation")
    return hashlib.sha256(encoded.encode()).hexdigest(), operator_hashes


def _composition_contract(spec):
    """Contrato matemático congelado; anotações e nome não versionam a aritmética."""
    return {key: spec.get(key) for key in ("feature_id", "algorithm_version", "family", "formula", "parameters",
        "dependencies", "unit", "value_type", "output_kind", "warmup_count", "warmup_unit", "source_id",
        "timeframe", "availability_rule")}


def compose_snapshot(snapshot, decisions, *, targets):
    """Avaliação exploratória de receitas congeladas, usando o mesmo executor de dependências."""
    import hashlib
    import json
    import numpy as np
    import pandas as pd
    from mt5_extracao.analytics import datasets, indicator_context, query_service

    if not isinstance(snapshot, datasets.Snapshot):
        raise ValueError("Informe um snapshot canônico carregado ou capturado.")
    pairs = datasets._feature_selection(targets)
    if not pairs:
        raise ValueError("Escolha ao menos uma composição ID/versão congelada.")
    if (not isinstance(decisions, pd.DataFrame) or not decisions.columns.is_unique
            or not all(isinstance(col, str) for col in decisions.columns)):
        raise ValueError("Informe decisões tabulares com colunas textuais únicas e decision_time com fuso.")
    known = {(s["feature_id"], s["algorithm_version"]):s for s in snapshot.feature_definitions}
    chosen, visiting, versions, recipes = {}, set(), {}, {}

    def collect(pair):
        if pair in visiting:
            raise ValueError("Dependências de características não podem formar ciclos.")
        if pair in chosen:
            return
        if len(chosen) + len(visiting) >= MAX_COMPOSITION_NODES:
            raise ValueError(f"Composição excede {MAX_COMPOSITION_NODES} nós; separe o estudo.")
        if pair not in known:
            raise ValueError(f"Pai/versão não congelado: {pair[0]}@{pair[1]}. Capture todas as entradas.")
        ident, version = pair
        if ident in versions and versions[ident] != version:
            raise ValueError("Duas versões do mesmo ID no grafo; separe as avaliações para não misturar contextos.")
        versions[ident] = version
        visiting.add(pair)
        spec = known[pair]
        recipe = _frozen_recipe(spec)
        if recipe:
            operation, parents = recipe
            for parent in parents:
                collect(parent)
            if spec["unit"] != _composite_unit([known[parent] for parent in parents]):
                raise ValueError("Unidade da composição diverge das entradas congeladas.")
            recipes[ident] = (operation, parents)
        else:
            _composite_unit([spec])
        visiting.remove(pair)
        chosen[pair] = spec

    for pair in pairs:
        collect(pair)
        if pair[0] not in recipes:
            raise ValueError("O alvo precisa ser uma receita composta congelada; use context para fatos armazenados.")
    if len(decisions) * len(chosen) > query_service.MAX_SELECTION_ROWS:
        raise ValueError(f"Composição excede {query_service.MAX_SELECTION_ROWS} pares nó/decisão; reduza a seleção.")
    if any(any(str(col).startswith(ident + "::") for ident in versions) for col in decisions.columns):
        raise ValueError("Decisões já contêm colunas reservadas dos indicadores; remova-as antes de compor.")
    common, operator_hashes = _composition_components({operation for operation, _parents in recipes.values()})
    computation = {}
    for pair, spec in chosen.items():
        ident = pair[0]
        contract = _composition_contract(spec)
        if ident in recipes:
            operation, parents = recipes[ident]
            operator = _COMPOSITE_OPERATORS[operation]
            payload = dict(scheme="frozen_node_components_v1", shared=common, contract=contract,
                operator=dict(name=operation, arity=operator["arity"], formula=operator["formula"],
                              implementation_sha256=operator_hashes[operation]),
                parents=[dict(feature_id=parent[0], algorithm_version=parent[1],
                              computation_sha256=computation[parent[0]]) for parent in parents])
        else:
            payload = dict(scheme="frozen_parent_contract_v1", contract=contract)
        computation[ident] = hashlib.sha256(feature_store._canonical_json(payload, "computation").encode()).hexdigest()
    leaves = [pair for pair in chosen if pair[0] not in recipes]
    groups = {}
    for ident, version in leaves:
        groups.setdefault(version, []).append(ident)
    base, columns, cached = None, {}, {}
    for version, ids in groups.items():
        frame = indicator_context.from_snapshot(snapshot, decisions, feature_ids=ids, algorithm_version=version)
        if base is None:
            base = frame[[col for col in frame.columns if not any(col.startswith(ident + "::") for ident in ids)]]
        for ident in ids:
            prefix = ident + "::"
            data = {col[len(prefix):]:frame[col] for col in frame if col.startswith(prefix)}
            cached[ident] = indicator_context.records(pd.DataFrame(data))
            columns.update({prefix + key:value for key, value in data.items()})

    def handler(_db, spec):
        ident = spec["feature_id"]
        operation, parents = recipes[ident]
        rows = []
        for position in range(len(decisions)):
            inputs = [cached[parent[0]][position] for parent in parents]
            valid = all(row["state"] == "AVAILABLE" for row in inputs)
            same_observation = len({row["observation_time"] for row in inputs}) == 1
            reason = ("PARENT_UNAVAILABLE: " + ", ".join(f"{p[0]}@{p[1]}={row['state']}" for p, row in zip(parents, inputs))
                      if not valid else "PARENT_OBSERVATIONS_DIFFER" if not same_observation else None)
            value = None
            if reason is None:
                numbers = [float(row["value"]) for row in inputs]
                value = _COMPOSITE_OPERATORS[operation]["evaluate"](numbers)
                if not np.isfinite(value):
                    reason, value = "NON_FINITE_RESULT", None
            provenance = dict(basis="frozen_context_exploration_not_materialized", operation=operation, operator_version=1,
                unit=spec["unit"], signature_scheme="frozen_node_components_v1", computation_sha256=computation[ident],
                parents=[dict(feature_id=p[0], algorithm_version=p[1], observation_time=row["observation_time"],
                              available_at_utc=row["available_at_utc"], revision=row["revision"],
                              source_revision=row["source_revision"], state=row["state"], value=row["value"], unit=row["unit"])
                         for p, row in zip(parents, inputs)])
            encoded = json.dumps(provenance, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
            rows.append(dict(value=value, state="AVAILABLE" if reason is None else "UNAVAILABLE", reason=reason,
                algorithm_version=spec["algorithm_version"], unit=spec["unit"],
                parameters=json.dumps(spec["parameters"], sort_keys=True, separators=(",", ":")),
                observation_time=inputs[0]["observation_time"] if reason is None else None,
                available_at_utc=max(row["available_at_utc"] for row in inputs) if reason is None else None,
                revision=None, source_revision="context:" + hashlib.sha256(encoded.encode()).hexdigest(),
                computation_sha256=computation[ident], metadata_json=encoded))
        cached[ident] = rows
        frame = pd.DataFrame(rows, columns=("value", "state", "reason", "algorithm_version", "unit", "parameters",
            "observation_time", "available_at_utc", "revision", "source_revision", "computation_sha256", "metadata_json"))
        columns.update({ident + "::" + key:frame[key] for key in frame})
        return dict(rows=len(rows), available=sum(row["state"] == "AVAILABLE" for row in rows))

    executed = recalculate_affected(None, ["feature:" + ident for ident, _version in leaves],
        {ident:handler for ident in recipes}, definitions=[s for s in chosen.values() if s["feature_id"] in recipes])
    frame = pd.concat([base.reset_index(drop=True), pd.DataFrame(columns, index=range(len(decisions)))], axis=1)
    implementation = hashlib.sha256(feature_store._canonical_json(
        dict(scheme="frozen_node_components_v1", nodes={ident: computation[ident] for ident in recipes}),
        "implementation").encode()).hexdigest()
    window_utc = [stamp.isoformat() for stamp in snapshot.feature_window_utc]
    evaluation = hashlib.sha256(feature_store._canonical_json(dict(manifest_id=snapshot.manifest_id,
        targets=pairs, decision_times=[row["decision_time"] for row in indicator_context.records(base)],
        implementation=implementation, composition_version=COMPOSITION_VERSION,
        window_utc=window_utc), "evaluation").encode()).hexdigest()
    return dict(composition_version=COMPOSITION_VERSION, snapshot_manifest_id=snapshot.manifest_id,
        signature_scheme="frozen_node_components_v1", implementation_sha256=implementation, evaluation_id=evaluation,
        window_utc=window_utc,
        targets=[list(pair) for pair in pairs], executed=executed, context=indicator_context.records(frame),
        definitions=[dict(feature_id=s["feature_id"], algorithm_version=s["algorithm_version"], unit=s["unit"],
                          parameters=s["parameters"], computation_sha256=computation[s["feature_id"]])
                     for s in chosen.values()],
        qualification=dict(exploratory=True, materialized=False, operational_acceptance=False,
                           historical_source_certification=False, availability_basis="declared_frozen_parent_facts"),
        limitations=["Cálculo em contexto congelado; não é fato materializado, sinal recebido ou ordem.",
                     "Entradas precisam ter a mesma observação e unidade declarada; ausências não viram zero."])
