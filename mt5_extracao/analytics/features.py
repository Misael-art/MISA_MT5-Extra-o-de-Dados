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
        "_composition_components", "_composition_contract", "compose_snapshot", "_composition_plan", "_compose_plan",
        "_frozen_recipe", "_composite_unit",
        "_composite_inputs", "affected_by", "recalculate_affected", "_latest_by_id", "_dependency_matches",
        "_feature_dependency", *sorted(set(functions.values()))))
    operator_hashes = {operation: own[name] for operation, name in functions.items()}
    common = dict(scheme="frozen_node_components_v1", recipe_version=RECIPE_VERSION,
        sources=dict(graph={name: digest for name, digest in own.items() if name not in functions.values()},
            context=source_components(indicator_context.__file__, (
                "from_snapshot", "_snapshot_context", "_snapshot_scope", "_snapshot_observations", "_context_rows",
                "_ids", "_version", "_state", "_value", "records")),
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


def compose_snapshot(snapshot, decisions, *, targets, observation_time=None):
    """Avaliação exploratória de receitas congeladas, usando o mesmo executor de dependências."""
    return _compose_plan(snapshot, decisions, _composition_plan(snapshot, targets), observation_time=observation_time)


def _composition_plan(snapshot, targets):
    """Valida a closure e identifica cada nó uma vez por avaliação/job, sem cache retido."""
    import hashlib
    from mt5_extracao.analytics import datasets

    if not isinstance(snapshot, datasets.Snapshot):
        raise ValueError("Informe um snapshot canônico carregado ou capturado.")
    pairs = datasets._feature_selection(targets)
    if not pairs:
        raise ValueError("Escolha ao menos uma composição ID/versão congelada.")
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
    return dict(pairs=pairs, chosen=chosen, versions=versions, recipes=recipes, computation=computation,
                leaves=leaves, definitions=[chosen[pair] for pair in leaves],
                operator_runtime={operation: (operator["evaluate"], operator["evaluate"].__code__)
                    for operation, operator in _COMPOSITE_OPERATORS.items()
                    if operation in {item[0] for item in recipes.values()}})


def _compose_plan(snapshot, decisions, plan, *, observation_time=None, read_context=None):
    """Executa o plano pelo executor existente; contexto opcional usa a mesma imagem congelada."""
    import hashlib
    import json
    import numpy as np
    import pandas as pd
    from mt5_extracao.analytics import indicator_context, query_service

    pairs, chosen, versions = plan["pairs"], plan["chosen"], plan["versions"]
    recipes, computation, leaves = plan["recipes"], plan["computation"], plan["leaves"]
    if (not isinstance(decisions, pd.DataFrame) or not decisions.columns.is_unique
            or not all(isinstance(col, str) for col in decisions.columns)):
        raise ValueError("Informe decisões tabulares com colunas textuais únicas e decision_time com fuso.")
    if len(decisions) * len(chosen) > query_service.MAX_SELECTION_ROWS:
        raise ValueError(f"Composição excede {query_service.MAX_SELECTION_ROWS} pares nó/decisão; reduza a seleção.")
    if any(any(str(col).startswith(ident + "::") for ident in versions) for col in decisions.columns):
        raise ValueError("Decisões já contêm colunas reservadas dos indicadores; remova-as antes de compor.")
    groups = {}
    for ident, version in leaves:
        groups.setdefault(version, []).append(ident)
    base, columns, cached, scope = None, {}, {}, None
    for version, ids in groups.items():
        frame = (read_context(decisions, ids, version, observation_time) if read_context is not None else
                 indicator_context.from_snapshot(snapshot, decisions, feature_ids=ids, algorithm_version=version,
                                                observation_time=observation_time))
        if base is None:
            base = frame[[col for col in frame.columns if not any(col.startswith(ident + "::") for ident in ids)]]
            scope = frame.attrs["snapshot_context_scope"]
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
                              available_at_utc=row["available_at_utc"], valid_until_utc=row.get("valid_until_utc"),
                              revision=row["revision"],
                              source_revision=row["source_revision"], state=row["state"], value=row["value"], unit=row["unit"])
                         for p, row in zip(parents, inputs)])
            encoded = json.dumps(provenance, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
            deadlines = [row["valid_until_utc"] for row in inputs if row.get("valid_until_utc") is not None]
            rows.append(dict(value=value, state="AVAILABLE" if reason is None else "UNAVAILABLE", reason=reason,
                algorithm_version=spec["algorithm_version"], unit=spec["unit"],
                parameters=json.dumps(spec["parameters"], sort_keys=True, separators=(",", ":")),
                observation_time=inputs[0]["observation_time"] if reason is None else None,
                available_at_utc=max((row["available_at_utc"] for row in inputs), key=pd.Timestamp) if reason is None else None,
                valid_until_utc=min(deadlines, key=pd.Timestamp) if reason is None and deadlines else None,
                revision=None, source_revision="context:" + hashlib.sha256(encoded.encode()).hexdigest(),
                computation_sha256=computation[ident], metadata_json=encoded))
        cached[ident] = rows
        frame = pd.DataFrame(rows, columns=("value", "state", "reason", "algorithm_version", "unit", "parameters",
            "observation_time", "available_at_utc", "valid_until_utc", "revision", "source_revision", "computation_sha256", "metadata_json"))
        columns.update({ident + "::" + key:frame[key] for key in frame})
        return dict(rows=len(rows), available=sum(row["state"] == "AVAILABLE" for row in rows))

    executed = recalculate_affected(None, ["feature:" + ident for ident, _version in leaves],
        {ident:handler for ident in recipes}, definitions=[s for s in chosen.values() if s["feature_id"] in recipes])
    frame = pd.concat([base.reset_index(drop=True), pd.DataFrame(columns, index=range(len(decisions)))], axis=1)
    implementation = hashlib.sha256(feature_store._canonical_json(
        dict(scheme="frozen_node_components_v1", nodes={ident: computation[ident] for ident in recipes}),
        "implementation").encode()).hexdigest()
    window_utc = scope["window_utc"]
    evaluation = hashlib.sha256(feature_store._canonical_json(dict(manifest_id=snapshot.manifest_id,
        targets=pairs, decision_times=[row["decision_time"] for row in indicator_context.records(base)],
        implementation=implementation, composition_version=COMPOSITION_VERSION,
        window_utc=window_utc, context_scope=scope), "evaluation").encode()).hexdigest()
    return dict(composition_version=COMPOSITION_VERSION, snapshot_manifest_id=snapshot.manifest_id,
        signature_scheme="frozen_node_components_v1", implementation_sha256=implementation, evaluation_id=evaluation,
        window_utc=window_utc, context_scope=scope,
        targets=[list(pair) for pair in pairs], executed=executed, context=indicator_context.records(frame),
        definitions=[dict(feature_id=s["feature_id"], algorithm_version=s["algorithm_version"], unit=s["unit"],
                          parameters=s["parameters"], computation_sha256=computation[s["feature_id"]])
                     for s in chosen.values()],
        qualification=dict(exploratory=True, materialized=False, operational_acceptance=False,
                           historical_source_certification=False, availability_basis="declared_frozen_parent_facts"),
        limitations=["Cálculo em contexto congelado; não é fato materializado, sinal recebido ou ordem.",
                     "Entradas precisam ter a mesma observação e unidade declarada; ausências não viram zero."])


def _materialization_signature():
    import hashlib
    from mt5_extracao.analytics import datasets, indicator_materializer
    source = indicator_materializer._source_components
    components = dict(scheme="frozen_materialization_v1", writer=source(__file__, (
        "_materialization_signature", "_materialization_definitions", "_materialization_events",
        "_materialization_records", "_materialization_batches", "_materialization_state", "_materialization_record",
        "_materialization_scope", "_materialization_history", "_materialization_guard",
        "_materialization_inputs", "_materialization_verify_inputs", "materialize_snapshot")),
        store=source(feature_store.__file__, ("_value_record", "_normalize_value", "_canonical_json", "_utc_text",
            "_utc_timestamp", "_validate_declared_value_type", "save_values")),
        origin=source(datasets.__file__, ("identity", "_slots", "bind")))
    return hashlib.sha256(feature_store._canonical_json(components, "materialization").encode()).hexdigest()


def _materialization_definitions(plan, projection):
    import hashlib
    outputs = {}
    for (ident, version), spec in plan["chosen"].items():
        if ident not in plan["recipes"]:
            continue
        digest = hashlib.sha256(feature_store._canonical_json(dict(
            computation=plan["computation"][ident], projection=projection), "materialized node").encode()).hexdigest()
        outputs[ident] = dict(feature_id=ident, name=spec["name"], algorithm_version=version + "+mat." + digest[:16],
            parameters=dict(materialization=dict(schema_version=1, recipe_version=version,
                recipe=spec["parameters"]["composition"], computation_sha256=plan["computation"][ident],
                projection_sha256=projection, materialization_sha256=digest,
                parents=[dict(feature_id=parent, algorithm_version=outputs[parent]["algorithm_version"]
                    if parent in outputs else parent_version) for parent, parent_version in plan["recipes"][ident][1]])),
            dependencies=spec["dependencies"], family="materialized_composite", formula=spec["formula"],
            unit=spec["unit"], value_type="NUMBER", warmup_count=0, warmup_unit="bars",
            source_id="frozen_feature_materialization", timeframe=spec["timeframe"],
            availability_rule="declared_parent_revision_or_active_expiry")
    return outputs


def _materialization_events(frame, leaves, observation):
    import pandas as pd
    selected = frame.loc[[(ident, version) in leaves for ident, version in
                         zip(frame.feature_id, frame.algorithm_version)]]
    events = set()
    for _key, group in selected.groupby(["feature_id", "algorithm_version"], sort=True):
        effective = group.sort_values(["available_at_utc", "observed_at_utc", "revision"]).drop_duplicates(
            "available_at_utc", keep="last")
        rows = list(effective.itertuples(index=False))
        for index, row in enumerate(rows):
            events.add(row.available_at_utc)
            if pd.notna(row.valid_until_utc):
                # Uma revisão recebida antes do vencimento substitui esse prazo.
                if index + 1 == len(rows) or row.valid_until_utc < rows[index + 1].available_at_utc:
                    events.add(row.valid_until_utc)
            for stamp in (observation, row.available_at_utc, row.observed_at_utc, row.valid_until_utc):
                if pd.notna(stamp) and pd.Timestamp(feature_store._utc_text(stamp, "evento")) != stamp:
                    raise ValueError("O store exige microssegundos exatos; não arredonde os eventos congelados.")
            if row.available_at_utc < observation:
                raise ValueError("Recibo anterior à observação exige outro contrato temporal; não invente um evento.")
    return sorted(events)


def _materialization_state(value, state, reason, valid_until, context):
    import hashlib
    import pandas as pd
    payload = dict(value=value, state=state, reason=reason, context=context,
                   valid_until_utc=None if valid_until is None else pd.Timestamp(valid_until).isoformat())
    return hashlib.sha256(feature_store._canonical_json(payload, "materialized state").encode()).hexdigest()


def _materialization_record(snapshot_info, definition, observation, event, value, state, reason, until, context):
    import hashlib
    manifest = snapshot_info["manifest"]
    contract = definition["parameters"]["materialization"]
    state_hash = _materialization_state(value, state, reason, until, context)
    source = dict(namespace=manifest["source"]["namespace"], symbol=manifest["symbol"], timeframe=manifest["timeframe"],
        feature_id=definition["feature_id"], algorithm_version=definition["algorithm_version"],
        observation_time=feature_store._utc_text(observation, "observation"),
        event_time=feature_store._utc_text(event, "event"), state_sha256=state_hash)
    revision = "frozen_composite:" + hashlib.sha256(feature_store._canonical_json(source, "source").encode()).hexdigest()
    return dict(symbol=manifest["symbol"], timeframe=manifest["timeframe"], feature_id=definition["feature_id"],
        algorithm_version=definition["algorithm_version"], observation_time=observation, value=value,
        available_at=event, observed_at=event, valid_until=until, source_revision=revision,
        quality="VALID" if state == "AVAILABLE" else "UNAVAILABLE",
        metadata=dict(basis="materialized_frozen_context", reconstructed=True, state=state, reason=reason,
            source_namespace=source["namespace"], context=context,
            materialization=dict(contract, source_manifest_id=snapshot_info["manifest_id"], state_sha256=state_hash)))


def _materialization_batches(history, leaves, node_count, as_of, guard):
    """Eventos em ordem observação/recebimento; o lote de cálculo não altera o lote SQL."""
    from mt5_extracao.analytics import query_service
    cells, observations, decisions = 0, [], []
    for observation, frame in history.groupby("observation_time", sort=True):
        guard()
        events = [event for event in _materialization_events(frame, leaves, observation) if event <= as_of]
        cells += len(events) * node_count
        if cells > query_service.MAX_SELECTION_ROWS:
            raise ValueError(f"Materialização excede {query_service.MAX_SELECTION_ROWS} pares nó/evento; reduza a janela.")
        for event in events:
            observations.append(observation)
            decisions.append(event)
            if len(decisions) == 256:
                yield observations, decisions
                observations, decisions = [], []
    if decisions:
        yield observations, decisions


def _materialization_records(snapshot, plan, outputs, history, as_of, guard):
    import json
    import pandas as pd
    from mt5_extracao.analytics import indicator_context
    manifest, window = snapshot.manifest, snapshot.feature_window_utc
    snapshot_info = dict(manifest=manifest, manifest_id=snapshot.manifest_id)
    previous, current_observation = {}, None
    for observations, decisions in _materialization_batches(history, set(plan["leaves"]), len(plan["chosen"]), as_of, guard):
        guard()
        frame = history.loc[history.observation_time.isin(observations)]
        def context(decisions, ids, version, selected_observation):
            return indicator_context._snapshot_context(decisions, manifest_id=snapshot.manifest_id, manifest=manifest,
                definitions=plan["definitions"], history=frame, window=window, feature_ids=ids,
                algorithm_version=version, observation_times=observations)
        result = _compose_plan(snapshot, pd.DataFrame(dict(decision_time=decisions)), plan, read_context=context)
        guard()
        for observation, row in zip(observations, result["context"]):
            if observation != current_observation:
                previous, current_observation = {}, observation
            for ident, definition in outputs.items():
                prefix = ident + "::"
                record = _materialization_record(snapshot_info, definition, observation, row["decision_time"],
                    row[prefix + "value"], row[prefix + "state"], row[prefix + "reason"],
                    row[prefix + "valid_until_utc"], json.loads(row[prefix + "metadata_json"]))
                state = record["metadata"]["materialization"]["state_sha256"]
                if previous.get(ident) != state:
                    previous[ident] = state
                    yield record


def _materialization_scope(snapshot, outputs):
    manifest = snapshot.manifest
    first, last = snapshot.feature_window_utc
    params = dict(symbol=manifest["symbol"], tf=manifest["timeframe"],
                  start=feature_store._utc_text(first, "start"), end=feature_store._utc_text(last, "end"))
    pairs = []
    for index, definition in enumerate(outputs.values()):
        pairs.append(f"(feature_id=:id{index} AND algorithm_version=:version{index})")
        params.update({f"id{index}":definition["feature_id"], f"version{index}":definition["algorithm_version"]})
    selection = "(" + " OR ".join(pairs) + ")"
    return dict(params=params, pairs=selection, where="symbol=:symbol AND tf=:tf AND observation_time>=:start "
                "AND observation_time<:end AND " + selection)


def _materialization_history(db, snapshot, outputs, scope, identity, guard):
    import json
    from sqlalchemy import text
    from mt5_extracao.analytics import datasets, indicator_materializer, query_service
    known, latest, count, revisions, origins = {}, {}, 0, {}, {}
    snapshot_info = dict(manifest=snapshot.manifest, manifest_id=snapshot.manifest_id)
    with db.consistent_read() as conn:
        if datasets.identity(db, connection=conn) != identity:
            raise indicator_materializer.SourceChanged("Identidade da base mudou durante a preparação; reabra o job.")
        rows = conn.execute(text("SELECT " + ",".join(feature_store.VALUE_COLUMNS) + ",value_hash FROM " +
            feature_store.VALUES_TABLE + " WHERE " + scope["where"] +
            " ORDER BY feature_id,observation_time,revision LIMIT :limit"),
            dict(scope["params"], limit=query_service.MAX_SELECTION_ROWS + 1)).mappings().yield_per(256)
        for row in rows:
            count += 1
            if count % 256 == 1:
                guard()
            if count > query_service.MAX_SELECTION_ROWS:
                raise ValueError("Histórico composto excede o limite da seleção; reduza a janela.")
            metadata = json.loads(row["metadata_json"])
            contract = metadata.get("materialization", {})
            definition = outputs[row["feature_id"]]
            for name, expected in definition["parameters"]["materialization"].items():
                if contract.get(name) != expected:
                    raise ValueError("Histórico composto diverge do contrato materializado; reconcilie a base.")
            origin = contract.get("source_manifest_id")
            if not isinstance(origin, str) or len(origin) != 64 or any(c not in "0123456789abcdef" for c in origin):
                raise ValueError("Origem do fato composto inválida; restaure seu manifesto.")
            value = row["numeric_value"]
            until = row["valid_until_utc"]
            rebuilt = _materialization_record(snapshot_info, definition, row["observation_time"], row["available_at_utc"],
                value, metadata.get("state"), metadata.get("reason"), until, metadata.get("context"))
            rebuilt["metadata"]["materialization"]["source_manifest_id"] = origin
            normalized = feature_store._value_record(**rebuilt)
            # Disponibilidade/observação também são vinculadas pelo source_revision.
            if (normalized["value_hash"] != row["value_hash"] or normalized["source_revision"] != row["source_revision"]
                    or normalized["metadata_json"] != row["metadata_json"]
                    or normalized["observed_at_utc"] != row["observed_at_utc"]
                    or normalized["quality"] != row["quality"] or row["categorical_value"] is not None):
                raise ValueError("Hash/proveniência do fato composto divergente; restaure ou reconcilie a base.")
            key = row["feature_id"], row["observation_time"]
            if row["revision"] != revisions.get(key, 0) + 1:
                raise ValueError("Histórico composto perdeu revisões; reconcilie antes de retomar.")
            revisions[key] = row["revision"]
            event_key = (*key, row["source_revision"])
            if event_key in known:
                raise ValueError("Revisão composta duplicada; reconcilie antes de retomar.")
            known[event_key] = row["value_hash"], origin
            latest[key] = row["available_at_utc"]
            origins[origin] = max(origins.get(origin, row["available_at_utc"]), row["available_at_utc"])
    return known, latest, count, origins


def _materialization_inputs(history, leaves, window, guard):
    import hashlib
    import json
    import pandas as pd
    first, last = window
    result = set()
    for index, row in enumerate(history.itertuples(index=False)):
        if index % 256 == 0:
            guard()
        if (row.feature_id, row.algorithm_version) not in leaves or not first <= row.observation_time < last:
            continue
        value = row.numeric_value if pd.notna(row.numeric_value) else None
        record = feature_store._value_record(row.symbol, row.tf, row.feature_id, row.algorithm_version,
            row.observation_time, value, available_at=row.available_at_utc, observed_at=row.observed_at_utc,
            valid_until=row.valid_until_utc if pd.notna(row.valid_until_utc) else None,
            quality=row.quality, source_revision=row.source_revision, metadata=json.loads(row.metadata_json))
        record["revision"] = int(row.revision)
        result.add(hashlib.sha256(feature_store._canonical_json(record, "parent history").encode()).hexdigest())
    return result


def _materialization_verify_inputs(db, snapshot, plan, history, origins, guard):
    from mt5_extracao.analytics import datasets, indicator_materializer
    leaves, window = set(plan["leaves"]), snapshot.feature_window_utc
    current = _materialization_inputs(history, leaves, window, guard)
    for origin, last_event in origins.items():
        if origin == snapshot.manifest_id:
            continue
        guard()
        previous = datasets.load(db, origin)
        if previous.manifest["source"]["namespace"] != snapshot.manifest["source"]["namespace"]:
            raise ValueError("Origem dos fatos materializados pertence a outra base; reconcilie a proveniência.")
        used = previous.feature_history
        used = used.loc[used.available_at_utc <= last_event]
        if not _materialization_inputs(used, leaves, window, guard).issubset(current):
            raise indicator_materializer.SourceChanged("Snapshot omite entradas já usadas pelos fatos compostos; não sobrescreva uma origem mais recente.")


def _materialization_guard(conn, db, scope, identity, expected):
    from sqlalchemy import text
    from mt5_extracao.analytics import datasets, indicator_materializer
    if conn.dialect.name == "postgresql":
        # Inclui inserção/alteração dos slots de identidade até o commit;
        # escritores de valores podem compartilhar esta trava de leitura.
        conn.execute(text("LOCK TABLE _dataset_origin IN SHARE MODE"))
        conn.execute(text("SELECT feature_id FROM " + feature_store.DEFINITIONS_TABLE + " WHERE " + scope["pairs"] +
                          " ORDER BY feature_id,algorithm_version FOR UPDATE"), scope["params"]).all()
    if datasets.identity(db, connection=conn) != identity:
        raise indicator_materializer.SourceChanged("Identidade da base mudou durante a materialização; retome na origem correta.")
    actual = conn.execute(text("SELECT COUNT(*) FROM " + feature_store.VALUES_TABLE + " WHERE " + scope["where"]),
                          scope["params"]).scalar_one()
    if actual != expected:
        raise indicator_materializer.SourceChanged("Histórico composto mudou durante o job; lotes confirmados preservados. Reexecute.")


def materialize_snapshot(db, manifest_id, *, targets, as_of, batch_size=5000, write_guard=None, cancel_check=None,
                         timeout_seconds=600, max_memory_mb=2048):
    """Persiste eventos reais da cadeia congelada no store/journal existentes, sem fonte live implícita."""
    import hashlib
    import pandas as pd
    from mt5_extracao.analytics import datasets, indicator_materializer, producer_jobs, producers
    if (isinstance(batch_size, bool) or not isinstance(batch_size, int) or not 1 <= batch_size <= producers.MAX_BATCH_SIZE
            or isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float))
            or not 0 < timeout_seconds <= producers.MAX_JOB_TIMEOUT_SECONDS
            or isinstance(max_memory_mb, bool) or not isinstance(max_memory_mb, (int, float))
            or not 0 < max_memory_mb <= producers.MAX_JOB_MEMORY_MB):
        raise ValueError("Confira batch_size, timeout e memória nos limites existentes dos jobs Analytics.")
    indicator_materializer._guard(None, write_guard, cancel_check, timeout_seconds, max_memory_mb)
    as_of = feature_store._utc_timestamp(as_of, "as_of")
    if as_of > pd.Timestamp.now(tz="UTC"):
        raise ValueError("Corte futuro não pode antecipar recebimentos/vencimentos; escolha as_of já ocorrido.")
    snapshot = datasets.load(db, manifest_id)
    plan = _composition_plan(snapshot, targets)
    projection = _materialization_signature()
    outputs = _materialization_definitions(plan, projection)
    identity = datasets.identity(db)
    if identity["namespace"] != snapshot.manifest["source"]["namespace"]:
        raise ValueError("Snapshot de outra base: a leitura é permitida, mas a gravação exige o namespace original.")
    scope = _materialization_scope(snapshot, outputs)
    configuration = dict(input_basis="frozen_snapshot", manifest_id=manifest_id, as_of=as_of.isoformat(),
        targets=plan["pairs"], outputs=outputs,
        limits=dict(batch_size=batch_size, timeout_seconds=timeout_seconds, max_memory_mb=max_memory_mb,
                    write_guard_enabled=write_guard is not None, cancellation_enabled=cancel_check is not None))
    config_hash = hashlib.sha256(feature_store._canonical_json(configuration, "materialization config").encode()).hexdigest()
    family = "frozen_composite:" + hashlib.sha256(feature_store._canonical_json(plan["pairs"], "targets").encode()).hexdigest()[:16]
    run = producer_jobs.start_run(db, snapshot.manifest["symbol"], snapshot.manifest["timeframe"], family,
                                  "frozen-v1+" + projection[:16], config_hash, 0, configuration=configuration)
    counts = dict(inserted=0, revised=0, unchanged=0)
    def guard():
        indicator_materializer._guard(run, write_guard, cancel_check, timeout_seconds, max_memory_mb)
        for operation, (function, code) in plan["operator_runtime"].items():
            current = _COMPOSITE_OPERATORS.get(operation, {}).get("evaluate")
            if current is not function or getattr(current, "__code__", None) is not code:
                raise indicator_materializer.SourceChanged("O cálculo mudou durante a materialização; lotes confirmados preservados.")
    try:
        with db.engine.begin() as conn:
            datasets.bind(db, run["run_id"], manifest_id, connection=conn)
        guard()
        known, latest, stored_count, origins = _materialization_history(db, snapshot, outputs, scope, identity, guard)
        history, proof = snapshot.feature_history, []
        for record in _materialization_records(snapshot, plan, outputs, history, as_of, guard):
            normalized = feature_store._value_record(**record)
            key = record["feature_id"], normalized["observation_time"]
            found = known.get((*key, normalized["source_revision"]))
            origin = found[1] if found else manifest_id
            record["metadata"]["materialization"]["source_manifest_id"] = origin
            normalized = feature_store._value_record(**record)
            if found and found[0] != normalized["value_hash"]:
                raise ValueError("Revisão composta existente diverge do snapshot; reconcilie antes de gravar.")
            if not found and key in latest and normalized["available_at_utc"] < latest[key]:
                raise indicator_materializer.SourceChanged("Novo evento retrocede o recebimento já materializado; reconcilie a origem sem reescrever o passado.")
            proof.append((*key, normalized["source_revision"], normalized["value_hash"], origin, bool(found)))
        if not proof:
            raise ValueError("Nenhuma observação dos pais contém fatos; não é possível inventar valores compostos.")
        if any(not item[-1] for item in proof):
            _materialization_verify_inputs(db, snapshot, plan, history, origins, guard)
        for definition in outputs.values():
            guard()
            register(db, **definition)
        counts["unchanged"] = sum(item[-1] for item in proof)
        def save(batch, cursor, status="RUNNING"):
            guard()
            def before(conn):
                guard()
                _materialization_guard(conn, db, scope, identity,
                                       stored_count + counts["inserted"] + counts["revised"])
            def hook(conn, current):
                guard()
                expected = stored_count + counts["inserted"] + counts["revised"] + current["inserted"] + current["revised"]
                _materialization_guard(conn, db, scope, identity, expected)
                datasets.bind(db, run["run_id"], manifest_id, connection=conn)
                producer_jobs.advance_checkpoint(conn, symbol=snapshot.manifest["symbol"], timeframe=snapshot.manifest["timeframe"],
                    family=family, algorithm_version="frozen-v1+" + projection[:16], config_hash=config_hash,
                    source_sequence=0, last_bar_start=proof[max(0, cursor - 1)][1], status=status,
                    state=dict(input_basis="frozen_snapshot", manifest_id=manifest_id, as_of=as_of.isoformat(), next_record=cursor,
                               record_count=len(proof), projection_sha256=projection))
            result = feature_store.save_values(db, batch, transaction_guard=before, transaction_hook=hook)
            for name in counts:
                counts[name] += result[name]
        batch, cursor = [], 0
        for record in _materialization_records(snapshot, plan, outputs, history, as_of, guard):
            if cursor >= len(proof):
                raise indicator_materializer.SourceChanged("O cálculo mudou entre preparação e gravação; reexecute.")
            expected = proof[cursor]
            record["metadata"]["materialization"]["source_manifest_id"] = expected[-2]
            normalized = feature_store._value_record(**record)
            actual = (record["feature_id"], normalized["observation_time"], normalized["source_revision"], normalized["value_hash"])
            if actual != expected[:4]:
                raise indicator_materializer.SourceChanged("O cálculo mudou entre preparação e gravação; lotes confirmados preservados.")
            cursor += 1
            if not expected[-1]:
                batch.append(record)
                if len(batch) >= batch_size:
                    save(batch, cursor)
                    batch = []
        if cursor != len(proof):
            raise indicator_materializer.SourceChanged("O cálculo perdeu eventos entre preparação e gravação; reexecute.")
        if batch:
            save(batch, cursor)
        save([], cursor, "COMPLETE")
        skipped = counts["inserted"] + counts["revised"] == 0
        job = producer_jobs.finish_run(db, run, source_sequence=0, status="SKIPPED" if skipped else "COMPLETE", counts=counts)
        return dict(manifest_id=manifest_id, as_of=as_of.isoformat(), input_basis="frozen_snapshot", family=family, skipped=skipped,
            limits=configuration["limits"],
            outputs=list(outputs.values()), transitions=len(proof), **counts, job=job,
            qualification=dict(materialized=True, reconstructed=True, latest_live_source_certified=False,
                               historical_source_certification=False, operational_acceptance=False))
    except BaseException as exc:
        producer_jobs.abort_run(db, run, source_sequence=0, counts=counts, error=exc,
                               cancelled=isinstance(exc, indicator_materializer.Cancelled))
        raise
