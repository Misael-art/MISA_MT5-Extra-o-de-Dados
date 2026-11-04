"""LAB-01: inventário real de indicadores para o Laboratório; somente leitura.

Une o registry de fórmulas (BD06), as definições/versões registradas pelo pilar de Dados
e a presença de fatos já materializados. Não calcula, não grava e não cria schema: uma
definição nova aparece aqui sem editar seletores.
"""
import base64
import hashlib
import json

from sqlalchemy import inspect, text

from mt5_extracao.analytics import feature_store, query_service

INVENTORY_VERSION = 1
MAX_PAGE_SIZE = 200
STATES = {
    "STORED": "Fatos materializados na base; consultáveis por as-of/revisão.",
    "COMPUTABLE": "Sem fatos na base; o Laboratório calcula pela fórmula do registry sobre barras congeladas.",
    "STORED_ONLY": "Definição registrada pelo pilar de Dados sem fórmula neste código; só fatos armazenados.",
    "UNAVAILABLE": "Definição registrada sem fórmula neste código e sem fatos na base.",
}
ACTIONS = {
    "STORED": None,
    "COMPUTABLE": "Use Séries e confluências; para fatos as-of, materialize pelo produtor de Dados.",
    "STORED_ONLY": "Consulte pelos fatos armazenados (query_service); a fórmula pertence ao produtor.",
    "UNAVAILABLE": "Materialize pelo produtor de Dados ou atualize o código que contém a fórmula.",
}


def _facts(db):
    """Contagem por definição/ativo/TF; leitura agregada da tabela existente."""
    if not feature_store._usable(db) or not inspect(db.engine).has_table(feature_store.VALUES_TABLE):
        return {}
    with db.engine.connect() as conn:
        rows = conn.execute(text(
            f"SELECT feature_id, algorithm_version, symbol, tf, COUNT(*) AS n, "
            f"MIN(observation_time) AS first, MAX(observation_time) AS last, MAX(available_at_utc) AS available "
            f"FROM {feature_store.VALUES_TABLE} GROUP BY feature_id, algorithm_version, symbol, tf "
            f"ORDER BY feature_id, algorithm_version, symbol, tf")).mappings()
        result = {}
        for row in rows:
            result.setdefault((row["feature_id"], row["algorithm_version"]), []).append(dict(
                symbol=row["symbol"], timeframe=row["tf"], facts=int(row["n"]), first=str(row["first"]),
                last=str(row["last"]), last_available_at=str(row["available"])))
        return result


def entries(db=None):
    """Uma linha por (feature_id, versão): registry, definições de Dados e fatos."""
    facts = _facts(db) if db is not None else {}
    result = {}
    for spec in query_service.definition_catalog(db):
        key = (spec["feature_id"], spec["algorithm_version"])
        registry = spec.get("registry_contract")
        name = (registry["name"] if registry and spec["computable"] and
                spec["name_status"] == "UNVERIFIED_LEGACY_NAME" else spec["name"])
        result[key] = dict(feature_id=key[0], algorithm_version=key[1], name=name,
            family=spec["family"] or key[0].split(".")[0],
            unit=spec["unit"], parameters=spec["parameters"], outputs=spec["buffers"], warmup=spec["warmup_count"],
            output_kind=spec["output_kind"], origin=spec["origin"], computable=spec["computable"],
            stored_name_status=spec["name_status"] if spec["registered_definition"] is not None else None,
            metadata_state=spec["metadata_state"], metadata_conflicts=spec["metadata_conflicts"])
    for key in facts:
        if key not in result:
            # Fatos sem definição só existem por corrupção/limpeza parcial; mostrar, nunca esconder.
            result[key] = dict(feature_id=key[0], algorithm_version=key[1], name=key[0], family=key[0].split(".")[0],
                unit=None, parameters={}, outputs=[], warmup=None, output_kind=None, origin="facts_only",
                computable=False)
    rows = []
    for key, row in result.items():
        row["series"] = facts.get(key, [])
        row["facts"] = sum(item["facts"] for item in row["series"])
        state = "STORED" if row["facts"] else "COMPUTABLE" if row["computable"] else "UNAVAILABLE"
        if state == "STORED" and not row["computable"]:
            state = "STORED_ONLY"
        row.update(state=state, reason=STATES[state], action=ACTIONS[state])
        if row.get("stored_name_status") == "UNVERIFIED_LEGACY_NAME":
            row["reason"] += " Nome legado não verificável; identifique a série pelo ID e versão, sem inferir o nome."
        if row.get("metadata_conflicts"):
            row["reason"] += " Definição da base difere da fórmula local: " + ", ".join(row["metadata_conflicts"]) + "."
        if row["origin"] == "facts_only":
            row.update(state="UNAVAILABLE", reason="Há fatos sem definição registrada; contrato ausente, não elegível para análise.",
                       action="Confira o catálogo no produtor; não infira a fórmula nem remova os fatos.")
        rows.append(row)
    return sorted(rows, key=lambda row: (row["family"], row["feature_id"], row["algorithm_version"]))


def _matches(row, query):
    haystack = " ".join(str(part) for part in (row["feature_id"], row["name"], row["family"], row["unit"],
                        row["algorithm_version"], json.dumps(row["parameters"], ensure_ascii=False, sort_keys=True))).casefold()
    return all(term in haystack for term in query.casefold().split())


def search(db=None, *, query="", family=None, state=None, symbol=None, timeframe=None, page_size=50, cursor=None):
    """Busca/filtros com paginação estável; cursor amarrado aos filtros."""
    if isinstance(page_size, bool) or not isinstance(page_size, int) or not 1 <= page_size <= MAX_PAGE_SIZE:
        raise ValueError(f"page_size deve estar entre 1 e {MAX_PAGE_SIZE}.")
    if state is not None and state not in STATES:
        raise ValueError("Estado desconhecido; use " + ", ".join(STATES) + ".")
    filters = dict(query=query or "", family=family, state=state, symbol=symbol, timeframe=timeframe)
    rows = entries(db)
    families = sorted({row["family"] for row in rows})
    selected = [row for row in rows if _matches(row, filters["query"])
                and (family is None or row["family"] == family) and (state is None or row["state"] == state)
                and ((symbol is None and timeframe is None) or any(
                    (symbol is None or item["symbol"] == symbol) and
                    (timeframe is None or item["timeframe"] == timeframe) for item in row["series"]))]
    generation = hashlib.sha256(json.dumps(rows, sort_keys=True, ensure_ascii=False,
                                          allow_nan=False).encode()).hexdigest()
    offset = 0
    if cursor:
        try:
            payload = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
            if (not isinstance(payload, dict) or not isinstance(payload.get("o"), int) or
                    isinstance(payload["o"], bool) or payload["o"] < 0 or
                    "f" not in payload or not isinstance(payload.get("g"), str)):
                raise ValueError
            offset = payload["o"]
        except (ValueError, TypeError, KeyError, AttributeError) as error:
            raise ValueError("Cursor inválido; recomece sem cursor.") from error
        if payload.get("f") != filters:
            raise ValueError("Cursor pertence a outros filtros; recomece sem cursor.")
        if payload["g"] != generation:
            raise ValueError("O inventário mudou durante a paginação; recomece sem cursor.")
        if offset > len(selected):
            raise ValueError("Cursor inválido; recomece sem cursor.")
    page = selected[offset:offset + page_size]
    following = offset + page_size
    next_cursor = base64.urlsafe_b64encode(json.dumps(dict(o=following, f=filters, g=generation)).encode()).decode() \
        if following < len(selected) else None
    reason = None
    if not selected:
        reason = ("Nenhum indicador corresponde aos filtros; limpe a busca ou o estado." if rows
                  else "Registry vazio; confira a instalação do código de indicadores.")
    return dict(inventory_version=INVENTORY_VERSION, total=len(rows), matched=len(selected), items=page,
        next_cursor=next_cursor, families=families, states=dict(STATES), filters=filters, reason=reason,
        counts={name: sum(row["state"] == name for row in rows) for name in STATES})
