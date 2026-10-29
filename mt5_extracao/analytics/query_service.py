"""BD18a (T18.6): serviço de consulta executável, sem UI e sem MT5, pronto para uma futura API.

Uma seleção (base implícita no banco, símbolo, TF, características, intervalo e as-of) devolve um
envelope JSON-seguro com a identidade da base, o contexto temporal, as definições/contratos usados,
os fatos disponíveis no corte (valor, qualidade, revisão, disponibilidade, versão) e paginação
estável por cursor. Erros são estruturados; a leitura não calcula nem altera fatos/definições.
A identidade pública pode carimbar o namespace da base, conforme datasets.identity (R04).
"""
import base64
import hashlib
import json
import math

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

from mt5_extracao.analytics import datasets, feature_store, indicator_contracts

QUERY_SCHEMA_VERSION = 1
MAX_PAGE_SIZE = 5000
MAX_SELECTION_ROWS = 200_000
_SUMMARY_FIELDS = ("name", "family", "unit", "output_kind", "warmup_count", "availability_rule",
                   "missing_rule", "alignment", "reference")
_VARIANT_FIELDS = (*_SUMMARY_FIELDS, "feature_id", "algorithm_version", "parameters", "dependencies", "buffers",
                   "formula", "warmup_unit", "source_id", "timeframe", "value_type", "name_status",
                   "definition_schema_version", "origin", "metadata_state", "metadata_conflicts", "computable",
                   "registered_at_utc")


class QueryError(ValueError):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


def _iso(value):
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    stamp = pd.Timestamp(value)
    if pd.isna(stamp):
        return None
    stamp = stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")
    return stamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _number(value):
    if value is None:
        return None
    value = float(value)
    return value if math.isfinite(value) else None


def _utc(value, label):
    try:
        stamp = pd.Timestamp(value)
    except (ValueError, TypeError, OverflowError) as exc:
        raise QueryError("BAD_TIME", f"{label} precisa ser uma data ISO válida com fuso explícito.") from exc
    if pd.isna(stamp):
        raise QueryError("BAD_TIME", f"{label} não pode estar ausente.")
    if stamp.tzinfo is None:
        raise QueryError("NAIVE_TIME", f"{label} precisa declarar fuso horário (ex.: 2026-10-14T17:25:00Z).")
    return stamp.tz_convert("UTC")


def _selection_hash(selection):
    return hashlib.sha256(json.dumps(selection, sort_keys=True).encode()).hexdigest()[:16]


def _encode_cursor(selection_hash, key, generation):
    return base64.urlsafe_b64encode(json.dumps({"v": 2, "s": selection_hash, "k": key,
                                              "g": generation}).encode()).decode()


def _decode_cursor(cursor, selection_hash):
    try:
        payload = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
        if (not isinstance(payload, dict) or payload.get("v") != 2 or
                not isinstance(payload.get("s"), str) or not isinstance(payload.get("g"), str) or
                len(payload["g"]) != 64 or not isinstance(payload.get("k"), list) or len(payload["k"]) != 3 or
                not all(isinstance(v, str) and v for v in payload["k"])):
            raise ValueError
        stamp = _utc(payload["k"][1], "cursor")
        if _iso(stamp) != payload["k"][1]:
            raise ValueError
    except (ValueError, TypeError, AttributeError) as exc:
        raise QueryError("BAD_CURSOR", "Cursor inválido; recomece a paginação sem cursor.") from exc
    if payload.get("s") != selection_hash:
        raise QueryError("CURSOR_MISMATCH", "O cursor pertence a outra seleção; recomece sem cursor.")
    return tuple(payload["k"]), payload["g"]


def _stored_contract(row):
    """Metadados declarados pelo produtor; campos ausentes não vêm de uma fórmula de outra versão."""
    result = dict(row)
    kind = {"NUMBER": "number", "CATEGORY": "category", "BOOLEAN": "boolean"}.get(row["value_type"], "unknown")
    if row["value_type"] == "NUMBER" and row["unit"] in indicator_contracts._CATEGORY_UNITS:
        kind = "category"  # unidade categórica declarada pelo produtor, inclusive versões antigas
    result.update(output_kind=kind, buffers=[dict(name=row["feature_id"], kind=kind, unit=row["unit"])],
                  missing_rule=None, alignment=None, reference=None, origin="data_store", computable=False,
                  metadata_state="STORED_PARTIAL", metadata_conflicts=[])
    return result


def definition_catalog(db=None):
    """Fonte única do catálogo registry + definições/versões persistidas. Só lê; não cria schema."""
    registry = {(s["feature_id"], s["algorithm_version"]):s for s in indicator_contracts.contracts()}
    result = {key:dict(s, origin="registry", computable=True, name_status="DECLARED",
                       metadata_state="REGISTRY_CONTRACT", metadata_conflicts=[], registered_definition=None,
                       registry_contract=s) for key, s in registry.items()}
    if db is not None:
        if not feature_store._usable(db):
            raise QueryError("BASE_UNAVAILABLE", "Base indisponível; confira a configuração e a conexão.")
        try:
            stored = feature_store.list_definitions(db)
        except json.JSONDecodeError as exc:
            raise QueryError("INVALID_DEFINITION", "Metadados de definição inválidos na base; confira o produtor.") from exc
        for row in stored:
            key = row["feature_id"], row["algorithm_version"]
            entry = _stored_contract(row)
            entry["registered_definition"] = row
            entry["registry_contract"] = registry.get(key)
            if key in registry:
                spec = registry[key]
                conflicts = [field for field in ("parameters", "formula", "unit", "warmup_count", "warmup_unit",
                                                 "availability_rule", "value_type")
                             if row.get(field) is not None and row[field] != spec.get(field)]
                entry.update(origin="registry+data_store", computable=not conflicts, metadata_conflicts=conflicts)
                if conflicts:
                    entry["metadata_state"] = "REGISTRY_CONFLICT"
                else:
                    # BD06: códigos de direção são categorias embora o armazenamento seja NUMBER.
                    # Esse significado só vale para a mesma versão e contrato compatível.
                    entry.update(output_kind=spec["output_kind"], buffers=spec["buffers"])
            result[key] = entry
    return [result[key] for key in sorted(result)]


def catalog(db=None):
    """Contratos/capacidades; sem db mantém a descoberta offline. Com db inclui todas as versões registradas."""
    try:
        envelope = {"query_schema_version": QUERY_SCHEMA_VERSION, "status": "ok",
                    "definitions": definition_catalog(db), "reasons": [], "errors": []}
        if db is not None:
            envelope["base"], envelope["time_context"] = _base(db)
        if any(d.get("name_status") == "UNVERIFIED_LEGACY_NAME" for d in envelope["definitions"]):
            envelope["reasons"].append("Há nomes legados não verificáveis; use os IDs e versões das definições.")
    except QueryError as exc:
        envelope = {"query_schema_version": QUERY_SCHEMA_VERSION, "status": "error", "definitions": [],
                    "errors": [{"code": exc.code, "message": str(exc)}]}
    except SQLAlchemyError:
        envelope = {"query_schema_version": QUERY_SCHEMA_VERSION, "status": "error", "definitions": [],
                    "errors": [{"code": "BASE_UNAVAILABLE", "message": "Não foi possível ler o catálogo da base."}]}
    json.dumps(envelope, allow_nan=False)
    return envelope


def _definitions_for_selection(frame, feature_ids, algorithm_version, entries):
    by_key = {(e["feature_id"], e["algorithm_version"]):e for e in entries}
    definitions = {}
    for ident in feature_ids:
        versions = sorted(set(frame.loc[frame.feature_id == ident, "algorithm_version"])) if not frame.empty else []
        known = {version:entry for (feature, version), entry in by_key.items() if feature == ident
                 and (algorithm_version is None or version == algorithm_version)}
        for version in versions:
            if version not in known or known[version]["registered_definition"] is None:
                raise QueryError("MISSING_DEFINITION", f"Fatos sem definição registrada: {ident}@{version}. "
                                 "Confira o produtor; a fórmula local não substitui o contrato ausente.")
        selected = {v:known[v] for v in versions} if versions else known
        variants = {v:{field:entry.get(field) for field in _VARIANT_FIELDS} for v, entry in selected.items()}
        values = list(variants.values())
        common = {field:(values[0].get(field) if values and all(v.get(field) == values[0].get(field)
                                                             for v in values) else None)
                  for field in _SUMMARY_FIELDS}
        states = {v["metadata_state"] for v in values}
        common.update(algorithm_versions=versions, known_algorithm_versions=sorted(known), variants=variants,
                      metadata_state=(next(iter(states)) if len(states) == 1 and len(values) == 1 else
                                      "VERSION_DEPENDENT" if values else "UNAVAILABLE_VERSION"))
        definitions[ident] = common
    return definitions


def _generation(frame, base, context, definitions):
    digest = hashlib.sha256(json.dumps(dict(base=base, context=context, definitions=definitions),
                                      sort_keys=True, ensure_ascii=False, allow_nan=False).encode())
    if not frame.empty:
        digest.update(pd.util.hash_pandas_object(frame[list(feature_store.VALUE_COLUMNS)], index=False).values.tobytes())
    return digest.hexdigest()


def _base(db):
    identity = datasets.identity(db)
    context = db.time_context() if hasattr(db, "time_context") else {"offset_state": "unknown"}
    return {key: identity[key] for key in ("namespace", "parent_namespace", "lineage", "provider", "server",
                                           "backend")}, context


def series(db, *, symbol, timeframe, feature_ids, as_of, start=None, end=None, algorithm_version=None,
           page_size=500, cursor=None):
    """Fatos em ``as_of``, sem cálculo ou alteração de fatos; identidade pública conforme R04."""
    selection = {"symbol": symbol if isinstance(symbol, str) else None,
                 "timeframe": timeframe if isinstance(timeframe, str) else None, "feature_ids": [],
                 "as_of": None, "start": None, "end": None,
                 "algorithm_version": algorithm_version if isinstance(algorithm_version, str) else None}
    try:
        if not isinstance(symbol, str) or not symbol.strip() or not isinstance(timeframe, str) or not timeframe.strip():
            raise QueryError("MISSING_SELECTION", "Informe símbolo e timeframe.")
        symbol, timeframe = symbol.strip(), timeframe.strip()
        selection.update(symbol=symbol, timeframe=timeframe)
        if feature_ids is None:
            raise QueryError("MISSING_SELECTION", "Informe ao menos uma característica.")
        if (not isinstance(feature_ids, (list, tuple, set, frozenset)) or
                not all(isinstance(ident, str) and ident.strip() for ident in feature_ids)):
            raise QueryError("BAD_FEATURES", "Características devem ser uma lista de IDs não vazios.")
        feature_ids = sorted({ident.strip() for ident in feature_ids})
        if not feature_ids:
            raise QueryError("MISSING_SELECTION", "Informe ao menos uma característica.")
        selection["feature_ids"] = feature_ids
        if algorithm_version is not None:
            if not isinstance(algorithm_version, str) or not algorithm_version.strip():
                selection["algorithm_version"] = None
                raise QueryError("BAD_VERSION", "algorithm_version precisa ser texto não vazio.")
            algorithm_version = algorithm_version.strip()
        selection["algorithm_version"] = algorithm_version
        if isinstance(page_size, bool) or not isinstance(page_size, int) or not 1 <= page_size <= MAX_PAGE_SIZE:
            raise QueryError("BAD_PAGE_SIZE", f"page_size deve estar entre 1 e {MAX_PAGE_SIZE}.")
        cut = _utc(as_of, "as_of")
        selection.update(as_of=_iso(cut), start=_iso(_utc(start, "start")) if start is not None else None,
                         end=_iso(_utc(end, "end")) if end is not None else None)
        if selection["start"] and selection["end"] and selection["start"] > selection["end"]:
            raise QueryError("BAD_RANGE", "O início do intervalo não pode vir depois do fim.")
        digest = _selection_hash(selection)
        after, expected_generation = _decode_cursor(cursor, digest) if cursor is not None else (None, None)
        if not feature_store._usable(db):
            raise QueryError("BASE_UNAVAILABLE", "Base indisponível; confira a configuração e a conexão.")
        entries = definition_catalog(db)
        unknown = [ident for ident in feature_ids if ident not in {d["feature_id"] for d in entries}]
        if unknown:
            raise QueryError("UNKNOWN_FEATURE", "Características desconhecidas no catálogo: " + ", ".join(unknown))
        base, context = _base(db)
        frames = [feature_store.query_as_of(db, cut, symbol=symbol, timeframe=timeframe, feature_id=ident,
                                            algorithm_version=algorithm_version, start=start, end=end)
                  for ident in feature_ids]
        frame = pd.concat([f for f in frames if not f.empty], ignore_index=True) if any(
            not f.empty for f in frames) else pd.DataFrame()
        if len(frame) > MAX_SELECTION_ROWS:
            raise QueryError("SELECTION_TOO_LARGE", f"A seleção tem {len(frame)} fatos; limite "
                             f"{MAX_SELECTION_ROWS}. Reduza o intervalo com start/end.")
        if not frame.empty:
            frame = frame.assign(_obs=frame["observation_time"].map(_iso)).sort_values(
                ["feature_id", "_obs", "algorithm_version"], kind="stable")
        definitions = _definitions_for_selection(frame, feature_ids, algorithm_version, entries)
        generation = _generation(frame, base, context, definitions)
        if expected_generation is not None and generation != expected_generation:
            raise QueryError("STALE_CURSOR", "Base, fatos, metadados ou relógio mudaram durante a paginação. "
                             "Recomece sem cursor para não misturar páginas de revisões diferentes.")
        rows = []
        if not frame.empty:
            for item in frame.to_dict("records"):
                key = (item["feature_id"], item["_obs"], item["algorithm_version"])
                if after is not None and key <= after:
                    continue
                rows.append({"feature_id": item["feature_id"], "algorithm_version": item["algorithm_version"],
                             "observation_time": item["_obs"], "available_at": _iso(item["available_at_utc"]),
                             "value": _number(item.get("numeric_value")),
                             "category": item.get("categorical_value") or None, "quality": item["quality"],
                             "revision": int(item["revision"]), "source_revision": item["source_revision"]})
                if len(rows) > page_size:
                    break
        more = len(rows) > page_size
        rows = rows[:page_size]
        reasons = []
        if any(d["metadata_state"] == "VERSION_DEPENDENT" for d in definitions.values()):
            reasons.append("Metadados variam por versão; consulte variants antes de comparar unidades ou parâmetros.")
        if any(v.get("name_status") == "UNVERIFIED_LEGACY_NAME" for d in definitions.values()
               for v in d["variants"].values()):
            reasons.append("Há nomes legados não verificáveis; use o ID e a versão, sem inferir o nome original.")
        if context.get("offset_state") == "unknown":
            reasons.append("Fuso do corretor desconhecido: horários de mercado não foram confirmados em UTC.")
        if not rows and not after:
            reasons.append("Nenhum fato disponível no corte para a seleção (não calculado, fora do intervalo "
                           "ou ainda não disponível).")
        envelope = {
            "query_schema_version": QUERY_SCHEMA_VERSION, "status": "ok", "base": base,
            "time_context": context, "selection": selection, "definitions": definitions, "values": rows,
            "page": {"size": page_size, "returned": len(rows),
                     "next_cursor": _encode_cursor(digest, [rows[-1]["feature_id"], rows[-1]["observation_time"],
                                                             rows[-1]["algorithm_version"]], generation) if more else None},
            "freshness": {"latest_available_at": max((r["available_at"] for r in rows), default=None)},
            "reasons": reasons, "errors": [],
        }
    except QueryError as exc:
        envelope = {"query_schema_version": QUERY_SCHEMA_VERSION, "status": "error", "selection": selection,
                    "values": [], "errors": [{"code": exc.code, "message": str(exc)}]}
    except SQLAlchemyError:
        envelope = {"query_schema_version": QUERY_SCHEMA_VERSION, "status": "error", "selection": selection,
                    "values": [], "errors": [{"code": "BASE_UNAVAILABLE", "message": "Não foi possível ler os fatos da base."}]}
    json.dumps(envelope, allow_nan=False)  # garante JSON sem NaN/infinito
    return envelope
