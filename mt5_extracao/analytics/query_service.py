"""BD18a (T18.6): serviço de consulta executável, sem UI e sem MT5, pronto para uma futura API.

Contrato ``query_schema_version=2`` (correções V3-01..05 da 3ª revisão):

- ``basis`` explícito na seleção, no envelope e no cursor: ``research`` reconstrói o passado pela
  disponibilidade declarada do produtor (útil para backtest, NÃO prova do que se sabia);
  ``knowledge`` só devolve revisões que este sistema já tinha gravado no corte. Como o fato derivado
  é gravado depois dos recebimentos e revisões das suas fontes, ``recorded_at`` limita também a
  disponibilidade das dependências. Cada fato traz ``reconstructed``, ``recorded_at`` e
  ``knowledge_state``.
- Cada fato aponta para a definição imutável ``feature_id + algorithm_version`` gravada no store;
  o contrato atual vem separado em ``current_contract``. Definição ausente é condição explícita.
- Leitura pura: não migra, não cria identidade, não grava (``datasets.read_identity``).
- Cursor ligado à base (namespace), ao ``basis``, à seleção normalizada e a uma fotografia
  ``recorded_through``: revisões gravadas depois da 1ª página não entram nas seguintes.
- Entradas validadas com códigos estáveis; falhas internas não são mascaradas como erro do usuário.
"""
import base64
import binascii
import hashlib
import json
import math
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import inspect, text

from mt5_extracao.analytics import datasets, feature_store, indicator_contracts, indicator_registry

QUERY_SCHEMA_VERSION = 2
MAX_PAGE_SIZE = 5000
MAX_SELECTION_ROWS = 200_000
MAX_CURSOR_CHARS = 2048
BASES = feature_store.BASES
_STORED_FIELDS = ("name", "family", "formula", "unit", "warmup_count", "warmup_unit", "source_id", "timeframe",
                  "availability_rule", "value_type")


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
    except (ValueError, TypeError) as exc:
        raise QueryError("BAD_TIME", f"{label} não é uma data e hora válida: {value!r}.") from exc
    if pd.isna(stamp):
        raise QueryError("BAD_TIME", f"{label} ausente ou inválido.")
    if stamp.tzinfo is None:
        raise QueryError("NAIVE_TIME", f"{label} precisa declarar fuso horário (ex.: 2026-10-14T17:25:00Z).")
    return stamp.tz_convert("UTC")


def _digest(payload):
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:16]


def _encode_cursor(binding, key, recorded_through):
    raw = json.dumps({"s": binding, "k": list(key), "r": recorded_through}, separators=(",", ":"))
    return base64.urlsafe_b64encode(raw.encode()).decode()


def _decode_cursor(cursor, binding):
    if not isinstance(cursor, str) or not 0 < len(cursor) <= MAX_CURSOR_CHARS:
        raise QueryError("BAD_CURSOR", "Cursor inválido; recomece a paginação sem cursor.")
    try:
        payload = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
    except (ValueError, TypeError, binascii.Error, UnicodeDecodeError) as exc:
        raise QueryError("BAD_CURSOR", "Cursor inválido; recomece a paginação sem cursor.") from exc
    if (not isinstance(payload, dict) or set(payload) != {"s", "k", "r"} or not isinstance(payload["s"], str)
            or not isinstance(payload["k"], list) or len(payload["k"]) != 3
            or not all(isinstance(part, str) for part in payload["k"]) or not isinstance(payload["r"], str)):
        raise QueryError("BAD_CURSOR", "Cursor com estrutura inválida; recomece a paginação sem cursor.")
    if payload["s"] != binding:
        raise QueryError("CURSOR_MISMATCH", "O cursor pertence a outra base, modo temporal ou seleção; "
                                            "recomece sem cursor.")
    _utc(payload["r"], "fotografia do cursor")
    return tuple(payload["k"]), payload["r"]


def catalog():
    """Contratos BD06 atuais de todas as definições (catálogo vigente, não histórico)."""
    return {"query_schema_version": QUERY_SCHEMA_VERSION, "status": "ok",
            "definitions": indicator_contracts.contracts()}


def _stored_definitions(db, pairs):
    """Definições gravadas no store para cada (feature_id, algorithm_version), só leitura."""
    if not pairs:
        return {}
    with db.engine.connect() as conn:
        if not inspect(conn).has_table(feature_store.DEFINITIONS_TABLE):
            return {}
        columns = {c["name"] for c in inspect(conn).get_columns(feature_store.DEFINITIONS_TABLE)}
        wanted = [c for c in ("feature_id", "algorithm_version", "parameters_json", "dependencies_json",
                              *_STORED_FIELDS) if c in columns]
        rows = conn.execute(text(f"SELECT {', '.join(wanted)} FROM {feature_store.DEFINITIONS_TABLE}")).mappings()
        stored = {(r["feature_id"], r["algorithm_version"]): dict(r) for r in rows
                  if (r["feature_id"], r["algorithm_version"]) in pairs}
    result = {}
    for pair, row in stored.items():
        item = {name: row.get(name) for name in _STORED_FIELDS}
        item["parameters"] = json.loads(row["parameters_json"]) if row.get("parameters_json") else None
        item["dependencies"] = json.loads(row["dependencies_json"]) if row.get("dependencies_json") else None
        item["state"] = "stored"
        result[pair] = item
    return result


def _validate_selection(symbol, timeframe, feature_ids, basis, page_size):
    if not isinstance(symbol, str) or not symbol.strip() or not isinstance(timeframe, str) or not timeframe.strip():
        raise QueryError("MISSING_SELECTION", "Informe símbolo e timeframe como texto.")
    if isinstance(feature_ids, str) or not isinstance(feature_ids, (list, tuple)) or not feature_ids:
        raise QueryError("MISSING_SELECTION", "Informe uma lista com ao menos uma característica.")
    if not all(isinstance(ident, str) and ident for ident in feature_ids):
        raise QueryError("BAD_SELECTION", "Características devem ser identificadores de texto.")
    known = {d["feature_id"] for d in indicator_registry.definitions()}
    unknown = sorted(set(feature_ids) - known)
    if unknown:
        raise QueryError("UNKNOWN_FEATURE", "Características desconhecidas no catálogo: " + ", ".join(unknown))
    if basis not in BASES:
        raise QueryError("BAD_BASIS", "basis deve ser 'research' (reconstrução) ou 'knowledge' (conhecimento real).")
    if isinstance(page_size, bool) or not isinstance(page_size, int) or not 1 <= page_size <= MAX_PAGE_SIZE:
        raise QueryError("BAD_PAGE_SIZE", f"page_size deve ser inteiro entre 1 e {MAX_PAGE_SIZE}.")
    return sorted(set(feature_ids))  # repetidas são normalizadas: uma chave por característica


def _knowledge_state(recorded, cut):
    if recorded is None:
        return "recording_unknown"
    return "known_at_cut" if recorded <= cut else "reconstructed_after_cut"


def series(db, *, symbol, timeframe, feature_ids, as_of, start=None, end=None, algorithm_version=None,
           basis="research", page_size=500, cursor=None):
    """Fatos da seleção no corte ``as_of``; leitura pura, nunca calcula nem grava."""
    selection = {"symbol": symbol, "timeframe": timeframe, "feature_ids": feature_ids, "basis": basis,
                 "as_of": None, "start": None, "end": None, "algorithm_version": algorithm_version}
    try:
        features = _validate_selection(symbol, timeframe, feature_ids, basis, page_size)
        cut = _utc(as_of, "as_of")
        first = _utc(start, "start") if start is not None else None
        last = _utc(end, "end") if end is not None else None
        if first is not None and last is not None and first >= last:
            raise QueryError("BAD_RANGE", "start precisa ser anterior a end.")
        if algorithm_version is not None and not isinstance(algorithm_version, str):
            raise QueryError("BAD_SELECTION", "algorithm_version deve ser texto.")
        selection.update(feature_ids=features, as_of=_iso(cut), start=_iso(first), end=_iso(last))
        base = datasets.read_identity(db)
        context = db.time_context() if hasattr(db, "time_context") else {"offset_state": "unknown"}
        binding = _digest({"namespace": base["namespace"], "selection": selection})
        after, recorded_through = (_decode_cursor(cursor, binding) if cursor else (None, None))
        recorded_through = recorded_through or _iso(datetime.now(timezone.utc))
        frames = [feature_store.query_as_of(db, cut, symbol=symbol, timeframe=timeframe, feature_id=ident,
                                            algorithm_version=algorithm_version, start=first, end=last,
                                            basis=basis, recorded_through=recorded_through, with_recorded=True)
                  for ident in features]
        frames = [f for f in frames if not f.empty]
        frame = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        if len(frame) > MAX_SELECTION_ROWS:  # limite de resposta, verificado após a leitura (BD18b: na origem)
            raise QueryError("SELECTION_TOO_LARGE", f"A seleção tem {len(frame)} fatos; limite "
                             f"{MAX_SELECTION_ROWS}. Reduza o intervalo com start/end.")
        rows, counts = [], {"reconstructed_after_cut": 0, "recording_unknown": 0}
        if not frame.empty:
            frame = frame.assign(_obs=frame["observation_time"].map(_iso)).sort_values(
                ["feature_id", "_obs", "algorithm_version"], kind="stable")
            for item in frame.to_dict("records"):
                key = (item["feature_id"], item["_obs"], item["algorithm_version"])
                if after is not None and key <= after:
                    continue
                recorded = _iso(item.get("recorded_at_utc"))
                state = _knowledge_state(recorded, _iso(cut))
                metadata = json.loads(item["metadata_json"]) if item.get("metadata_json") else {}
                rows.append({"feature_id": item["feature_id"], "algorithm_version": item["algorithm_version"],
                             "observation_time": item["_obs"], "available_at": _iso(item["available_at_utc"]),
                             "recorded_at": recorded, "knowledge_state": state,
                             "reconstructed": bool(metadata.get("reconstructed", False)),
                             "value": _number(item.get("numeric_value")),
                             "category": item.get("categorical_value") or None, "quality": item["quality"],
                             "revision": int(item["revision"]), "source_revision": item["source_revision"]})
                if len(rows) > page_size:
                    break
        more = len(rows) > page_size
        rows = rows[:page_size]
        for row in rows:
            if row["knowledge_state"] in counts:
                counts[row["knowledge_state"]] += 1
        if more and base["namespace"] is None:
            raise QueryError("PAGINATION_REQUIRES_IDENTITY", "Base sem identidade lógica: a paginação não pode "
                             "ser ligada à base. Inicialize a identidade (datasets.identity) ou reduza a seleção.")
        pairs = {(r["feature_id"], r["algorithm_version"]) for r in rows}
        stored = _stored_definitions(db, pairs)
        definitions = {}
        for ident in features:
            contract = indicator_contracts.contract(ident)
            versions = {}
            for _, version in sorted(p for p in pairs if p[0] == ident):
                versions[version] = stored.get((ident, version), {"state": "missing",
                                                                   "reason": "definição desta versão não gravada"})
            definitions[ident] = {"versions": versions, "current_contract": {name: contract[name] for name in (
                "name", "family", "unit", "output_kind", "warmup_count", "availability_rule", "missing_rule",
                "alignment", "reference")}}
        reasons = []
        if base["state"] == "unidentified":
            reasons.append("Base sem identidade lógica registrada: origem não verificável nesta leitura.")
        if context.get("offset_state") == "unknown":
            reasons.append("Fuso do corretor desconhecido: horários de mercado não foram confirmados em UTC.")
        if basis == "research" and counts["reconstructed_after_cut"]:
            reasons.append(f"{counts['reconstructed_after_cut']} fato(s) foram calculados depois do corte: "
                           "reconstrução de pesquisa, não conhecimento do sistema naquele instante.")
        if basis == "research" and counts["recording_unknown"]:
            reasons.append(f"{counts['recording_unknown']} fato(s) sem instante de gravação (anteriores ao "
                           "contrato): não se sabe quando o sistema os conheceu.")
        if any(v.get("state") == "missing" for d in definitions.values() for v in d["versions"].values()):
            reasons.append("Há fatos cuja definição de versão não está gravada; não use o contrato atual "
                           "para interpretá-los.")
        if not rows and not after:
            reasons.append("Nenhum fato no corte para a seleção e o modo temporal (não calculado, fora do "
                           "intervalo, ainda não disponível ou ainda não conhecido pelo sistema).")
        next_cursor = (_encode_cursor(binding, (rows[-1]["feature_id"], rows[-1]["observation_time"],
                                                rows[-1]["algorithm_version"]), recorded_through) if more else None)
        envelope = {
            "query_schema_version": QUERY_SCHEMA_VERSION, "status": "ok",
            "base": {key: base[key] for key in ("state", "namespace", "parent_namespace", "lineage", "provider",
                                                 "server", "backend")},
            "time_context": context, "temporal_basis": basis, "selection": selection, "definitions": definitions,
            "values": rows, "snapshot": {"recorded_through": recorded_through},
            "page": {"size": page_size, "returned": len(rows), "next_cursor": next_cursor},
            "freshness": {"latest_available_at": max((r["available_at"] for r in rows), default=None),
                          "latest_recorded_at": max((r["recorded_at"] for r in rows if r["recorded_at"]),
                                                    default=None)},
            "counts": counts, "reasons": reasons, "errors": [],
        }
    except QueryError as exc:
        selection = json.loads(json.dumps(selection, default=repr))  # entrada inválida ainda vira JSON
        envelope = {"query_schema_version": QUERY_SCHEMA_VERSION, "status": "error", "selection": selection,
                    "values": [], "errors": [{"code": exc.code, "message": str(exc)}]}
    json.dumps(envelope, allow_nan=False)
    return envelope
