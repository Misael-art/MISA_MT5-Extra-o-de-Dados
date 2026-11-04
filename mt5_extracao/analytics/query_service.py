"""BD18a (T18.6): serviço de consulta executável, sem UI e sem MT5, pronto para uma futura API.

Uma seleção (base implícita no banco, símbolo, TF, características, intervalo e as-of) devolve um
envelope JSON-seguro com a identidade da base, o contexto temporal, as definições/contratos usados,
os fatos disponíveis no corte (valor, qualidade, revisão, disponibilidade, versão) e paginação
estável por cursor. Erros são estruturados; nada é calculado nem gravado numa leitura.
"""
import base64
import hashlib
import json
import math

import pandas as pd

from mt5_extracao.analytics import datasets, feature_store, indicator_contracts, indicator_registry

QUERY_SCHEMA_VERSION = 1
MAX_PAGE_SIZE = 5000
MAX_SELECTION_ROWS = 200_000


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
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise QueryError("NAIVE_TIME", f"{label} precisa declarar fuso horário (ex.: 2026-10-14T17:25:00Z).")
    return stamp.tz_convert("UTC")


def _selection_hash(selection):
    return hashlib.sha256(json.dumps(selection, sort_keys=True).encode()).hexdigest()[:16]


def _encode_cursor(selection_hash, key):
    return base64.urlsafe_b64encode(json.dumps({"s": selection_hash, "k": key}).encode()).decode()


def _decode_cursor(cursor, selection_hash):
    try:
        payload = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
    except (ValueError, TypeError) as exc:
        raise QueryError("BAD_CURSOR", "Cursor inválido; recomece a paginação sem cursor.") from exc
    if payload.get("s") != selection_hash:
        raise QueryError("CURSOR_MISMATCH", "O cursor pertence a outra seleção; recomece sem cursor.")
    return tuple(payload["k"])


def catalog():
    """Contratos BD06 de todas as definições, JSON-seguros."""
    return {"query_schema_version": QUERY_SCHEMA_VERSION, "status": "ok",
            "definitions": indicator_contracts.contracts()}


def _base(db):
    identity = datasets.identity(db)
    context = db.time_context() if hasattr(db, "time_context") else {"offset_state": "unknown"}
    return {key: identity[key] for key in ("namespace", "parent_namespace", "lineage", "provider", "server",
                                           "backend")}, context


def series(db, *, symbol, timeframe, feature_ids, as_of, start=None, end=None, algorithm_version=None,
           page_size=500, cursor=None):
    """Fatos disponíveis em ``as_of`` para a seleção; nunca dispara cálculo ou escrita."""
    selection = {"symbol": symbol, "timeframe": timeframe, "feature_ids": sorted(feature_ids or []),
                 "as_of": None, "start": None, "end": None, "algorithm_version": algorithm_version}
    try:
        if not symbol or not timeframe:
            raise QueryError("MISSING_SELECTION", "Informe símbolo e timeframe.")
        if not feature_ids:
            raise QueryError("MISSING_SELECTION", "Informe ao menos uma característica.")
        unknown = [ident for ident in feature_ids if ident not in {d["feature_id"] for d in indicator_registry.definitions()}]
        if unknown:
            raise QueryError("UNKNOWN_FEATURE", "Características desconhecidas no catálogo: " + ", ".join(unknown))
        if isinstance(page_size, bool) or not isinstance(page_size, int) or not 1 <= page_size <= MAX_PAGE_SIZE:
            raise QueryError("BAD_PAGE_SIZE", f"page_size deve estar entre 1 e {MAX_PAGE_SIZE}.")
        cut = _utc(as_of, "as_of")
        selection.update(as_of=_iso(cut), start=_iso(_utc(start, "start")) if start is not None else None,
                         end=_iso(_utc(end, "end")) if end is not None else None)
        digest = _selection_hash(selection)
        after = _decode_cursor(cursor, digest) if cursor else None
        base, context = _base(db)
        frames = [feature_store.query_as_of(db, cut, symbol=symbol, timeframe=timeframe, feature_id=ident,
                                            algorithm_version=algorithm_version, start=start, end=end)
                  for ident in sorted(feature_ids)]
        frame = pd.concat([f for f in frames if not f.empty], ignore_index=True) if any(
            not f.empty for f in frames) else pd.DataFrame()
        if len(frame) > MAX_SELECTION_ROWS:
            raise QueryError("SELECTION_TOO_LARGE", f"A seleção tem {len(frame)} fatos; limite "
                             f"{MAX_SELECTION_ROWS}. Reduza o intervalo com start/end.")
        rows = []
        if not frame.empty:
            frame = frame.assign(_obs=frame["observation_time"].map(_iso)).sort_values(
                ["feature_id", "_obs", "algorithm_version"], kind="stable")
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
        definitions = {}
        for ident in sorted(feature_ids):
            spec = indicator_contracts.contract(ident)
            definitions[ident] = {name: spec[name] for name in (
                "name", "family", "unit", "output_kind", "warmup_count", "availability_rule", "missing_rule",
                "alignment", "reference")}
            definitions[ident]["algorithm_versions"] = sorted({r["algorithm_version"] for r in rows
                                                               if r["feature_id"] == ident})
        reasons = []
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
                                                             rows[-1]["algorithm_version"]]) if more else None},
            "freshness": {"latest_available_at": max((r["available_at"] for r in rows), default=None)},
            "reasons": reasons, "errors": [],
        }
    except QueryError as exc:
        envelope = {"query_schema_version": QUERY_SCHEMA_VERSION, "status": "error", "selection": selection,
                    "values": [], "errors": [{"code": exc.code, "message": str(exc)}]}
    json.dumps(envelope, allow_nan=False)  # garante JSON sem NaN/infinito
    return envelope
