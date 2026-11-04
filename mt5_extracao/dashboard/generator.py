"""Fotografia de apresentação sobre leituras canônicas, sem cálculos/materialização.

Histórico: reconstrução conhecida no corte, nunca conhecimento em cada decisão.
Nenhuma consulta inicializa identidade, schema, produtor, backtest ou cache.
"""
from datetime import datetime, timezone
import hashlib
import json
import math

import pandas as pd
from sqlalchemy import inspect
from sqlalchemy.exc import SQLAlchemyError

from mt5_extracao.analytics import datasets, feature_store, query_service

SCHEMA_VERSION = 1
GENERATOR_VERSION = "DASH-02/1"
MAX_FACTS = 20_000
MAX_BYTES = 8 * 1024 * 1024
MAX_DAYS = 31

# Mapa do inventário, não um catálogo paralelo de indicadores ou fórmulas.
CAPABILITY_SOURCES = {
    "identity": "analytics.datasets.identity",
    "quality": "analytics.feature_store.query_history",
    "calendar": "analytics.session_structure / feature_store",
    "returns": "analytics.session_structure / feature_store",
    "volatility": "analytics.market_regimes / feature_store",
    "regime": "analytics.market_regimes / feature_store",
    "volume": "analytics.volume_profiles / feature_store",
    "relations": "analytics.query_service.relations / relation_dto",
    "studies": "laboratory.service",
    "hypotheses": "T13.4 (produtor pendente)",
    "validation": "strategies.validation / resultados persistidos",
    "robots": "robots.catalog / robots.store",
    "comparisons": "laboratory.financial / resultados comparáveis persistidos",
    "linked_sources": "laboratory.links / snapshots canônicos",
    "validity": "analytics.feature_store",
    "surprise": "Medida de surpresa comparável não encontrada no produtor",
}


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _stamp(value, label):
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError(f"{label} exige data e hora com fuso explícito.")
    return stamp.tz_convert("UTC")


def _iso(value):
    return None if value is None or pd.isna(value) else pd.Timestamp(value).isoformat()


def _selection(symbol, timeframe, as_of, start, end, versions):
    from mt5_extracao.timeframes import Timeframe
    if not isinstance(symbol, str) or not symbol.strip() or len(symbol) > 128:
        raise ValueError("Informe um ativo de até128 caracteres.")
    if timeframe not in {tf.name for tf in Timeframe}:
        raise ValueError("Escolha um timeframe nativo do catálogo.")
    cut = _stamp(as_of, "as_of")
    left = _stamp(start, "start") if start is not None else cut - pd.Timedelta(days=30)
    right = _stamp(end, "end") if end is not None else cut
    if left >= right or right > cut or right - left > pd.Timedelta(days=MAX_DAYS):
        raise ValueError("Use uma janela de até31 dias, início antes do fim e fim até as_of.")
    if not isinstance(versions, dict) or not all(isinstance(k, str) and k and isinstance(v, str) and v
                                              for k, v in versions.items()):
        raise ValueError("versions precisa mapear IDs para versões explícitas.")
    return {"mode": "asset", "symbol": symbol.strip(), "timeframe": timeframe,
            "start_utc": left.isoformat(), "end_utc": right.isoformat(),
            "as_of_utc": cut.isoformat(), "versions": dict(sorted(versions.items()))}, cut


def _capabilities():
    return {key: {"source": source, "state": "NOT_INTEGRATED", "reason_code": "READER_PENDING",
                  "reason": "Fonte identificada; leitor desta família ainda não integrado."}
            for key, source in CAPABILITY_SOURCES.items()}


def _unavailable_surprise(cut):
    reason = "O produtor ainda não declara uma medida de surpresa comparável; nenhum escore foi calculado."
    return {"id": "surprise", "kind": "surprise", "title": "Surpresa indisponível",
            "subject_ref": "subject", "source_refs": [], "as_of_utc": cut,
            "status_namespace": "dashboard", "source_status": None, "display_state": "UNAVAILABLE",
            "default_visible": True, "hidden_reason_code": None, "hidden_reason_short": None,
            "n_observations": None, "unit": None, "quality": None, "temporal": {},
            "evidence": {"value": None, "baseline": None, "reason_code": "SURPRISE_PRODUCER_MISSING"},
            "priority": {"policy": "pending-DASH-11", "score": None, "score_state": "UNAVAILABLE"},
            "visual": {"value": None, "series": [], "direction": None},
            "details": {"reason": reason}}


def _read(db, selection, cut):
    if db is None or getattr(db, "engine", None) is None:
        raise ValueError("BASE_UNAVAILABLE: base indisponível.")
    # O contexto e todas as leituras pertencem à mesma transação/imagem.
    with db.consistent_read() as conn:
        if not inspect(conn).has_table("_dataset_origin"):
            raise ValueError("BASE_IDENTITY_MISSING: declare a identidade pelo serviço de Dados antes da consulta.")
        base = datasets.identity(db, connection=conn)
        context = db.time_context()
        definitions = [d for d in query_service.definition_catalog(db, connection=conn)
                       if d.get("registered_definition") is not None]
        eligible = []
        for definition in definitions:
            registered = definition["registered_definition"]
            if registered.get("definition_schema_version", 1) not in {1, 2}:
                raise ValueError("DEFINITION_SCHEMA_UNKNOWN: contrato de definição não suportado.")
            if _stamp(registered["registered_at_utc"], "registered_at") > cut:
                continue  # Não antecipar um contrato registrado depois do corte.
            if registered.get("timeframe") not in {None, selection["timeframe"]}:
                continue
            eligible.append(definition)
        present = {d["feature_id"]: set() for d in eligible}
        for definition in eligible:
            present[definition["feature_id"]].add(definition["algorithm_version"])
        selected = selection["versions"]
        for ident, version in selected.items():
            if version not in present.get(ident, set()):
                raise ValueError("UNKNOWN_VERSION: versão ausente, fora do TF ou registrada após o corte.")
        ambiguous = [ident for ident, choices in present.items() if len(choices) > 1 and ident not in selected]
        if ambiguous:
            raise ValueError("AMBIGUOUS_VERSION: informe versions para " + ", ".join(sorted(ambiguous)))
        chosen = {ident: selected.get(ident, next(iter(choices))) for ident, choices in present.items()}
        definitions = [d for d in eligible if d["algorithm_version"] == chosen[d["feature_id"]]]
        history = feature_store.query_history(db, symbol=selection["symbol"], timeframe=selection["timeframe"],
                     start=selection["start_utc"], end=selection["end_utc"], connection=conn, max_rows=MAX_FACTS)
    # O histórico contém revisões futuras. Elas nunca entram na fotografia.
    if not history.empty:
        clocks = ["observation_time", "available_at_utc", "observed_at_utc"]
        if history[clocks].isna().any().any():
            raise ValueError("INVALID_CLOCK: fato com relógio obrigatório inválido.")
        history = history[(history["available_at_utc"] <= cut) & (history["observed_at_utc"] <= cut)
                          & (history["observation_time"] <= cut)]
        history = history[history.apply(lambda r: chosen.get(r.feature_id) == r.algorithm_version, axis=1)]
        # Mesma precedência canônica de query_as_of, conservando vencidos para auditoria.
        history = history.sort_values(["available_at_utc", "observed_at_utc", "revision"])
        history = history.drop_duplicates(["feature_id", "algorithm_version", "observation_time"], keep="last")
        history = history.sort_values(["feature_id", "algorithm_version", "observation_time"])
    contracts = [{k: d.get(k) for k in ("feature_id", "algorithm_version", "name", "family", "formula", "unit",
                 "timeframe", "availability_rule", "value_type", "output_kind", "metadata_state", "name_status")}
                 for d in definitions]
    facts = []
    for row in history.to_dict("records"):
        number = row["numeric_value"]
        if pd.notna(number) and not math.isfinite(number):
            raise ValueError("NON_FINITE_FACT: valor numérico inválido na origem.")
        metadata = json.loads(row["metadata_json"])
        if not isinstance(metadata, dict):
            raise ValueError("INVALID_METADATA: metadados do fato precisam ser objeto JSON.")
        if pd.isna(number) and row["categorical_value"] is None and row["quality"] not in {"UNAVAILABLE", "INVALIDATED"}:
            raise ValueError("INVALID_SOURCE: fato válido sem valor; confira o produtor.")
        until = row["valid_until_utc"]
        state = ("INVALIDATED" if row["quality"] == "INVALIDATED" else
                 "STALE" if pd.notna(until) and until <= cut else
                 "AVAILABLE" if row["quality"] == "VALID" else "UNAVAILABLE")
        facts.append({"feature_id": row["feature_id"], "algorithm_version": row["algorithm_version"],
                      "value": float(number) if pd.notna(number) else None,
                      "category": row["categorical_value"], "quality": row["quality"], "display_state": state,
                      "observation_time": _iso(row["observation_time"]),
                      "available_at_utc": _iso(row["available_at_utc"]),
                      "observed_at_utc": _iso(row["observed_at_utc"]), "valid_until_utc": _iso(until),
                      "revision": int(row["revision"]), "source_revision": row["source_revision"],
                      "reconstructed": metadata.get("reconstructed"),
                      "minute_of_session": metadata.get("minute_of_session"),
                      "source_window_bars": metadata.get("source_window_bars")})
    return base, context, contracts, facts


def generate(db, *, symbol, timeframe, as_of, start=None, end=None, versions=None, fixture_label=None):
    """Gera um envelope estrito ou erro explícito; não retorna resultados parciais silenciosos."""
    selection, cut = _selection(symbol, timeframe, as_of, start, end, versions if versions is not None else {})
    if fixture_label is not None and (not isinstance(fixture_label, str) or not fixture_label.strip()
                                      or len(fixture_label) > 128):
        raise ValueError("fixture_label precisa identificar a fixture em até128 caracteres.")
    data = {"schema_version": SCHEMA_VERSION, "snapshot_id": None,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(), "as_of_utc": cut.isoformat(),
            "data_cutoff_utc": None, "subject": {"mode": "asset", "symbol": selection["symbol"],
            "timeframe": timeframe}, "selection": selection,
            "provenance": {"generator_version": GENERATOR_VERSION, "history_semantics": "reconstructed_at_cut",
                           "fixture_label": fixture_label,
                           "data_cutoff_semantics": "latest_observation_time",
                           "read_consistency": "DatabaseManager.consistent_read",
                           "limits": {"history_rows": MAX_FACTS, "days": MAX_DAYS, "payload_bytes": MAX_BYTES}},
            "capabilities": _capabilities(), "sections": [_unavailable_surprise(cut.isoformat())],
            "alerts": [], "baseline_snapshot_id": None, "contracts": [], "facts": [], "errors": []}
    try:
        base, context, contracts, facts = _read(db, selection, cut)
        data["subject"]["base_namespace"] = base["namespace"]
        data["provenance"].update(base=base, time_context=context)
        data.update(contracts=contracts, facts=facts)
        data["data_cutoff_utc"] = max((r["observation_time"] for r in facts), default=None)
        data["capabilities"]["identity"].update(state="AVAILABLE", reason_code=None, reason="Identidade lógica preservada.")
        data["capabilities"]["quality"].update(state="AVAILABLE" if facts else "NO_DATA", reason_code=None,
                                               reason="Qualidade e clocks preservados por fato; sem certificar a fonte.")
    except (ValueError, SQLAlchemyError) as exc:
        # Erros SQL nunca expõem URL, credenciais, query ou caminho do banco.
        message = str(exc) if isinstance(exc, ValueError) else "BASE_UNAVAILABLE: não foi possível ler a base."
        code = message.split(":", 1)[0] if ":" in message else "INVALID_SOURCE"
        data["errors"] = [{"code": code, "message": message}]
    data["capabilities"]["surprise"].update(state="DEPENDENCY_PENDING", reason_code="SURPRISE_PRODUCER_MISSING",
        reason=data["sections"][0]["details"]["reason"])
    data["capabilities"]["hypotheses"].update(state="DEPENDENCY_PENDING", reason_code="T13_4_PENDING",
        reason="Catálogo de hipóteses estatísticas ainda não implementado; bateria de robô não é aprovação estatística.")
    return seal(data)


def seal(data):
    data = json.loads(_json(data))
    data.pop("integrity", None)
    data["snapshot_id"] = None
    digest = hashlib.sha256(_json(data).encode()).hexdigest()
    data["snapshot_id"] = digest
    data["integrity"] = {"algorithm": "sha256", "scope": "canonical JSON excluding integrity, snapshot_id=null",
                         "digest": digest}
    if len(_json(data).encode()) > MAX_BYTES:
        raise ValueError("PAYLOAD_TOO_LARGE: fotografia excede8MiB; reduza a janela explicitamente.")
    return data


def validate(data):
    if not isinstance(data, dict) or data.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("Schema de fotografia não suportado.")
    required = {"snapshot_id", "selection", "subject", "provenance", "as_of_utc", "generated_at_utc",
                "capabilities", "sections", "alerts", "facts", "contracts", "errors", "integrity"}
    if not required <= data.keys():
        raise ValueError("Fotografia incompleta.")
    expected = seal(data)
    if data.get("integrity") != expected["integrity"] or data["snapshot_id"] != expected["snapshot_id"]:
        raise ValueError("Digest da fotografia divergente.")
    if len({s["id"] for s in data["sections"]}) != len(data["sections"]):
        raise ValueError("IDs de faixa duplicados.")
    cut = _stamp(data["as_of_utc"], "as_of")
    selection = data["selection"]
    if (data["subject"].get("mode") != "asset" or selection.get("mode") != "asset"
            or selection.get("symbol") != data["subject"].get("symbol")
            or selection.get("timeframe") != data["subject"].get("timeframe")
            or _stamp(selection["as_of_utc"], "selection.as_of") != cut):
        raise ValueError("Identidade da seleção diverge da fotografia.")
    states = {"AVAILABLE", "INSUFFICIENT", "UNAVAILABLE", "STALE", "INVALIDATED", "NOT_APPLICABLE"}
    for section in data["sections"]:
        fields = {"id", "kind", "title", "subject_ref", "source_refs", "as_of_utc", "status_namespace",
                  "source_status", "display_state", "default_visible", "hidden_reason_code", "hidden_reason_short",
                  "n_observations", "unit", "quality", "temporal", "evidence", "priority", "visual", "details"}
        if not fields <= section.keys() or section["display_state"] not in states:
            raise ValueError("Contrato de faixa inválido.")
        if _stamp(section["as_of_utc"], "section.as_of") != cut:
            raise ValueError("Faixa pertence a outro corte.")
    contracts = {(d["feature_id"], d["algorithm_version"]) for d in data["contracts"]}
    for fact in data["facts"]:
        if (fact["feature_id"], fact["algorithm_version"]) not in contracts:
            raise ValueError("Fato sem contrato capturado.")
        observed = _stamp(fact["observation_time"], "observation")
        available = _stamp(fact["available_at_utc"], "available")
        received = _stamp(fact["observed_at_utc"], "observed_at")
        if not observed <= available <= cut or received > available:
            raise ValueError("Fato futuro ou relógios incompatíveis.")
        if not _stamp(selection["start_utc"], "start") <= observed < _stamp(selection["end_utc"], "end"):
            raise ValueError("Fato fora da janela capturada.")
        if fact["display_state"] not in states:
            raise ValueError("Estado do fato desconhecido.")
    return data
