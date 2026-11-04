"""Contrato MEN: identidade e valores Q/P/N para o feature_store existente.

Não faz rede, classificação ou persistência implícitas. Textos/consultas privadas
não entram nos parâmetros exportáveis das definições nem nos valores preparados.
"""
import hashlib

from mt5_extracao import timeframes
from mt5_extracao.analytics import datasets, feature_store

CONTRACT_VERSION = 1
MAX_EXACT_COUNT = 2**53 - 1
MEASURES = {"quantity": "Quantidade", "positive": "Positivo", "negative": "Negativo"}
PUBLICATION_TYPES = {"original", "comment", "reply", "quote_with_comment"}


def _text(value, name, *, limit=256):
    if not isinstance(value, str) or not value.strip() or len(value) > limit or any(
            ord(c) < 32 for c in value):
        raise ValueError(f"{name} precisa ser texto não vazio, sem controles, até{limit} caracteres.")
    return value.strip()


def _object(value, required, optional, name):
    if not isinstance(value, dict) or set(value) - required - optional or required - set(value):
        raise ValueError(f"Campos inválidos em {name}; obrigatórios: {', '.join(sorted(required))}.")
    return dict(value)


def _labels(values, name):
    if not isinstance(values, (list, tuple)) or len(values) > 128:
        raise ValueError(f"{name} precisa ser uma lista com até128 textos.")
    return sorted({_text(v, name) for v in values})


def _hash(value):
    return hashlib.sha256(feature_store._canonical_json(value, "Contrato MEN").encode()).hexdigest()


def describe(db, *, symbol, timeframe, subject, source, query, classifier=None, universe="documents"):
    """Resolve três definições/versões e a família do journal sem registrar séries.

    O escopo completo participa do hash; parâmetros exportáveis conservam somente
    identidades públicas e fingerprints, não expressão, aliases ou filtros privados.
    """
    symbol = _text(symbol, "symbol")
    tf = timeframes.parse(timeframe)
    if tf is None or tf.minutes > 1440:
        raise ValueError("Menções exigem resolução fixa atéD1; semanas/meses precisam de contrato de calendário próprio.")
    subject = _object(subject, {"id", "kind", "label", "aliases", "mapping_version"},
                      {"include_terms", "exclude_terms"}, "subject")
    if not isinstance(subject["kind"], str) or subject["kind"] not in {"instrument", "company", "topic"}:
        raise ValueError("Objeto precisa ser instrument, company ou topic.")
    for key in ("id", "label", "mapping_version"):
        subject[key] = _text(subject[key], "subject."+key)
    for key in ("aliases", "include_terms", "exclude_terms"):
        subject[key] = _labels(subject.get(key, []), "subject."+key)
    if not subject["aliases"]:
        raise ValueError("Informe aliases explícitos; não há descoberta automática de objetos.")
    source = _object(source, {"provider", "corpus", "access_profile", "terms_version", "mode", "retention"},
                     {"publication_types"}, "source")
    for key in ("provider", "corpus", "access_profile", "terms_version", "retention"):
        source[key] = _text(source[key], "source."+key)
    if not isinstance(source["mode"], str) or source["mode"] not in {"documents", "aggregate"}:
        raise ValueError("Fonte precisa declarar documents ou aggregate.")
    source["publication_types"] = _labels(source.get("publication_types", sorted(PUBLICATION_TYPES)), "publication_types")
    if not source["publication_types"] or set(source["publication_types"]) - PUBLICATION_TYPES:
        raise ValueError("Tipos de publicação incompatíveis com a política MEN; repostagem simples não conta.")
    query = _object(query, {"id", "version", "expression"}, {"languages", "region", "authors", "channels", "communities"}, "query")
    for key in ("id", "version", "expression"):
        query[key] = _text(query[key], "query."+key, limit=4096 if key == "expression" else 256)
    for key in ("languages", "authors", "channels", "communities"):
        query[key] = _labels(query.get(key, []), "query."+key)
    query["region"] = _text(query["region"], "query.region") if query.get("region") is not None else None
    classifier = {"id": "unavailable", "version": "1", "languages": []} if classifier is None else classifier
    classifier = _object(classifier, {"id", "version", "languages"}, set(), "classifier")
    classifier["id"] = _text(classifier["id"], "classifier.id")
    classifier["version"] = _text(classifier["version"], "classifier.version")
    classifier["languages"] = _labels(classifier["languages"], "classifier.languages")
    if not isinstance(universe, str) or universe not in {"documents", "sample", "provider_total"}:
        raise ValueError("Universo precisa ser documents, sample ou provider_total; populações não são extrapoladas.")
    if universe == "provider_total" and source["mode"] != "aggregate":
        raise ValueError("Total do provedor exige fonte de agregados; documentos recuperados não são um censo.")
    scope = dict(contract_version=CONTRACT_VERSION, base_namespace=datasets.identity(db)["namespace"],
                 symbol=symbol, timeframe=tf.name, window_minutes=tf.minutes, subject=subject, source=source,
                 query=query, classifier=classifier, universe=universe, window_rule="[start,end); UTC epoch anchor",
                 deduplication="publication_id_per_subject; transport duplicates and simple reposts excluded")
    scope_hash = _hash(scope)
    version = f"1.0+men{CONTRACT_VERSION}."+scope_hash
    public = dict(contract_version=CONTRACT_VERSION, scope_hash=scope_hash, base_namespace=scope["base_namespace"],
                  subject_id=subject["id"], subject_kind=subject["kind"], mapping_version=subject["mapping_version"],
                  source_id=source["provider"], corpus=source["corpus"], query_id=query["id"], query_version=query["version"],
                  query_sha256=_hash(query), classifier=classifier, universe=universe,
                  window_minutes=tf.minutes, window_rule=scope["window_rule"], deduplication=scope["deduplication"])
    definitions = [dict(feature_id="mentions."+key, name=f"{label} · {subject['label']} · {source['provider']} · {query['id']}",
        algorithm_version=version, parameters=dict(mentions=public, measure=key), dependencies=["mentions:"+scope_hash],
        family="mentions", formula=f"Contagem {key} de publicações elegíveis distintas do mesmo universo e revisão",
        unit="publication/window", warmup_count=0, warmup_unit="windows", source_id=source["provider"],
        timeframe=tf.name, availability_rule="closed_covered_window_received; classification only when received; no implicit carry",
        value_type="NUMBER") for key, label in MEASURES.items()]
    return dict(scope=scope, scope_hash=scope_hash, algorithm_version=version,
                job_family="mentions:"+scope_hash, definitions=definitions)


def _utc(value, label):
    stamp = feature_store._utc_timestamp(value, label)
    if stamp.nanosecond:
        raise ValueError(f"{label} exige precisão de microssegundos, sem arredondamento.")
    return stamp


def _count(value, label):
    if value is not None and (type(value) is not int or not 0 <= value <= MAX_EXACT_COUNT):
        raise ValueError(f"{label} precisa ser contagem inteira entre0 e{MAX_EXACT_COUNT}; score/percentual não é contagem.")
    return value


def value_records(contract, *, start, end, captured_at, processed_at, population_revision, quantity=None,
                  positive=None, negative=None, coverage="complete", classification="pending", classified_at=None,
                  reason=None):
    """Prepara Q/P/N coerentes para uma única transação ``feature_store.save_values``.

    Publicação real/ingestão e journal pertencem aos incrementos seguintes. Q tem
    relógio próprio; sua metadata não incorpora classificação recebida no futuro.
    """
    scope = contract["scope"]
    if _hash(scope) != contract["scope_hash"] or contract["algorithm_version"] != f"1.0+men{CONTRACT_VERSION}."+_hash(scope):
        raise ValueError("Contrato MEN diverge da identidade declarada.")
    start, end, captured, processed = [_utc(v, k) for v, k in
        ((start, "start"), (end, "end"), (captured_at, "captured_at"), (processed_at, "processed_at"))]
    duration = scope["window_minutes"]*60*1_000_000_000
    if end.value-start.value != duration or start.value % duration:
        raise ValueError("Janela precisa ter a resolução declarada e âncora UTC; intervalo é [início,fim).")
    if processed < captured:
        raise ValueError("Processamento não pode anteceder a captura.")
    if (not isinstance(coverage, str) or coverage not in {"complete", "partial", "unavailable"} or
            not isinstance(classification, str) or classification not in {"complete", "pending", "unavailable"}):
        raise ValueError("Estado de cobertura/classificação desconhecido.")
    if coverage == "complete" and captured < end:
        raise ValueError("Janela aberta não pode ser publicada como cobertura completa.")
    revision = _text(population_revision, "population_revision")
    counts = {k: _count(v, k) for k, v in zip(MEASURES, (quantity, positive, negative))}
    if coverage != "complete":
        if any(v is not None for v in counts.values()):
            raise ValueError("Cobertura incompleta não publica contagens finais; conserve a quantidade parcial no recibo da captura.")
        reason = _text(reason, "Motivo da cobertura incompleta")
    elif quantity is None:
        raise ValueError("Cobertura completa exige Quantidade, inclusive zero observado.")
    if classification != "complete":
        if positive is not None or negative is not None:
            raise ValueError("Classificação pendente/indisponível não autoriza Positivo=Negativo=0.")
        if classified_at is not None:
            raise ValueError("Classificação não concluída não declara instante de conclusão.")
    elif (coverage != "complete" or positive is None or negative is None or positive+negative > quantity):
        raise ValueError("Tripla exige mesma população completa, P/N inteiros e P+N<=Q.")
    elif scope["classifier"]["id"] == "unavailable":
        raise ValueError("Classificação completa exige classificador/provedor versionado.")
    classified = _utc(classified_at, "classified_at") if classification == "complete" else None
    if classified is not None and classified < processed:
        raise ValueError("Classificação não pode anteceder o processamento da população.")
    q_at = processed
    common = dict(contract_version=CONTRACT_VERSION, scope_hash=contract["scope_hash"],
                  population_revision=revision, window_start=start.isoformat(), window_end=end.isoformat(),
                  coverage=coverage, universe=scope["universe"], captured_at=captured.isoformat(),
                  retrospective=True, reason=reason if coverage != "complete" else None)
    records = []
    for measure, value in counts.items():
        sentiment = measure != "quantity"
        available = classified if sentiment and classified is not None else q_at
        metadata = dict(common)
        if sentiment:
            metadata.update(classification=classification, classifier=scope["classifier"],
                            reason=(reason or "CLASSIFICATION_"+classification.upper()) if classification != "complete" else None)
        records.append(dict(symbol=scope["symbol"], timeframe=scope["timeframe"], feature_id="mentions."+measure,
            algorithm_version=contract["algorithm_version"], observation_time=start.isoformat(),
            value=value, available_at=available.isoformat(), observed_at=available.isoformat(),
            source_revision=revision, quality="VALID" if value is not None else "UNAVAILABLE", metadata=metadata))
    return records
