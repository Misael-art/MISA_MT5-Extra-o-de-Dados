"""BD19c2: transporte de um recibo e suas origens nos stores canônicos."""
import json
from datetime import datetime, timezone
from pathlib import Path
import time

from mt5_extracao.analytics import datasets, feature_store, indicator_materializer, producers

FORMAT = "mt5x.feature-relation.v1"
MAX_PACKAGE_BYTES = 64 * 1024 * 1024


def _guard(*, timeout_seconds, max_memory_mb, cancel_check, write_guard=None):
    for value, ceiling in ((timeout_seconds, producers.MAX_JOB_TIMEOUT_SECONDS),
                           (max_memory_mb, producers.MAX_JOB_MEMORY_MB)):
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 < value <= ceiling:
            raise ValueError("Timeout/memória fora dos limites existentes dos jobs Analytics.")
    run = dict(started_monotonic=time.monotonic())
    def check(connection=None):
        if cancel_check is not None and cancel_check():
            raise indicator_materializer.Cancelled("Transferência de relação cancelada; nenhum pacote parcial publicado.")
        if write_guard is not None and (reason := write_guard()):
            raise producers.WriteBlocked(f"Importação de relação bloqueada: {reason}.")
        producers._check_limits(run, timeout_seconds, max_memory_mb)
    check()
    return check


def _receipt(receipt):
    if not isinstance(receipt, dict) or set(receipt) != {"analysis_id", "payload_sha256", "created_at_utc", "relation"}:
        raise ValueError("Recibo de relação incompleto; exporte novamente o original.")
    dto, raw, digest, semantic = feature_store._relation_payload(receipt["relation"])
    if receipt["analysis_id"] != dto["provenance"]["analysis_id"] or receipt["payload_sha256"] != digest:
        raise ValueError("Recibo e resultado divergem; preserve o original.")
    try:
        stamp = datetime.fromisoformat(receipt["created_at_utc"].replace("Z", "+00:00"))
        if stamp.tzinfo is None or stamp.utcoffset() != timezone.utc.utcoffset(stamp):
            raise ValueError("horário sem UTC")
    except (ValueError, TypeError, AttributeError) as exc:
        raise ValueError("Recibo sem data UTC de publicação válida.") from exc
    return dto, raw, digest, semantic


def _content(package):
    return {key: value for key, value in package.items() if key != "package_sha256"}


def _validate(package, guard):
    try:
        if (not isinstance(package, dict) or set(package) != {"format", "receipt", "snapshots", "package_sha256"}
                or package["format"] != FORMAT or not isinstance(package["snapshots"], dict)):
            raise ValueError("Formato de pacote desconhecido.")
        if datasets._hash(datasets._json(_content(package))) != package["package_sha256"]:
            raise ValueError("Hash do pacote divergente.")
        payload = _receipt(package["receipt"])
        dto = payload[0]
        if set(package["snapshots"]) != set(dto["provenance"]["lineage_manifests"]):
            raise ValueError("Pacote precisa conter exatamente todas as origens declaradas.")
        images = {}
        for ident, bundle in package["snapshots"].items():
            guard();images[ident] = datasets._validated(bundle);guard()
            if ident != images[ident].manifest_id:
                raise ValueError("Identificador de snapshot divergente.")
        feature_store._relation_sources(None, dto, snapshots=images)
        root = images[dto["provenance"]["snapshot_manifest_id"]]
        definitions = {(d["feature_id"], d["algorithm_version"]): d for d in root.feature_definitions}
        if (dto["provenance"]["source"] != root.manifest["source"] or any(
                definitions.get((dto[s]["feature_id"], dto[s]["algorithm_version"])) != dto[s]["definition"]
                for s in ("left", "right"))):
            raise ValueError("Contratos do par divergem da captura congelada.")
        guard()
        return payload
    except (KeyError, TypeError, AttributeError, IndexError) as exc:
        raise ValueError("Pacote de relação incompleto; recupere uma exportação íntegra.") from exc


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:raise ValueError("Chave JSON duplicada; recupere uma exportação íntegra.")
        result[key] = value
    return result


def export_relation(db, analysis_id, destination, *, cancel_check=None, timeout_seconds=120, max_memory_mb=2048):
    """Exporta bytes originais e snapshots declarados, sem consultar fatos live."""
    # O publicador já utilizado pelo pacote Manual usa hardlink atômico, fsync,
    # arquivo temporário no mesmo filesystem e nunca sobrescreve o destino.
    from mt5_extracao.presets.manual import _publish
    guard = _guard(timeout_seconds=timeout_seconds, max_memory_mb=max_memory_mb, cancel_check=cancel_check)
    receipt = feature_store._load_pair_relation_receipt(db, analysis_id)
    package = dict(format=FORMAT, receipt=receipt, snapshots={})
    for ident in receipt["relation"]["provenance"]["lineage_manifests"]:
        guard();package["snapshots"][ident] = datasets._bundle(db, ident)
    package["package_sha256"] = datasets._hash(datasets._json(package))
    _validate(package, guard)
    data = datasets._json(package).encode("utf-8")
    if len(data) > MAX_PACKAGE_BYTES:
        raise ValueError("Pacote excede64MiB; exporte uma relação com janela menor.")
    guard();path = Path(destination);path.parent.mkdir(parents=True, exist_ok=True)
    _publish(path, data)
    return dict(path=str(path), analysis_id=analysis_id, package_sha256=package["package_sha256"],
        payload_sha256=receipt["payload_sha256"], snapshots=len(package["snapshots"]), bytes=len(data))


def import_relation(db, source, *, cancel_check=None, write_guard=None, timeout_seconds=120, max_memory_mb=2048):
    """Valida tudo antes da escrita; snapshots, resultado e referências num commit."""
    guard = _guard(timeout_seconds=timeout_seconds, max_memory_mb=max_memory_mb,
                   cancel_check=cancel_check, write_guard=write_guard)
    try:
        path = Path(source)
        if not path.is_file():raise ValueError("Escolha um arquivo de pacote de relação.")
        with path.open("rb") as handle:data = handle.read(MAX_PACKAGE_BYTES + 1)
        if len(data) > MAX_PACKAGE_BYTES:
            raise ValueError("Pacote excede64MiB; confira o arquivo ou exporte uma janela menor.")
        guard();package = json.loads(data, object_pairs_hook=_unique_object)
    except (OSError, UnicodeError, json.JSONDecodeError, RecursionError) as exc:
        raise ValueError("Arquivo de relação ilegível; recupere uma exportação íntegra.") from exc
    dto, raw, digest, semantic = _validate(package, guard)
    guard();datasets._schema(db);feature_store._relation_schema(db)
    with db.engine.begin() as conn:
        if conn.dialect.name == "sqlite" and not conn.connection.driver_connection.in_transaction:
            conn.exec_driver_sql("BEGIN IMMEDIATE")
        guard(conn)
        for bundle in package["snapshots"].values():
            datasets._persist_validated(conn, bundle, guard=guard)
        return feature_store._save_pair_relation(conn, dto, raw, digest, semantic, db=db,
            created_at_utc=package["receipt"]["created_at_utc"], transaction_guard=guard)
