"""Store canônico aditivo: protocolos, pedidos, execuções e eventos de exposição."""
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import threading
import uuid

import pandas as pd
from sqlalchemy import inspect, text

from .schema import encoded, digest

_SCHEMA_LOCK = threading.RLock()


def now():
    return datetime.now(timezone.utc).isoformat()


def ensure_schema(db):
    with _SCHEMA_LOCK, db.engine.begin() as conn:
        if db.dialect == "postgresql":
            conn.execute(text("SELECT pg_advisory_xact_lock(20201201)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS _experiment_lock (id INTEGER PRIMARY KEY, epoch BIGINT NOT NULL)"))
        conn.execute(text("INSERT INTO _experiment_lock(id,epoch) VALUES(1,0) ON CONFLICT(id) DO NOTHING"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS _experiment_protocols (protocol_id TEXT PRIMARY KEY, name TEXT NOT NULL, "
                          "revision INTEGER NOT NULL, payload_json TEXT NOT NULL, context_json TEXT NOT NULL, created_at TEXT NOT NULL, UNIQUE(name,revision))"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS _experiment_requests (request_id TEXT PRIMARY KEY, protocol_id TEXT NOT NULL, "
                          "alternative_id TEXT NOT NULL, payload_json TEXT NOT NULL, state TEXT NOT NULL, run_id TEXT, "
                          "batch_id TEXT NOT NULL, error TEXT NOT NULL, created_at TEXT NOT NULL)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS _experiment_executions (execution_id TEXT PRIMARY KEY, request_id TEXT NOT NULL, "
                          "run_id TEXT NOT NULL, state TEXT NOT NULL, identity_json TEXT NOT NULL, observation_id TEXT, error TEXT NOT NULL, created_at TEXT NOT NULL)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS _experiment_events (event_id TEXT PRIMARY KEY, event_seq BIGINT NOT NULL UNIQUE, "
                          "protocol_id TEXT NOT NULL, request_id TEXT NOT NULL, kind TEXT NOT NULL, payload_json TEXT NOT NULL, created_at TEXT NOT NULL)"))


@contextmanager
def transaction(db):
    ensure_schema(db)
    with db.engine.begin() as conn:
        lock(conn)
        yield conn


def lock(conn):
    # UPDATE serializa pedidos e acessos também entre processos e no PostgreSQL.
    conn.execute(text("UPDATE _experiment_lock SET epoch=epoch+1 WHERE id=1"))


def event(conn, event_id, protocol_id, request_id, kind, payload):
    old = conn.execute(text("SELECT kind,payload_json FROM _experiment_events WHERE event_id=:i"), {"i": event_id}).mappings().first()
    raw = encoded(payload)
    if old:
        if old["kind"] != kind or old["payload_json"] != raw:
            raise ValueError("Evento idempotente contém outro conteúdo; use outro identificador.")
        return
    seq = conn.execute(text("SELECT COALESCE(MAX(event_seq),0)+1 FROM _experiment_events")).scalar_one()
    conn.execute(text("INSERT INTO _experiment_events VALUES(:i,:s,:p,:r,:k,:v,:t)"),
                 dict(i=event_id, s=seq, p=protocol_id, r=request_id, k=kind, v=raw, t=now()))


def protocol(db, protocol_id, conn=None):
    if conn is None:
        if not inspect(db.engine).has_table("_experiment_protocols"):
            raise ValueError("Protocolo não registrado.")
        with db.engine.connect() as connection:
            return protocol(db, protocol_id, connection)
    row = conn.execute(text("SELECT * FROM _experiment_protocols WHERE protocol_id=:i"), {"i": protocol_id}).mappings().first()
    if row is None:
        raise ValueError("Protocolo não registrado.")
    return dict(protocol_id=row["protocol_id"], definition=json.loads(row["payload_json"]), context=json.loads(row["context_json"]))


def put_protocol(db, revision, context):
    definition = revision.to_dict()
    with transaction(db) as conn:
        conn.execute(text("INSERT INTO _experiment_protocols VALUES(:i,:n,:r,:p,:c,:t) ON CONFLICT DO NOTHING"),
                     dict(i=revision.protocol_id, n=definition["name"], r=definition["revision"], p=encoded(definition), c=encoded(context), t=now()))
        row = conn.execute(text("SELECT payload_json,context_json FROM _experiment_protocols WHERE protocol_id=:i"), {"i": revision.protocol_id}).first()
        if row is None or row[0] != encoded(definition) or row[1] != encoded(context):
            raise ValueError("Revisão de protocolo imutável; declare outra revisão para mudar conteúdo.")
        event(conn, "protocol:"+revision.protocol_id, revision.protocol_id, "", "protocol_registered", {})
    return protocol(db, revision.protocol_id)


def request(db, request_id, conn=None):
    if conn is None:
        if not inspect(db.engine).has_table("_experiment_requests"):
            raise ValueError("Pedido não registrado.")
        with db.engine.connect() as connection:
            return request(db, request_id, connection)
    row = conn.execute(text("SELECT * FROM _experiment_requests WHERE request_id=:i"), {"i": request_id}).mappings().first()
    if row is None:
        raise ValueError("Pedido não registrado.")
    result = dict(row)
    result["payload"] = json.loads(result.pop("payload_json"))
    return result


def overlapping(left, right):
    if left["origin_key"] != right["origin_key"]:
        return False
    try:
        return max(pd.Timestamp(left["first"]), pd.Timestamp(right["first"])) <= min(pd.Timestamp(left["last"]), pd.Timestamp(right["last"]))
    except TypeError:
        return True  # Relógios incompatíveis não provam ausência de exposição.


def accesses(conn, scopes):
    rows = conn.execute(text("SELECT payload_json FROM _experiment_events WHERE kind='data_access' ORDER BY event_seq")).scalars()
    values = [json.loads(raw) for raw in rows]
    return [value for value in values if any(overlapping(scope, value["scope"]) for scope in scopes)]


def access(conn, protocol_id, role, scope, key, purpose, request_id=""):
    if purpose not in ("evaluation", "tuning", "proposal_after_access"):
        raise ValueError("Finalidade de acesso desconhecida.")
    event(conn, "access:"+digest([protocol_id, key]), protocol_id, request_id, "data_access",
          dict(role=role, scope=scope, purpose=purpose, request_id=request_id))


def put_request(db, protocol_id, key, payload, alternative_id, error="", discard=False):
    request_id = digest([protocol_id, key])
    with transaction(db) as conn:
        old = conn.execute(text("SELECT request_id FROM _experiment_requests WHERE request_id=:i"), {"i": request_id}).first()
        if old:
            current = request(db, request_id, conn)
            if current["payload"] != payload:
                raise ValueError("Chave de pedido já usada com outro conteúdo; use uma nova chave deliberada.")
            return current
        p = protocol(db, protocol_id, conn)
        seen = conn.execute(text("SELECT request_id FROM _experiment_requests WHERE alternative_id=:a"), {"a": alternative_id}).first()
        if not seen:
            for role, scope in p["context"]["roles"].items():
                if accesses(conn, [scope]):
                    access(conn, protocol_id, role, scope, "proposal:"+request_id+":"+role, "proposal_after_access", request_id)
        state = "rejected" if error else "discarded" if discard else "requested"
        conn.execute(text("INSERT INTO _experiment_requests VALUES(:i,:p,:a,:v,:s,NULL,:b,:e,:t)"),
                     dict(i=request_id, p=protocol_id, a=alternative_id, v=encoded(payload), s=state,
                          b="experiment_"+request_id, e=error, t=now()))
        event(conn, "request:"+request_id, protocol_id, request_id, "request_registered", dict(state=state, origin=payload["origin"], error=error))
    return request(db, request_id)


def put_pattern_attempt(db, protocol_id, key, payload, *, discard_reason=""):
    """Reserva uma alternativa T13.4 sob o lock e o teto da campanha existente."""
    request_id = digest([protocol_id, key])
    with transaction(db) as conn:
        old = conn.execute(text("SELECT request_id FROM _experiment_requests WHERE request_id=:i"), {"i": request_id}).first()
        if old:
            current = request(db, request_id, conn)
            original = {key: value for key, value in current["payload"].items()
                        if key not in ("planned", "prior_holdout_exposure")}
            if original != payload:
                raise ValueError("Chave de tentativa já usada com outro conteúdo.")
            return current
        campaign = protocol(db, protocol_id, conn)
        definition = campaign["definition"]
        if definition.get("type") != "pattern_campaign_v1":
            raise ValueError("Tentativa de padrão exige campanha T13.4 registrada.")
        alternative_id = digest([protocol_id, payload["definition_hash"], payload["candidate_id"]])
        admitted = conn.execute(text("SELECT DISTINCT alternative_id FROM _experiment_requests "
                                     "WHERE protocol_id=:p AND state NOT IN ('rejected','discarded')"),
                                {"p": protocol_id}).scalars().all()
        planned = payload["definition_hash"] in definition["grid"]
        scopes = campaign["context"]["campaign_roles"]["holdout"]
        exposed = bool(accesses(conn, scopes))
        error = ("Reserva holdout fechada para busca; registre o acesso e aguarde gate temporal." if
                 payload["role"] == "holdout" and (payload["origin"] == "automatic" or payload["purpose"] == "tuning") else
                 "Nova busca automática após exposição da reserva recusada." if payload["origin"] == "automatic" and exposed else
                 "Alternativa automática fora da grade congelada." if payload["origin"] == "automatic" and not planned else
                 "Orçamento agregado de tentativas esgotado." if alternative_id not in admitted and
                 len(admitted) >= definition["budget"]["max_attempts"] else "")
        stored = dict(payload, planned=planned, prior_holdout_exposure=exposed)
        state = "rejected" if error else "discarded" if discard_reason else "requested"
        conn.execute(text("INSERT INTO _experiment_requests VALUES(:i,:p,:a,:v,:s,NULL,:b,:e,:t)"),
                     dict(i=request_id, p=protocol_id, a=alternative_id, v=encoded(stored), s=state,
                          b="experiment_" + request_id, e=error, t=now()))
        event(conn, "request:" + request_id, protocol_id, request_id, "request_registered",
              dict(state=state, origin=payload["origin"], error=error, planned=planned))
        if exposed:
            event(conn, "proposal_after_access:" + request_id, protocol_id, request_id,
                  "proposal_after_access", dict(role="holdout", request_id=request_id))
    return request(db, request_id)


def pattern_role_access(db, protocol_id, role, key, purpose):
    """Registra acesso a cada fonte antes de entregar resultados de um papel."""
    with transaction(db) as conn:
        campaign = protocol(db, protocol_id, conn)
        if campaign["definition"].get("type") != "pattern_campaign_v1" or role not in campaign["context"]["campaign_roles"]:
            raise ValueError("Campanha ou papel não registrado.")
        scopes = campaign["context"]["campaign_roles"][role]
        for scope in scopes:
            access(conn, protocol_id, role, scope, key + ":" + digest(scope), purpose)
    return dict(recorded=True, role=role, scopes=len(scopes), qualified_final_holdout=False)


def persist_pattern_evaluation(db, protocol_id, request_id, receipt, records):
    """Grava revisão de eventos no feature_store e recibo no ledger na mesma transação."""
    from mt5_extracao.analytics import feature_store

    if not isinstance(receipt, dict) or not isinstance(records, list):
        raise ValueError("Avaliação e fatos precisam de objetos explícitos.")
    if receipt.get("protocol_id") != protocol_id or receipt.get("request_id") != request_id:
        raise ValueError("Recibo de avaliação diverge da tentativa registrada.")
    suffix = digest([request_id, receipt["data_manifest_id"]])
    signal_id, outcome_id = "pattern.signal." + suffix, "pattern.outcome." + suffix
    version = "1"
    for feature_id, kind in ((signal_id, "signal"), (outcome_id, "outcome")):
        feature_store.register_definition(
            db, feature_id, "Padrão de pesquisa: " + kind, version,
            {"protocol_id": protocol_id, "request_id": request_id,
             "data_manifest_id": receipt["data_manifest_id"], "source_namespace": receipt["source_namespace"],
             "kind": kind},
            [{"kind": "snapshot", "data_manifest_id": receipt["data_manifest_id"]}],
            family="pattern_" + kind, source_id=receipt["data_manifest_id"],
            timeframe=receipt["timeframe"], availability_rule="closed_bar_then_received_outcome",
            value_type="CATEGORY")
    prepared = []
    for item in records:
        if item.get("feature_id") not in {signal_id, outcome_id} or item.get("algorithm_version") != version:
            raise ValueError("Fato de outra avaliação ou definição no lote.")
        prepared.append(item)
    ensure_schema(db)
    event_id = "pattern_evaluation:" + digest([request_id, receipt["data_manifest_id"], receipt["role"]])

    def finish(conn, _counts):
        lock(conn)
        attempt = request(db, request_id, conn)
        if attempt["protocol_id"] != protocol_id or attempt["state"] != "requested":
            raise ValueError("Só tentativa registrada e admitida pode receber avaliação.")
        event(conn, event_id, protocol_id, request_id, "pattern_evaluated", receipt)

    counts = feature_store.save_values(db, prepared, transaction_hook=finish)
    return dict(signal_feature_id=signal_id, outcome_feature_id=outcome_id,
                counts=counts, receipt=receipt)


def pattern_evaluations(db, protocol_id):
    """Consulta somente recibos persistidos; consumidores não recalculam pesquisa."""
    return [row["payload"] for row in status(db, protocol_id)["events"]
            if row["kind"] == "pattern_evaluated"]


def start(conn, item, run_id, identity, execution_id):
    conn.execute(text("INSERT INTO _experiment_executions VALUES(:i,:r,:run,'running',:v,NULL,'',:t)"),
                 dict(i=execution_id, r=item["request_id"], run=run_id, v=encoded(identity), t=now()))
    conn.execute(text("UPDATE _experiment_requests SET state='running',run_id=:run,error='' WHERE request_id=:i"), dict(i=item["request_id"], run=run_id))
    event(conn, "execution:"+execution_id, item["protocol_id"], item["request_id"], "execution_admitted", dict(execution_id=execution_id, run_id=run_id))


def finish(conn, item, state, run_id=None, execution_id=None, identity=None, error=""):
    conn.execute(text("UPDATE _experiment_requests SET state=:s,run_id=:r,error=:e WHERE request_id=:i"),
                 dict(s=state, r=run_id, e=error, i=item["request_id"]))
    if execution_id:
        conn.execute(text("UPDATE _experiment_executions SET state=:s,observation_id=:o,error=:e WHERE execution_id=:i"),
                     dict(s=state, o=identity.get("digest") if identity and identity.get("cache_eligible") else None, e=error, i=execution_id))
    event(conn, "result:"+(execution_id or uuid.uuid4().hex), item["protocol_id"], item["request_id"], state,
          dict(run_id=run_id, execution_id=execution_id, observation_id=identity.get("digest") if identity and identity.get("cache_eligible") else None, error=error))


def status(db, protocol_id):
    p = protocol(db, protocol_id)
    with db.engine.connect() as conn:
        requests = [request(db, value, conn) for value in conn.execute(text("SELECT request_id FROM _experiment_requests WHERE protocol_id=:i ORDER BY created_at,request_id"), {"i": protocol_id}).scalars()]
        events = [dict(row) for row in conn.execute(text("SELECT * FROM _experiment_events WHERE protocol_id=:i ORDER BY event_seq"), {"i": protocol_id}).mappings()]
        exposures = {}
        roles = p["context"].get("campaign_roles", p["context"].get("roles", {}))
        for role, scope in roles.items():
            found = accesses(conn, scope if isinstance(scope, list) else [scope])
            exposures[role] = dict(status="used_for_tuning" if any(v["purpose"] != "evaluation" for v in found) else "access_recorded" if found else "no_recorded_access",
                                   recorded_accesses=len(found), prior_external_exposure="unknown", qualified_final_holdout=False)
    for row in events:
        row["payload"] = json.loads(row.pop("payload_json"))
    return dict(protocol=p, requests=requests, events=events, exposure=exposures,
                statistical_approval=False, qualified_final_holdout=False, ledger_qualified=False)


def list_protocols(db):
    """Índice de metadados para descoberta; não cria schema nem entrega resultados."""
    if not inspect(db.engine).has_table("_experiment_protocols"):
        return []
    with db.engine.connect() as conn:
        return [dict(row) for row in conn.execute(text(
            "SELECT protocol_id,name,revision,created_at FROM _experiment_protocols "
            "ORDER BY created_at DESC,protocol_id")).mappings()]
