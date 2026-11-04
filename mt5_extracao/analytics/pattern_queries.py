"""Leitura comum das avaliações T13.4 persistidas no ledger de experimentos."""

import json

import pandas as pd
from sqlalchemy import inspect, text

from . import datasets, feature_store
from mt5_extracao.experiments.schema import digest


def _cut(as_of):
    if as_of is None:
        return None
    stamp = pd.Timestamp(as_of)
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError("as_of exige instante com fuso explícito.")
    return stamp.tz_convert("UTC")


def evaluations(db, *, protocol_id=None, symbol=None, timeframe=None, as_of=None):
    """Lê recibos já gravados; identidade da base e disponibilidade são obrigatórias."""
    cut = _cut(as_of)
    if not inspect(db.engine).has_table("_experiment_events"):
        return []
    namespace = datasets.identity(db)["namespace"]
    statement = ("SELECT protocol_id,request_id,payload_json,created_at,event_seq "
                 "FROM _experiment_events WHERE kind='pattern_evaluated' ")
    params = {}
    if protocol_id is not None:
        statement += "AND protocol_id=:protocol_id "
        params["protocol_id"] = protocol_id
    if cut is not None:
        statement += "AND created_at<=:cut "
        params["cut"] = cut.isoformat()
    statement += "ORDER BY event_seq"
    with db.engine.connect() as conn:
        rows = conn.execute(text(statement), params).mappings().all()
    result = []
    for row in rows:
        receipt = json.loads(row["payload_json"])
        if receipt.get("source_namespace") != namespace:
            raise ValueError("SOURCE_NAMESPACE_MISMATCH: avaliação pertence a outra base.")
        if receipt.get("protocol_id") != row["protocol_id"] or receipt.get("request_id") != row["request_id"]:
            raise ValueError("INVALID_SOURCE: identidade do recibo diverge do ledger.")
        if symbol is not None and receipt.get("symbol") != symbol:
            continue
        if timeframe is not None and receipt.get("timeframe") != timeframe:
            continue
        suffix = digest([row["request_id"], receipt["data_manifest_id"]])
        result.append({"receipt": receipt, "available_at_utc": row["created_at"],
                       "signal_feature_id": "pattern.signal." + suffix,
                       "outcome_feature_id": "pattern.outcome." + suffix,
                       "source_namespace": namespace})
    return result


def evaluation(db, protocol_id, request_id, data_manifest_id, *, as_of=None, include_facts=False):
    """Inspeciona um recibo exato e, opcionalmente, seus fatos no mesmo corte."""
    matches = [row for row in evaluations(db, protocol_id=protocol_id, as_of=as_of)
               if row["receipt"]["request_id"] == request_id
               and row["receipt"]["data_manifest_id"] == data_manifest_id]
    if not matches:
        raise ValueError("Avaliação persistida não encontrada no corte e na base escolhidos.")
    if len(matches) != 1:
        raise ValueError("Avaliação ambígua; informe identidade única.")
    result = dict(matches[0])
    if include_facts:
        cut = _cut(as_of) or pd.Timestamp.now(tz="UTC")
        facts = {}
        for kind in ("signal", "outcome"):
            frame = feature_store.query_as_of(db, cut, feature_id=result[kind + "_feature_id"])
            facts[kind] = [{key: value.isoformat() if isinstance(value, pd.Timestamp)
                            else (None if pd.isna(value) else value)
                            for key, value in row.items()}
                           for row in frame.to_dict("records")]
        result["facts"] = facts
    return result
