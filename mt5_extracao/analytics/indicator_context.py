"""Leitura causal e projeções do store canônico, sem cálculo nos consumidores."""
import json

import pandas as pd

from mt5_extracao.analytics import feature_store, indicator_materializer, indicator_registry, resampling


def records(frame):
    """JSON sem NaT/NaN, preservando nanossegundos e fuso dos horários."""
    return [{name: None if pd.isna(value) else value.isoformat() if isinstance(value, pd.Timestamp)
             else value.item() if hasattr(value, "item") else value for name, value in row.items()}
            for row in frame.to_dict("records")]


def _state(row):
    quality = row.get("quality")
    metadata = json.loads(row.get("metadata_json")) if pd.notna(row.get("metadata_json")) else {}
    if quality == "VALID":
        return "AVAILABLE", None
    if quality == "UNAVAILABLE":
        return metadata.get("state", "UNAVAILABLE"), metadata.get("reason", "SOURCE_UNAVAILABLE")
    if quality in {"INVALIDATED", "EXPIRED"}:
        return quality, metadata.get("reason") or "SOURCE_OR_VALIDITY_CHANGED"
    return "UNAVAILABLE", "NO_VALUE_AVAILABLE_AT_DECISION"


def context(db, decisions, *, symbol, timeframe, feature_ids=None, algorithm_version=None):
    """Uma linha por decisão, com valor/estado/proveniência; exige horários com fuso.

    Nenhuma associação/aderência é calculada. A versão atual inclui o hash de
    implementação da materialização; versões antigas podem ser solicitadas.
    """
    ids = indicator_materializer.selected_features(feature_ids)
    timeframe = resampling.parse_target(timeframe).label
    frame = decisions.copy().reset_index(drop=True)
    if "decision_time" not in frame:
        raise ValueError("Informe decision_time com fuso para consultar os indicadores.")
    frame["decision_time"] = [feature_store._utc_timestamp(value, "decision_time") for value in frame.decision_time]
    frame["_context_order"] = range(len(frame))
    frame["symbol"], frame["tf"] = symbol, timeframe
    implementation = indicator_materializer.implementation_signature()
    columns = {}
    for ident in ids:
        spec = indicator_registry.get_definition(ident)
        version = algorithm_version or indicator_materializer.algorithm_version(ident, implementation)
        history = feature_store.query_history(db, symbol=symbol, timeframe=timeframe, feature_id=ident,
                                              algorithm_version=version)
        joined = feature_store.asof_join(frame[["decision_time", "_context_order", "symbol", "tf"]], history)
        joined = joined.sort_values("_context_order").reset_index(drop=True)
        prefix = ident + "::"
        states = [_state(row) for row in joined.to_dict("records")]
        values = []
        for row, (state, _reason) in zip(joined.to_dict("records"), states):
            value = row.get("numeric_value")
            if pd.isna(value):
                value = row.get("categorical_value")
            values.append(value if state == "AVAILABLE" and pd.notna(value) else None)
        columns[prefix + "value"] = values
        columns[prefix + "state"] = [state for state, _reason in states]
        columns[prefix + "reason"] = [reason for _state_, reason in states]
        columns[prefix + "algorithm_version"] = version
        columns[prefix + "unit"] = spec["unit"]
        columns[prefix + "parameters"] = json.dumps(spec["parameters"], sort_keys=True, separators=(",", ":"))
        for name in ("observation_time", "available_at_utc", "source_revision", "metadata_json"):
            columns[prefix + name] = list(joined[name]) if name in joined else None
    return pd.concat([frame.drop(columns="_context_order"), pd.DataFrame(columns, index=frame.index)], axis=1)


def projection(db, *, symbol, timeframe, as_of, feature_ids=None, algorithm_version=None, start=None, end=None):
    """Colunas por observação no retrato as-of, incluindo aquecimento/ausências.

    A consulta de contexto é por decisão; esta projeção exporta fatos históricos,
    sem preencher uma observação ausente com o valor de outra barra.
    """
    ids = indicator_materializer.selected_features(feature_ids)
    timeframe = resampling.parse_target(timeframe).label
    implementation = indicator_materializer.implementation_signature()
    parts = []
    for ident in ids:
        version = algorithm_version or indicator_materializer.algorithm_version(ident, implementation)
        rows = feature_store.query_as_of(db, as_of, symbol=symbol, timeframe=timeframe, feature_id=ident,
                                         algorithm_version=version, start=start, end=end)
        if rows.empty:
            continue
        spec = indicator_registry.get_definition(ident)
        part = rows.set_index("observation_time")
        states = [_state(row) for row in rows.to_dict("records")]
        out = pd.DataFrame(index=part.index)
        out["value"] = [row.numeric_value if state == "AVAILABLE" else None
                        for row, (state, _reason) in zip(rows.itertuples(), states)]
        out["state"] = [state for state, _reason in states]
        out["reason"] = [reason for _state_, reason in states]
        out["algorithm_version"] = version
        out["unit"] = spec["unit"]
        out["parameters"] = json.dumps(spec["parameters"], sort_keys=True, separators=(",", ":"))
        for name in ("available_at_utc", "source_revision", "metadata_json"):
            out[name] = part[name]
        parts.append(out.add_prefix(ident + "::"))
    if not parts:
        return pd.DataFrame(columns=["symbol", "tf", "observation_time"])
    result = pd.concat(parts, axis=1).sort_index().reset_index()
    result.insert(0, "tf", timeframe)
    result.insert(0, "symbol", symbol)
    return result
