"""Leitura causal e projeções do store canônico, sem cálculo nos consumidores."""
import json

import pandas as pd

from mt5_extracao import timeframes
from mt5_extracao.analytics import feature_store, indicator_materializer, indicator_registry, query_service, resampling


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


def _ids(feature_ids, defaults):
    if feature_ids is None:
        return sorted(defaults)
    if (not isinstance(feature_ids, (list, tuple, set, frozenset)) or
            not feature_ids or not all(isinstance(ident, str) and ident.strip() for ident in feature_ids)):
        raise ValueError("Selecione uma lista de IDs não vazios do catálogo.")
    return sorted({ident.strip() for ident in feature_ids})


def _version(value):
    if value is not None and (not isinstance(value, str) or not value.strip()):
        raise ValueError("algorithm_version precisa ser texto não vazio.")
    return value.strip() if value is not None else None


def _selection(db, feature_ids, algorithm_version):
    """Mesmo catálogo da consulta pública; o padrão legado continua na implementação local atual."""
    entries = query_service.definition_catalog(db)
    contracts = {(item["feature_id"], item["algorithm_version"]):item for item in entries}
    local_ids = {item["feature_id"] for item in indicator_registry.definitions()}
    ids = _ids(feature_ids, local_ids)
    algorithm_version = _version(algorithm_version)
    known = {item["feature_id"] for item in entries}
    unknown = set(ids) - known
    if unknown:
        raise ValueError("Características desconhecidas no catálogo: " + ", ".join(sorted(unknown)))
    if algorithm_version is None and set(ids) - local_ids:
        raise ValueError("Indicador sem implementação local: informe algorithm_version explicitamente; "
                         "consulte as versões em features catalog.")
    implementations = indicator_materializer._implementation_signatures(ids) if algorithm_version is None else {}
    selected = []
    for ident in ids:
        version = algorithm_version or indicator_materializer.algorithm_version(ident, implementations[ident])
        entry = contracts.get((ident, version))
        registered = entry is not None and entry["registered_definition"] is not None
        if not registered and algorithm_version is not None:
            raise ValueError(f"Característica não registrada: {ident}@{version}. Confira a versão disponível no store.")
        spec = entry if registered else indicator_registry.get_definition(ident)
        selected.append((ident, version, spec, registered))
    return selected


def _value(numeric, category, state):
    if state != "AVAILABLE":
        return None
    value = category if pd.isna(numeric) else numeric
    return value if pd.notna(value) else None


def _timeframe(value, explicit_version):
    # Ler fatos existentes não exige que o materializador local saiba produzir esse TF.
    parsed = timeframes.parse(value)
    if explicit_version is not None and parsed in (timeframes.Timeframe.M1, timeframes.Timeframe.W1,
                                                   timeframes.Timeframe.MN1):
        return parsed.name
    return resampling.parse_target(value).label


def context(db, decisions, *, symbol, timeframe, feature_ids=None, algorithm_version=None):
    """Uma linha por decisão, com valor/estado/proveniência; exige horários com fuso.

    Nenhuma associação/aderência é calculada. A versão atual inclui o hash de
    implementação da materialização; versões registradas podem ser solicitadas.
    Um indicador sem implementação local exige algorithm_version explícito.
    """
    selected = _selection(db, feature_ids, algorithm_version)
    timeframe = _timeframe(timeframe, algorithm_version)
    return _context_rows(decisions, symbol, timeframe, selected,
        lambda ident, version: feature_store.query_history(db, symbol=symbol, timeframe=timeframe,
                                                         feature_id=ident, algorithm_version=version))


def _context_rows(decisions, symbol, timeframe, selected, read_history, *, window=None, max_cells=None):
    if not isinstance(decisions, pd.DataFrame):
        raise ValueError("Informe uma tabela de decisões com decision_time e fuso explícito.")
    if max_cells is not None and len(decisions) * len(selected) > max_cells:
        raise ValueError(f"Contexto excede {max_cells} pares indicador/decisão; reduza a seleção.")
    frame = decisions.copy().reset_index(drop=True)
    if "decision_time" not in frame:
        raise ValueError("Informe decision_time com fuso para consultar os indicadores.")
    frame["decision_time"] = [feature_store._utc_timestamp(value, "decision_time") for value in frame.decision_time]
    if window is not None:
        first, last = window
        if any(stamp < first or stamp > last for stamp in frame.decision_time):
            raise ValueError(f"Decisão fora da janela congelada ({first.isoformat()} a {last.isoformat()}); "
                             "escolha um instante coberto ou capture outra janela.")
    frame["_context_order"] = range(len(frame))
    frame["symbol"], frame["tf"] = symbol, timeframe
    columns = {}
    for ident, version, spec, registered in selected:
        history = read_history(ident, version)
        if not registered and not history.empty:
            raise ValueError(f"Fatos sem definição registrada: {ident}@{version}. Confira o produtor.")
        joined = feature_store.asof_join(frame[["decision_time", "_context_order", "symbol", "tf"]], history)
        joined = joined.sort_values("_context_order").reset_index(drop=True)
        prefix = ident + "::"
        states = [_state(row) for row in joined.to_dict("records")]
        columns[prefix + "value"] = [_value(numeric, category, state) for numeric, category, (state, _reason)
                                      in zip(joined.numeric_value, joined.categorical_value, states)]
        columns[prefix + "state"] = [state for state, _reason in states]
        columns[prefix + "reason"] = [reason for _state_, reason in states]
        columns[prefix + "algorithm_version"] = version
        columns[prefix + "unit"] = spec["unit"]
        columns[prefix + "parameters"] = json.dumps(spec["parameters"], sort_keys=True, separators=(",", ":"))
        for name in ("observation_time", "available_at_utc", "valid_until_utc", "source_revision", "revision", "metadata_json"):
            columns[prefix + name] = list(joined[name]) if name in joined else None
    return pd.concat([frame.drop(columns="_context_order"), pd.DataFrame(columns, index=frame.index)], axis=1)


def _snapshot_scope(snapshot, history, observation_time):
    """Janela de mercado no padrão; em observação fixa, apenas seus recibos congelados."""
    first, last = snapshot.feature_window_utc
    if observation_time is None:
        return history, (first, last), None
    observation = feature_store._utc_timestamp(observation_time, "observation_time")
    if not first <= observation < last:
        raise ValueError("Observação fora da janela congelada; capture esse período antes de consultar.")
    history = history.loc[history.observation_time == observation]
    if history.empty:
        raise ValueError("Observação não consta nos fatos congelados; não é possível inventar uma revisão.")
    # A extensão cobre recibos/vencimentos declarados, não novas barras ou mercado.
    ends = [last, history.available_at_utc.max()]
    latest = history.sort_values(["available_at_utc", "revision"]).drop_duplicates(
        ["feature_id", "algorithm_version"], keep="last")
    expiry = latest.valid_until_utc.max()
    if pd.notna(expiry):
        ends.append(expiry)
    return history, (observation, max(ends)), observation


def from_snapshot(snapshot, decisions, *, feature_ids=None, algorithm_version=None, observation_time=None):
    """Contexto causal somente dos fatos/contratos congelados, sem banco atual ou registry local."""
    from mt5_extracao.analytics.datasets import Snapshot

    if not isinstance(snapshot, Snapshot):
        raise ValueError("Informe um snapshot canônico carregado ou capturado.")
    definitions = snapshot.feature_definitions
    ids = _ids(feature_ids, {entry["feature_id"] for entry in definitions})
    explicit = _version(algorithm_version)
    selected = []
    for ident in ids:
        matches = [entry for entry in definitions if entry["feature_id"] == ident and
                   (explicit is None or entry["algorithm_version"] == explicit)]
        if not matches:
            raise ValueError(f"Indicador/versão não congelado: {ident}@{explicit or '*'}. "
                             "Capture essa versão; a base atual não completa o snapshot.")
        if len(matches) != 1:
            raise ValueError(f"Mais de uma versão congelada de {ident}; informe algorithm_version explicitamente.")
        entry = matches[0]
        selected.append((ident, entry["algorithm_version"], entry, True))
    manifest = snapshot.manifest
    identity = dict(symbol=manifest["symbol"], tf=manifest["timeframe"], snapshot_manifest_id=snapshot.manifest_id,
        base_namespace=manifest["source"]["namespace"], base_provider=manifest["source"]["provider"],
        base_server=manifest["source"]["server"])
    history, window, observation = _snapshot_scope(snapshot, snapshot.feature_history, observation_time)
    if observation is not None:
        identity["selected_observation_time"] = observation
    if isinstance(decisions, pd.DataFrame):
        for name, expected in identity.items():
            values = decisions[name].dropna() if name in decisions else ()
            if name == "selected_observation_time":
                values = [feature_store._utc_timestamp(value, name) for value in values]
            if any(value != expected for value in values):
                raise ValueError(f"Decisões de outra origem/seleção em {name}; use o contexto do snapshot escolhido.")
    frame = _context_rows(decisions, identity["symbol"], identity["tf"], selected,
        lambda ident, version: history.loc[(history.feature_id == ident) & (history.algorithm_version == version)],
        window=window, max_cells=query_service.MAX_SELECTION_ROWS)
    for name, value in identity.items():
        frame[name] = value
    frame.attrs["snapshot_context_scope"] = dict(
        mode="fixed_observation" if observation is not None else "latest_observation",
        observation_time=observation.isoformat() if observation is not None else None,
        window_utc=[stamp.isoformat() for stamp in window])
    return frame


def projection(db, *, symbol, timeframe, as_of, feature_ids=None, algorithm_version=None, start=None, end=None):
    """Colunas por observação no retrato as-of, incluindo aquecimento/ausências.

    A consulta de contexto é por decisão; esta projeção exporta fatos históricos,
    sem preencher uma observação ausente com o valor de outra barra.
    """
    selected = _selection(db, feature_ids, algorithm_version)
    timeframe = _timeframe(timeframe, algorithm_version)
    parts = []
    for ident, version, spec, registered in selected:
        rows = feature_store.query_as_of(db, as_of, symbol=symbol, timeframe=timeframe, feature_id=ident,
                                         algorithm_version=version, start=start, end=end)
        if rows.empty:
            continue
        if not registered:
            raise ValueError(f"Fatos sem definição registrada: {ident}@{version}. Confira o produtor.")
        part = rows.set_index("observation_time")
        states = [_state(row) for row in rows.to_dict("records")]
        out = pd.DataFrame(index=part.index)
        out["value"] = [_value(numeric, category, state) for numeric, category, (state, _reason)
                        in zip(rows.numeric_value, rows.categorical_value, states)]
        out["state"] = [state for state, _reason in states]
        out["reason"] = [reason for _state_, reason in states]
        out["algorithm_version"] = version
        out["unit"] = spec["unit"]
        out["parameters"] = json.dumps(spec["parameters"], sort_keys=True, separators=(",", ":"))
        for name in ("available_at_utc", "source_revision", "revision", "metadata_json"):
            out[name] = part[name]
        parts.append(out.add_prefix(ident + "::"))
    if not parts:
        return pd.DataFrame(columns=["symbol", "tf", "observation_time"])
    result = pd.concat(parts, axis=1).sort_index().reset_index()
    result.insert(0, "tf", timeframe)
    result.insert(0, "symbol", symbol)
    return result
