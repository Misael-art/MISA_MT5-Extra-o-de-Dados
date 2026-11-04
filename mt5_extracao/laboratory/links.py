"""Vínculos explícitos de fatos congelados ao gráfico; junção canônica, sem coleta."""
from collections import Counter
import json

import pandas as pd

from mt5_extracao import timeframes
from mt5_extracao.analytics import datasets, feature_store, indicator_context, query_service

PERSPECTIVES = ("received", "publication")
VERSION = 2


def normalize(sources, symbol):
    if not isinstance(sources, (list, tuple)) or len(sources) > 32:
        raise ValueError("Vínculos exigem lista explícita de até32 snapshots de fontes.")
    result, seen = [], set()
    for item in sources:
        if not isinstance(item, dict) or set(item) != {"snapshot", "features", "target_symbol", "relationship"}:
            raise ValueError("Vínculo exige snapshot, features, target_symbol e relationship.")
        snapshot = item["snapshot"]
        if not isinstance(snapshot, datasets.Snapshot) or item["target_symbol"] != symbol:
            raise ValueError("Vínculo precisa de snapshot canônico e do ativo estudado explícito.")
        relationship = item["relationship"]
        if not isinstance(relationship, str) or not 1 <= len(relationship.strip()) <= 300:
            raise ValueError("Descreva a associação entre objeto e ativo em até300 caracteres.")
        tf = timeframes.parse(snapshot.manifest["timeframe"])
        if tf is None or tf.minutes is None or tf.name in ("W1", "MN1"):
            raise ValueError("Vínculos exigem resolução fixa; semanas/meses não são minutos nominais.")
        pairs = datasets._feature_selection(item["features"])
        if len(pairs) != len(item["features"]):
            raise ValueError("Vínculo duplicado; selecione cada versão uma vez.")
        if not pairs:
            raise ValueError("Escolha as versões do snapshot que deseja vincular.")
        definitions = {(d["feature_id"], d["algorithm_version"]) for d in snapshot.feature_definitions}
        for pair in pairs:
            key = snapshot.manifest_id, pair
            if pair not in definitions or key in seen:
                raise ValueError("Versão ausente do snapshot ou vínculo duplicado.")
            seen.add(key)
        result.append(dict(snapshot=snapshot, features=pairs, target_symbol=symbol,
                           relationship=relationship.strip()))
    return sorted(result, key=lambda s: (s["snapshot"].manifest_id, s["features"]))


def selection(sources):
    return [dict(manifest_id=s["snapshot"].manifest_id, features=[list(p) for p in s["features"]],
                 target_symbol=s["target_symbol"], relationship=s["relationship"]) for s in sources]


def _iso(value):
    return value.isoformat() if pd.notna(value) else None


def channels(sources, grid, cutoff, perspective, guard, *, first_open):
    """Emite um pulso por mudança/observação; nunca carrega Q de um candle a outro."""
    if perspective not in PERSPECTIVES:
        raise ValueError("Perspectiva deve ser received ou publication.")
    cutoff = feature_store._utc_timestamp(cutoff, "as_of")
    if cutoff > pd.Timestamp.now(tz="UTC"):
        raise ValueError("O corte precisa já ter ocorrido.")
    grid = pd.DatetimeIndex(grid)
    first_open = feature_store._utc_timestamp(first_open, "first_open")
    built, event_count = [], 0
    total = sum(len(s["features"]) for s in sources) * len(grid)
    if total > query_service.MAX_SELECTION_ROWS:
        raise ValueError("Vínculos excedem200 mil células; reduza os canais ou a janela.")
    for link in sources:
        guard()
        snapshot = link["snapshot"]
        manifest, history = snapshot.manifest, snapshot.feature_history
        step = pd.Timedelta(minutes=timeframes.parse(manifest["timeframe"]).minutes)
        definitions = {(d["feature_id"], d["algorithm_version"]): d for d in snapshot.feature_definitions}
        for ident, version in link["features"]:
            guard()
            spec = definitions[ident, version]
            source = history.loc[(history.feature_id == ident) & (history.algorithm_version == version)
                                 & (history.available_at_utc <= cutoff)].copy()
            event_count += len(source)
            if event_count > query_service.MAX_SELECTION_ROWS:
                raise ValueError("Mais de200 mil revisões congeladas no vínculo; reduza o recorte.")
            decisions = pd.DataFrame(dict(decision_time=grid))
            joined = feature_store.asof_join(decisions, source, by=[]) if perspective == "received" else None
            events, by_bar = [], {}
            for index, row in enumerate(source.itertuples(index=False)):
                if index % 256 == 0:
                    guard()
                meta = json.loads(row.metadata_json) if isinstance(row.metadata_json, str) else {}
                end = feature_store._utc_timestamp(meta["window_end"], "window_end") if meta.get("window_end") else row.observation_time + step
                axis = row.available_at_utc if perspective == "received" else end
                at = int(grid.searchsorted(axis))
                mapped = axis >= first_open and at < len(grid) and (perspective == "received" or grid[at] == axis) and grid[at] <= cutoff
                state, reason = indicator_context._state(row._asdict())
                event = dict(observation_time=_iso(row.observation_time), window_end=_iso(end),
                    captured_at=(meta["capture"].get("captured_at") if isinstance(meta.get("capture"), dict) else meta.get("captured_at")) or (_iso(row.observed_at_utc) if meta.get("capture_clock")=="stored_observed_at" else None),
                    available_at=_iso(row.available_at_utc), revision=int(row.revision),
                    quality=row.quality, state=state, reason=reason,
                    value=indicator_context._value(row.numeric_value, row.categorical_value, state),
                    population_revision=meta.get("population_revision"), label_counts=meta.get("label_counts"),
                    source_id=spec.get("source_id"), bar_index=at if mapped else None,
                    plotted_at=_iso(grid[at]) if mapped else None,
                    alignment_delay_seconds=float((grid[at]-axis).total_seconds()) if mapped else None,
                    metadata={k:meta[k] for k in ("synthetic_fixture", "classification", "classifier", "coverage",
                                                  "universe", "retrospective", "scope_hash", "reason", "macro", "reference", "attribution", "period_convention", "source_period_start") if k in meta})
                receipt_at = int(grid.searchsorted(row.available_at_utc))
                publication_at = int(grid.searchsorted(end))
                event["receipt_bar_index"] = (receipt_at if first_open <= row.available_at_utc <= cutoff
                    and receipt_at < len(grid) and grid[receipt_at] <= cutoff else None)
                event["publication_bar_index"] = (publication_at if first_open <= end <= cutoff
                    and publication_at < len(grid) and grid[publication_at] == end else None)
                events.append(event)
                if mapped:
                    by_bar.setdefault(at, []).append(event)
            values, raw, states, observed, available, revisions = ([None]*len(grid) for _ in range(6))
            numeric = spec.get("value_type") == "NUMBER"
            for at, rows in by_bar.items():
                guard()
                observations = {r["observation_time"] for r in rows}
                if len(observations) > 1:
                    states[at] = "MULTIPLE_OBSERVATIONS_IN_CANDLE"
                    continue
                chosen = max(rows, key=lambda r: (r["available_at"], r["revision"]))
                # A chegada tardia de observação antiga não substitui uma mais nova.
                if perspective == "received":
                    actual = joined.iloc[at]
                    if pd.isna(actual.observation_time):
                        states[at] = "EXPIRED_AT_CANDLE"
                        continue
                    if actual.observation_time != pd.Timestamp(chosen["observation_time"]):
                        states[at] = "OLDER_OBSERVATION_RECEIVED"
                        continue
                observed[at], available[at], revisions[at] = chosen["observation_time"], chosen["available_at"], chosen["revision"]
                raw[at], states[at] = chosen["value"], chosen["state"]
                if numeric and chosen["state"] == "AVAILABLE" and isinstance(chosen["value"], (int, float)):
                    values[at] = float(chosen["value"])
            states = [s or "NO_NEW_OBSERVATION" for s in states]
            count = sum(v is not None for v in values)
            diagnosis = (dict(code="NON_NUMERIC_STORED_FEATURE", message="Categoria preservada na cronologia; não codificada como número.",
                              action="Consulte os eventos e a visão do candle.")
                         if not numeric else dict(code="NO_FROZEN_VALUES", message="Sem observações numéricas mapeadas no corte/grade escolhidos.",
                                                  action="Confira corte, janela, recibos e perspectiva; ausência não é zero.")
                         if not count else None)
            built.append(dict(id=f"linked:{snapshot.manifest_id}:{ident}@{version}", feature=ident,
                name=f"{spec['name']} · {manifest['symbol']} · {manifest['timeframe']} · {spec.get('source_id') or 'fonte declarada'}",
                timeframe=manifest["timeframe"], algorithm_version=version, parameters=spec.get("parameters", {}),
                unit=spec.get("unit"), output_kind=spec.get("output_kind"), source_kind="stored", linked_source=True,
                snapshot_manifest_id=snapshot.manifest_id, linked_symbol=manifest["symbol"], source=manifest["source"],
                relationship=link["relationship"], stored_definition=spec, values=values, raw_values=raw,
                directions=[None]*len(grid), source_ends=available, observation_times=observed, revisions=revisions,
                available=count, total=len(grid), coverage=count/len(grid), source_rows=int(source.observation_time.nunique()),
                source_available=len(set(o for o in observed if o is not None)), frozen_revision_count=len(source),
                stored_available=sum(s == "AVAILABLE" for s in states), states=dict(Counter(states)), per_bar_states=states,
                events=None, receipt_events=events, diagnosis=diagnosis, rule="stored", warmup=spec.get("warmup_count", 0),
                direction_rule=None, direction_diagnosis="Nenhuma direção de negociação inferida da fonte vinculada.",
                temporal_basis=perspective, legend="Pulso por observação; ausência entre recibos não é zero.",
                eligibility=dict(eligible=True, status="ELIGIBLE", reasons=[], limitations=["Associação explícita; não prova causa do preço."])))
    return built
