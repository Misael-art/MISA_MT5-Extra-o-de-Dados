"""T19.3: consumidor do registry, com revisões e checkpoints no store existente.

Sem fonte nova ou fórmula paralela. Retomadas confirmam apenas o delta; a avaliação
numérica reutiliza o prefixo exato do registry, inclusive suas sementes recursivas.
"""
import ast
import hashlib
import inspect as source_inspect
import json
import logging
from pathlib import Path
import platform
import textwrap

import numpy as np
import pandas as pd
from sqlalchemy import inspect, text

from mt5_extracao.analytics import feature_store, features, indicator_registry, producer_jobs, producers, resampling
from mt5_extracao.strategies import indicators

log = logging.getLogger(__name__)
FAMILY = "registry_indicators"


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _source_components(filename, names):
    """AST das funções selecionadas, sem comentários, finais de linha ou caminho local."""
    tree = ast.parse(Path(filename).read_text(encoding="utf-8"))
    functions = {node.name: node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
    missing = set(names) - functions.keys()
    if missing:
        raise ValueError("Componentes de implementação ausentes: " + ", ".join(sorted(missing)))
    return {name: hashlib.sha256(ast.dump(functions[name], include_attributes=False).encode()).hexdigest()
            for name in names}


def _object_source_component(obj):
    """Mesmo formato AST para classes/propriedades de representação congelada."""
    nodes = ast.parse(textwrap.dedent(source_inspect.getsource(obj))).body
    if len(nodes) != 1 or not isinstance(nodes[0], (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        raise ValueError("Objeto de implementação precisa ter uma definição Python única.")
    return hashlib.sha256(ast.dump(nodes[0], include_attributes=False).encode()).hexdigest()


def implementation_scheme(feature_id):
    return "return_components_v1" if feature_id.startswith("price.return_") else "legacy_modules_v1"


def _return_components():
    # Lista de componentes pertence ao produtor desta família, nunca aos consumidores.
    # Mudanças na orquestração/validação compartilhada permanecem conservadoras.
    return {
        "registry": _source_components(indicator_registry.__file__, (
            "_return_series", "_calculated_series", "_column", "_bad_denominator", "iter_core", "get_definition")),
        "materializer": _source_components(__file__, (
            "_source_components", "_return_components", "_return_signature", "_implementation_signatures",
            "implementation_scheme", "algorithm_version", "_return_inputs",
            "materialize", "selected_features", "_register", "_guard", "_confirm_source")),
        "source_revisions": _source_components(producers.__file__, (
            "_source_tokens", "_source_revision", "_json_safe", "_correction_times",
            "_prefix_source_revisions", "_extend_source_chain")),
    }


def _return_signature(spec, components=None):
    return hashlib.sha256(_json(dict(scheme=implementation_scheme(spec["feature_id"]),
        components=_return_components() if components is None else components,
        contract={key: value for key, value in spec.items() if key != "name"}, family=FAMILY,
        python=platform.python_version(), pandas=pd.__version__, numpy=np.__version__)).encode()).hexdigest()


def implementation_signature(feature_id=None, *, components=None):
    """Normaliza finais de linha; caminhos locais não entram no identificador."""
    if feature_id is not None:
        spec = indicator_registry.get_definition(feature_id)
        if implementation_scheme(feature_id) == "return_components_v1":
            return _return_signature(spec, components)
    sources = [hashlib.sha256(Path(module).read_text(encoding="utf-8").encode()).hexdigest()
               for module in (__file__, indicator_registry.__file__, indicators.__file__)]
    return hashlib.sha256(_json({"sources": sources, "pandas": pd.__version__, "numpy": np.__version__}).encode()).hexdigest()


def _implementation_signatures(feature_ids):
    """Uma leitura dos componentes por seleção; nada persiste entre chamadas."""
    signatures, components, legacy = {}, None, None
    for ident in feature_ids:
        indicator_registry.get_definition(ident)
        if implementation_scheme(ident) == "return_components_v1":
            if components is None:
                components = _return_components()
            signatures[ident] = implementation_signature(ident, components=components)
        else:
            if legacy is None:
                legacy = implementation_signature(ident)
            signatures[ident] = legacy
    return signatures


def algorithm_version(feature_id, implementation=None):
    spec = indicator_registry.get_definition(feature_id)
    return spec["algorithm_version"] + "+" + (implementation or implementation_signature(feature_id))[:12]


def selected_features(feature_ids=None):
    ids = [item["feature_id"] for item in indicator_registry.definitions()] if feature_ids is None else feature_ids
    ids = sorted(set(ids))
    if not ids:
        raise ValueError("Selecione pelo menos um indicador do registry.")
    for ident in ids:
        indicator_registry.get_definition(ident)
    return ids


def _register(db, ids, implementation):
    for ident in ids:
        spec = indicator_registry.get_definition(ident)
        signature = implementation[ident] if isinstance(implementation, dict) else implementation
        scheme = implementation_scheme(ident) if isinstance(implementation, dict) else "legacy_modules_v1"
        contract = {name: spec[name] for name in ("family", "formula", "unit", "warmup_count", "warmup_unit",
                                                 "availability_rule", "value_type")}
        features.register(db, ident, spec["name"], algorithm_version(ident, signature),
                          parameters={**spec["parameters"], "implementation_sha256": signature,
                                      "implementation_scheme": scheme},
                          dependencies=["bars_derived:{timeframe}"], timeframe="same_as_source",
                          source_id="bars_derived:{timeframe}:" + spec["source_id"].split(".", 1)[1], **contract)


class Cancelled(RuntimeError):
    pass


class SourceChanged(RuntimeError):
    pass


def _confirm_source(conn, db, symbol, label, sequence):
    # SQLite já tem reserva de escrita; PostgreSQL bloqueia a geração que o
    # produtor atualiza na mesma transação das barras, até o commit deste lote.
    if conn.dialect.name == "postgresql" and inspect(conn).has_table("_analytics_source_sequences"):
        conn.execute(text("SELECT sequence FROM _analytics_source_sequences "
                          "WHERE symbol = :symbol AND tf = :tf FOR UPDATE"),
                     {"symbol": str(symbol), "tf": label}).scalar()
    current = resampling.latest_source_sequence(db, symbol, label, connection=conn) or 0
    if current != sequence:
        raise SourceChanged("A fonte mudou durante a materialização. Reexecute o comando para usar "
                            "a revisão atual; lotes confirmados foram preservados.")


def _guard(run, write_guard, cancel_check, timeout_seconds, max_memory_mb):
    if cancel_check is not None and cancel_check():
        raise Cancelled("Materialização cancelada; lotes confirmados preservados para retomada.")
    if write_guard is not None and (reason := write_guard()):
        raise producers.WriteBlocked(f"Materialização bloqueada: {reason}. Libere recursos e retome o comando.")
    if run is not None:
        producers._check_limits(run, timeout_seconds, max_memory_mb)


def _return_inputs(raw, bars, ids, tokens):
    """Dependências dos retornos do registry; elegibilidade vem do leitor canônico.

    O valor usa os dois extremos, não todo o prefixo. A disponibilidade também
    considera correções no intervalo: remover/restaurar uma barra intermediária
    pode mudar a contagem ordinal mesmo sem corrigir os novos extremos.
    """
    horizons = {ident: indicator_registry.get_definition(ident)["parameters"]["horizon_bars"]
                for ident in ids if ident.startswith("price.return_")}
    if not horizons or bars.empty:
        return {}
    eligible = set(bars.bar_start_utc)
    receipts, latest = [], None
    for row in raw.itertuples():
        if row.revision > 1:
            latest = max(latest, row.computed_at_utc) if latest is not None else row.computed_at_utc
        if row.bar_start_utc in eligible:
            receipts.append(latest)
            latest = None
    # Agrupa recibos das barras inelegíveis com a próxima elegível. O max móvel
    # já existente conserva esses recibos enquanto podem afetar a janela ordinal.
    projected = bars[["revision", "computed_at_utc"]].copy()
    projected["revision"] = [2 if stamp is not None else 1 for stamp in receipts]
    projected["computed_at_utc"] = pd.to_datetime(receipts, utc=True)
    result = {}
    starts = bars.bar_start_utc.tolist()
    for ident, horizon in horizons.items():
        corrections = producers._correction_times(projected, window=horizon + 1).tolist()
        dependencies = [([position - horizon, position] if position >= horizon else list(range(position + 1)))
                        for position in range(len(bars))]
        result[ident] = dict(revisions=[producers._source_revision(tokens, indices) for indices in dependencies],
            corrections=corrections,
            observations=[[starts[index].isoformat() for index in indices] for indices in dependencies],
            horizon=horizon)
    return result


def materialize(db, symbol, timeframe, *, feature_ids=None, batch_size=5000, write_guard=None, cancel_check=None,
                timeout_seconds=600, max_memory_mb=2048):
    """Materializa somente barras fechadas; não modifica barras, legado ou alvos futuros.

    Fonte inalterada é atendida antes de ler o histórico. Append puro grava só
    observações novas. Correção/revisão reconstrói o prefixo exato, sem arredondar
    hashes; o instante da correção limita a disponibilidade de toda a cauda causal.
    """
    if (isinstance(batch_size, bool) or int(batch_size) != batch_size or
            not 1 <= batch_size <= producers.MAX_BATCH_SIZE or
            not 0 < timeout_seconds <= producers.MAX_JOB_TIMEOUT_SECONDS or
            not 0 < max_memory_mb <= producers.MAX_JOB_MEMORY_MB):
        raise ValueError("Confira batch_size, timeout e memória nos limites existentes dos jobs Analytics.")
    if not str(symbol).strip():
        raise ValueError("Informe o símbolo da série derivada.")
    label = resampling.parse_target(timeframe).label
    ids = selected_features(feature_ids)
    implementations = _implementation_signatures(ids)
    implementation = hashlib.sha256(_json(implementations).encode()).hexdigest()
    config_hash = hashlib.sha256(_json({"features": ids, "implementation": implementation}).encode()).hexdigest()
    version = algorithm_version(ids[0], implementations[ids[0]])
    _guard(None, write_guard, cancel_check, timeout_seconds, max_memory_mb)
    prior = producer_jobs.get_checkpoint(db, symbol, label, FAMILY)
    sequence = resampling.latest_source_sequence(db, symbol, label) or 0
    run = producer_jobs.start_run(db, symbol, label, FAMILY, version, config_hash, sequence)
    counts = dict(inserted=0, revised=0, unchanged=0)
    try:
        same = bool(prior and prior["config_hash"] == config_hash and prior["algorithm_version"] == version)
        if (same and prior["status"] == "COMPLETE" and prior["source_sequence"] == sequence and
                prior["last_bar_start_utc"] and
                not resampling.has_closed_bars_after(db, symbol, label, prior["last_bar_start_utc"],
                                                     require_complete_session=False)):
            feature_store.save_values(db, [], transaction_hook=lambda conn, _counts:
                                      _confirm_source(conn, db, symbol, label, sequence))
            job = producer_jobs.finish_run(db, run, source_sequence=sequence, status="SKIPPED")
            return dict(symbol=symbol, timeframe=label, features=len(ids), skipped=True, **counts, job=job)
        _guard(run, write_guard, cancel_check, timeout_seconds, max_memory_mb)
        with db.consistent_read() as conn:
            sequence = resampling.latest_source_sequence(db, symbol, label, connection=conn) or 0
            raw = resampling.latest_bars(db, symbol, label, closed_only=False, connection=conn)
            bars = resampling.latest_bars(db, symbol, label, connection=conn)
        if raw.empty:
            raise ValueError("Nenhuma barra derivada disponível. Gere a série com resample --session-aware; "
                             "valores anteriores foram preservados.")
        _register(db, ids, implementations)
        previous = {ident: feature_store.latest_values(db, symbol=symbol, timeframe=label, feature_ids=[ident],
                                                       algorithm_version=algorithm_version(ident, implementations[ident]))
                    for ident in ids}
        raw_starts = set(raw.bar_start_utc)
        if any(not set(frame.observation_time).issubset(raw_starts) for frame in previous.values()):
            raise ValueError("O histórico derivado perdeu observações já materializadas. Restaure ou reconcilie "
                             "a revisão da fonte antes de retomar; valores anteriores foram preservados.")
        value_tokens = producers._source_tokens(bars) if not bars.empty else []
        value_chains = producers._prefix_source_revisions(value_tokens)
        return_inputs = _return_inputs(raw, bars, ids, value_tokens)
        chains, pivot_bars = value_chains, bars
        has_pivots = any(ident.startswith("structure.") for ident in ids)
        has_channels = any(ident.startswith("channel.donchian_") for ident in ids)
        if (has_pivots or has_channels) and not bars.empty:
            # Não comprimir uma ausência conhecida na janela de pivôs. Gaps de
            # calendário sem barra observada não são candles sintetizados.
            raw_positions = {start: index for index, start in enumerate(raw.bar_start_utc)}
            positions = [raw_positions[start] for start in bars.bar_start_utc]
            pivot_bars = bars.copy()
            pivot_bars["_pivot_reset_before"] = [i > 0 and position > positions[i - 1] + 1
                                                 for i, position in enumerate(positions)]
            if has_channels:
                pivot_bars["_channel_reset_before"] = pivot_bars["_pivot_reset_before"]
            raw_chains = producers._prefix_source_revisions(producers._source_tokens(raw))
            # Mesmo inserir uma barra inelegível entre observações antigas muda
            # o prefixo/checkpoint; os tokens elegíveis sozinhos não detectariam isso.
            chains = [raw_chains[position] for position in positions]
        digest = hashlib.sha256(_json({"chain": chains[-1] if chains else None,
                                      "bars": len(bars), "sequence": sequence}).encode()).hexdigest()
        last_bar = bars.iloc[-1].bar_start_utc.isoformat() if not bars.empty else None
        write_from, cursor = 0, 0
        if same and prior["source_sequence"] == sequence and prior["state"].get("input_digest") == digest:
            if prior["status"] == "RUNNING":
                write_from = prior["state"]["write_from"]
                cursor = prior["state"]["next_record"]
        elif same and prior["status"] == "COMPLETE":
            old_count = prior["state"].get("bar_count", 0)
            if (old_count and len(bars) >= old_count and chains[old_count - 1] == prior["state"].get("prefix_chain") and
                    pd.Timestamp(prior["last_bar_start_utc"]) == bars.iloc[old_count - 1].bar_start_utc):
                write_from = old_count

        # Inclui revisões que tornaram uma barra inelegível, sem antecipar sua remoção.
        corrections = []
        latest_correction = None
        pivot_latest_correction, pivot_correction_by_start = None, {}
        eligible_starts = set(bars.bar_start_utc) if not bars.empty else set()
        if not raw.empty:
            for row in raw.itertuples():
                if row.revision > 1:
                    latest_correction = max(latest_correction, row.computed_at_utc) if latest_correction is not None else row.computed_at_utc
                corrections.append((row.bar_start_utc, latest_correction))
                if row.revision > 1 or row.bar_start_utc not in eligible_starts:
                    pivot_latest_correction = max(pivot_latest_correction, row.computed_at_utc) if pivot_latest_correction is not None else row.computed_at_utc
                pivot_correction_by_start[row.bar_start_utc] = pivot_latest_correction
        correction_by_start = dict(corrections)

        def records():
            if not bars.empty:
                for item in indicator_registry.iter_core(pivot_bars, feature_ids=ids):
                    position = item["bar_index"]
                    if position < write_from:
                        continue
                    bar = bars.iloc[position]
                    is_pivot = "pivot_metadata" in item
                    is_window = "window_metadata" in item
                    receipts = pivot_correction_by_start if is_pivot or is_window else correction_by_start
                    available = max(bar.bar_end_utc, receipts.get(bar.bar_start_utc) or bar.bar_end_utc)
                    return_input = return_inputs.get(item["feature_id"])
                    source_revision = chains[position] if is_pivot or is_window else value_chains[position]
                    if return_input is not None:
                        correction = return_input["corrections"][position]
                        available = max(bar.bar_end_utc, correction) if pd.notna(correction) else bar.bar_end_utc
                        source_revision = return_input["revisions"][position]
                    # real_volume_known prevalece sobre zero/valor preenchido pela fonte.
                    if item["feature_id"] == "volume.real" and (pd.isna(bar.get("real_volume_known")) or
                                                                 bar.get("real_volume_known") != 1):
                        item = dict(item, value=None, state="SOURCE_VALUE_MISSING", reason="REAL_VOLUME_NOT_OBSERVED")
                    metadata = {"state": item["state"], "reason": item["reason"], "reconstructed": True,
                                "registry_version": item["algorithm_version"],
                                "implementation_sha256": implementations[item["feature_id"]],
                                "implementation_scheme": implementation_scheme(item["feature_id"]),
                                "bar_end_utc": bar.bar_end_utc.isoformat(), "source_scope": "causal_prefix"}
                    if return_input is not None:
                        metadata.update(source_scope="return_endpoints_with_eligible_bar_order",
                            dependency_bar_starts_utc=return_input["observations"][position],
                            horizon_bars=return_input["horizon"],
                            availability_basis="corrections_in_ordinal_window_including_ineligible_receipts")
                    if "window_metadata" in item:
                        metadata.update(item["window_metadata"])
                    if "display_offset_bars" in item:
                        metadata.update(calculation_bar_start_utc=bar.bar_start_utc.isoformat(),
                                        calculation_bar_end_utc=bar.bar_end_utc.isoformat(),
                                        display_anchor_bar_start_utc=bar.bar_start_utc.isoformat(),
                                        display_offset_bars=item["display_offset_bars"], display_time_utc=None,
                                        display_time_rule="source_bar_ordinal_plus_offset; no UTC forecast")
                    if "pivot_metadata" in item:
                        pivot = item["pivot_metadata"]
                        metadata.update(pivot)
                        metadata["source_scope"] = "causal_prefix_including_known_ineligible_bars"
                        metadata.update(calculation_bar_start_utc=bar.bar_start_utc.isoformat(),
                                        calculation_bar_end_utc=bar.bar_end_utc.isoformat())
                        if pivot.get("pivot_status") == "CONFIRMED":
                            origin = bars.iloc[pivot["pivot_origin_position"]]
                            confirmation = bars.iloc[pivot["pivot_confirmation_position"]]
                            known = max(confirmation.bar_end_utc,
                                        pivot_correction_by_start.get(confirmation.bar_start_utc) or confirmation.bar_end_utc)
                            metadata.update(pivot_origin_time_utc=origin.bar_start_utc.isoformat(),
                                            pivot_confirmation_time_utc=confirmation.bar_end_utc.isoformat(),
                                            pivot_known_at_utc=known.isoformat(),
                                            display_time_utc=origin.bar_start_utc.isoformat())
                        if "pivot_candidates" in pivot:
                            metadata["pivot_candidates"] = [
                                {**candidate, "origin_time_utc": bars.iloc[candidate["origin"]].bar_start_utc.isoformat(),
                                 "confirmation_time_utc": None, "known_at_utc": None}
                                for candidate in pivot["pivot_candidates"]]
                    yield dict(symbol=symbol, timeframe=label, feature_id=item["feature_id"],
                               algorithm_version=algorithm_version(item["feature_id"], implementations[item["feature_id"]]),
                               observation_time=bar.bar_start_utc, value=item["value"], available_at=available,
                               observed_at=available, source_revision=source_revision,
                               quality="VALID" if item["state"] == "AVAILABLE" else "UNAVAILABLE",
                               metadata=metadata)
            current = set(bars.bar_start_utc) if not bars.empty else set()
            for ident in ids:
                for old in previous[ident].itertuples():
                    if old.observation_time in current:
                        continue
                    source = raw[raw.bar_start_utc == old.observation_time] if not raw.empty else raw
                    available = max(old.available_at_utc, source.computed_at_utc.max()) if not source.empty else old.available_at_utc
                    token = b"".join(producers._source_tokens(source)) if not source.empty else str(old.observation_time).encode()
                    yield dict(symbol=symbol, timeframe=label, feature_id=ident, algorithm_version=old.algorithm_version,
                               observation_time=old.observation_time, value=None, quality="INVALIDATED",
                               available_at=available, observed_at=available,
                               source_revision="invalidated:" + hashlib.sha256(token).hexdigest(),
                               metadata={"state": "INVALIDATED", "reason": "SOURCE_BAR_NOT_CLOSED_OR_COMPLETE",
                                         "reconstructed": True, "implementation_sha256": implementations[ident],
                                         "implementation_scheme": implementation_scheme(ident)})

        def save(batch, next_record, status="RUNNING"):
            _guard(run, write_guard, cancel_check, timeout_seconds, max_memory_mb)
            state = dict(input_digest=digest, next_record=next_record, write_from=write_from,
                         bar_count=len(bars), prefix_chain=chains[-1] if chains else None,
                         implementation_sha256=implementation, implementations=implementations)
            def hook(conn, _counts):
                _confirm_source(conn, db, symbol, label, sequence)
                producer_jobs.advance_checkpoint(conn, symbol=symbol, timeframe=label, family=FAMILY,
                                                 algorithm_version=version, config_hash=config_hash,
                                                 source_sequence=sequence, last_bar_start=last_bar,
                                                 state=state, status=status)
            result = feature_store.save_values(db, batch, transaction_hook=hook)
            for name in counts:
                counts[name] += result[name]

        batch, next_record = [], 0
        for index, record in enumerate(records()):
            next_record = index + 1
            if index < cursor:
                continue
            batch.append(record)
            if len(batch) == batch_size:
                save(batch, next_record)
                batch = []
        if batch:
            save(batch, next_record)
        save([], next_record, "COMPLETE")
        job = producer_jobs.finish_run(db, run, source_sequence=sequence, status="COMPLETE", counts=counts)
        return dict(symbol=symbol, timeframe=label, features=len(ids), skipped=False, bars=len(bars),
                    write_from=write_from, evaluation="exact_prefix", **counts, job=job)
    except BaseException as exc:
        producer_jobs.abort_run(db, run, source_sequence=sequence, counts=counts, error=exc,
                                cancelled=isinstance(exc, Cancelled))
        raise
