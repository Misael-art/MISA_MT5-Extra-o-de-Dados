"""T19.3: consumidor do registry, com revisões e checkpoints no store existente.

Sem fonte nova ou fórmula paralela. Retomadas confirmam apenas o delta; a avaliação
numérica reutiliza o prefixo exato do registry, inclusive suas sementes recursivas.
"""
import hashlib
import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import inspect, text

from mt5_extracao.analytics import feature_store, features, indicator_registry, producer_jobs, producers, resampling
from mt5_extracao.strategies import indicators

log = logging.getLogger(__name__)
FAMILY = "registry_indicators"


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def implementation_signature():
    """Normaliza finais de linha; caminhos locais não entram no identificador."""
    sources = [hashlib.sha256(Path(module).read_text(encoding="utf-8").encode()).hexdigest()
               for module in (__file__, indicator_registry.__file__, indicators.__file__)]
    return hashlib.sha256(_json({"sources": sources, "pandas": pd.__version__, "numpy": np.__version__}).encode()).hexdigest()


def algorithm_version(feature_id, implementation=None):
    spec = indicator_registry.get_definition(feature_id)
    return spec["algorithm_version"] + "+" + (implementation or implementation_signature())[:12]


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
        contract = {name: spec[name] for name in ("family", "formula", "unit", "warmup_count", "warmup_unit",
                                                 "availability_rule", "value_type")}
        features.register(db, ident, spec["name"], algorithm_version(ident, implementation),
                          parameters={**spec["parameters"], "implementation_sha256": implementation},
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
    implementation = implementation_signature()
    config_hash = hashlib.sha256(_json({"features": ids, "implementation": implementation}).encode()).hexdigest()
    version = algorithm_version(ids[0], implementation)
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
        _register(db, ids, implementation)
        previous = {ident: feature_store.latest_values(db, symbol=symbol, timeframe=label, feature_ids=[ident],
                                                       algorithm_version=algorithm_version(ident, implementation))
                    for ident in ids}
        raw_starts = set(raw.bar_start_utc)
        if any(not set(frame.observation_time).issubset(raw_starts) for frame in previous.values()):
            raise ValueError("O histórico derivado perdeu observações já materializadas. Restaure ou reconcilie "
                             "a revisão da fonte antes de retomar; valores anteriores foram preservados.")
        chains = producers._prefix_source_revisions(producers._source_tokens(bars)) if not bars.empty else []
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
        if not raw.empty:
            for row in raw.itertuples():
                if row.revision > 1:
                    latest_correction = max(latest_correction, row.computed_at_utc) if latest_correction is not None else row.computed_at_utc
                corrections.append((row.bar_start_utc, latest_correction))
        correction_by_start = dict(corrections)

        def records():
            if not bars.empty:
                for item in indicator_registry.iter_core(bars, feature_ids=ids):
                    position = item["bar_index"]
                    if position < write_from:
                        continue
                    bar = bars.iloc[position]
                    available = max(bar.bar_end_utc, correction_by_start.get(bar.bar_start_utc) or bar.bar_end_utc)
                    # real_volume_known prevalece sobre zero/valor preenchido pela fonte.
                    if item["feature_id"] == "volume.real" and (pd.isna(bar.get("real_volume_known")) or
                                                                 bar.get("real_volume_known") != 1):
                        item = dict(item, value=None, state="SOURCE_VALUE_MISSING", reason="REAL_VOLUME_NOT_OBSERVED")
                    metadata = {"state": item["state"], "reason": item["reason"], "reconstructed": True,
                                "registry_version": item["algorithm_version"],
                                "implementation_sha256": implementation,
                                "bar_end_utc": bar.bar_end_utc.isoformat(), "source_scope": "causal_prefix"}
                    if "display_offset_bars" in item:
                        metadata.update(calculation_bar_start_utc=bar.bar_start_utc.isoformat(),
                                        calculation_bar_end_utc=bar.bar_end_utc.isoformat(),
                                        display_anchor_bar_start_utc=bar.bar_start_utc.isoformat(),
                                        display_offset_bars=item["display_offset_bars"], display_time_utc=None,
                                        display_time_rule="source_bar_ordinal_plus_offset; no UTC forecast")
                    yield dict(symbol=symbol, timeframe=label, feature_id=item["feature_id"],
                               algorithm_version=algorithm_version(item["feature_id"], implementation),
                               observation_time=bar.bar_start_utc, value=item["value"], available_at=available,
                               observed_at=available, source_revision=chains[position],
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
                                         "reconstructed": True, "implementation_sha256": implementation})

        def save(batch, next_record, status="RUNNING"):
            _guard(run, write_guard, cancel_check, timeout_seconds, max_memory_mb)
            state = dict(input_digest=digest, next_record=next_record, write_from=write_from,
                         bar_count=len(bars), prefix_chain=chains[-1] if chains else None,
                         implementation_sha256=implementation)
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
    except Exception as exc:
        producer_jobs.finish_run(db, run, source_sequence=sequence,
                                 status="CANCELLED" if isinstance(exc, Cancelled) else "FAILED", counts=counts, error=exc)
        raise
