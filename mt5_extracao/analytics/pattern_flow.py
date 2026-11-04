"""Orquestração T13.4-P06 sobre snapshots, experimentos e motor existentes."""

import hashlib
import json
from pathlib import Path

import pandas as pd

from mt5_extracao import timeframes
from . import datasets, hypotheses, pattern_engine
from mt5_extracao.experiments import store


def _implementation_hashes():
    root = Path(__file__).resolve().parents[1]
    names = ("analytics/pattern_flow.py", "analytics/pattern_engine.py",
             "analytics/associations.py", "strategies/validation.py",
             "strategies/pattern_adapter.py", "experiments/store.py")
    return {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in names}


def _baseline_samples(definition, observations, emitted, *, as_of):
    """Eventos e controles usam o mesmo resolvedor de desfechos P02."""
    horizon = definition.body["outcome_definition"]["horizon_bars"]
    by_start = {item["bar_start_utc"]: item for item in emitted}
    pseudo = []
    for row in observations:
        signal = by_start.get(row["bar_start_utc"])
        if signal is None:
            signal = {"event_id": "control:" + hypotheses.digest(
                [definition.definition_hash, row["source_namespace"], row["bar_start_utc"]]),
                "definition_hash": definition.definition_hash,
                "source_namespace": row["source_namespace"], "symbol": row["symbol"],
                "timeframe": row["timeframe"], "side": definition.body["side"],
                "bar_start_utc": row["bar_start_utc"], "bar_end_utc": row["bar_end_utc"],
                "decision_at_utc": row["available_at_utc"]}
        pseudo.append(signal)
    resolved = pattern_engine.resolve_outcomes(definition, pseudo, observations, as_of=as_of)
    received_by_end = {row["bar_end_utc"]: row["available_at_utc"] for row in observations}
    rows = []
    for index, (bar, item) in enumerate(zip(observations, resolved)):
        outcome = item["outcome"]
        potential_end = (observations[index + horizon]["bar_end_utc"]
                         if index + horizon < len(observations) else None)
        end = outcome.get("end_bar_end_utc") or potential_end
        received = received_by_end.get(outcome.get("end_bar_end_utc")) if outcome["status"] == "complete" else None
        rows.append({"source_namespace": bar["source_namespace"], "symbol": bar["symbol"],
                     "timeframe": bar["timeframe"], "horizon_bars": horizon,
                     "session_id": bar["session_id"],
                     "stratum_id": "session_hour:" + str(bar["session_minute"] // 60),
                     "signal_bar_start_utc": bar["bar_start_utc"],
                     "decision_at_utc": bar["available_at_utc"],
                     "feature_available_at_utc": bar["available_at_utc"],
                     "is_pattern": bar["bar_start_utc"] in by_start,
                     "outcome_status": outcome["status"],
                     "outcome_value": float(outcome["terminal_return"]) if outcome["status"] == "complete" else None,
                     "label_end_utc": end, "label_available_at_utc": received})
    return pd.DataFrame(rows)


def _economic_result(snapshot, prepared, definition, events, campaign, *, role, session_mode):
    plan = campaign["definition"]["economic_plan"]
    required = {"schema_version", "spec_source", "symbol_spec", "policy", "costs"}
    if not isinstance(plan, dict) or set(plan) != required or plan.get("schema_version") != 1:
        return {"status": "blocked", "reason": "Plano econômico tipado não foi congelado na campanha."}
    try:
        from mt5_extracao.strategies.backtester import SymbolSpec
        from mt5_extracao.strategies.pattern_adapter import (
            PatternCostModel, PatternExecutionPolicy, run_pattern_simulation)

        if plan["spec_source"] == "snapshot" and plan["symbol_spec"] is None:
            spec = snapshot.spec
        elif (plan["spec_source"] == "synthetic_override" and session_mode == "synthetic_utc_day"
              and isinstance(plan["symbol_spec"], dict)):
            spec = SymbolSpec(**plan["symbol_spec"])
        else:
            raise ValueError("Especificação econômica sem origem compatível com o cenário.")
        policy = PatternExecutionPolicy(**plan["policy"])
        costs = PatternCostModel(**plan["costs"])
        if plan["spec_source"] == "synthetic_override" and costs.scenario != "hypothetical":
            raise ValueError("Especificação sintética exige custo hipotético explícito.")
        window = campaign["definition"]["roles"][role]
        first, last = (_utc(window[key], key) for key in ("start_utc", "end_utc"))
        bars = prepared["backtest_bars"]
        starts = pd.to_datetime(bars["bar_start_utc"], utc=True)
        subset = bars.loc[(starts >= first) & (starts < last)].reset_index(drop=True)
        if subset.empty:
            return {"status": "insufficient", "reason": "Nenhuma barra econômica na janela global."}
        simulation = run_pattern_simulation(
            subset, events, definition, source_namespace=prepared["source_namespace"],
            symbol_spec=spec, policy=policy, costs=costs)
        backtest = simulation["backtest"]
        return {"status": "simulated_" + simulation["cost_status"],
                "metrics": backtest.metrics,
                "trades": [{key: value.isoformat() if isinstance(value, pd.Timestamp) else value
                            for key, value in vars(trade).items()} for trade in backtest.trades],
                "ending_account_balance": float(backtest.account_balance.iloc[-1]),
                "ending_floating_equity": float(backtest.floating_equity.iloc[-1]),
                "ambiguous_stop_first_bars": simulation["ambiguous_stop_first_bars"],
                "event_execution": simulation["events"],
                "cost_model_id": costs.model_id,
                "economic_approval": False, "portfolio_qualified": False,
                "native_set_equivalent": False}
    except (TypeError, ValueError) as exc:
        return {"status": "blocked", "reason": str(exc)}


def _utc(value, label):
    try:
        stamp = pd.Timestamp(value)
    except (ValueError, TypeError, OverflowError) as exc:
        raise ValueError(f"{label} exige instante com fuso explícito.") from exc
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError(f"{label} exige instante com fuso explícito.")
    return stamp.tz_convert("UTC")


def snapshot_observations(snapshot, *, session_mode="observed"):
    """Converte o snapshot congelado em barras P02 sem consultar a base viva.

    Na falta de recibo de disponibilidade da barra, replay histórico usa o
    fechamento como hipótese e a reporta; não vira prova prospectiva.
    ``synthetic_utc_day`` é somente para QA controlada sem calendário real.
    """
    if not isinstance(snapshot, datasets.Snapshot):
        raise ValueError("Fonte precisa ser snapshot canônico carregado.")
    manifest = snapshot.manifest
    if manifest["time"]["offset_state"] != "declared":
        raise ValueError("Snapshot sem offset fixo comprovado não tem relógio UTC elegível.")
    tf = timeframes.parse(manifest["timeframe"])
    if tf is None or tf.minutes is None or tf.minutes < 1:
        raise ValueError("Timeframe do snapshot sem duração de barra suportada.")
    if session_mode not in {"observed", "synthetic_utc_day"}:
        raise ValueError("Modo de sessão desconhecido; calendário não pode ser inferido.")
    bars = snapshot.bars.reset_index(drop=True)
    if bars.empty:
        raise ValueError("Snapshot sem barras não produz pesquisa de preço.")
    offset = pd.Timedelta(hours=manifest["time"]["broker_utc_offset_hours"])
    derived = snapshot.derived_bars
    evidence = {}
    if not derived.empty:
        for _, row in derived.iterrows():
            start = _utc(row.bar_start_utc, "bar_start_utc")
            revision = int(row.revision)
            current = evidence.get(start)
            if current is None or revision > int(current.revision):
                evidence[start] = row
    sessions = snapshot.market_calendar
    if session_mode == "observed":
        if sessions.empty:
            raise ValueError("Família exige calendário de sessões observado; use apenas QA sintética explicitamente.")
        sessions = sessions.sort_values("revision").drop_duplicates("session_id", keep="last")
        session_rows = [(str(row.session_id), _utc(row.open_utc, "open_utc"),
                         _utc(row.close_utc, "close_utc")) for _, row in sessions.iterrows()]
        if any(opened >= closed for _, opened, closed in session_rows):
            raise ValueError("Calendário observado contém sessão inválida.")
    else:
        session_rows = []
    observations, frames = [], []
    used_derived = 0
    synthetic_starts = {}
    last_start, last_available = None, None
    for _, row in bars.iterrows():
        broker = pd.Timestamp(row.time)
        if pd.isna(broker) or broker.tzinfo is not None:
            raise ValueError("Snapshot espera horário broker ingênuo e offset congelado.")
        start = broker.tz_localize("UTC") - offset
        end = start + pd.Timedelta(minutes=tf.minutes)
        if last_start is not None and start < last_start + pd.Timedelta(minutes=tf.minutes):
            raise ValueError("Snapshot contém barras sobrepostas ou fora de ordem.")
        item = evidence.get(start)
        used_derived += item is not None
        available = _utc(item.computed_at_utc, "computed_at_utc") if item is not None else end
        if available < end or (last_available is not None and available < last_available):
            raise ValueError("Disponibilidade de revisão fora da ordem causal da série.")
        if item is not None and any(float(getattr(item, key)) != float(getattr(row, key))
                                    for key in ("open", "high", "low", "close")):
            raise ValueError("Barras derivadas divergem do OHLC congelado; não misture revisões.")
        if session_mode == "observed":
            found = [(ident, opened) for ident, opened, closed in session_rows if opened <= start < closed]
            if len(found) != 1:
                raise ValueError("Barra sem sessão observada única no calendário congelado.")
            session_id, session_start = found[0]
        else:
            day = start.date().isoformat()
            session_start = synthetic_starts.setdefault(day, start)
            session_id = "synthetic_utc_day:" + day
        minute = int((start - session_start) / pd.Timedelta(minutes=1))
        revision = int(item.revision) if item is not None else 1
        source_revision = str(item.input_hash) if item is not None else snapshot.manifest_id
        base = {"source_namespace": manifest["source"]["namespace"], "symbol": manifest["symbol"],
                "timeframe": manifest["timeframe"], "bar_start_utc": start.isoformat(),
                "bar_end_utc": end.isoformat(), "available_at_utc": available.isoformat(),
                "revision": revision, "session_id": session_id, "session_minute": minute,
                "source_revision": source_revision,
                **{key: None if pd.isna(row[key]) else str(row[key]) for key in ("open", "high", "low", "close")}}
        observations.append(base)
        frames.append({"time": start.isoformat(), "bar_start_utc": start.isoformat(),
                       "bar_end_utc": end.isoformat(), "available_at_utc": available.isoformat(),
                       "revision": revision, "source_namespace": manifest["source"]["namespace"],
                       "symbol": manifest["symbol"], "timeframe": manifest["timeframe"],
                       **{key: row[key] for key in ("open", "high", "low", "close")},
                       "spread": row["spread"] if "spread" in bars else None})
        last_start, last_available = start, available
    clock_basis = ("derived_computed_at" if used_derived == len(bars) else
                   "historical_bar_close_assumption" if used_derived == 0 else
                   "mixed_derived_and_bar_close_assumption")
    return {"observations": observations, "backtest_bars": pd.DataFrame(frames),
            "clock_basis": clock_basis, "session_basis": session_mode,
            "manifest_id": snapshot.manifest_id,
            "source_namespace": manifest["source"]["namespace"],
            "symbol": manifest["symbol"], "timeframe": manifest["timeframe"]}


def evaluate_registered_attempt(db, request_id, data_manifest_id, *, session_mode="observed"):
    """Avalia uma tentativa admitida contra um snapshot; persiste sinais e labels.

    Qualquer leitura de resultados da reserva é registrada antes de abrir o
    snapshot. O retorno é um recibo de pesquisa, nunca aprovação econômica.
    """
    attempt = store.request(db, request_id)
    if attempt["state"] != "requested":
        raise ValueError("Tentativa recusada ou descartada não pode ser avaliada.")
    protocol_id = attempt["protocol_id"]
    campaign = store.protocol(db, protocol_id)
    if campaign["definition"].get("type") != "pattern_campaign_v1":
        raise ValueError("Tentativa não pertence à campanha T13.4.")
    role = attempt["payload"]["role"]
    roster = campaign["definition"]["roster"]
    source = next((item for item in roster if item["data_manifest_id"] == data_manifest_id), None)
    if source is None:
        raise ValueError("Snapshot fora do roster congelado da campanha.")
    store.pattern_role_access(db, protocol_id, role, "evaluate:" + request_id,
                              attempt["payload"]["purpose"])
    snapshot = datasets.load(db, data_manifest_id)
    prepared = snapshot_observations(snapshot, session_mode=session_mode)
    if (prepared["source_namespace"], prepared["symbol"], prepared["timeframe"]) != (
            source["source_namespace"], source["symbol"], source["timeframe"]):
        raise ValueError("Snapshot diverge da identidade congelada.")
    raw_definition = dict(attempt["payload"]["definition"])
    expected_hash = raw_definition.pop("definition_hash")
    definition = hypotheses.compile_definition(raw_definition)
    if definition.definition_hash != expected_hash or expected_hash != attempt["payload"]["definition_hash"]:
        raise ValueError("Definição da tentativa mudou após registro.")
    family = definition.body["pattern_id"]
    if family not in {"reversal", "continuation_time", "gap", "breakout_failure"}:
        raise ValueError("P06 só avalia as quatro famílias iniciais declaradas.")
    observations = prepared["observations"]
    if family == "breakout_failure":
        from .pattern_families import prior_breakout_references
        observations = prior_breakout_references(
            observations, definition.body["complexity"]["parameters"]["lookback_bars"])
    window = campaign["definition"]["roles"][role]
    start, cutoff = (_utc(window[key], key) for key in ("start_utc", "end_utc"))
    known = [row for row in observations if _utc(row["available_at_utc"], "available_at_utc") <= cutoff]
    _, all_events = pattern_engine.evaluate_batch(definition, prepared["source_namespace"],
                                                   prepared["symbol"], known)
    events = [item for item in all_events if start <= _utc(item["bar_start_utc"], "bar_start_utc") < cutoff]
    resolved = pattern_engine.resolve_outcomes(definition, events, known, as_of=cutoff.isoformat())
    samples = _baseline_samples(definition, known, all_events, as_of=cutoff.isoformat())
    role_samples = samples.loc[(pd.to_datetime(samples["signal_bar_start_utc"], utc=True) >= start) &
                               (pd.to_datetime(samples["signal_bar_start_utc"], utc=True) < cutoff)]
    negatives = role_samples.loc[~role_samples["is_pattern"]]
    validation = {"status": "not_validation_role" if role == "train" else "blocked",
                  "reason": "P04 exige plano temporal e suporte pareado congelados."}
    if role in {"validation", "holdout"}:
        try:
            from mt5_extracao.experiments.service import pattern_validation_context
            from mt5_extracao.strategies.validation import plan_pattern_folds
            from .associations import fit_pattern_baseline, analyze_pattern_baseline

            context = pattern_validation_context(db, protocol_id)
            plan = context["plan"]
            train_end = _utc(context["roles"]["train"]["end_utc"], "train_end_utc")
            train_known = [item for item in observations if
                           _utc(item["available_at_utc"], "available_at_utc") <= train_end]
            _, train_events = pattern_engine.evaluate_batch(definition, prepared["source_namespace"],
                                                            prepared["symbol"], train_known)
            train_samples = _baseline_samples(definition, train_known, train_events,
                                              as_of=train_end.isoformat())
            train_folds = plan_pattern_folds(train_samples, context["roles"],
                                            embargo_seconds=plan["embargo_seconds"])
            training = train_samples.loc[train_folds["roles"]["train"]["selected"]]
            frozen = fit_pattern_baseline(training, as_of=train_end.isoformat())
            folds = plan_pattern_folds(samples, context["roles"],
                                       embargo_seconds=plan["embargo_seconds"])
            scored = samples.loc[folds["roles"][role]["scored"]]
            if scored.empty:
                validation = {"status": "insufficient", "reason": "Nenhum par maduro após purga/embargo.",
                              "fold": folds["roles"][role], "baseline_fit_hash": frozen["fit_hash"],
                              "tests_counted": context["tests_counted"]}
            else:
                estimate = analyze_pattern_baseline(
                    scored, frozen=frozen, as_of=cutoff.isoformat(),
                    alpha=context["criteria"]["alpha"], confidence=context["criteria"]["confidence"],
                    tests_counted=context["tests_counted"],
                    bootstrap_replicates=plan["bootstrap_replicates"], seed=context["seed"],
                    minimum_sessions=plan["minimum_sessions"],
                    block_length_sessions=plan["block_length_sessions"])
                validation = {"status": estimate["status"], "estimate": estimate,
                              "fold": folds["roles"][role],
                              "confirmatory_family_frozen": context["confirmatory_family_frozen"],
                              "prior_external_exposure": "unknown", "qualified_final_holdout": False}
        except ValueError as exc:
            validation = {"status": "blocked", "reason": str(exc)}
    availability_by_end = {item["bar_end_utc"]: item["available_at_utc"] for item in known}
    suffix = store.digest([request_id, data_manifest_id])
    signal_id, outcome_id = "pattern.signal." + suffix, "pattern.outcome." + suffix
    records = []
    for signal, outcome in zip(events, resolved):
        shared = {"symbol": prepared["symbol"], "timeframe": prepared["timeframe"],
                  "algorithm_version": "1", "observation_time": signal["bar_start_utc"],
                  "source_revision": signal["event_id"]}
        decision = signal["decision_at_utc"]
        records.append(dict(shared, feature_id=signal_id, value="emitted",
                            observed_at=decision, available_at=decision,
                            metadata={"event": signal, "data_manifest_id": data_manifest_id,
                                      "source_namespace": prepared["source_namespace"]}))
        label = outcome["outcome"]
        if label["status"] == "complete":
            received = availability_by_end.get(label["end_bar_end_utc"])
            if received is None:
                raise ValueError("Desfecho completo sem barra final recebida no snapshot.")
        else:
            received = cutoff.isoformat()
        records.append(dict(shared, feature_id=outcome_id, value=label["status"],
                            observed_at=received, available_at=received,
                            metadata={"event_id": signal["event_id"], "outcome": label,
                                      "data_manifest_id": data_manifest_id,
                                      "source_namespace": prepared["source_namespace"]}))
    encoded = json.dumps(resolved, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    results_sha256 = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    implementation = _implementation_hashes()
    economic = _economic_result(snapshot, prepared, definition, events, campaign,
                                role=role, session_mode=session_mode)
    from mt5_extracao.presets.research import build_research_preset
    research_preset = build_research_preset(
        campaign, attempt, data_manifest_id=data_manifest_id,
        source_namespace=prepared["source_namespace"], results_sha256=results_sha256,
        implementation=implementation)
    receipt = {"schema_version": 1, "protocol_id": protocol_id, "request_id": request_id,
               "data_manifest_id": data_manifest_id, "source_namespace": prepared["source_namespace"],
               "symbol": prepared["symbol"], "timeframe": prepared["timeframe"], "family": family,
               "definition_hash": definition.definition_hash, "role": role, "as_of_utc": cutoff.isoformat(),
               "clock_basis": prepared["clock_basis"], "session_basis": prepared["session_basis"],
               "event_count": len(resolved),
               "opportunity_count": len(role_samples), "negative_count": len(negatives),
               "negative_complete_count": int(negatives["outcome_status"].eq("complete").sum()),
               "negative_censored_count": int((~negatives["outcome_status"].eq("complete")).sum()),
               "complete_count": sum(item["outcome"]["status"] == "complete" for item in resolved),
               "censored_count": sum(item["outcome"]["status"] != "complete" for item in resolved),
               "results_sha256": results_sha256, "implementation": implementation,
               "validation": validation, "economic": economic, "research_preset": research_preset,
               "statistical_approval": False, "economic_approval": False,
               "qualified_final_holdout": False, "native_set_equivalent": False}
    return store.persist_pattern_evaluation(db, protocol_id, request_id, receipt, records)
