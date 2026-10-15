"""Validação temporal, catálogo e trilha de tentativas de padrões (T13.4)."""
import configparser
import hashlib
import itertools
import json
import threading
import uuid
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from sqlalchemy import inspect, text

from mt5_extracao.analytics.hypotheses import STATUSES, validate_definition
from mt5_extracao.analytics.pattern_engine import condition_mask
from mt5_extracao.trade_analytics import rules, stats

DEFINITIONS_TABLE = "_pattern_definitions"
EVALUATIONS_TABLE = "_pattern_evaluations"
RESERVATIONS_TABLE = "_pattern_reservations"
_LOCK = threading.RLock()
_TRANSITIONS = {
    "DRAFT": {"EXPLORATORY", "RETIRED"},
    "EXPLORATORY": {"VALIDATING", "RETIRED"},
    "VALIDATING": {"EXPLORATORY", "APPROVED", "RETIRED"},
    "APPROVED": {"MONITORING", "RETIRED"},
    "MONITORING": {"VALIDATING", "RETIRED"},
    "RETIRED": set(),
}


def _check_write_guard(write_guard):
    if write_guard is not None:
        reason = write_guard()
        if reason:
            raise RuntimeError("Gravação analítica bloqueada pelo guarda de recursos: " + str(reason))


def ensure_schema(db, *, write_guard=None):
    """Cria apenas tabelas internas novas; os dados brutos e tabelas de usuário não são alterados."""
    if db is None or getattr(db, "engine", None) is None:
        raise ValueError("O banco analítico não está conectado.")
    _check_write_guard(write_guard)
    with _LOCK, db.engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE IF NOT EXISTS {DEFINITIONS_TABLE} ("
                          "pattern_id TEXT NOT NULL, version TEXT NOT NULL, title TEXT NOT NULL, "
                          "definition_json TEXT NOT NULL, definition_hash TEXT NOT NULL, status TEXT NOT NULL, "
                          "registered_at_utc TEXT NOT NULL, PRIMARY KEY (pattern_id, version))"))
        conn.execute(text(f"CREATE TABLE IF NOT EXISTS {EVALUATIONS_TABLE} ("
                          "evaluation_id TEXT PRIMARY KEY, pattern_id TEXT NOT NULL, version TEXT NOT NULL, "
                          "symbol TEXT NOT NULL, timeframe TEXT NOT NULL, attempt_no INTEGER NOT NULL, "
                          "tests_counted INTEGER NOT NULL, dataset_hash TEXT NOT NULL, holdout_key TEXT NOT NULL, "
                          "holdout_reused INTEGER NOT NULL, status TEXT NOT NULL, result_json TEXT NOT NULL, "
                          "validated_at_utc TEXT NOT NULL)"))
        conn.execute(text(f"CREATE TABLE IF NOT EXISTS {RESERVATIONS_TABLE} ("
                          "pattern_id TEXT NOT NULL, version TEXT NOT NULL, symbol TEXT NOT NULL, "
                          "timeframe TEXT NOT NULL, evaluation_id TEXT NOT NULL, consumed_at_utc TEXT NOT NULL, "
                          "PRIMARY KEY (pattern_id, version, symbol, timeframe))"))
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS ix_pattern_evaluations_lookup "
                          f"ON {EVALUATIONS_TABLE} (pattern_id, version, symbol, timeframe, attempt_no)"))


def _canonical_definition(definition):
    immutable = {key: value for key, value in definition.items() if key != "status"}
    payload = json.dumps(immutable, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return payload, hashlib.sha256(payload.encode("utf-8")).hexdigest()


def register_definition(db, definition, *, write_guard=None):
    """Registra uma hipótese imutável por versão; status pode avançar pelo fluxo governado."""
    validate_definition(definition)
    _check_write_guard(write_guard)
    ensure_schema(db, write_guard=write_guard)
    payload, fingerprint = _canonical_definition(definition)
    with _LOCK, db.engine.begin() as conn:
        existing = conn.execute(text(f"SELECT definition_hash FROM {DEFINITIONS_TABLE} WHERE pattern_id=:id AND version=:v"),
                                {"id": definition["pattern_id"], "v": definition["version"]}).scalar()
        if existing:
            if existing != fingerprint:
                raise ValueError("A hipótese já existe nesta versão com outra definição; incremente a versão.")
            return False
        conn.execute(text(f"INSERT INTO {DEFINITIONS_TABLE} "
                          "(pattern_id, version, title, definition_json, definition_hash, status, registered_at_utc) "
                          "VALUES (:id, :v, :title, :definition, :hash, :status, :registered)"),
                     {"id": definition["pattern_id"], "v": definition["version"], "title": definition["title"],
                      "definition": payload, "hash": fingerprint, "status": definition["status"],
                      "registered": datetime.now(timezone.utc).isoformat()})
    return True


def list_patterns(db):
    if db is None or getattr(db, "engine", None) is None or not inspect(db.engine).has_table(DEFINITIONS_TABLE):
        return []
    with db.engine.connect() as conn:
        rows = conn.execute(text(f"SELECT pattern_id, version, title, definition_json, status "
                                 f"FROM {DEFINITIONS_TABLE} ORDER BY pattern_id, version")).mappings()
        result = []
        for row in rows:
            item = dict(row)
            item["definition"] = json.loads(item.pop("definition_json"))
            item["definition"]["status"] = item["status"]
            result.append(item)
        return result


def _parameter_candidates(definition):
    return _grid_combinations(definition.get("parameter_grid", {}))


def _parameter_scenarios(definition, candidates, selected_parameters):
    perturbations = _grid_combinations(definition.get("sensitivity_grid", {}))
    scenarios = []
    seen = set()
    for candidate in candidates:
        parameters = dict(definition["parameters"])
        parameters.update(candidate)
        for perturbation in perturbations:
            scenario = dict(parameters)
            scenario.update(perturbation)
            fingerprint = json.dumps(scenario, sort_keys=True, separators=(",", ":"))
            if fingerprint not in seen:
                scenarios.append(scenario)
                seen.add(fingerprint)
    if selected_parameters:
        fingerprint = json.dumps(selected_parameters, sort_keys=True, separators=(",", ":"))
        if fingerprint not in seen:
            scenarios.insert(0, dict(selected_parameters))
    return scenarios


def _grid_combinations(grid):
    if not grid:
        return [{}]
    keys = list(grid)
    return [dict(zip(keys, values)) for values in itertools.product(*(grid[key] for key in keys))]


def _resolve_parameters(condition, parameters):
    resolved = dict(condition)
    if "field" in resolved:
        if "parameter" in resolved:
            resolved["value"] = parameters[resolved.pop("parameter")]
        return resolved
    resolved["conditions"] = [_resolve_parameters(child, parameters) for child in resolved["conditions"]]
    return resolved


def _condition_for(samples, definition, parameter_overrides=None):
    parameters = dict(definition["parameters"])
    parameters.update(parameter_overrides or {})
    mask = condition_mask(samples, _resolve_parameters(definition["condition"], parameters))
    target = definition["target"]
    resolved_target = _resolve_parameters(target, parameters)
    field = resolved_target["field"]
    if field not in samples:
        success = pd.Series(pd.NA, index=samples.index, dtype="boolean")
    else:
        success = condition_mask(samples, resolved_target).astype("boolean")
        success = success.mask(samples[field].isna(), pd.NA)
    return mask, success


def _select_parameters(frame, search_mask, definition, candidates, minimum):
    search = frame.loc[search_mask]
    results, best_override, best_score = [], {}, None
    for index, override in enumerate(candidates):
        candidate_condition, candidate_success = _condition_for(frame, definition, override)
        chosen = search.loc[candidate_condition.loc[search.index] & candidate_success.loc[search.index].notna()]
        executed = chosen.get("trade_executed", pd.Series(True, index=chosen.index)).fillna(False).astype(bool)
        returns = pd.to_numeric(chosen.loc[executed, "outcome_return"], errors="coerce").dropna()
        score = float(returns.mean()) if len(returns) >= minimum else None
        results.append({"candidate": index, "parameters": override, "n_executed": int(len(returns)),
                        "train_expectancy_net": score})
        if score is not None and (best_score is None or score > best_score):
            best_override, best_score = override, score
    return best_override, best_score, results


def _empty_metric():
    return {"n_total": 0, "n_condition": 0, "n_success": 0, "p_conditional": None, "p_base": None,
            "lift": None, "expectancy_net": None, "expectancy_net_per_observation": None,
            "expectancy_ci": [None, None], "difference_ci": [None, None],
            "p_value": None, "p_value_method": "moving-block wild bootstrap under zero-effect null",
            "n_executed": 0, "worst_outcome": None, "max_drawdown": None,
            "max_drawdown_basis": "cumulative one-unit net returns; no portfolio sizing",
            "stability_by_year": {},
            "top_five_percent_profit_share": None, "cost_observed_fraction": None}


def _block_difference(frame, condition, success, n_boot=1000, seed=7):
    valid = success.notna()
    data = frame.loc[valid].copy().sort_values("observation_time", kind="stable")
    if data.empty:
        return [None, None], 1.0
    flags = condition.loc[data.index].astype(bool).to_numpy()
    target = success.loc[data.index].astype(float).to_numpy()
    day = pd.to_datetime(data["observation_time"]).dt.normalize().to_numpy()
    dates = list(dict.fromkeys(day))
    clusters = [np.flatnonzero(day == date) for date in dates]
    if len(clusters) < 2 or not flags.any() or flags.all():
        return [None, None], 1.0

    def difference(subset_flags, subset_target):
        left, right = subset_target[subset_flags], subset_target[~subset_flags]
        return float(left.mean() - right.mean()) if len(left) and len(right) else np.nan

    observed = difference(flags, target)
    rng = np.random.default_rng(seed)
    # Circular moving blocks resample whole session-days and retain nearby-day dependence.
    block_length = max(1, int(round(len(clusters) ** (1.0 / 3.0))))
    estimates, null_estimates = [], []
    for _ in range(n_boot):
        start_count = int(np.ceil(len(clusters) / block_length))
        starts = rng.integers(0, len(clusters), size=start_count)
        selected_days = [int((start + offset) % len(clusters))
                         for start in starts for offset in range(block_length)][:len(clusters)]
        positions = np.concatenate([clusters[index] for index in selected_days])
        value = difference(flags[positions], target[positions])
        if np.isfinite(value):
            estimates.append(value)
        # Fixed labels plus centered, block-sign-flipped residuals test the zero-effect null.
        day_signs = np.ones(len(clusters), dtype=float)
        for start in range(0, len(clusters), block_length):
            day_signs[start:start + block_length] = rng.choice((-1.0, 1.0))
        row_signs = np.concatenate([np.repeat(day_signs[index], len(cluster))
                                    for index, cluster in enumerate(clusters)])
        null_target = float(target.mean()) + (target - float(target.mean())) * row_signs
        null_value = difference(flags, null_target)
        if np.isfinite(null_value):
            null_estimates.append(null_value)
    if len(estimates) < 2 or len(null_estimates) < 2:
        return [None, None], 1.0
    low, high = np.quantile(estimates, [0.05, 0.95])
    null_values = np.asarray(null_estimates)
    p_value = float((1 + np.count_nonzero(np.abs(null_values) >= abs(observed))) / (len(null_values) + 1))
    return [float(low), float(high)], p_value


def _metric(frame, condition, success, n_boot=1000, seed=7):
    if frame.empty:
        return _empty_metric()
    valid_target = success.notna()
    total = frame.loc[valid_target]
    signal = total.loc[condition.loc[total.index].astype(bool)]
    rest = total.loc[~condition.loc[total.index].astype(bool)]
    if total.empty:
        return _empty_metric()
    p_base = float(success.loc[total.index].astype(float).mean())
    p_cond = float(success.loc[signal.index].astype(float).mean()) if not signal.empty else None
    lift = p_cond / p_base if p_cond is not None and p_base > 0 else None
    observed_outcomes = pd.to_numeric(signal.get("outcome_return"), errors="coerce").dropna() \
        if not signal.empty else pd.Series(dtype=float)
    executed = signal.get("trade_executed", pd.Series(True, index=signal.index)).fillna(False).astype(bool)
    traded_signal = signal.loc[executed]
    outcomes = pd.to_numeric(traded_signal.get("outcome_return"), errors="coerce").dropna() \
        if not traded_signal.empty else pd.Series(dtype=float)
    ci = stats.bootstrap_ci(outcomes, pd.to_datetime(signal.loc[outcomes.index, "observation_time"]),
                            n_boot=n_boot, alpha=0.10, seed=seed) if len(outcomes) else (None, None)
    diff_ci, p_value = _block_difference(total, condition.loc[total.index], success.loc[total.index],
                                         n_boot=n_boot, seed=seed + 1)
    sorted_signal = traded_signal.sort_values("observation_time")
    pnl = pd.to_numeric(sorted_signal.get("outcome_return"), errors="coerce").dropna() \
        if not sorted_signal.empty else pd.Series(dtype=float)
    equity = pnl.cumsum().to_numpy(dtype=float)
    peak = np.maximum.accumulate(np.maximum(equity, 0.0)) if len(equity) else np.array([])
    drawdown = float(np.max(peak - equity)) if len(equity) else 0.0
    gains = pnl[pnl > 0].sort_values(ascending=False)
    top_n = max(1, int(np.ceil(len(gains) * 0.05))) if len(gains) else 0
    positive_total = float(gains.sum())
    concentration = float(gains.iloc[:top_n].sum() / positive_total) if positive_total > 0 else 0.0
    by_year = {}
    if not traded_signal.empty:
        years = pd.to_datetime(traded_signal["observation_time"]).dt.year
        for year, group in traded_signal.assign(_year=years).groupby("_year"):
            values = pd.to_numeric(group["outcome_return"], errors="coerce").dropna()
            by_year[str(year)] = {"n": int(len(values)), "expectancy_net": float(values.mean()) if len(values) else None}
    observed_cost = traded_signal.get("cost_observed", pd.Series(False, index=traded_signal.index))
    cost_fraction = float(pd.Series(observed_cost, index=traded_signal.index).fillna(False).mean()) \
        if not traded_signal.empty else 0.0
    return {"n_total": int(len(total)), "n_condition": int(len(signal)),
            "n_success": int(success.loc[signal.index].sum()) if not signal.empty else 0,
            "p_conditional": p_cond, "p_base": p_base, "lift": float(lift) if lift is not None else None,
            "expectancy_net": float(outcomes.mean()) if len(outcomes) else None,
            "expectancy_net_per_observation": float(observed_outcomes.mean()) if len(observed_outcomes) else None,
            "expectancy_ci": [ci[0], ci[1]], "difference_ci": diff_ci, "p_value": p_value,
            "p_value_method": "moving-block wild bootstrap under zero-effect null",
            "n_executed": int(executed.sum()),
            "worst_outcome": float(outcomes.min()) if len(outcomes) else None, "max_drawdown": drawdown,
            "max_drawdown_basis": "cumulative one-unit net returns; no portfolio sizing",
            "stability_by_year": by_year, "top_five_percent_profit_share": concentration,
            "cost_observed_fraction": cost_fraction}


def _sensitivity(frame, condition, definition):
    selected = frame.loc[condition]
    if "trade_executed" in selected:
        selected = selected.loc[selected["trade_executed"].fillna(False).astype(bool)]
    scenarios = []
    if selected.empty:
        return scenarios
    gross = pd.to_numeric(selected.get("gross_return", pd.Series(np.nan, index=selected.index)), errors="coerce")
    costs = pd.to_numeric(selected.get("cost_bps", pd.Series(0.0, index=selected.index)), errors="coerce")
    if gross.notna().any():
        for multiplier in (0.0, 1.0, 2.0):
            values = gross - costs.fillna(0) * multiplier / 10000.0
            scenarios.append({"type": "cost_multiplier", "value": multiplier, "n": int(values.notna().sum()),
                              "expectancy_net": float(values.mean()) if values.notna().any() else None})
    for delay in (0, 1, 3):
        gross_name, cost_name = f"gross_return_delay_{delay}", f"cost_bps_delay_{delay}"
        if gross_name not in selected:
            continue
        delayed_cost = selected.get(cost_name, pd.Series(0.0, index=selected.index))
        delayed = pd.to_numeric(selected[gross_name], errors="coerce") - \
            pd.to_numeric(delayed_cost, errors="coerce").fillna(0) / 10000.0
        scenarios.append({"type": "entry_delay_bars", "value": delay, "n": int(delayed.notna().sum()),
                          "expectancy_net": float(delayed.mean()) if delayed.notna().any() else None})
    return scenarios


def _setting(definition):
    config = configparser.ConfigParser()
    config["TRADE_ANALYTICS"] = {
        "wf_folds": "4", "holdout_fraction": "0.2",
        "min_trades_cell": str(definition["validation"]["min_samples"]),
    }
    return config


def evaluate_samples(samples, definition, *, include_test=True, n_boot=1000, attempt_count=1, tests_counted=None):
    """Walk-forward expanding, com embargo até o fim do rótulo, bootstrap em dias e reserva final separada."""
    validate_definition(definition)
    candidates = _parameter_candidates(definition)
    family_tests = int(tests_counted) if tests_counted is not None else max(1, int(attempt_count)) * len(candidates)
    if samples is None or samples.empty:
        empty_status = "DRAFT" if definition["status"] == "DRAFT" else "EXPLORATORY"
        return {"pattern_id": definition["pattern_id"], "version": definition["version"], "status": empty_status,
                "weak": True, "note": "sem observações avaliáveis", "sample_count": 0, "folds": [],
                "train": _empty_metric(), "validation": _empty_metric(), "test": {"reused": not include_test},
                "parameter_selection": {"method": "maximum net expectancy on training partition",
                                        "candidate_count": len(candidates), "selected": None},
                "parameter_sensitivity": [], "sensitivity": [],
                "multiple_testing": {"method": "Bonferroni family-wise", "tests_counted": family_tests,
                "p_adjusted": 1.0}, "eligible_for_approval": False}
    frame = samples.sort_values("observation_time", kind="stable").reset_index(drop=True).copy()
    if "horizon_end" not in frame:
        frame["horizon_end"] = pd.to_datetime(frame["observation_time"]) + pd.to_timedelta(
            definition["validation"]["horizon_minutes"], unit="m")
    frame["observation_time"] = pd.to_datetime(frame["observation_time"])
    frame["horizon_end"] = pd.to_datetime(frame["horizon_end"])
    if "outcome_return" not in frame:
        raise ValueError("As observações precisam conter outcome_return.")
    entry_times = frame.get("entry_time", frame["observation_time"])
    frame["entry_time"] = pd.to_datetime(entry_times, errors="coerce").fillna(frame["observation_time"])
    frame["exit_time"] = frame["horizon_end"]
    frame["pnl"] = pd.to_numeric(frame["outcome_return"], errors="coerce")
    plan = rules.split(frame, _setting(definition))
    search_mask = ~plan.holdout
    train_mask = plan.folds[-1][0] if plan.folds else search_mask
    validation_mask = plan.folds[-1][1] if plan.folds else pd.Series(False, index=frame.index)
    train = frame.loc[train_mask]
    validation = frame.loc[validation_mask]
    holdout = frame.loc[plan.holdout]
    seed = int(hashlib.sha1(f"{definition['pattern_id']}|{definition['version']}".encode()).hexdigest()[:8], 16)

    # Candidate ranking sees only the final expanding-search partition. Validation and holdout
    # are applied after the selected parameter set is fixed.
    minimum = int(definition["validation"]["min_samples"])
    best_override, best_score, candidate_scores = _select_parameters(
        frame, train_mask, definition, candidates, minimum)
    selected_parameters = dict(definition["parameters"])
    selected_parameters.update(best_override)
    condition, success = _condition_for(frame, definition, best_override)
    frame["_condition"], frame["_success"] = condition, success
    quality = {"evaluated_count": int(success.notna().sum()),
               "excluded_target_count": int(success.isna().sum()),
               "ambiguous_breakout_count": int(frame.get("breakout_ambiguous", pd.Series(False, index=frame.index))
                                                .fillna(False).astype(bool).sum()),
               "unavailable_breakout_count": int((~frame.get("breakout_available", pd.Series(True, index=frame.index))
                                                  .fillna(False).astype(bool)).sum())}
    train, validation, holdout = frame.loc[train_mask], frame.loc[validation_mask], frame.loc[plan.holdout]
    train_metric = _metric(train, train["_condition"], train["_success"], n_boot, seed)
    validation_metric = _metric(validation, validation["_condition"], validation["_success"], n_boot, seed + 1)
    test_metric = _metric(holdout, holdout["_condition"], holdout["_success"], n_boot, seed + 2) if include_test \
        else {"reused": True}
    fold_results = []
    for index, (search, check) in enumerate(plan.folds, 1):
        fold_override, fold_score, fold_candidates = _select_parameters(
            frame, search, definition, candidates, minimum)
        fold_condition, fold_success = _condition_for(frame, definition, fold_override)
        part = frame.loc[check].copy()
        part["_condition"] = fold_condition.loc[part.index]
        part["_success"] = fold_success.loc[part.index]
        search_exit = frame.loc[search, "exit_time"].max()
        check_entry = frame.loc[check, "entry_time"].min()
        fold_results.append({"fold": index, "search_n": int(search.sum()), "selected_parameters": fold_override,
                             "selected_train_expectancy_net": fold_score,
                             "candidate_scores": fold_candidates, "check":
                             _metric(part, part["_condition"], part["_success"], n_boot, seed + index + 2),
                             "search_latest_label_end": search_exit, "check_first_entry": check_entry,
                             "check_last_entry": frame.loc[check, "entry_time"].max(),
                             "purged": bool(pd.notna(search_exit) and pd.notna(check_entry) and
                                            search_exit < check_entry)})
    sensitivity = _sensitivity(validation, validation["_condition"], definition)
    parameter_sensitivity = []
    for parameters in _parameter_scenarios(definition, candidates, selected_parameters):
        candidate_condition, candidate_success = _condition_for(frame, definition, parameters)
        chosen = validation.loc[candidate_condition.loc[validation.index] &
                                candidate_success.loc[validation.index].notna()]
        executed = chosen.get("trade_executed", pd.Series(True, index=chosen.index)).fillna(False).astype(bool)
        returns = pd.to_numeric(chosen.loc[executed, "outcome_return"], errors="coerce").dropna()
        parameter_sensitivity.append({"parameters": parameters, "n_executed": int(len(returns)),
                                      "validation_expectancy_net": float(returns.mean()) if len(returns) else None,
                                      "selected_by_train": parameters == selected_parameters})
    signal_rows = frame.loc[condition & frame.get("trade_executed", pd.Series(True, index=frame.index))
                            .fillna(False).astype(bool)]
    observed_costs = signal_rows.get("cost_observed", pd.Series(False, index=signal_rows.index)).fillna(False)
    p_value = test_metric.get("p_value") if isinstance(test_metric, dict) and not test_metric.get("reused") else None
    p_adjusted = min(1.0, float(p_value) * family_tests) if p_value is not None else 1.0
    weak = bool(plan.weak or train_metric["n_condition"] < minimum or validation_metric["n_condition"] < minimum or
                (include_test and test_metric.get("n_condition", 0) < minimum))
    fold_positive = bool(fold_results) and all(
        result["check"]["expectancy_net"] is not None and result["check"]["expectancy_net"] > 0
        for result in fold_results)
    holdout_positive = bool(include_test and test_metric.get("expectancy_net") is not None and
                            test_metric["expectancy_net"] > 0)
    eligible = bool(definition["status"] != "DRAFT" and not weak and
                    p_adjusted < float(definition["validation"].get("alpha", 0.05)) and
                    fold_positive and holdout_positive and test_metric.get("expectancy_ci", [None])[0] is not None and
                    test_metric["expectancy_ci"][0] > 0)
    result_status = "DRAFT" if definition["status"] == "DRAFT" else "VALIDATING" if eligible else "EXPLORATORY"
    return {"pattern_id": definition["pattern_id"], "version": definition["version"],
            "title": definition["title"], "status": result_status,
            "weak": weak, "note": plan.note, "sample_count": int(len(frame)),
            "sample_quality": quality,
            "condition_count": int(condition.sum()), "attempt_count": int(attempt_count),
            "parameter_selection": {"method": "maximum net expectancy on training partition" if definition.get(
                                        "parameter_grid") else "single declared configuration",
                                    "candidate_count": len(candidates), "selected": best_override or None,
                                    "selected_parameters": selected_parameters,
                                    "selected_train_expectancy_net": best_score,
                                    "candidates": candidate_scores},
            "parameter_sensitivity": parameter_sensitivity,
            "train": train_metric, "validation": validation_metric, "folds": fold_results,
            "test": test_metric, "sensitivity": sensitivity,
            "multiple_testing": {"method": "Bonferroni family-wise", "tests_counted": family_tests,
                                 "p_adjusted": p_adjusted},
            "cost_model": {"observed_spread_fraction": float(observed_costs.mean()) if len(signal_rows) else None,
                           "expectancy_basis": "per executed trade; per-observation value is reported separately",
                           "missing_components": definition["costs"].get("missing_components", [])},
            "eligible_for_approval": eligible, "approved": False}


def summarize_market_stability(evaluations, comparable_groups):
    """Resume expectativas OOS por grupos comparáveis, sem inferência nem aprovação agregada.

    ``evaluations`` contém linhas com ``market`` (ou ``symbol``), ``timeframe`` e ``result``
    produzido por :func:`evaluate_samples`. ``comparable_groups`` mapeia o nome do grupo para
    os mercados declarados comparáveis pelo chamador. Só são comparadas avaliações da mesma
    hipótese, versão e timeframe.
    """
    if not isinstance(comparable_groups, dict):
        raise ValueError("comparable_groups precisa mapear nomes de grupo para mercados.")
    normalized = []
    for item in evaluations:
        if not isinstance(item, dict):
            raise ValueError("Cada avaliação precisa ser um objeto.")
        market = item.get("market", item.get("symbol"))
        timeframe = item.get("timeframe")
        result = item.get("result", item)
        if not market or not timeframe or not isinstance(result, dict):
            raise ValueError("Cada avaliação precisa declarar market, timeframe e result.")
        pattern_id, version = result.get("pattern_id"), result.get("version")
        if not pattern_id or not version:
            raise ValueError("Cada result precisa declarar pattern_id e version.")
        test_metric = result.get("test")
        expectancy = test_metric.get("expectancy_net") if isinstance(test_metric, dict) else None
        if expectancy is None:
            expectancy = result.get("expectancy_net")
        try:
            expectancy = float(expectancy) if expectancy is not None else None
        except (TypeError, ValueError, OverflowError):
            expectancy = None
        if expectancy is not None and not np.isfinite(expectancy):
            expectancy = None
        normalized.append({"market": str(market), "timeframe": str(timeframe), "pattern_id": str(pattern_id),
                           "version": str(version), "expectancy_net": expectancy,
                           "result_status": result.get("status")})

    output = []
    for group_name, markets in comparable_groups.items():
        if not isinstance(markets, (list, tuple)) or not markets:
            raise ValueError(f"O grupo comparável {group_name} precisa listar mercados.")
        expected = [str(market) for market in markets]
        if len(set(expected)) != len(expected):
            raise ValueError(f"O grupo comparável {group_name} contém mercados duplicados.")
        members = [item for item in normalized if item["market"] in expected]
        keys = sorted({(item["pattern_id"], item["version"], item["timeframe"]) for item in members})
        for pattern_id, version, timeframe in keys:
            matches = [item for item in members if (item["pattern_id"], item["version"], item["timeframe"])
                       == (pattern_id, version, timeframe)]
            by_market = {}
            for item in matches:
                if item["market"] in by_market:
                    raise ValueError("Há mais de uma avaliação por hipótese/versão/timeframe/mercado; "
                                     "selecione explicitamente qual tentativa deve ser resumida.")
                by_market[item["market"]] = item
            values = [by_market[market]["expectancy_net"] for market in expected
                      if market in by_market and by_market[market]["expectancy_net"] is not None]
            coverage = len(values) / len(expected)
            signs = {"positive": sum(value > 0 for value in values),
                     "negative": sum(value < 0 for value in values),
                     "zero": sum(value == 0 for value in values)}
            sign_consistency = max(signs.values()) / len(values) if values else None
            if len(expected) < 2:
                status = "INSUFFICIENT_COMPARABLE_MARKETS"
            elif len(values) < 2:
                status = "INSUFFICIENT_COVERAGE"
            else:
                status = "DESCRIPTIVE"
            quartiles = np.percentile(values, [25, 50, 75]).tolist() if values else [None, None, None]
            output.append({"group": str(group_name), "pattern_id": pattern_id, "version": version,
                           "timeframe": timeframe, "status": status,
                           "expected_markets": expected,
                           "market_results": [{"market": market,
                                               "expectancy_net": by_market.get(market, {}).get("expectancy_net"),
                                               "result_status": by_market.get(market, {}).get("result_status")}
                                              for market in expected],
                           "coverage": {"observed": len(values), "expected": len(expected),
                                        "fraction": float(coverage)},
                           "expectancy_net": {"median": float(quartiles[1]) if values else None,
                                              "q25": float(quartiles[0]) if values else None,
                                              "q75": float(quartiles[2]) if values else None,
                                              "iqr": float(quartiles[2] - quartiles[0]) if values else None},
                           "sign_counts": signs, "sign_consistency": sign_consistency,
                           "interpretation": "Resumo descritivo; não representa significância, "
                                             "probabilidade de lucro ou aprovação."})
    return output


def _json_safe(value):
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, np.generic):
        value = value.item()
    if value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, (pd.Timestamp, datetime)):
        return pd.Timestamp(value).isoformat()
    if isinstance(value, float) and not np.isfinite(value):
        return None
    try:
        missing = pd.isna(value)
    except (TypeError, ValueError):
        missing = False
    if isinstance(missing, (bool, np.bool_)) and missing:
        return None
    return value if isinstance(value, (str, int, float, bool)) or value is None else str(value)


def _dataset_hash(samples):
    if samples is None or samples.empty:
        return hashlib.sha256(b"empty").hexdigest()
    frame = samples.astype(str)
    return hashlib.sha256(pd.util.hash_pandas_object(frame, index=False).values.tobytes()).hexdigest()


def evaluate_and_store(db, definition, samples, symbol, timeframe, *, n_boot=1000, write_guard=None):
    """Registra tentativas iniciadas e consome a reserva final antes de avaliar os dados."""
    _check_write_guard(write_guard)
    register_definition(db, definition, write_guard=write_guard)
    symbol, timeframe = str(symbol), str(timeframe)
    holdout_key = f"{definition['pattern_id']}|{definition['version']}|{symbol}|{timeframe}"
    candidate_count = len(_parameter_candidates(definition))
    evaluation_id = uuid.uuid4().hex
    started_at = datetime.now(timezone.utc).isoformat()
    with _LOCK, db.engine.begin() as conn:
        attempt_no = int(conn.execute(text(f"SELECT COUNT(*) FROM {EVALUATIONS_TABLE} WHERE pattern_id=:id "
                                           "AND version=:v AND symbol=:s AND timeframe=:tf"),
                                      {"id": definition["pattern_id"], "v": definition["version"],
                                       "s": symbol, "tf": timeframe}).scalar() or 0) + 1
        prior_tests = int(conn.execute(text(f"SELECT COALESCE(SUM(tests_counted), 0) FROM {EVALUATIONS_TABLE}")).scalar() or 0)
        total_tests = prior_tests + candidate_count
        reservation = conn.execute(text(f"SELECT evaluation_id FROM {RESERVATIONS_TABLE} WHERE pattern_id=:id AND version=:v "
                                        "AND symbol=:s AND timeframe=:tf"),
                                   {"id": definition["pattern_id"], "v": definition["version"],
                                    "s": symbol, "tf": timeframe}).scalar()
        reused = reservation is not None
        if not reused:
            conn.execute(text(f"INSERT INTO {RESERVATIONS_TABLE} "
                              "(pattern_id, version, symbol, timeframe, evaluation_id, consumed_at_utc) "
                              "VALUES (:id, :v, :s, :tf, :e, :stamp)"),
                         {"id": definition["pattern_id"], "v": definition["version"], "s": symbol,
                          "tf": timeframe, "e": evaluation_id, "stamp": started_at})
        initial = {"status": "RUNNING", "attempt_number": attempt_no, "holdout_reused": reused}
        result_json = json.dumps(initial, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        dataset_hash = _dataset_hash(samples)
        conn.execute(text(f"INSERT INTO {EVALUATIONS_TABLE} "
                          "(evaluation_id, pattern_id, version, symbol, timeframe, attempt_no, tests_counted, "
                          "dataset_hash, holdout_key, holdout_reused, status, result_json, validated_at_utc) "
                          "VALUES (:e, :id, :v, :s, :tf, :attempt, :tests, :hash, :holdout, :reused, :status, :result, :stamp)"),
                     {"e": evaluation_id, "id": definition["pattern_id"], "v": definition["version"], "s": symbol,
                      "tf": timeframe, "attempt": attempt_no, "tests": candidate_count, "hash": dataset_hash,
                      "holdout": holdout_key, "reused": int(reused), "status": "RUNNING",
                      "result": result_json, "stamp": started_at})

    error = None
    try:
        report = evaluate_samples(samples, definition, include_test=not reused, n_boot=n_boot,
                                  attempt_count=attempt_no, tests_counted=total_tests)
    except Exception as exc:  # Keep failed attempts and their consumed OOS reservation auditable.
        error = {"type": type(exc).__name__, "message": str(exc)[:1000]}
        report = {"pattern_id": definition["pattern_id"], "version": definition["version"],
                  "status": "FAILED", "weak": True, "eligible_for_approval": False, "approved": False,
                  "error": error, "test": {"available": False, "reason": "evaluation_failed"}}

    stamp = datetime.now(timezone.utc).isoformat()
    if reused:
        report["test"] = {"reused": True, "first_evaluation_id": reservation}
        report["eligible_for_approval"] = False
        report["approved"] = False
        if error is None:
            report["status"] = "EXPLORATORY"
    report["attempt_number"] = attempt_no
    report["evaluation_id"] = evaluation_id
    report["holdout_reused"] = reused
    report.setdefault("multiple_testing", {"method": "Bonferroni family-wise"})
    if error is None:
        raw_p = report.get("test", {}).get("p_value") if not reused else None
        with db.engine.connect() as conn:
            count_all = int(conn.execute(text(f"SELECT COALESCE(SUM(tests_counted), 0) FROM {EVALUATIONS_TABLE}")).scalar() or 0)
        adjusted = min(1.0, float(raw_p) * count_all) if raw_p is not None else 1.0
        report["multiple_testing"].update({"tests_counted": count_all, "p_adjusted": adjusted})
        if report.get("eligible_for_approval") and adjusted >= float(definition["validation"].get("alpha", 0.05)):
            report["eligible_for_approval"] = False
            report["status"] = "EXPLORATORY"
    else:
        report["multiple_testing"].update({"tests_counted": total_tests, "p_adjusted": 1.0})
    result_json = json.dumps(_json_safe(report), ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                             allow_nan=False)
    with _LOCK, db.engine.begin() as conn:
        conn.execute(text(f"UPDATE {EVALUATIONS_TABLE} SET holdout_reused=:reused, status=:status, "
                          "result_json=:result, validated_at_utc=:stamp WHERE evaluation_id=:e"),
                     {"reused": int(reused), "status": report["status"], "result": result_json,
                      "stamp": stamp, "e": evaluation_id})
    return report


def list_evaluations(db, pattern_id=None, symbol=None, timeframe=None):
    if db is None or getattr(db, "engine", None) is None or not inspect(db.engine).has_table(EVALUATIONS_TABLE):
        return []
    where, params = [], {}
    for column, value in (("pattern_id", pattern_id), ("symbol", symbol), ("timeframe", timeframe)):
        if value is not None:
            where.append(f"{column} = :{column}")
            params[column] = str(value)
    sql = f"SELECT * FROM {EVALUATIONS_TABLE}" + (" WHERE " + " AND ".join(where) if where else "") + \
        " ORDER BY validated_at_utc, evaluation_id"
    with db.engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings()
        return [{**dict(row), "result": json.loads(row["result_json"])} for row in rows]


def transition_status(db, pattern_id, version, new_status, *, write_guard=None):
    """Avança o estado; APPROVED requer uma primeira reserva OOS íntegra e todos os critérios atendidos."""
    if new_status not in STATUSES:
        raise ValueError("Estado desconhecido: " + str(new_status))
    _check_write_guard(write_guard)
    ensure_schema(db, write_guard=write_guard)
    with _LOCK, db.engine.begin() as conn:
        row = conn.execute(text(f"SELECT status FROM {DEFINITIONS_TABLE} WHERE pattern_id=:id AND version=:v"),
                           {"id": pattern_id, "v": version}).mappings().first()
        if row is None:
            raise ValueError("Hipótese ainda não registrada.")
        current = row["status"]
        if new_status not in _TRANSITIONS[current]:
            raise ValueError(f"Transição inválida: {current} → {new_status}.")
        if new_status == "APPROVED":
            latest = conn.execute(text(f"SELECT result_json, holdout_reused FROM {EVALUATIONS_TABLE} "
                                       "WHERE pattern_id=:id AND version=:v ORDER BY validated_at_utc DESC LIMIT 1"),
                                  {"id": pattern_id, "v": version}).mappings().first()
            if latest is None or latest["holdout_reused"] or not json.loads(latest["result_json"]).get(
                    "eligible_for_approval"):
                raise ValueError("A hipótese não passou pelos critérios OOS ou a reserva final já foi reutilizada.")
        conn.execute(text(f"UPDATE {DEFINITIONS_TABLE} SET status=:status WHERE pattern_id=:id AND version=:v"),
                     {"status": new_status, "id": pattern_id, "v": version})
    return new_status
