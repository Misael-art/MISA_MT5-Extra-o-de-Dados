"""DTOs sem Tkinter para as telas de pesquisa Analytics (T13.6)."""
import hashlib
import math
from datetime import datetime, timezone

import numpy as np
import pandas as pd

SCHEMA_VERSION = 1
COST_COMPONENTS = ("spread", "commission", "slippage", "swap", "fees")


def _now_utc():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _utc_text(value, label):
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError(f"{label} precisa incluir fuso horário (use UTC).")
    return stamp.tz_convert("UTC").isoformat().replace("+00:00", "Z")


def _json_safe(value):
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, (pd.Timestamp, datetime)):
        stamp = pd.Timestamp(value)
        return stamp.isoformat().replace("+00:00", "Z") if stamp.tzinfo else stamp.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "item"):
        try:
            return _json_safe(value.item())
        except (AttributeError, ValueError):
            pass
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        missing = pd.isna(value)
    except (TypeError, ValueError):
        missing = False
    if isinstance(missing, (bool, np.bool_)) and missing:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _metric(value, unit, state):
    return {"value": _json_safe(value), "unit": unit, "state": state}


def _frame_hash(frame):
    columns = sorted(frame.columns, key=str)
    if frame.empty:
        return hashlib.sha256(f"empty:{len(columns)}:{columns}".encode("utf-8")).hexdigest()
    stable = frame.loc[:, columns].copy()
    stable = stable.astype("string").fillna("<NULL>")
    if columns:
        stable = stable.sort_values(columns, kind="mergesort", na_position="first")
    digest = hashlib.sha256()
    digest.update(f"rows={len(stable)}\x1f".encode("ascii"))
    digest.update("\x1f".join(map(str, columns)).encode("utf-8"))
    digest.update(pd.util.hash_pandas_object(stable, index=False).values.tobytes())
    return digest.hexdigest()


def _period(frame):
    if frame.empty or any(column not in frame.columns for column in ("entry_time", "exit_time")):
        return {"period_start_utc": None, "period_end_utc": None, "period_start_source_time": None,
                "period_end_source_time": None, "time_basis": "UNAVAILABLE"}
    values = []
    for column in ("entry_time", "exit_time"):
        for value in frame[column].tolist():
            if pd.isna(value):
                return {"period_start_utc": None, "period_end_utc": None,
                        "period_start_source_time": None, "period_end_source_time": None,
                        "time_basis": "INCOMPLETE_TIME_DATA"}
            try:
                stamp = pd.Timestamp(value)
            except (TypeError, ValueError, OverflowError):
                return {"period_start_utc": None, "period_end_utc": None,
                        "period_start_source_time": None, "period_end_source_time": None,
                        "time_basis": "INCOMPLETE_TIME_DATA"}
            if pd.isna(stamp):
                return {"period_start_utc": None, "period_end_utc": None,
                        "period_start_source_time": None, "period_end_source_time": None,
                        "time_basis": "INCOMPLETE_TIME_DATA"}
            values.append(stamp)
    aware = [stamp.tzinfo is not None for stamp in values]
    if any(aware) and not all(aware):
        return {"period_start_utc": None, "period_end_utc": None,
                "period_start_source_time": None, "period_end_source_time": None,
                "time_basis": "MIXED_TIMEZONES"}
    start, end = min(values), max(values)
    if all(aware):
        return {"period_start_utc": start.tz_convert("UTC").isoformat().replace("+00:00", "Z"),
                "period_end_utc": end.tz_convert("UTC").isoformat().replace("+00:00", "Z"),
                "period_start_source_time": start.isoformat(), "period_end_source_time": end.isoformat(),
                "time_basis": "UTC_NORMALIZED"}
    return {"period_start_utc": None, "period_end_utc": None,
            "period_start_source_time": start.isoformat(), "period_end_source_time": end.isoformat(),
            "time_basis": "UNKNOWN_TIMEZONE"}


def _costs(frame, pnl_unit):
    component_values = {}
    coverage = {}
    missing = []
    for component in COST_COMPONENTS:
        column = f"cost_{component}"
        if frame.empty or column not in frame.columns:
            component_values[component] = None
            coverage[component] = 0.0
            missing.append(component)
            continue
        values = pd.to_numeric(frame[column], errors="coerce")
        array = values.to_numpy(dtype=float, na_value=np.nan)
        valid = np.isfinite(array)
        coverage[component] = float(valid.sum() / len(frame)) if len(frame) else 0.0
        if len(array) != len(frame) or not valid.all():
            component_values[component] = None
            missing.append(component)
        else:
            component_values[component] = float(array.sum())
    complete = bool(len(frame) and not missing)
    total = sum(component_values.values()) if complete else None
    return {"complete": complete, "total": total, "unit": pnl_unit,
            "components": component_values, "coverage_by_component": coverage, "missing": missing}


def _drawdown(values, initial_equity):
    equity = float(initial_equity or 0.0) + np.concatenate(([0.0], np.cumsum(values)))
    peaks = np.maximum.accumulate(equity)
    drawdowns = np.maximum(0.0, peaks - equity)
    amount = float(drawdowns.max()) if len(drawdowns) else None
    if initial_equity is None or not math.isfinite(float(initial_equity)) or float(initial_equity) <= 0:
        return amount, None
    peak = float(peaks.max()) if len(peaks) else 0.0
    return amount, (amount / peak * 100.0 if peak > 0 else None)


def _ordered_values(frame, values):
    """Retorna PnL ordenado por saída; sem timestamps completos, drawdown é indisponível."""
    if "exit_time" not in frame.columns or len(frame) != len(values) or frame["exit_time"].isna().any():
        return None
    try:
        stamps = [pd.Timestamp(value) for value in frame["exit_time"]]
        aware = [stamp.tzinfo is not None for stamp in stamps]
        if any(aware) and not all(aware):
            return None
        order = sorted(range(len(stamps)), key=lambda index: stamps[index].value)
        return np.asarray(values, dtype=float)[order]
    except (TypeError, ValueError, OverflowError):
        return None


def _peak_exposure(frame):
    required = ("entry_time", "exit_time", "volume", "symbol")
    if frame.empty:
        return _metric(None, "source_volume_units", "NOT_PROVIDED")
    if any(column not in frame.columns for column in required):
        return _metric(None, "source_volume_units", "NOT_PROVIDED")
    symbols = frame["symbol"].dropna().astype(str).unique().tolist()
    if len(symbols) != 1 or frame["symbol"].isna().any():
        return _metric(None, "source_volume_units", "MIXED_SYMBOLS")
    volume = pd.to_numeric(frame["volume"], errors="coerce")
    if volume.isna().any() or not np.isfinite(volume.to_numpy(dtype=float)).all():
        return _metric(None, f"volume_units:{symbols[0]}", "INCOMPLETE_DATA")
    events = []
    timezone_flags = []
    for index, row in frame.iterrows():
        try:
            entry, exit_time = pd.Timestamp(row["entry_time"]), pd.Timestamp(row["exit_time"])
        except (TypeError, ValueError, OverflowError):
            return _metric(None, f"volume_units:{symbols[0]}", "INVALID_INTERVALS")
        if (pd.isna(entry) or pd.isna(exit_time) or
                (entry.tzinfo is None) != (exit_time.tzinfo is None)):
            return _metric(None, f"volume_units:{symbols[0]}", "INVALID_INTERVALS")
        if entry.tzinfo is not None:
            entry, exit_time = entry.tz_convert("UTC"), exit_time.tz_convert("UTC")
        if entry >= exit_time:
            return _metric(None, f"volume_units:{symbols[0]}", "INVALID_INTERVALS")
        timezone_flags.extend((entry.tzinfo is not None, exit_time.tzinfo is not None))
        amount = abs(float(row["volume"]))
        events.append((exit_time, 0, str(index), -amount))
        events.append((entry, 1, str(index), amount))
    if any(timezone_flags) and not all(timezone_flags):
        return _metric(None, f"volume_units:{symbols[0]}", "MIXED_TIMEZONES")
    active = peak = 0.0
    for _stamp, _order, _key, delta in sorted(events):
        active += delta
        peak = max(peak, active)
    return _metric(float(peak), f"volume_units:{symbols[0]}", "AVAILABLE")


def _pnl_metrics(frame, pnl_basis, pnl_unit, initial_equity, minimum_operations):
    count = int(len(frame))
    values = pd.to_numeric(frame.get("pnl", pd.Series(index=frame.index, dtype=float)), errors="coerce")
    raw = values.to_numpy(dtype=float, na_value=np.nan)
    valid = np.isfinite(raw)
    has_values = bool(valid.any())
    all_values = bool(count and valid.all())
    recorded_state = "AVAILABLE" if all_values else "PARTIAL" if has_values else "NOT_PROVIDED"
    recorded = raw[valid]
    recorded_total = float(recorded.sum()) if has_values else None
    recorded_mean = float(recorded.mean()) if has_values else None
    recorded_median = float(np.median(recorded)) if has_values else None
    cost_summary = _costs(frame, pnl_unit)
    if not count or not has_values:
        net_state = "NOT_PROVIDED"
        net_values = None
    elif not all_values:
        net_state = "INCOMPLETE_DATA"
        net_values = None
    elif not cost_summary["complete"]:
        net_state = "INCOMPLETE_COSTS"
        net_values = None
    elif pnl_basis not in {"gross", "net"}:
        net_state = "UNKNOWN_PNL_BASIS"
        net_values = None
    elif pnl_basis == "gross":
        row_costs = sum(pd.to_numeric(frame[f"cost_{component}"], errors="coerce")
                        for component in COST_COMPONENTS)
        net_values = raw - row_costs.to_numpy(dtype=float)
        net_state = "AVAILABLE"
    else:
        net_values = raw
        net_state = "AVAILABLE"

    net_total = float(net_values.sum()) if net_values is not None else None
    net_mean = float(net_values.mean()) if net_values is not None else None
    net_median = float(np.median(net_values)) if net_values is not None else None
    chronological_recorded = _ordered_values(frame, raw) if all_values else None
    chronological_net = _ordered_values(frame, net_values) if net_values is not None else None
    recorded_dd_state = (recorded_state if not all_values else
                         "AVAILABLE" if chronological_recorded is not None else "UNAVAILABLE_TIME_ORDER")
    net_dd_state = (net_state if chronological_net is not None else
                    "UNAVAILABLE_TIME_ORDER" if net_values is not None else net_state)
    recorded_dd_amount, recorded_dd_pct = (_drawdown(chronological_recorded, initial_equity)
                                           if chronological_recorded is not None else (None, None))
    net_dd_amount, net_dd_pct = (_drawdown(chronological_net, initial_equity)
                                 if chronological_net is not None else (None, None))
    small_state = "AVAILABLE" if count >= minimum_operations else "INSUFFICIENT_SAMPLE"
    return {
        "n_operations": _metric(count, "operations", "AVAILABLE"),
        "recorded_pnl_total": _metric(recorded_total, pnl_unit, recorded_state),
        "recorded_pnl_mean_per_operation": _metric(recorded_mean, f"{pnl_unit}/operation", recorded_state),
        "recorded_pnl_median_per_operation": _metric(recorded_median, f"{pnl_unit}/operation", recorded_state),
        "net_pnl_total": _metric(net_total, pnl_unit, net_state),
        "net_pnl_mean_per_operation": _metric(net_mean, f"{pnl_unit}/operation", net_state),
        "net_pnl_median_per_operation": _metric(net_median, f"{pnl_unit}/operation", net_state),
        "recorded_pnl_max_drawdown_amount": _metric(recorded_dd_amount, pnl_unit, recorded_dd_state),
        "recorded_pnl_max_drawdown_pct": _metric(recorded_dd_pct, "percent", recorded_dd_state),
        "max_drawdown_amount": _metric(net_dd_amount, pnl_unit, net_dd_state),
        "max_drawdown_pct": _metric(net_dd_pct, "percent", net_dd_state),
        "peak_exposure": _peak_exposure(frame),
        "sample_adequacy": _metric(count, "operations", small_state),
        "costs": cost_summary,
    }


def _delta(original, candidate, key):
    before, after = original[key], candidate[key]
    if before["state"] != "AVAILABLE" or after["state"] != "AVAILABLE":
        state = before["state"] if before["state"] != "AVAILABLE" else after["state"]
        return _metric(None, before["unit"], state)
    return _metric(after["value"] - before["value"], before["unit"], "AVAILABLE")


def _sources(frame):
    if "source" not in frame.columns:
        return []
    return sorted({str(value) for value in frame["source"].dropna().tolist()})


def build_filter_comparison_payload(original, candidate, evaluation, *, scope=None, as_of_utc=None,
                                    generated_at_utc=None, valid_until_utc=None, pnl_basis="unknown",
                                    pnl_unit="source_pnl_units", initial_equity=None, minimum_operations=30):
    """Serializa uma comparação sobre ledger fixo; nunca afirma reexecução da estratégia.

    Custos opcionais usam colunas explícitas `cost_spread`, `cost_commission`, `cost_slippage`,
    `cost_swap` e `cost_fees`. `pnl_basis` precisa ser declarado como `gross` ou `net` antes
    de produzir métricas líquidas. O padrão é desconhecido e mantém os campos líquidos nulos.
    """
    if not isinstance(original, pd.DataFrame) or not isinstance(candidate, pd.DataFrame):
        raise TypeError("original e candidate precisam ser DataFrames do ledger.")
    if pnl_basis not in {"unknown", "gross", "net"}:
        raise ValueError("pnl_basis deve ser unknown, gross ou net.")
    if int(minimum_operations) < 1:
        raise ValueError("minimum_operations precisa ser positivo.")
    if initial_equity is not None and (not math.isfinite(float(initial_equity)) or float(initial_equity) <= 0):
        raise ValueError("initial_equity precisa ser positivo e finito.")
    generated = _utc_text(generated_at_utc, "generated_at_utc") if generated_at_utc else _now_utc()
    as_of = _utc_text(as_of_utc, "as_of_utc") if as_of_utc else generated
    valid_until = _utc_text(valid_until_utc, "valid_until_utc") if valid_until_utc else None

    before = _pnl_metrics(original, pnl_basis, pnl_unit, initial_equity, int(minimum_operations))
    after = _pnl_metrics(candidate, pnl_basis, pnl_unit, initial_equity, int(minimum_operations))
    folds = evaluation.get("folds") or [] if isinstance(evaluation, dict) else []
    compared_folds = []
    for fold in folds:
        if not isinstance(fold, dict):
            continue
        before_fold = fold.get("before", {}).get("expectancy") if isinstance(fold.get("before"), dict) else None
        after_fold = fold.get("after", {}).get("expectancy") if isinstance(fold.get("after"), dict) else None
        if isinstance(before_fold, (int, float)) and isinstance(after_fold, (int, float)):
            if math.isfinite(float(before_fold)) and math.isfinite(float(after_fold)):
                compared_folds.append(float(after_fold) > float(before_fold))
    folds_improved = sum(compared_folds)
    weak = bool(evaluation.get("weak", True)) if isinstance(evaluation, dict) else True
    enough_data = (before["n_operations"]["value"] >= minimum_operations and
                   after["n_operations"]["value"] >= minimum_operations)
    evidence_state = "INSUFFICIENT" if weak or not enough_data else "EXPLORATORY"
    reason = ("O walk-forward indicou amostra fraca." if weak else
              "A amostra está abaixo do mínimo de triagem." if not enough_data else
              "Comparação de ledger fixo; não promove uma regra a validada.")
    freshness = "UNAVAILABLE" if valid_until is None else "STALE" if pd.Timestamp(as_of) >= pd.Timestamp(valid_until) else "CURRENT"

    source_rows = pd.concat([original, candidate], ignore_index=True, sort=False)
    source_revision_values = []
    if "source_revision" in source_rows.columns:
        source_revision_values = sorted({str(value) for value in source_rows["source_revision"].dropna().tolist()})
    source_revision = source_revision_values[0] if len(source_revision_values) == 1 else None
    time_info = _period(source_rows)
    provenance = {
        "sources": _sources(source_rows),
        "run_ids": sorted({str(value) for value in source_rows.get("run_id", pd.Series(dtype=object)).dropna()}),
        **time_info,
        "dataset_hash": hashlib.sha256((_frame_hash(original) + _frame_hash(candidate)).encode("ascii")).hexdigest(),
        "fingerprint_scope": "comparison_rows",
        "source_revision": source_revision,
        "source_revisions": source_revision_values,
        "available_at_utc": None,
        "valid_until_utc": valid_until,
        "availability_state": "UNAVAILABLE",
        "availability_reason": "O ledger não registra quando a fonte foi recebida.",
    }
    stability_state = "EXPLORATORY" if compared_folds and not weak else "INSUFFICIENT"
    stability = {"state": stability_state,
                 "method": "walk_forward_fold_improvement_fraction" if compared_folds else None,
                 "folds": len(compared_folds) if compared_folds else None,
                 "folds_improved": folds_improved if compared_folds else None,
                 "estimate": (folds_improved / len(compared_folds)) if compared_folds else None,
                 "interval": None,
                 "note": "Fração descritiva de provas que melhoraram; não é probabilidade."
                         if compared_folds else None}
    rules = evaluation.get("rules", []) if isinstance(evaluation, dict) else []
    removed = evaluation.get("removed") if isinstance(evaluation, dict) else None
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": generated,
        "as_of_utc": as_of,
        "scope": _json_safe(scope or {}),
        "comparison": {"kind": "FIXED_LEDGER_COUNTERFACTUAL",
                       "label": "Contrafactual sobre operações existentes; sem reexecução da estratégia",
                       "state_changes_replayed": False,
                       "candidate_rules": _json_safe(rules),
                       "removed_operations": _json_safe(removed)},
        "evidence": {"state": evidence_state, "reason": reason, "freshness": freshness},
        "provenance": provenance,
        "original": before,
        "candidate": after,
        "delta": {key: _delta(before, after, key) for key in
                  ("recorded_pnl_total", "net_pnl_total", "max_drawdown_amount", "max_drawdown_pct",
                   "peak_exposure")},
        "signals": {"accepted": None, "blocked": None, "source": "NOT_RECORDED"},
        "stability": stability,
        "walk_forward": {"weak": weak, "note": _json_safe(evaluation.get("note")) if isinstance(evaluation, dict)
                         else "Sem avaliação walk-forward.", "folds": len(compared_folds),
                         "folds_improved": folds_improved if compared_folds else None},
    }


def _evaluation_state(row):
    result = row.get("result") or {}
    if not isinstance(result, dict):
        result = {}
    status = str(row.get("status", "")).upper()
    test = result.get("test") if isinstance(result.get("test"), dict) else {}
    if (status == "APPROVED" and result.get("eligible_for_approval") is True and
            not bool(row.get("holdout_reused"))):
        return "VALIDATED", "A avaliação passou pelos critérios OOS registrados."
    if (status in {"RUNNING", "FAILED"} or result.get("weak") or
            test.get("available") is False or test.get("reused") is True):
        return "INSUFFICIENT", "A tentativa está incompleta, falhou ou não dispõe de evidência OOS suficiente."
    return "EXPLORATORY", "Resultado informativo; não equivale a aprovação operacional."


def build_hypothesis_catalog(db, *, pattern_id=None, symbol=None, timeframe=None, generated_at_utc=None):
    """Devolve catálogo e todas as tentativas numa forma serializável e somente de leitura."""
    from mt5_extracao.analytics import validation

    generated = _utc_text(generated_at_utc, "generated_at_utc") if generated_at_utc else _now_utc()
    patterns = validation.list_patterns(db)
    if pattern_id is not None:
        patterns = [item for item in patterns if item.get("pattern_id") == str(pattern_id)]
    evaluations = validation.list_evaluations(db, pattern_id=pattern_id, symbol=symbol, timeframe=timeframe)
    grouped = {}
    for row in evaluations:
        grouped.setdefault((row.get("pattern_id"), row.get("version")), []).append(row)

    items = []
    for pattern in patterns:
        pattern_key = (pattern.get("pattern_id"), pattern.get("version"))
        attempts = []
        for row in grouped.get(pattern_key, []):
            state, reason = _evaluation_state(row)
            attempts.append({
                "evaluation_id": _json_safe(row.get("evaluation_id")),
                "attempt_no": _json_safe(row.get("attempt_no")),
                "status": _json_safe(row.get("status")),
                "evidence_state": state,
                "evidence_reason": reason,
                "symbol": _json_safe(row.get("symbol")),
                "timeframe": _json_safe(row.get("timeframe")),
                "tests_counted": _json_safe(row.get("tests_counted")),
                "dataset_hash": _json_safe(row.get("dataset_hash")),
                "holdout_reused": bool(row.get("holdout_reused")),
                "evaluated_at_utc": _json_safe(row.get("validated_at_utc")),
                "freshness": "UNAVAILABLE",
                "provenance": {"source_revision": None, "period_start_utc": None, "period_end_utc": None,
                               "available_at_utc": None, "valid_until_utc": None,
                               "missing": ["source_revision", "period", "available_at_utc", "valid_until_utc"]},
                "details": _json_safe(row.get("result")),
            })
        items.append({"pattern_id": pattern.get("pattern_id"), "version": pattern.get("version"),
                      "title": pattern.get("title"), "lifecycle_status": pattern.get("status"),
                      "definition": _json_safe(pattern.get("definition")), "evaluations": attempts})
    return {"schema_version": SCHEMA_VERSION, "kind": "HYPOTHESIS_CATALOG", "generated_at_utc": generated,
            "filters": {"pattern_id": pattern_id, "symbol": symbol, "timeframe": timeframe}, "items": items}
