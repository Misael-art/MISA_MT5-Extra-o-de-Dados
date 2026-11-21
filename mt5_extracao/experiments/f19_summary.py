"""Comparação exploratória dos braços F19 executados contra o mesmo snapshot WIN."""
from collections import defaultdict
from math import fsum

import numpy as np
import pandas as pd

from ..analytics import datasets, instruments, opening_coverage
from ..analytics.associations import _moving_block_indices
from ..trade_analytics.trajectories import opportunities
from . import f19, store
from .schema import digest


def query(db, protocol_id, role):
    protocol = store.protocol(db, protocol_id)
    definition = protocol["definition"]
    plan = definition.get("objective", {})
    if plan.get("id") != f19.OBJECTIVE or role not in definition["roles"] or "calendar" not in plan:
        raise ValueError("A comparação exige protocolo F19, papel registrado e calendário WIN congelado.")
    with db.consistent_read() as conn:
        if role == "holdout":
            store.check_f19_holdout(db, conn, protocol)
        runs = {}
        for arm in f19.ARMS:
            for scenario in f19.SCENARIOS:
                key = digest([protocol_id, f"{role}.{arm}.{scenario}"])
                request = store.request(db, key, conn)
                if (request["state"] not in ("run_recorded", "reused") or
                        request["payload"].get("comparison") != {"arm": arm, "scenario": scenario}):
                    raise ValueError("Conclua os quatro pedidos congelados antes de comparar os pregões.")
                runs[arm, scenario] = request["run_id"]

    snapshot = datasets.load(db, definition["roles"][role]["data_manifest_id"])
    bound = protocol["context"]["roles"][role]
    roster, coverage_id = _roster(snapshot, plan["calendar"], bound)
    books = {key: opportunities.query(db, run) for key, run in runs.items()}
    identities = set()
    for book in books.values():
        if book["status"] != "observed_model" or book["data_manifest_id"] != snapshot.manifest_id:
            raise ValueError("Cada braço precisa de livro conciliado com o snapshot congelado deste papel.")
        code = book["producer_identity"].get("code_sha256")
        if not code:
            raise ValueError("O código do produtor precisa estar identificado em cada braço.")
        identities.add(code)
    if len(identities) != 1:
        raise ValueError("Os braços precisam usar a mesma revisão do executor; refaça as quatro avaliações.")

    cash = {key: _cash_by_day(roster, book["opportunities"]) for key, book in books.items()}
    rows = []
    for date, day in roster.items():
        reasons = list(day["reasons"])
        eligible = not reasons
        models = {}
        if eligible:
            for scenario in f19.SCENARIOS:
                base, variant = (cash[arm, scenario].get(date, 0.0) for arm in f19.ARMS)
                models[scenario] = {"reference": base, "candidate": variant, "difference": variant - base}
        rows.append({"date": date, "eligible": eligible, "reasons": reasons, "scenarios": models})

    criteria = definition["criteria"]
    selected = [row for row in rows if row["eligible"]]
    dates = [row["date"] for row in selected]
    differences = {scenario: [row["scenarios"][scenario]["difference"] for row in selected]
                   for scenario in f19.SCENARIOS}
    estimates = {scenario: _estimate(sample, plan, criteria, definition["seed"])
                 for scenario, sample in differences.items()}
    sensitivity = {scenario: _sensitivity(dates, sample, plan, criteria, definition["seed"])
                   for scenario, sample in differences.items()}
    return {"version": 1, "protocol_id": protocol_id, "role": role, "snapshot_id": snapshot.manifest_id,
            "coverage_manifest_id": coverage_id, "executor_sha256": next(iter(identities)),
            "runs": {scenario: {arm: runs[arm, scenario] for arm in f19.ARMS} for scenario in f19.SCENARIOS},
            "sessions": rows, "estimates": estimates, "sensitivity": sensitivity, "tests_counted": len(f19.SCENARIOS),
            "minimum_net_cash_difference": criteria["minimum_net_cash_difference"],
            "unit": "account_money_per_session", "prior_external_exposure": "unknown",
            "qualified_final_holdout": False, "economic_approval": False}


def _roster(snapshot, calendar, bound):
    offset = snapshot.manifest["time"]["broker_utc_offset_hours"]
    first = instruments.broker_utc(bound["first"], offset)
    end = instruments.broker_utc(bound["last"], offset) + opening_coverage.MINUTE
    coverage = opening_coverage.report([snapshot], calendar, as_of=end)
    bars = snapshot.bars
    times = pd.DatetimeIndex(pd.to_datetime(bars.time, format="ISO8601")).tz_localize("UTC")
    bars.index = times - pd.Timedelta(hours=offset)
    first_day, last_day = (time.tz_convert(coverage["timezone"]).date().isoformat() for time in (first, end))
    roster = {}
    for row in coverage["sessions"]:
        if not row["expected_open_utc"]:
            if first_day <= row["date"] <= last_day:
                roster[row["date"]] = {"open": None, "close": None, "reasons": list(row["reasons"])}
            continue
        opened, closed = (pd.Timestamp(row[key]) for key in ("expected_open_utc", "expected_close_utc"))
        if opened >= end or closed <= first:
            continue
        reasons = list(row["reasons"])
        if opened < first or closed > end:
            reasons.append("session_crosses_role_boundary")
        elif not reasons:
            observed = bars.loc[(bars.index >= opened) & (bars.index < closed)]
            whole = opening_coverage._coverage(observed, opened, closed)
            if whole["missing_utc"]:
                reasons.append("incomplete_session_m1")
            elif not whole["valid_ohlc"]:
                reasons.append("invalid_session_m1")
        roster[row["date"]] = {"open": opened, "close": closed, "reasons": reasons}
    return roster, coverage["manifest_id"]


def _cash_by_day(roster, events):
    amounts = defaultdict(list)
    for signal in events:
        if signal["status"] != "executed":
            continue
        observed = pd.Timestamp(signal["signal_time_utc"])
        date = observed.tz_convert(opening_coverage.VENUE_TZ).date().isoformat()
        session = roster.get(date)
        last_fill = pd.Timestamp(signal["fills"][-1]["end_utc"])
        if session is None or session["open"] is None or observed < session["open"] or last_fill > session["close"]:
            raise ValueError("Há ciclo fora do pregão ou carregado entre sessões. Use uma execução intradiária conciliada.")
        if not f19._finite(signal["net_pnl"]):
            raise ValueError("Resultado líquido ausente ou inválido em ciclo do braço.")
        amounts[date].append(signal["net_pnl"])
    return {date: fsum(values) for date, values in amounts.items()}


def _estimate(differences, plan, criteria, seed):
    count = len(differences)
    block = plan["block_sessions"]
    result = {"sessions": count, "mean_difference": fsum(differences) / count if count else None,
              "low": None, "high": None, "block_sessions": block, "replicates": 0, "status": "insufficient_sessions"}
    if count <= block:
        return result
    sample = np.asarray(differences, dtype=float)
    rng = np.random.default_rng(seed)
    means = [float(sample[_moving_block_indices(count, block, rng)].mean())
             for _ in range(plan["bootstrap_replicates"])]
    confidence = criteria["confidence"]
    tail = (1 - confidence) / (2 * len(f19.SCENARIOS))
    low, high = np.quantile(means, [tail, 1 - tail])
    minimum = criteria["minimum_net_cash_difference"]
    status = "above_threshold" if low > minimum else "below_threshold" if high < minimum else "inconclusive"
    return {**result, "low": float(low), "high": float(high), "replicates": len(means),
            "family_confidence": confidence, "status": status}


def _sensitivity(dates, differences, plan, criteria, seed):
    without_best = {"excluded_date": None, "excluded_difference": None, "sessions": 0, "mean_difference": None}
    if differences:
        best = max(range(len(differences)), key=differences.__getitem__)
        remaining = differences[:best] + differences[best + 1:]
        without_best.update(excluded_date=dates[best], excluded_difference=differences[best], sessions=len(remaining),
                            mean_difference=fsum(remaining) / len(remaining) if remaining else None)
    block = plan["block_sessions"]
    alternatives = sorted({max(1, block // 2), min(f19.MAX_BLOCK_SESSIONS, block * 2)} - {block})
    return {"exploratory": True, "without_best_session": without_best,
            "block_sizes": [_estimate(differences, dict(plan, block_sessions=size), criteria, seed)
                            for size in alternatives]}
