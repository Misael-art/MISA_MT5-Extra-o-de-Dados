"""Livro de oportunidades observadas no backtest Python e conciliadas à trajetória."""
from collections import Counter
from math import fsum

import pandas as pd

from mt5_extracao.analytics import datasets, opening_coverage
from . import cycles, store
from .clocks import Clock, bar_end_in_source


def query(db, run_id, *, calendar=None):
    record = store.load(db, run_id)
    if record is None or "opportunities" not in record["observations"]:
        return {"version": 1, "run_id": run_id, "status": "unavailable",
                "reason": "missing_producer_opportunities", "opportunities": None, "sessions": None}
    payload = record["observations"]
    clock = Clock(**record["clock"])
    positions = {unit["identity"]["seq"]: unit for unit in cycles.summarize(
        payload, run_id=run_id, source=record["source"], clock=clock)}
    rows, linked = [], set()
    for seq, signal in enumerate(payload["opportunities"]):
        available = clock.timestamp(signal["available_at"])
        evaluated = clock.timestamp(signal["evaluated_at"])
        start = clock.timestamp(signal["signal_time"])
        if signal["seq"] != seq or signal["direction"] not in (1, -1) or not start <= available <= evaluated:
            raise ValueError("Identidade, direção ou disponibilidade inválida na oportunidade.")
        fills = []
        cycle = signal["cycle_seq"]
        unit = None
        if signal["status"] == "executed":
            unit = positions.get(cycle)
            if (unit is None or cycle in linked or pd.Timestamp(unit["start_utc"]) != evaluated or
                    unit["points"][0]["direction"] != signal["direction"]):
                raise ValueError("Oportunidade executada não concilia com a entrada registrada no ciclo.")
            linked.add(cycle)
            fills = [{"kind": event["kind"], "price": event["fill_price"], "volume": event["fill_volume"],
                      "start_utc": event["start_utc"], "end_utc": event["end_utc"],
                      "volume_open": event["volume_open"], "commission": event.get("commission"),
                      "swap": event.get("swap")} for event in unit["points"] if event["fill_volume"] is not None]
        elif cycle is not None or signal["status"] not in ("vetoed", "expired") or not signal["reason"]:
            raise ValueError("Estado da oportunidade não executada inválido.")
        rows.append({**signal, "signal_time_utc": start.isoformat(), "available_at_utc": available.isoformat(),
                     "evaluated_at_utc": evaluated.isoformat(), "fills": fills,
                     "fill_delay_seconds": (evaluated - available).total_seconds() if fills else None,
                     "net_pnl": unit["final_net_pnl"] if unit else None,
                     "mae_observed": unit["mae_observed"] if unit else None,
                     "mae_time": unit["mae_time"] if unit else None,
                     "mfe_observed": unit["mfe_observed"] if unit else None,
                     "mfe_time": unit["mfe_time"] if unit else None})
    if linked != positions.keys():
        raise ValueError("Ciclos executados sem oportunidade correspondente no journal.")

    population, coverage_id = None, None
    if calendar is not None:
        snapshot = datasets.for_run(db, run_id)
        if snapshot is None or snapshot.manifest_id != record["manifest_id"]:
            raise ValueError("A população de pregões exige a revisão de dados congelada da execução.")
        cutoff = clock.timestamp(bar_end_in_source(snapshot.manifest["period"]["last"], payload["timeframe"]))
        coverage = opening_coverage.report([snapshot], calendar, as_of=cutoff)
        coverage_id = coverage["manifest_id"]
        population = _sessions(coverage, rows, snapshot.manifest["symbol"])
    totals = Counter(signal["status"] for signal in rows)
    known_cash = all(signal["net_pnl"] is not None for signal in rows if signal["status"] == "executed")
    return {"version": 1, "run_id": run_id, "status": "observed_model", "trajectory_id": record["trajectory_id"],
            "data_manifest_id": record["manifest_id"], "producer_identity": record["producer_identity"],
            "population_basis": "visible_strategy_signals_in_backtest",
            "counts": {"opportunities": len(rows), "vetoed": totals["vetoed"], "expired": totals["expired"],
                       "executed": totals["executed"], "closed_cycles": len(linked),
                       "fills": sum(len(signal["fills"]) for signal in rows), "broker_rejections": None},
            "net_pnl": fsum(row["net_pnl"] for row in rows if row["net_pnl"] is not None) if known_cash else None,
            "coverage_manifest_id": coverage_id, "sessions": population, "opportunities": rows}


def _sessions(coverage, rows, contract):
    """Preserva o roster mesmo quando nenhuma oportunidade é emitida."""
    by_day = {}
    population = []
    for session in coverage["sessions"]:
        row = {"date": session["date"], "contract": session["contract"], "status": session["status"],
               "reasons": session["reasons"], "opportunity_seqs": [], "executed_opportunities": 0}
        population.append(row)
        if session["expected_open_utc"] and session["contract"] == contract:
            by_day[session["date"]] = (pd.Timestamp(session["expected_open_utc"]),
                                       pd.Timestamp(session["expected_close_utc"]), row)
    for signal in rows:
        stamp = pd.Timestamp(signal["signal_time_utc"])
        day = stamp.tz_convert(coverage["timezone"]).date().isoformat()
        session = by_day.get(day)
        signal["session_date"] = None
        if session and session[0] <= stamp < session[1]:
            signal["session_date"] = day
            session[2]["opportunity_seqs"].append(signal["seq"])
            session[2]["executed_opportunities"] += signal["status"] == "executed"
    return population
