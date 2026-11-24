"""Comparações pareadas em patrimônio marcado. Unidades monetárias em centésimos."""
from copy import deepcopy
from math import ceil, fsum

from .. import profit_factor
from ..experiments.schema import digest
from . import ultra


def metrics(receipt):
    frames = receipt["timeline"]
    initial = peak = receipt["policy"]["cash"]
    end = initial
    dd = margin = exposure = 0
    daily = {}
    for frame in frames:
        mark = frame["mark"]
        end = mark["equity"]
        peak = max(peak, end)
        dd = max(dd, peak - end)
        margin = max(margin, mark["margin"])
        exposure = max(exposure, mark["exposure_steps"])
        daily[mark["key"][0] // 86_400_000] = end
    previous = initial
    changes = []
    for balance in daily.values():
        changes.append(balance - previous)
        previous = balance
    pnl = [cycle["net"] for cycle in receipt["account"]["completed"]]
    tail = sorted(changes)[:ceil(len(changes) * .05)]
    return {**profit_factor.fields(pnl), "initial_cash": initial, "end_equity": end,
            "net_change": end - initial, "completed_cycles": len(pnl),
            "expectancy_per_cycle": fsum(pnl) / len(pnl) if pnl else None,
            "max_drawdown": dd, "max_reserved_margin": margin, "peak_exposure_steps": exposure,
            "worst_day": min(changes, default=None), "session_tail_mean_5pct": fsum(tail) / len(tail) if tail else None,
            "session_changes": changes, "commission": receipt["account"]["cost"],
            "spread_cost_vs_mid": receipt["account"]["spread_cost_twice"] / 2,
            "open_cycles": len(receipt["account"]["cycles"])}


def _run_arm(db, definition):
    r = ultra.freeze(db, definition)
    if r["status"] not in ("frozen", "running", "complete"):
        raise ValueError("Braço sem dados/execução elegível: " + str(r["reasons"]))
    ultra.replay(db, r["receipt"])
    return ultra.inspect_receipt(db, r["receipt"], timeline=True)


def _opportunities(run):
    return {e["cycle_id"]: e for frame in run["timeline"] for e in frame["events"] if e["event"] == "armed"}


def _cycle_results(run):
    return {item["cycle_id"]: item["net"] for item in run["account"]["completed"]}


def _management_comparison(db, definition, candidate, origin):
    hold = deepcopy(definition)
    for module in hold["modules"]:
        module["breakeven"] = 0
    reference = _run_arm(db, hold)
    events = _opportunities(reference)
    by_start = {(e["owner"], e["available_msc"]): key for key, e in events.items()}
    base_net, candidate_net = _cycle_results(reference), _cycle_results(candidate)
    pairs = []
    for frame in candidate["timeline"]:
        for event in frame["events"]:
            if event["event"] != "management":
                continue
            cycle_id = event["cycle_id"]
            start = origin[cycle_id]["available_msc"]
            other = by_start.get((event["owner"], start))
            before = base_net.get(other)
            after = candidate_net.get(cycle_id)
            pairs.append({"owner": event["owner"], "rule": event["reason"], "key": event["key"],
                          "delay_from_signal_ms": event["key"][0] - start, "cycle": cycle_id, "hold_cycle": other,
                          "managed_net": after, "hold_net": before,
                          "difference": None if before is None or after is None else after - before})
    held = metrics(reference)
    return {"hold_receipt": reference["receipt"], "hold_metrics": held, "eligible_transitions": pairs,
            "net_difference": metrics(candidate)["end_equity"] - held["end_equity"]}


def compare(db, receipt):
    candidate = ultra.inspect_receipt(db, receipt, timeline=True)
    if candidate["status"] != "complete":
        raise ValueError("A comparação exige carteira concluída.")
    definition = {k: deepcopy(v) for k, v in candidate["policy"].items() if k not in ("revision", "tick_cash", "engine_hash")}
    static = deepcopy(definition)
    for module in static["modules"]:
        module["invalidate"] = 0
        if module["strategy"] == "pattern_definition":
            module["params"]["apply_invalidation"] = False
    base = _run_arm(db, static)
    outcomes = _cycle_results(base)
    candidate_outcomes = _cycle_results(candidate)
    still_open = {c["id"] for c in candidate["account"]["cycles"].values()}
    reference_orders = _opportunities(base)
    reference_by_key = {(e["owner"], e["available_msc"]): cid for cid, e in reference_orders.items()}
    origin = _opportunities(candidate)
    vetoes = []
    for blocked, reference in zip(candidate["timeline"], base["timeline"], strict=True):
        if blocked["mark"]["key"] != reference["mark"]["key"]:
            raise ValueError("Os braços não compartilham o mesmo relógio de ticks.")
        for event in blocked["events"]:
            if event["event"] not in ("invalidated", "cancel") or event["reason"] not in ("reference_lost", "pattern_invalidation"):
                continue
            # A chave de oportunidade independe da política. Ciclos continuam identificando a versão.
            armed = origin[event["cycle_id"]]
            other_id = reference_by_key.get((event["owner"], armed["available_msc"]))
            net = outcomes.get(other_id)
            candidate_net = candidate_outcomes.get(event["cycle_id"])
            if candidate_net is None and event["cycle_id"] not in still_open:
                candidate_net = 0
            difference = None if net is None or candidate_net is None else candidate_net - net
            vetoes.append({"key": event["key"], "delay_ms": event["key"][0] - armed["available_msc"], "owner": event["owner"], "reference_cycle": other_id,
                           "reference_net": net, "candidate_net": candidate_net, "change": difference, "effect": "unknown" if difference is None else
                           "loss_avoided" if difference > 0 else "gain_discarded" if difference < 0 else "neutral"})
    candidate_metrics, base_metrics = metrics(candidate), metrics(base)
    delta = candidate_metrics["end_equity"] - base_metrics["end_equity"]
    ablations = {}
    for module in definition["modules"]:
        subset = [m for m in definition["modules"] if m["owner"] != module["owner"]]
        if subset:
            other = _run_arm(db, dict(definition, modules=subset))
            ablations[module["owner"]] = {"receipt": other["receipt"], "metrics": metrics(other)}
        else:
            ablations[module["owner"]] = {"end_equity_cash": definition["cash"]}
    result = {"version": 1, "sample_kind": definition["sample_kind"], "manifest_id": definition["manifest_id"],
              "candidate_receipt": receipt, "reference_receipt": base["receipt"],
              "cash": definition["cash"], "conditional": candidate_metrics, "static": base_metrics,
              "management": _management_comparison(db, definition, candidate, origin),
              "without_module": ablations, "paired_vetoes": vetoes, "net_difference": delta,
              "verdict": "reject_on_sample" if delta <= 0 else "positive_on_sample",
              "economic_approval": False}
    return dict(result, comparison_id=digest(result))
