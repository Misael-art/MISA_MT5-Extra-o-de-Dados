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


def compare(db, receipt):
    candidate = ultra.inspect_receipt(db, receipt, timeline=True)
    if candidate["status"] != "complete":
        raise ValueError("A comparação exige carteira concluída.")
    definition = {k: deepcopy(v) for k, v in candidate["policy"].items() if k not in ("revision", "tick_cash")}
    static = deepcopy(definition)
    for module in static["modules"]:
        module["invalidate"] = 0
    base = _run_arm(db, static)
    outcomes = {item["cycle_id"]: item["net"] for item in base["account"]["completed"]}
    vetoes = []
    for blocked, reference in zip(candidate["timeline"], base["timeline"], strict=True):
        if blocked["mark"]["key"] != reference["mark"]["key"]:
            raise ValueError("Os braços não compartilham o mesmo relógio de ticks.")
        for event in blocked["events"]:
            if event["event"] not in ("invalidated", "cancel") or event["reason"] != "reference_lost":
                continue
            # A chave de oportunidade independe da política. Ciclos continuam identificando a versão.
            armed = next((e for e in reference["events"] if e["owner"] == event["owner"]
                          and e["event"] == "order"), None)
            other_id = armed["cycle_id"] if armed else None
            net = outcomes.get(other_id)
            vetoes.append({"key": event["key"], "owner": event["owner"], "reference_cycle": other_id,
                           "reference_net": net, "effect": "unknown" if net is None else
                           "loss_avoided" if net < 0 else "gain_discarded" if net > 0 else "neutral"})
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
              "without_module": ablations, "paired_vetoes": vetoes, "net_difference": delta,
              "verdict": "reject_on_sample" if delta <= 0 else "positive_on_sample",
              "economic_approval": False}
    return dict(result, comparison_id=digest(result))
