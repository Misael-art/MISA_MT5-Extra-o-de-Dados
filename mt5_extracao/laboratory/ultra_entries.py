"""Experimento pareado de entrada em uma conta compartilhada, nas cotações do pacote."""
from hashlib import sha256
from pathlib import Path

from ..analytics import datasets
from ..composition import ultra as book
from ..experiments.schema import digest
from ..presets.compiler import verify_ultra_research
from . import ultra, ultra_evaluation


def _fill_grid(state, policy, tick, spacing, events):
    """A reserva cobre todos os passos ao pior preço. Cada nível corresponde a um passo."""
    bid, ask = tick["bid"], tick["ask"]
    capacity = policy["fill_steps_per_tick"]
    for module in policy["modules"]:
        owner = module["owner"]
        cycle = state["cycles"].get(owner)
        if not cycle or not cycle["remaining"] or capacity <= 0:
            continue
        side = cycle["side"]
        price = ask if side == 1 else bid
        improvement = side * (cycle["limit"] - price)
        if improvement < 0:
            continue
        # Um salto de cotação pode deixar vários níveis executáveis no mesmo tick.
        levels = improvement // spacing + 1 if spacing else module["steps"]
        quantity = min(capacity, levels - cycle["filled"], cycle["remaining"])
        if quantity <= 0:
            continue
        common = {"key": [tick["time_msc"], tick["seq"]], "bid": bid, "ask": ask,
                  "owner": owner, "cycle_id": cycle["id"], "cycle_version": cycle["version"]}
        before = book.account(state, policy, bid, ask)
        if before["margin"] + before["pending_entry_cost"] > before["equity"]:
            remaining = cycle["remaining"]
            cycle["remaining"] = 0
            if not cycle["filled"]:
                del state["cycles"][owner]
            events.append(dict(common, event="cancel", reason="shared_margin_at_fill", cancelled_steps=remaining,
                               **book.account(state, policy, bid, ask)))
            continue
        first_level = cycle["filled"]
        fee = quantity * policy["fee"]
        cycle["remaining"] -= quantity
        cycle["filled"] += quantity
        cycle["basis"] += price * quantity
        cycle["entry_fees"] += fee
        state["cash"] -= fee
        state["local"][owner]["pnl"] -= fee
        state["cost"] += fee
        state["spread_cost_twice"] += (ask - bid) * quantity * policy["tick_cash"]
        capacity -= quantity
        event = dict(common, event="fill", reason="partial" if cycle["remaining"] else "complete",
                     **book.account(state, policy, bid, ask), steps=quantity, price=price, fee=fee,
                     remaining=cycle["remaining"], stop=cycle["stop"], target=cycle["target"])
        if spacing:
            event.update(first_level=first_level, last_level=cycle["filled"]-1, spacing_ticks=spacing)
        events.append(event)


def _structure(previous, tick, events, invalid, waiting):
    changes = []
    key = [tick["time_msc"], tick["seq"]]
    for owner, (cid, side, reference, distance, filled) in previous.items():
        executable = tick["bid"] if side == 1 else tick["ask"]
        if filled and distance and cid not in invalid and side*(executable-reference) <= -distance:
            invalid.add(cid)
            changes.append(dict(owner=owner, cycle_id=cid, key=key, state="open_structure_lost",
                                reference=reference, distance=distance, observed=executable))
    for event in events:
        owner, cid = event["owner"], event["cycle_id"]
        if event["event"] in ("cancel", "invalidated") and event["reason"] in ("reference_lost", "pattern_invalidation"):
            waiting[owner] = cid
        elif event["event"] == "exit":
            distance = previous[owner][3]
            integrity = "reference_lost" if cid in invalid else "reference_intact" if distance else "reference_unobserved"
            changes.append(dict(owner=owner, cycle_id=cid, key=key, state=integrity, net=event["net"], exit=event["reason"]))
            if cid in invalid:
                waiting[owner] = cid
        elif event["event"] == "order" and owner in waiting:
            changes.append(dict(owner=owner, cycle_id=cid, key=key, state="new_signal_confirmed",
                                previous_cycle=waiting.pop(owner)))
    return changes


def _arm(policy, ticks, signals, spacing):
    state = book.initial_state(policy["cash"])
    # A passagem de decisão conserva a suspensão local, o TTL, as proteções e as reservas originais.
    decision = dict(policy, fill_steps_per_tick=0)
    timeline, structure = [], []
    invalid, waiting = set(), {}
    for i, tick in enumerate(ticks):
        prior = {owner: (c["id"], c["side"], c["reference"], c["invalidate"], c["filled"])
                 for owner, c in state["cycles"].items()}
        events, mark = book.advance(state, decision, tick, signals.get(i, {}))
        _fill_grid(state, policy, tick, spacing, events)
        mark.update(book.account(state, policy, tick["bid"], tick["ask"]))
        structure.extend(_structure(prior, tick, events, invalid, waiting))
        timeline.append({"events": events, "mark": mark})
    return timeline, {"metrics": ultra_evaluation.metrics({"timeline": timeline, "account": state, "policy": policy}),
                      "structure": structure, "completed": state["completed"],
                      "decisions": [e for frame in timeline for e in frame["events"]]}


def compare(db, package, spacing):
    verify_ultra_research(db, package)
    ultra._integer(spacing, 1, "Distância do gradiente em ticks")
    policy = package["policy"]
    if policy["engine_hash"] != ultra.source_identity():
        raise ValueError("O experimento exige a versão de origem do motor congelado.")
    if any((m["steps"]-1)*spacing >= m["stop"] for m in policy["modules"]):
        raise ValueError("Todos os níveis da grade devem ficar antes do stop congelado.")
    protocol = {"package": package["package_hash"], "spacing_ticks": spacing,
                "levels": "one_volume_step_per_level", "protection": "original_stop_target",
                "capital": policy["cash"], "risk_limit": policy["risk_limit"], "liquidity": policy["fill_steps_per_tick"],
                "executor": sha256(Path(__file__).read_text(encoding="utf-8").encode()).hexdigest()}
    experiment = digest(protocol)
    reference = ultra.inspect_receipt(db, package["receipt"], timeline=True)["timeline"]
    ticks, signals = ultra._inputs(datasets.load(db, package["source_manifest"]), policy)
    direct, single = _arm(policy, ticks, signals, 0)
    for index, (old, repeated) in enumerate(zip(reference, direct, strict=True)):
        if old != repeated:
            return {"status": "direct_control_diverged", "experiment": experiment, "protocol": protocol,
                    "key": old["mark"]["key"], "ordinal": index, "expected": old, "observed": repeated}
    gradient_policy = dict(policy, revision=digest([policy["revision"], experiment, "linear"]))
    _, gradient = _arm(gradient_policy, ticks, signals, spacing)
    return {"status": "compared", "experiment": experiment, "protocol": protocol, "sample_kind": package["sample_kind"],
            "frames_compared": len(reference), "direct": single, "linear": gradient, "economic_approval": False,
            "net_difference": gradient["metrics"]["end_equity"]-single["metrics"]["end_equity"],
            "native_status": "capability_absent", "structure_scope": "frozen_reference_on_observed_executable_quotes"}
