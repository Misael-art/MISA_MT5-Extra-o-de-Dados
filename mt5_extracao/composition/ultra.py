"""Replay de pesquisa em uma conta hedging modelada. Preços em ticks, volumes em passos."""
from ..experiments.schema import digest


def initial_state(cash):
    return {"cash": cash, "cursor": 0, "session": None, "local": {}, "cycles": {},
            "cost": 0, "spread_cost_twice": 0, "completed": [], "last_key": None}


def account(state, policy, bid, ask):
    floating = risk = margin = exposure = pending_cost = 0
    value = policy["tick_cash"]
    for cycle in state["cycles"].values():
        side = cycle["side"]
        mark = bid if side == 1 else ask
        filled, remaining = cycle["filled"], cycle["remaining"]
        basis = cycle["basis"]
        floating += side * (mark * filled - basis) * value
        # Risco até o stop a partir do patrimônio marcado. Pendentes reservam
        # os dois lados da comissão; fills existentes já pagaram a primeira.
        risk += (max(0, side * (mark - cycle["stop"])) * value + policy["fee"]) * filled
        risk += (max(0, side * (cycle["limit"] - cycle["stop"])) * value + 2 * policy["fee"]) * remaining
        pending_cost += remaining * ((ask - bid) * value + policy["fee"])
        exposure += filled
        margin += (filled + remaining) * policy["margin"]
    return {"cash": state["cash"], "equity": state["cash"] + floating,
            "risk": risk, "margin": margin, "pending_entry_cost": pending_cost, "exposure_steps": exposure, "cost": state["cost"],
            "spread_cost_twice": state["spread_cost_twice"]}


def advance(state, policy, tick, signals):
    """Uma chave (epoch ms, seq), admitida uma vez. Sem chamada de terminal ou de rede."""
    msc, seq, bid, ask = (tick[k] for k in ("time_msc", "seq", "bid", "ask"))
    key = [msc, seq]
    if state["last_key"] is not None and key <= state["last_key"]:
        raise ValueError("Chave de tick duplicada ou fora de ordem.")
    if bid <= 0 or ask < bid:
        raise ValueError("Cotação imprópria para execução.")
    events = []
    cycles = state["cycles"]
    modules = policy["modules"]
    session = msc // 86_400_000

    def record(owner, kind, reason, cycle=None, **detail):
        events.append({"key": key, "owner": owner, "event": kind, "reason": reason,
                       "cycle_id": cycle["id"] if cycle else None, "cycle_version": cycle["version"] if cycle else None,
                       "bid": bid, "ask": ask,
                       **account(state, policy, bid, ask), **detail})

    if state["session"] != session:
        state["local"] = {m["owner"]: {"attempts": 0, "losses": 0, "pnl": 0} for m in modules}
        state["session"] = session
        record(None, "session", "utc_day_reset")

    def cancel(owner, cycle, reason):
        remaining = cycle["remaining"]
        cycle["remaining"] = 0
        if not cycle["filled"]:
            del cycles[owner]
        record(owner, "cancel", reason, cycle, cancelled_steps=remaining)

    for module in modules:
        owner = module["owner"]
        c = cycles.get(owner)
        if not c:
            continue
        side, qty = c["side"], c["filled"]
        mark = bid if side == 1 else ask
        if c["remaining"] and (session != c["session"] or msc >= c["expires"]):
            cancel(owner, c, "pending_expired")
        if not qty:
            continue
        stopped = side * (mark - c["stop"]) <= 0
        if stopped or side * (mark - c["target"]) >= 0:
            if c["remaining"]:
                cancel(owner, c, "exit_with_pending")
            fee = policy["fee"] * qty
            realized = side * (mark * qty - c["basis"]) * policy["tick_cash"] - fee
            state["cash"] += realized
            state["cost"] += fee
            state["spread_cost_twice"] += (ask - bid) * qty * policy["tick_cash"]
            net = realized - c["entry_fees"]
            local = state["local"][owner]
            local["pnl"] += realized
            local["losses"] = local["losses"] + 1 if net < 0 else 0
            del cycles[owner]
            state["completed"].append({"owner": owner, "cycle_id": c["id"], "net": net, "key": key})
            record(owner, "exit", "stop" if stopped else "target", c, steps=qty,
                   price=mark, net=net, fee=fee)
        elif (c["breakeven"] and not c["managed"] and
              side * (mark - c["limit"]) >= c["breakeven"]):
            if c["remaining"]:
                cancel(owner, c, "management_transition")
            c["managed"], c["stop"] = True, c["limit"]
            record(owner, "management", "breakeven", c, stop=c["stop"], target=c["target"])

    for module in modules:  # Ordem congelada de precedência, inclusive para fills.
        owner = module["owner"]
        local = state["local"][owner]
        signal = signals.get(owner)
        if signal is None:
            continue
        side, reference = signal["side"], signal["reference"]
        c = {"id": digest([policy["revision"], owner, signal["available_msc"]]), "side": side,
             "limit": ask if side == 1 else bid, "session": session,
             "expires": signal["available_msc"] + module["ttl_ms"], "reference": reference,
             "version": policy["revision"], "breakeven": module["breakeven"], "invalidate": module["invalidate"],
             "remaining": module["steps"], "filled": 0, "basis": 0, "entry_fees": 0, "managed": False}
        c["stop"] = c["limit"] - side * module["stop"]
        c["target"] = c["limit"] + side * module["target"]
        record(owner, "armed", "closed_bar", c, available_msc=signal["available_msc"],
               reference=reference, side=side, **{k: signal[k] for k in ("pattern_event", "definition_hash") if k in signal})
        entry = signal.get("entry_decision")
        if (entry and module["params"]["apply_invalidation"] and entry["status"] != "confirmed"):
            record(owner, "invalidated" if entry["status"] == "invalidated" else "veto",
                   "pattern_invalidation" if entry["status"] == "invalidated" else "unknown_pattern_condition",
                   c, entry_decision=entry, pattern_event=signal["pattern_event"])
            continue
        mark = bid if side == 1 else ask
        if owner in cycles:
            record(owner, "veto", "owner_cycle_open", c)
            continue
        if local["losses"] >= module["max_losses"] or local["pnl"] >= module["daily_goal"]:
            record(owner, "veto", "local_loss_streak" if local["losses"] >= module["max_losses"]
                   else "local_daily_goal", c, local=dict(local))
            continue
        if msc >= c["expires"]:
            record(owner, "expired", "signal_ttl", c)
            continue
        if module["invalidate"] and side * (mark - reference) <= -module["invalidate"]:
            record(owner, "invalidated", "reference_lost", c, reference=reference)
            continue
        if ask - bid >= module["stop"]:
            record(owner, "veto", "stop_inside_spread", c)
            continue
        before = account(state, policy, bid, ask)
        risk = c["remaining"] * (module["stop"] * policy["tick_cash"] + 2 * policy["fee"])
        margin = c["remaining"] * policy["margin"]
        # Ao preencher, o spread e a comissão já reduzem o patrimônio livre.
        entry_cost = c["remaining"] * ((ask - bid) * policy["tick_cash"] + policy["fee"])
        if before["risk"] + risk > policy["risk_limit"]:
            record(owner, "veto", "shared_risk", c, requested_risk=risk)
        elif before["margin"] + before["pending_entry_cost"] + margin + entry_cost > before["equity"]:
            record(owner, "veto", "shared_margin", c, requested_margin=margin)
        else:
            cycles[owner] = c
            local["attempts"] += 1
            record(owner, "order", "reserved", c, steps=c["remaining"], stop=c["stop"], target=c["target"])

    capacity = policy["fill_steps_per_tick"]
    for module in modules:
        owner = module["owner"]
        c = cycles.get(owner)
        if not c or not c["remaining"]:
            continue
        side = c["side"]
        mark, price = (bid, ask) if side == 1 else (ask, bid)
        if c["invalidate"] and side * (mark - c["reference"]) <= -c["invalidate"]:
            cancel(owner, c, "reference_lost")
            continue
        if side * (mark - c["stop"]) <= 0:
            cancel(owner, c, "stop_before_fill")
            continue
        if capacity <= 0 or side * (price - c["limit"]) > 0:
            continue
        available = account(state, policy, bid, ask)
        if available["margin"] + available["pending_entry_cost"] > available["equity"]:
            cancel(owner, c, "shared_margin_at_fill")
            continue
        qty = min(capacity, c["remaining"])
        fee = qty * policy["fee"]
        c["remaining"] -= qty
        c["filled"] += qty
        c["basis"] += price * qty
        c["entry_fees"] += fee
        state["cash"] -= fee
        state["cost"] += fee
        state["spread_cost_twice"] += (ask - bid) * qty * policy["tick_cash"]
        state["local"][owner]["pnl"] -= fee
        capacity -= qty
        record(owner, "fill", "partial" if c["remaining"] else "complete", c, steps=qty,
               remaining=c["remaining"], stop=c["stop"], target=c["target"], price=price, fee=fee)
    state["last_key"] = key
    state["cursor"] += 1
    return events, {"key": key, "bid": bid, "ask": ask, **account(state, policy, bid, ask)}
