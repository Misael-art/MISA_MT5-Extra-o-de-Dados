"""Seleção temporal de carteiras discretas. Cada revisão decide antes do primeiro tick do corte."""
from collections import defaultdict
from datetime import date
import json

from sqlalchemy import text

from ..analytics import datasets
from ..composition import ultra as engine
from ..experiments import store
from ..experiments.schema import closed, digest, encoded
from . import ultra


def register(db, receipt, plan):
    campaign = ultra.inspect_receipt(db, receipt)
    fields = ("objective", "train_start", "validation_start", "reserve_start", "end_exclusive",
              "trials", "attempt_budget", "min_sessions", "switch_cost", "minimum_gain", "max_drawdown")
    closed(plan, fields, "Protocolo Ultra")
    if plan["objective"] != "net_marked_equity":
        raise ValueError("Objetivo de seleção desconhecido.")
    bounds = [date.fromisoformat(plan[k]) for k in fields[1:5]]
    if any(a >= b for a, b in zip(bounds, bounds[1:])):
        raise ValueError("Treino, validação e reserva exigem cortes cronológicos sem sobreposição.")
    for key in ("attempt_budget", "min_sessions", "switch_cost", "minimum_gain", "max_drawdown"):
        ultra._integer(plan[key], 0 if key in ("switch_cost", "minimum_gain") else 1, key)
    owners = [m["owner"] for m in campaign["policy"]["modules"]]
    if (not isinstance(plan["trials"], list) or not plan["trials"]
            or len(plan["trials"]) > min(plan["attempt_budget"], 128)):
        raise ValueError("Grade vazia ou acima do orçamento congelado.")
    for trial in plan["trials"]:
        closed(trial, ("owners", "invalidation"), "Alternativa Ultra")
        if (type(trial["invalidation"]) is not bool or not isinstance(trial["owners"], list)
                or trial["owners"] != [owner for owner in owners if owner in trial["owners"]]):
            raise ValueError("Alternativa com dono desconhecido, duplicado ou fora da precedência.")
    ids = [digest(t) for t in plan["trials"]]
    if len(set(ids)) != len(ids):
        raise ValueError("Tentativa duplicada.")
    mandatory = [[]] + [[owner] for owner in owners] + [owners]
    if any({"owners": arm, "invalidation": False} not in plan["trials"] for arm in mandatory):
        raise ValueError("Inclua caixa, estratégias estáticas individuais e carteira estática.")
    ident = digest([1, receipt, plan])
    with store.transaction(db) as conn:
        conn.execute(text("CREATE TABLE IF NOT EXISTS _ultra_studies (id TEXT PRIMARY KEY, receipt TEXT NOT NULL, "
                          "plan TEXT NOT NULL, result TEXT)"))
        conn.execute(text("INSERT INTO _ultra_studies VALUES(:id,:r,:p,NULL) ON CONFLICT DO NOTHING"),
                     dict(id=ident, r=receipt, p=encoded(plan)))
    return {"study_id": ident, "receipt": receipt, "status": "frozen", "plan": plan}


def _variants(policy, trials):
    variants = []
    for trial in trials:
        modules = [dict(m, invalidate=m["invalidate"] if trial["invalidation"] else 0) for m in policy["modules"]]
        for m in modules:
            if m["strategy"] == "pattern_definition":
                m["params"] = dict(m["params"], apply_invalidation=m["params"]["apply_invalidation"] and trial["invalidation"])
        variants.append((dict(policy, revision=digest([policy["revision"], trial]), modules=modules), set(trial["owners"])))
    return variants


def _day(state, variant, batch):
    policy, owners = variant
    start = state.get("last_equity", state["cash"])
    peak = state.get("peak", start)
    worst = state.get("drawdown", 0)
    completed = len(state["completed"])
    for tick, all_signals in batch:
        signals = {k: v for k, v in all_signals.items() if k in owners}
        _, mark = engine.advance(state, policy, tick, signals)
        peak = max(peak, mark["equity"])
        worst = max(worst, peak - mark["equity"])
    state.update(last_equity=mark["equity"], peak=peak, drawdown=worst)
    outcomes = [c["net"] for c in state["completed"][completed:]]
    return {"delta": mark["equity"] - start, "equity": mark["equity"], "drawdown": worst,
            "wins_closed": sum(v > 0 for v in outcomes), "cycles_closed": len(outcomes)}


def simulate(policy, plan, ticks, signals):
    epoch = date(1970, 1, 1)
    start, validation, reserve, end = [(date.fromisoformat(plan[key]) - epoch).days
                                      for key in ("train_start", "validation_start", "reserve_start", "end_exclusive")]
    days = defaultdict(list)
    for index, tick in enumerate(ticks):
        day = tick["time_msc"] // 86_400_000
        if start <= day < end:
            days[day].append((tick, signals.get(index, {})))
    train = [d for d in days if d < validation]
    if len(train) < plan["min_sessions"] or not any(validation <= d < reserve for d in days) or not any(d >= reserve for d in days):
        return {"status": "insufficient_data", "reason": "insufficient_chronological_sessions", "observed_sessions": len(days)}
    trials = plan["trials"]
    variants = _variants(policy, trials)
    states = [engine.initial_state(policy["cash"]) for _ in trials]
    history = [[] for _ in trials]
    cash = next(i for i, t in enumerate(trials) if not t["owners"])
    static = [i for i, t in enumerate(trials) if len(t["owners"]) == 1 and not t["invalidation"]]
    full = next(i for i, t in enumerate(trials) if len(t["owners"]) == len(policy["modules"]) and not t["invalidation"])
    paths = {name: {"state": engine.initial_state(policy["cash"]), "active": cash, "next": validation,
                    "reviews": [], "daily": []} for name in ("cash", "static_single", "static_portfolio", "fixed", "weekly", "fortnightly")}
    for day, batch in days.items():
        if day >= validation:
            # Somente sessões já concluídas entram em pontuação e previsão.
            gains = [sum(row["delta"] for row in rows) for rows in history]
            eligible = [i for i, s in enumerate(states) if s["drawdown"] <= plan["max_drawdown"]
                        and gains[i] >= plan["minimum_gain"]]
            best = max(eligible or [cash], key=lambda i: (gains[i], -i))
            for name, path in paths.items():
                if day >= path["next"]:
                    old = path["active"]
                    selected = (cash if name == "cash" else full if name == "static_portfolio"
                                else max(static, key=lambda i: (gains[i], -i)) if name == "static_single" and static
                                else best)
                    first = not path["reviews"]
                    # Um empate mantém a versão instalada. A data apenas abre a revisão.
                    if not first and old in eligible and gains[selected] - gains[old] <= plan["switch_cost"]:
                        selected = old
                    changed = selected != old and not first
                    fee = plan["switch_cost"] if changed else 0
                    path["state"]["cash"] -= fee
                    path["active"] = selected
                    cadence = {"weekly": 7, "fortnightly": 14}.get(name)
                    path["next"] = (validation + ((day - validation) // cadence + 1) * cadence) if cadence else end
                    decision = ("keep" if old == selected else "suspend" if selected == cash else
                                "reactivate" if old == cash and not first else "select" if first else "replace")
                    total = sum(r["cycles_closed"] for r in history[selected])
                    path["p_win"] = sum(r["wins_closed"] for r in history[selected]) / total if total else None
                    path["reviews"].append({"day": day, "last_training_day": history[0][-1]["day"],
                         "scores": list(gains), "old": old, "selected": selected, "decision": decision,
                         "switch_cost": fee, "revision": variants[selected][0]["revision"],
                         "open_origin_versions": [c["version"] for c in path["state"]["cycles"].values()]})
                result = _day(path["state"], variants[path["active"]], batch)
                path["daily"].append(dict(result, p_win=path["p_win"], day=day, role="reserve" if day >= reserve else "validation"))
        for state, variant, rows in zip(states, variants, history, strict=True):
            rows.append(dict(_day(state, variant, batch), day=day))
    out = {}
    for name, path in paths.items():
        daily = path["daily"]
        net = path["state"]["last_equity"] - policy["cash"]
        scored = [r for r in daily if r["p_win"] is not None and r["cycles_closed"]]
        count = sum(r["cycles_closed"] for r in scored)
        brier = sum(r["wins_closed"] * (1 - r["p_win"]) ** 2
                    + (r["cycles_closed"] - r["wins_closed"]) * r["p_win"] ** 2 for r in scored)
        out[name] = {"probability_event": "net_positive_given_cycle_closed_in_scored_session",
                     "prediction_cut": "most_recent_review_before_session",
                     "oos_brier": brier / count if count else None, "scored_cycles": count, "net": net, "max_drawdown": path["state"]["drawdown"],
                     "worst_session": min(d["delta"] for d in daily), "reviews": path["reviews"], "daily": daily,
                     "switch_cost": sum(r["switch_cost"] for r in path["reviews"])}
    for item in out.values():
        for j, review in enumerate(item["reviews"]):
            next_day = item["reviews"][j + 1]["day"] if j + 1 < len(item["reviews"]) else end
            def forward(i):
                return sum(r["delta"] for r in history[i] if review["day"] <= r["day"] < next_day)
            benefit = forward(review["selected"]) - forward(review["old"]) - review["switch_cost"]
            review["next_window_shadow_difference"] = benefit
            review["false_alarm"] = (benefit <= 0 if j > 0 and review["selected"] != review["old"] else None)
        item["net_vs_fixed"] = item["net"] - out["fixed"]["net"]
    deltas = [a["delta"] - b["delta"] for a, b in zip(out["weekly"]["daily"], out["fixed"]["daily"], strict=True)]
    from ..experiments.f19_summary import _estimate
    interval = _estimate(deltas, {"block_sessions": 5, "bootstrap_replicates": 500},
                         {"confidence": .95, "minimum_net_cash_difference": 0}, seed=1)
    return {"status": "evaluated", "weekly_minus_fixed_session_interval": interval,
            "selection_status": "no_candidate" if paths["fixed"]["active"] == cash else "research_candidate", "policies": out, "attempts": [{"trial": t, "days": days}
            for t, days in zip(trials, history, strict=True)], "economic_approval": False,
            "boundary_policy": "positions_marked_at_cut_open_cycles_keep_original_version",
            "observed_sessions": len(days)}


def evaluate(db, ident):
    with db.engine.connect() as conn:
        row = conn.execute(text("SELECT receipt,plan,result FROM _ultra_studies WHERE id=:i"), {"i": ident}).first()
    if row is None:
        raise ValueError("Protocolo Ultra desconhecido.")
    receipt, plan, cached = row
    if cached:
        return json.loads(cached)
    campaign = ultra.inspect_receipt(db, receipt)
    if campaign["status"] in ("insufficient_data", "no_candidate"):
        result = {"status": campaign["status"], "reasons": campaign["reasons"]}
    else:
        policy = campaign["policy"]
        ticks, signals = ultra._inputs(datasets.load(db, policy["manifest_id"]), policy)
        result = simulate(policy, json.loads(plan), ticks, signals)
    result.update(study_id=ident, receipt=receipt, sample_kind=campaign["sample_kind"])
    with store.transaction(db) as conn:
        conn.execute(text("UPDATE _ultra_studies SET result=:result WHERE id=:i AND result IS NULL"),
                     {"i": ident, "result": encoded(result)})
    return result
