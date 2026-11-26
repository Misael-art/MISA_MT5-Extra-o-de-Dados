"""Contrato de QA para executar cada fatia congelada no relógio do Tester."""
from copy import deepcopy
import re

from ..analytics import datasets
from ..composition import ultra as portfolio
from ..experiments.schema import digest
from . import ultra

STATE_MARKER = "MT5X_ULTRA_SLICED_STATE1|"


def replay(db, package, original):
    """Congela uma troca na primeira execução parcial e recalcula só a versão futura."""
    policy = package["policy"]
    for index, frame in enumerate(original):
        partial = next((e for e in frame["events"] if e["event"] == "fill" and e["reason"] == "partial"), None)
        if partial:
            break
    else:
        return original, None

    alternate = deepcopy(policy)
    slot = next(i for i, module in enumerate(alternate["modules"]) if module["owner"] == partial["owner"])
    module = alternate["modules"][slot]
    module["breakeven"] = max(0, module["breakeven"] - 1) if module["breakeven"] else 1
    alternate["revision"] = digest([policy["revision"], "tester_sliced", alternate["modules"]])
    ticks, signals = ultra._inputs(datasets.load(db, package["source_manifest"]), policy)
    state = portfolio.initial_state(policy["cash"])
    frames = []
    for i, tick in enumerate(ticks):
        events, mark = portfolio.advance(state, policy if i <= index else alternate, tick, signals.get(i, {}))
        frames.append({"events": events, "mark": mark})
    if frames[:index + 1] != original[:index + 1]:
        raise ValueError("O replay anterior à troca diverge do livro congelado.")
    return frames, {"checkpoint": index + 1, "slot": slot, "old": policy["revision"],
                    "new": alternate["revision"], "breakeven": module["breakeven"],
                    "key": partial["key"], "filled": partial["steps"], "pending": partial["remaining"],
                    "cycle": partial["cycle_id"]}


def check_state(package, frames, lines, proof, plan, book_marker):
    records = []
    processed = 0
    for line in lines:
        if book_marker + "FRAME|" in line:
            processed += 1
        if STATE_MARKER not in line:
            continue
        record = line.partition(STATE_MARKER)[2].split("|")
        if record[0] in ("SAVED", "LOADED", "SWITCH") and (not plan or processed != plan["checkpoint"]):
            raise ValueError("O checkpoint e a troca devem ocorrer na mesma fronteira de cotações.")
        records.append(record)
    versions = [r for r in records if r[0] == "VERSION"]
    other = [r for r in records if r[0] != "VERSION"]
    policy = package["policy"]
    owners = {m["owner"]: i for i, m in enumerate(policy["modules"])}
    expected = []
    for ordinal, frame in enumerate(frames, 1):
        msc, sequence = frame["mark"]["key"]
        for event in frame["events"]:
            if event["event"] != "armed":
                continue
            slot = owners[event["owner"]]
            threshold = policy["modules"][slot]["breakeven"]
            if plan and ordinal > plan["checkpoint"] and slot == plan["slot"]:
                threshold = plan["breakeven"]
            expected.append(["VERSION", *map(str, (msc, sequence, slot, event["available_msc"], event["cycle_version"], threshold))])
    if versions != expected:
        raise ValueError("A versão ou a regra da intenção diverge do percurso congelado.")
    if not plan:
        if other:
            raise ValueError("A execução sem fill parcial não pode comprovar uma retomada.")
        return {"status": "no_partial", "new_version_intents": 0}
    if len(other) != 3 or [r[0] for r in other] != ["SAVED", "LOADED", "SWITCH"]:
        raise ValueError("O agente precisa gravar e restaurar o checkpoint antes da troca.")
    saved, loaded, switch = other
    if len(saved) != 15 or saved[1:] != loaded[1:] or not re.fullmatch(r"[a-f0-9]{64}", saved[1]):
        raise ValueError("Os recibos de gravação e leitura do checkpoint divergem.")
    ordinal, msc, sequence, slot, opened, ticket, filled, pending, cash, equity, margin, stop, target = map(int, saved[2:])
    old = policy["modules"][plan["slot"]]["breakeven"]
    if ([ordinal, msc, sequence, slot, filled, pending] !=
            [plan["checkpoint"], *plan["key"], plan["slot"], plan["filled"], plan["pending"]]
            or switch != ["SWITCH", *map(str, (ordinal, plan["old"], plan["new"], slot, old, plan["breakeven"]))]):
        raise ValueError("A retomada ou a transição de proteção diverge do plano de ensaio.")
    cycle = next((c for c in proof["cycles"] if c["cycle_id"] == plan["cycle"]), None)
    stride = max(2, len(owners))
    if not cycle or cycle["magic"] != opened*stride+slot or cycle["version"] != plan["old"]:
        raise ValueError("O checkpoint não corresponde ao ciclo físico de origem.")
    snapshot = next((s for s in cycle["snapshots"] if s["key"] == plan["key"]), None)
    if (not snapshot or not any([r["ticket"], r["steps"], r["stop"], r["target"]] == [ticket, filled, stop, target]
                                for r in snapshot["positions"])
            or [snapshot["account"][k] for k in ("cash", "equity", "margin")] != [cash, equity, margin]):
        raise ValueError("Tickets, proteção ou saldo do checkpoint divergem do snapshot físico.")
    return {"status": "reconciled_in_same_pass", "binding": saved[1], "checkpoint": ordinal, "key": [msc, sequence],
            "cycle_id": cycle["cycle_id"], "position_ticket": ticket, "filled": filled, "reserved": pending,
            "cash": cash, "equity": equity, "margin": margin, "stop": stop, "target": target,
            "old_version": plan["old"], "active_version": plan["new"], "open_breakeven": old,
            "new_breakeven": plan["breakeven"], "new_version_intents": sum(r[5] == plan["new"] for r in versions),
            "new_version_filled_cycles": sum(c["version"] == plan["new"] and any(d["entry"] == "DEAL_ENTRY_IN" for d in c["deals"])
                                             for c in proof["cycles"])}
