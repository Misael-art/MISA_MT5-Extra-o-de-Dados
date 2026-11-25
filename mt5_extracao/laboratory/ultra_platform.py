"""Confronto entre as decisões congeladas e os registros do agente do Strategy Tester."""
from collections import defaultdict
from decimal import Decimal, InvalidOperation

MARKER = "MT5X_ULTRA_ORDERS1|"
SCHEMAS = {
    "BEGIN": "package manifest revision symbol build mode currency cash",
    "VETO": "msc seq slot opened reason",
    "REQUEST": "msc seq slot opened action order position type steps price stop target check sent retcode result_order result_deal request_id safety_state",
    "ORDER": "msc seq ticket magic type steps price stop target",
    "POSITION": "msc seq ticket magic position_id type steps basis stop target",
    "ACCOUNT": "msc seq cash equity margin orders positions",
    "TX": "type order deal position magic request_id retcode steps price",
    "HISTORY": "ticket magic position_id state type done_msc initial remaining stop target",
    "DEAL": "ticket order position_id magic msc entry type reason steps price commission fee swap profit",
    "END": "frames history orders deals cash equity",
}
TEXT = {"package", "manifest", "revision", "symbol", "mode", "currency", "action", "type", "reason", "entry", "state"}


def _records(lines):
    rows = defaultdict(list)
    order = []
    for line in lines:
        if MARKER not in line:
            continue
        kind, *values = line.partition(MARKER)[2].split("|")
        if kind not in SCHEMAS:
            raise ValueError(f"Registro físico desconhecido: {kind}")
        fields = SCHEMAS[kind].split()
        if len(fields) != len(values):
            raise ValueError(f"Registro físico incompleto: {kind}")
        try:
            row = {k: v if k in TEXT else Decimal(v) if k == "basis" else int(v)
                   for k, v in zip(fields, values)}
        except (ValueError, InvalidOperation) as exc:
            raise ValueError(f"Campo inválido no registro {kind}.") from exc
        if kind == "POSITION" and not row["basis"].is_finite():
            raise ValueError("Preço de posição não finito.")
        order.append(kind)
        rows[kind].append(row)
    if order[:1] != ["BEGIN"] or order[-1:] != ["END"] or order.count("BEGIN") != 1 or order.count("END") != 1:
        raise ValueError("O journal exige uma execução completa do Expert de ordens.")
    return rows


def _by_key(rows):
    result = defaultdict(list)
    for row in rows:
        result[row["msc"], row["seq"]].append(row)
    return result


def _tickets(rows):
    result = {row["ticket"]: row for row in rows}
    if len(result) != len(rows) or any(t <= 0 for t in result):
        raise ValueError("Ticket ausente ou duplicado no histórico do agente.")
    return result


def _executions(rows):
    grouped = defaultdict(lambda: [0, 0, 0, 0, 0])
    for magic, entry, side, reason, steps, price, commission, fee, swap, profit in rows:
        amounts = grouped[magic, entry, side, reason, price]
        for i, value in enumerate((steps, commission, fee, swap, profit)):
            amounts[i] += value
    return [list(key) + amounts for key, amounts in sorted(grouped.items())]


def reconcile(package, frames, lines, symbol, revision, build):
    rows = _records(lines)
    head, end = rows["BEGIN"][0], rows["END"][0]
    policy = package["policy"]
    if (head["package"], head["manifest"], head["revision"], head["symbol"], head["build"], head["currency"], head["cash"]) != (
            package["package_hash"], package["source_manifest"], revision, symbol, build, policy["currency"], policy["cash"]):
        raise ValueError("Identidade da execução de ordens incompatível com o pacote.")
    if head["mode"] not in ("ACCOUNT_MARGIN_MODE_RETAIL_HEDGING", "ACCOUNT_MARGIN_MODE_RETAIL_NETTING", "ACCOUNT_MARGIN_MODE_EXCHANGE"):
        raise ValueError("Modalidade de conta desconhecida no journal do agente.")
    if (end["history"], end["frames"], end["orders"], end["deals"]) != (1, len(frames), len(rows["HISTORY"]), len(rows["DEAL"])):
        raise ValueError("Histórico físico incompleto no encerramento.")
    keys = [tuple(f["mark"]["key"]) for f in frames]
    if [(r["msc"], r["seq"]) for r in rows["ACCOUNT"]] != keys:
        raise ValueError("A conta observada requer uma leitura por cotação na ordem congelada.")
    snapshots = {k: _by_key(rows[k]) for k in ("ORDER", "POSITION", "REQUEST", "VETO")}
    valid_keys = set(keys)
    if any(set(index) - valid_keys for index in snapshots.values()):
        raise ValueError("Registro físico fora da sequência de cotações.")
    history = _tickets(rows["HISTORY"])
    deals = _tickets(rows["DEAL"])
    slots = {m["owner"]: i for i, m in enumerate(policy["modules"])}
    stride = max(2, len(slots))
    cycles = {}
    by_id = {}
    for frame in frames:
        for event in frame["events"]:
            if event["event"] == "armed":
                magic = stride * event["available_msc"] + slots[event["owner"]]
                cycle = {"owner": event["owner"], "cycle_id": event["cycle_id"], "version": event["cycle_version"],
                         "magic": magic, "side": event["side"], "events": [], "requests": [], "vetoes": [],
                         "history": [], "deals": [], "transactions": [], "snapshots": [],
                         "rule": policy["modules"][slots[event["owner"]]]}
                if magic in cycles or event["cycle_id"] in by_id:
                    raise ValueError("Chave de ordem física ambígua na campanha.")
                cycles[magic] = by_id[event["cycle_id"]] = cycle
            if event["cycle_id"] is not None:
                by_id[event["cycle_id"]]["events"].append(event)

    # A ordem de chegada dos callbacks não é a ordem do ciclo. Use os vínculos do histórico.
    order_magic = {t: r["magic"] for t, r in history.items() if r["magic"] in cycles}
    for r in rows["REQUEST"]:
        magic = stride*r["opened"] + r["slot"]
        if not 0 <= r["slot"] < len(slots) or r["opened"] <= 0 or magic not in cycles:
            raise ValueError("Solicitação sem intenção do pacote.")
        cycles[magic]["requests"].append(r)
        if r["result_order"] and r["action"] == "TRADE_ACTION_PENDING":
            order_magic[r["result_order"]] = magic
    for r in rows["VETO"]:
        magic = stride*r["opened"] + r["slot"]
        if not 0 <= r["slot"] < len(slots) or r["opened"] <= 0 or magic not in cycles:
            raise ValueError("Veto sem ciclo do pacote.")
        cycles[magic]["vetoes"].append(r)
    positions = {}
    for r in deals.values():
        if r["entry"] == "DEAL_ENTRY_IN" and r["magic"] in cycles:
            positions[r["position_id"]] = r["magic"]
    unbound = []
    per_tick = defaultdict(list)
    for r in deals.values():
        magic = order_magic.get(r["order"], r["magic"])
        if magic not in cycles:
            magic = positions.get(r["position_id"])
        if magic not in cycles:
            unbound.append(r)
            continue
        cycles[magic]["deals"].append(r)
        order_magic.setdefault(r["order"], magic)
        per_tick[r["msc"]].append((magic, r))
    for t, r in history.items():
        magic = order_magic.get(t, positions.get(r["position_id"]))
        if magic in cycles:
            cycles[magic]["history"].append(r)
        else:
            unbound.append(r)
    request_magics = {r["request_id"]: stride*r["opened"]+r["slot"] for r in rows["REQUEST"] if r["request_id"]}
    for r in rows["TX"]:
        if r["type"] == "TRADE_TRANSACTION_REQUEST":
            magic = request_magics.get(r["request_id"])
        else:
            magic = order_magic.get(r["order"], r["magic"])
        if magic in cycles:
            cycles[magic]["transactions"].append(r)
        elif r["position"] in positions:
            cycles[positions[r["position"]]]["transactions"].append(r)
        else:
            unbound.append(r)
    first = None
    ordinal = {key: index for index, key in enumerate(keys, 1)}

    def check(key, field, expected, observed, cycle=None):
        nonlocal first
        if expected != observed and (first is None or ordinal[key] < first["ordinal"]):
            first = {"ordinal": ordinal[key], "key": list(key), "field": field, "expected": expected, "observed": observed,
                     "cycle_id": cycle and cycle["cycle_id"]}

    pending, held, basis = {}, {}, {}
    entry_orders = {}
    unit = Decimal(str(package["specification"]["tick_size"]))
    tick_keys = {key[0]: key for key in reversed(keys)}
    previous_msc = None
    for frame, account, key in zip(frames, rows["ACCOUNT"], keys, strict=True):
        mark = frame["mark"]
        check(key, "platform.veto", [], snapshots["VETO"][key])
        wants = []
        fills = []
        for e in frame["events"]:
            if e["cycle_id"] is None:
                continue
            cycle = by_id[e["cycle_id"]]
            magic = cycle["magic"]
            kind = e["event"]
            side = cycle["side"]
            if kind == "order":
                entry_orders[magic] = key
                limit = mark["ask"] if side == 1 else mark["bid"]
                pending[magic] = [e["steps"], limit, e["stop"], e["target"]]
                wants.append((magic, "TRADE_ACTION_PENDING"))
            elif kind == "fill":
                pending[magic][0] -= e["steps"]
                if not pending[magic][0]:
                    del pending[magic]
                held[magic] = [held.get(magic, [0])[0]+e["steps"], e["stop"], e["target"]]
                basis[magic] = basis.get(magic, 0) + e["steps"]*e["price"]
                fills.append((magic, "DEAL_ENTRY_IN", "DEAL_TYPE_BUY" if side == 1 else "DEAL_TYPE_SELL",
                              "DEAL_REASON_EXPERT", e["steps"], e["price"], -e["fee"], 0, 0, 0))
            elif kind == "exit":
                gross = side * (e["steps"]*e["price"]-basis[magic]) * policy["tick_cash"]
                del held[magic], basis[magic]
                fills.append((magic, "DEAL_ENTRY_OUT", "DEAL_TYPE_SELL" if side == 1 else "DEAL_TYPE_BUY",
                              "DEAL_REASON_SL" if e["reason"] == "stop" else "DEAL_REASON_TP",
                              e["steps"], e["price"], -e["fee"], 0, 0, gross))
            elif kind == "cancel":
                pending.pop(magic, None)
                wants.append((magic, "TRADE_ACTION_REMOVE"))
            elif kind == "management":
                held[magic][1:] = [e["stop"], e["target"]]
                wants.append((magic, "TRADE_ACTION_SLTP"))
        requests = snapshots["REQUEST"][key]
        check(key, "platform.requests", wants, [(stride*r["opened"]+r["slot"], r["action"]) for r in requests])
        for r in requests:
            magic = stride*r["opened"]+r["slot"]
            cycle = cycles[magic]
            check(key, "request.accepted", True, bool(r["sent"]) and r["retcode"] in (10008, 10009, 10010), cycle)
            if r["action"] == "TRADE_ACTION_PENDING":
                event = next((e for e in frame["events"] if e["event"] == "order" and e["cycle_id"] == cycle["cycle_id"]), None)
                if event:
                    side = cycle["side"]
                    check(key, "request.limit_order", ["ORDER_TYPE_BUY_LIMIT" if side == 1 else "ORDER_TYPE_SELL_LIMIT",
                          event["steps"], mark["ask"] if side == 1 else mark["bid"], event["stop"], event["target"]],
                          [r[v] for v in ("type", "steps", "price", "stop", "target")], cycle)
        actual_deals = per_tick.get(key[0], [])
        if previous_msc == key[0] and actual_deals:
            check(key, "deal.tick_sequence", "identifiable", "multiple_ticks_at_deal_millisecond")
        previous_msc = key[0]
        observed = [(m, r["entry"], r["type"], r["reason"], r["steps"], r["price"], r["commission"], r["fee"], r["swap"], r["profit"]) for m, r in actual_deals]
        expected_fills, observed_fills = _executions(fills), _executions(observed)
        differing = next((v[0] for v in expected_fills if v not in observed_fills), None)
        if differing is None:
            differing = next((v[0] for v in observed_fills if v not in expected_fills), None)
        check(key, "deal.execution", expected_fills, observed_fills, cycles.get(differing))
        active_orders = snapshots["ORDER"][key]
        active_positions = snapshots["POSITION"][key]
        for objects in (active_orders, active_positions):
            _tickets(objects)
        for r in active_orders:
            if r["magic"] in cycles:
                cycle = cycles[r["magic"]]
                check(key, "order.side", "ORDER_TYPE_BUY_LIMIT" if cycle["side"] == 1 else "ORDER_TYPE_SELL_LIMIT", r["type"], cycle)
        affected = {r["magic"] for r in active_positions + active_orders}
        affected.update(by_id[e["cycle_id"]]["magic"] for e in frame["events"] if e["cycle_id"] is not None)
        for magic in affected.intersection(cycles):
            cycle = cycles[magic]
            cycle["snapshots"].append({"key": list(key), "bid": mark["bid"], "ask": mark["ask"],
                "model": {k: v for k, v in mark.items() if k not in ("key", "bid", "ask")}, "account": account,
                "orders": [r for r in active_orders if r["magic"] == magic],
                "positions": [dict(r, basis=str(r["basis"])) for r in active_positions if r["magic"] == magic]})
        check(key, "account.object_counts", [account["orders"], account["positions"]], [len(active_orders), len(active_positions)])
        check(key, "order.pending", sorted((m, *v) for m, v in pending.items()),
              sorted((r["magic"], r["steps"], r["price"], r["stop"], r["target"]) for r in active_orders))
        check(key, "position.protected", sorted((m, *v) for m, v in held.items()),
              sorted((r["magic"], r["steps"], r["stop"], r["target"]) for r in active_positions))
        for r in active_positions:
            m = r["magic"]
            if m in held:
                check(key, "position.side", "POSITION_TYPE_BUY" if cycles[m]["side"]==1 else "POSITION_TYPE_SELL",r["type"],cycles[m])
                expected_basis = unit*basis[m]
                observed_basis = r["basis"]*r["steps"]
                if observed_basis != expected_basis:
                    check(key, "position.price_times_steps", str(expected_basis), str(observed_basis), cycles[m])
        for field in ("margin", "cash", "equity"):
            check(key, "account."+field, mark[field], account[field])
    volume_by_order = defaultdict(int)
    for d in deals.values():
        volume_by_order[d["order"]] += d["steps"]
    for ticket, order in history.items():
        cycle = cycles.get(order_magic.get(ticket))
        observed_steps = volume_by_order[ticket]
        key = tick_keys.get(order["done_msc"], keys[-1])
        if observed_steps:
            check(key, "history.fill_state", True, order["state"] in ("ORDER_STATE_FILLED", "ORDER_STATE_CANCELED", "ORDER_STATE_EXPIRED")
                  and 0 < observed_steps <= order["initial"], cycle)
        if order["state"] == "ORDER_STATE_FILLED":
            check(key, "history.filled_volume", order["initial"], observed_steps, cycle)
    submitted = {r["result_order"] for r in rows["REQUEST"] if r["action"] == "TRADE_ACTION_PENDING"}
    linked = submitted.union(d["order"] for d in deals.values())
    check(keys[-1], "history.extra_orders", [], sorted(history.keys() - linked))
    for magic, key in entry_orders.items():
        cycle = cycles[magic]
        for request in cycle["requests"]:
            request_key = (request["msc"], request["seq"])
            if request["action"] == "TRADE_ACTION_PENDING":
                ticket = request["result_order"]
                remains = ticket in {o["ticket"] for o in snapshots["ORDER"][keys[-1]]}
                if ticket in history:
                    order = history[ticket]
                    check(request_key, "history.request_binding", [magic, request["type"], request["steps"]],
                          [order["magic"], order["type"], order["initial"]], cycle)
                check(request_key, "history.order_ticket", True, ticket > 0 and (ticket in history or remains), cycle)
            if request["action"] == "TRADE_ACTION_REMOVE":
                ticket = request["order"]
                canceled = history.get(ticket, {})
                check(request_key, "history.cancel_confirmed", True, ticket > 0 and order_magic.get(ticket) == magic
                      and canceled.get("state") == "ORDER_STATE_CANCELED", cycle)
            if request["action"] == "TRADE_ACTION_SLTP":
                applied = any(p["ticket"] == request["position"] and p["magic"] == magic and p["stop"] == request["stop"]
                              and p["target"] == request["target"] for p in snapshots["POSITION"][request_key])
                check(request_key, "position.protection_applied", True, applied, cycle)
            check(request_key, "transaction.request", True, request["request_id"] > 0 and any(t["type"]=="TRADE_TRANSACTION_REQUEST" and t["request_id"]==request["request_id"]
                  and t["retcode"]==request["retcode"] for t in cycle["transactions"]), cycle)
        for d in cycle["deals"]:
            deal_key = tick_keys.get(d["msc"], keys[-1])
            check(deal_key, "deal.ticket_links", True, d["position_id"]>0 and d["order"] in history and
                  history[d["order"]]["position_id"] == d["position_id"] and
                  any(t["type"]=="TRADE_TRANSACTION_DEAL_ADD" and t["deal"]==d["ticket"] and t["order"]==d["order"] for t in cycle["transactions"]),cycle)
            if d["msc"] not in tick_keys:
                check(keys[-1], "deal.outside_sample", None, d["ticket"],cycle)
    check(keys[-1], "history.unbound", [], unbound)
    confirmed = {r["deal"] for r in rows["TX"] if r["type"] == "TRADE_TRANSACTION_DEAL_ADD"}
    check(keys[-1], "transaction.extra_deals", [], sorted(confirmed - deals.keys()))
    check(keys[-1], "final.cash", frames[-1]["mark"]["cash"], end["cash"])
    check(keys[-1], "final.equity", frames[-1]["mark"]["equity"], end["equity"])
    if first:
        first["diagnosis"] = {
            "deal.execution": "Conferir a liquidez por tick, o tipo de negócio, o preço e os custos do Tester.",
            "account.margin": "Conferir a reserva de pendentes e o cálculo de margem da plataforma.",
            "platform.veto": "A recusa e a modalidade da conta constam no journal físico.",
            "request.accepted": "A solicitação foi recusada ou ficou incerta. Consultar check, retcode e Safety.",
        }.get(first["field"], "Conferir o registro do agente no tick indicado.")
    no_orders = None
    if not history and not rows["ORDER"] and not deals:
        no_orders = "no_confirmed_signal" if not cycles else "no_entry_reserved" if not entry_orders else "no_platform_order_observed"
    if not slots and no_orders:
        no_orders = "no_candidate"
    return {"status": "diverged" if first else "platform_orders_match", "no_order_reason": no_orders,
            "native_order_parity": "diverged" if first else "journal_matches",
            "first_divergence": first, "platform_account": head, "platform_final": end,
            "tolerance": {"ticks": 0, "volume_steps": 0, "currency_cents": 0},
            "cycles": list(cycles.values()), "unbound_history": unbound,
            "policy_limits": {key: policy[key] for key in ("cash", "currency", "risk_limit", "margin", "fee", "tick_cash")},
            "global_events": [e for f in frames for e in f["events"] if e["cycle_id"] is None]}


def compare_report(package, comparison, html):
    """Confere a tabela de negócios do HTML do Tester contra o journal do agente."""
    from html import unescape
    from ..robots.backtest import _CELL, _DEAL_COLUMNS, _ROW, _TAG, _TIME

    def number(value):
        value = "".join(value.split())
        if "." in value and "," in value:
            value = value.replace("," if value.rfind(".") > value.rfind(",") else ".", "")
        return Decimal((value or "0").replace(",", "."))

    def difference(field, expected, actual, ticket=None):
        return {"status": "diverged", "field": field, "expected": str(expected), "observed": str(actual), "deal": ticket}

    trades = {d["ticket"]: d for c in comparison["cycles"] for d in c["deals"]}
    if comparison.get("unbound_history"):
        return {"status": "unbound_agent_history"}
    head = comparison["platform_account"]
    initial = None
    observed = set()
    balance = None
    for row in _ROW.findall(html):
        cells = [unescape(_TAG.sub("", c)).strip() for c in _CELL.findall(row)]
        if len(cells) != _DEAL_COLUMNS or not _TIME.fullmatch(cells[0]):
            continue
        kind = cells[3].lower()
        if kind == "balance" and initial is None and balance is None:
            initial = number(cells[10])*100
            balance = number(cells[11])*100
            if initial != head["cash"] or balance != initial:
                return difference("deposit", head["cash"], (initial, balance))
            continue
        if not cells[1].isdigit() or initial is None:
            return difference("deal_identity", "deposit_then_ticket", cells[1])
        ticket = int(cells[1])
        if ticket not in trades or ticket in observed:
            return difference("deal_ticket", "one_agent_deal", ticket, ticket)
        d = trades[ticket]
        observed.add(ticket)
        fields = {"symbol": (head["symbol"], cells[2]), "type": (d["type"].removeprefix("DEAL_TYPE_").lower(), kind),
                  "direction": (d["entry"].removeprefix("DEAL_ENTRY_").lower(), cells[4].lower()),
                  "order": (str(d["order"]), cells[7]),
                  "volume": (d["steps"]*Decimal(str(package["specification"]["volume_step"])), number(cells[5])),
                  "price": (d["price"]*Decimal(str(package["specification"]["tick_size"])), number(cells[6])),
                  "commission_and_fee": (d["commission"]+d["fee"], number(cells[8])*100),
                  "swap": (d["swap"], number(cells[9])*100), "profit": (d["profit"], number(cells[10])*100)}
        balance += d["commission"]+d["fee"]+d["swap"]+d["profit"]
        fields["cash"] = (balance, number(cells[11])*100)
        for field, (expected, actual) in fields.items():
            if expected != actual:
                return difference(field, expected, actual, ticket)
    if initial is None:
        return {"status": "deal_table_unavailable"}
    if observed != trades.keys():
        return difference("missing_deals", sorted(trades.keys()-observed), sorted(observed))
    if balance != comparison["platform_final"]["cash"]:
        return difference("closing_cash", comparison["platform_final"]["cash"], balance)
    return {"status": "report_matches", "deals": len(observed), "cash": int(balance)}
