"""Concilia o journal de um Expert de carteira com a fita e o relatório do Tester."""
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from hashlib import file_digest
from html import unescape
from itertools import groupby, zip_longest
import re
import shutil

from ..analytics import hypotheses, pattern_engine
from ..robots.backtest import _CELL, _ROW, _TAG, _TIME
from ..robots.ultra_market_portfolio import MARKER
from . import ultra_market
from .ultra_platform import compare_report


_WIDTHS = {"OWNER": 4, "RULES": 8, "ACCOUNT": 4, "BAR": 12, "ENTRY": 10, "PEAK": 3,
           "PROTECT": 10, "CLOSE": 10, "ORDER": 11, "DEAL": 16, "FINAL": 11,
           "PLAN": 11, "RESPONSE": 7, "DEAL_EVENT": 3, "BOUNDARY": 4,
           "PRESET": 5, "INPUTS": 3, "RAW": 14, "RATE": 7}


def _records(journal, *, management_order=None):
    rows = defaultdict(list)
    for line in journal.splitlines():
        if MARKER not in line:
            continue
        kind, _, payload = line.partition(MARKER)[2].partition("|")
        width = _WIDTHS.get(kind, 0)
        values = payload.split("|", width - 1)
        if len(values) != _WIDTHS.get(kind):
            raise ValueError(f"Registro de carteira inválido em {kind}.")
        rows[kind].append(values)
        if management_order is not None and kind in ("PROTECT", "CLOSE"):
            management_order.append((kind, values))
    return rows


def _atoms(value, quantum):
    atoms = Decimal(value) / quantum
    if not atoms.is_finite() or atoms != atoms.to_integral_value():
        raise ValueError("Preço, volume ou custo fora da unidade nativa do Tester.")
    return int(atoms)


def _deals(rows, tick, step):
    for row in rows:
        ticket, owner, position, order, msc, entry, side, cause, volume, price, sl, tp, *costs = row
        yield dict(ticket=int(ticket), owner=int(owner), position=int(position), order=int(order), msc=int(msc),
                   entry=entry, type=side, cause=cause, steps=_atoms(volume, step), price=_atoms(price, tick),
                   sl=_atoms(sl, tick), tp=_atoms(tp, tick),
                   **dict(zip(("commission", "fee", "swap", "profit"), (_atoms(v, Decimal("0.01")) for v in costs))))


def _journal(common, output, name, checksum):
    if not re.fullmatch(r"HDKPR2\.[0-9A-Fa-f]+_RESERVATIONS\.state", name) or not re.fullmatch(r"[a-f0-9]{64}", checksum):
        return None
    origin = common / name
    if not origin.is_file() or origin.resolve().parent != common.resolve():
        return None
    target = output / "portfolio.state"
    shutil.copyfile(origin, target)
    with target.open("rb") as stream:
        sha = file_digest(stream, "sha256").hexdigest()
    return dict(file=target.name, sha256=sha, matches=sha == checksum)


def _pattern_comparison(snapshot, manifest, policy, journal, tick):
    definition = hypotheses.compile_definition(policy["definition"])
    symbol = manifest["symbol"]
    namespace = manifest["manifest_hash"]
    state = pattern_engine.initial_state(definition, namespace, symbol)

    def axis(msc):
        # O avaliador usa timestamps rotulados. Aqui são coordenadas do eixo MT5,
        # com uma transformação numérica reversível e sem declarar o fuso histórico.
        return datetime.fromtimestamp(msc / 1000, timezone.utc).isoformat()

    def grid_price(price):
        atoms = ultra_market._price_atoms(price, tick)
        if atoms is None:
            raise ValueError("Cotação observada fora da grade de preço da hipótese.")
        return str(atoms * tick)

    previous, previous_end = None, None
    expected = []
    for minute, quotes in groupby(ultra_market._csv_rows(snapshot, manifest), key=lambda r: r[0] // 60000):
        first = next(quotes)
        if previous is not None:
            state, event = pattern_engine.evaluate_prefix(definition, state, previous)
            if event:
                result = event.get("entry_decision", {}).get("status", "confirmed")
                invalid = {"confirmed": 0, "invalidated": 1, "unknown": -1}[result] if policy["apply_invalidation"] else 0
                expected.append((first[0], previous_end, 1 if event["side"] == "long" else -1, invalid,
                                 _atoms(event["signal_values"]["reference"], tick)))
        opening = closing = high = low = first[1]
        for _, bid, *_ in quotes:
            high, low, closing = max(high, bid), min(low, bid), bid
        end_msc = (minute + 1) * 60000
        previous = dict(source_namespace=namespace, symbol=symbol, timeframe="M1", revision=1, session_id=None,
                        bar_start_utc=axis(minute * 60000), bar_end_utc=axis(end_msc), available_at_utc=axis(end_msc),
                        open=grid_price(opening), high=grid_price(high), low=grid_price(low), close=grid_price(closing))
        # A coordenada inteira fica separada da observação validada pelo motor.
        previous_end = end_msc
    marker = "MT5X_ULTRA_MARKET_PATTERN|"
    observed = [tuple(map(int, line.partition(marker)[2].split("|")))
                for line in journal.splitlines() if marker in line]
    divergence = next((dict(ordinal=i, expected=pair[0], observed=pair[1])
                       for i, pair in enumerate(zip_longest(expected, observed)) if pair[0] != pair[1]), None)
    return dict(definition_hash=definition.definition_hash, expected=len(expected), observed=len(observed),
                invalidated=sum(row[3] == 1 for row in observed), first_divergence=divergence)


def _report_orders(html, orders, symbol, tick, step):
    def atoms(text, unit):
        return _atoms(text.replace(",", ".") or "0", unit)

    matched = set()
    for row in _ROW.findall(html):
        cells = [unescape(_TAG.sub("", cell)).strip() for cell in _CELL.findall(row)]
        if len(cells) != 11 or not _TIME.fullmatch(cells[0]) or not _TIME.fullmatch(cells[8]):
            continue
        ticket = int(cells[1])
        if ticket in matched or ticket not in orders:
            return dict(status="diverged", ticket=ticket, field="ticket")
        _, _, _, msc, kind, state, initial, remaining, price, stop, target = orders[ticket]
        initial, remaining = _atoms(initial, step), _atoms(remaining, step)
        when = datetime.fromtimestamp(int(msc) // 1000, timezone.utc).strftime("%Y.%m.%d %H:%M:%S")
        values = dict(symbol=(symbol, cells[2]), order_type=({"0": "buy", "1": "sell"}[kind], cells[3].lower()),
                      volume=((initial, initial - remaining), tuple(atoms(q, step) for q in cells[4].split("/"))),
                      price=(_atoms(price, tick), atoms(cells[5], tick)), sl=(_atoms(stop, tick), atoms(cells[6], tick)),
                      tp=(_atoms(target, tick), atoms(cells[7], tick)), done=(when, cells[8]),
                      state=("filled" if state == "4" else state, cells[9].lower()))
        for field, (expected, observed) in values.items():
            if expected != observed:
                return dict(status="diverged", ticket=ticket, field=field, expected=expected, observed=observed)
        matched.add(ticket)
    if matched != orders.keys():
        return dict(status="diverged", field="missing_orders", tickets=sorted(orders.keys() - matched))
    return dict(status="report_matches", orders=len(matched))


def compare(bundle, tape, agent_journal, tester_html, common, output, *, snapshot=None, manifest=None):
    management_order = []
    rows = _records(agent_journal, management_order=management_order)
    if len(rows["ACCOUNT"]) != 1 or len(rows["FINAL"]) != 1 or rows["FINAL"][0][0] != bundle["binding"]:
        raise ValueError("A conta e o fechamento do journal não confirmam a mesma tentativa.")
    issues = []

    def check(condition, code):
        if not condition and code not in issues:
            issues.append(code)

    symbol, deposit, tick_size, volume_step = rows["ACCOUNT"][0]
    tick, step, cent = Decimal(tick_size), Decimal(volume_step), Decimal("0.01")
    _, cash, equity, positions, pending, peak, settled, audited, journal, checksum, reason = rows["FINAL"][0]
    final = dict(cash=_atoms(cash, cent), equity=_atoms(equity, cent), open_positions=int(positions),
                 pending_orders=int(pending), settled=settled == "1", audit_confirmed=audited == "1", reason=reason)
    state_file = _journal(common, output, journal, checksum) if final["audit_confirmed"] else None
    check(state_file and state_file["matches"], "portfolio_journal")
    check(tape["quotes_exact"] and tape["account_margin_mode"] == 2, "real_tick_stream")
    check(symbol == bundle["symbol"] and _atoms(deposit, cent) == 100000 and tick > 0 and step > 0, "account_contract")
    expected = {str(row["magic"]): row["key"] for row in bundle["slots"]}
    if "preset_sha256" in bundle:
        check(rows["PRESET"] == [[bundle["preset_sha256"], str(bundle["preset_version"]), bundle["manifest_hash"],
                                  bundle["source_closure_sha256"], bundle["binary_sha256"]]], "preset_binding")
        check(rows["INPUTS"] == [[str(row["magic"]), row["preset_sha256"], row["inputs_sha256"]]
                                 for row in bundle["slots"]], "module_inputs")
    pattern = next((row["pattern"] for row in bundle["slots"] if "pattern" in row), None)
    pattern_result = None
    if pattern is not None:
        if snapshot is None or manifest is None:
            raise ValueError("A conciliação da hipótese exige a fita de mercado congelada.")
        pattern_result = _pattern_comparison(snapshot, manifest, pattern, agent_journal, tick)
        check(pattern_result["first_divergence"] is None, "python_pattern_parity")
    check([(magic, key, bound) for magic, key, bound, _ in rows["OWNER"]]
          == [(magic, key, "1") for magic, key in expected.items()], "owner_bindings")
    rules = {r[0]: r[1:] for r in rows["RULES"]}
    check(rules.keys() == expected.keys() and len(rows["RULES"]) == len(expected), "owner_rules")
    decisions, signals, seen_bars = Counter(), Counter(), defaultdict(set)
    for magic, msc, bar, closed, buy, sell, *_ in rows["BAR"]:
        check(magic in expected and int(closed) < int(bar) <= int(msc) // 1000
              and bar not in seen_bars[magic], "closed_bar_causality")
        decisions[magic] += 1
        signals[magic] += buy == "1" or sell == "1"
        seen_bars[magic].add(bar)
    check(decisions.keys() == expected.keys(), "all_owners_evaluated")
    if "preset_sha256" in bundle:
        check([raw[:3] + raw[4:5] for raw in rows["RAW"]] == [bar[:4] for bar in rows["BAR"]], "raw_decision_coverage")
        rates = [int(rate[0]) for rate in rows["RATE"]]
        check(bool(rates) and rates == sorted(set(rates)), "decision_rate_order")
    raw_signals = Counter(row[0] for row in rows["RAW"] if "1" in row[6:8])
    observed_orders = {int(o[0]): o for o in rows["ORDER"]}
    deals = list(_deals(rows["DEAL"], tick, step))
    if window := bundle.get("window"):
        check(rows["BOUNDARY"] == [[str(window["last_msc"]), str(window["ticks"]), "0", "0"]]
              and all(window["start_msc"] <= d["msc"] <= window["last_msc"] for d in deals), "window_settlement")
    deal_by_ticket = {d["ticket"]: d for d in deals}
    check(len(observed_orders) == len(rows["ORDER"]) and len(deal_by_ticket) == len(deals), "unique_tickets")
    observed_entries = Counter()
    plans = {(row[0], row[1], row[2]): row[3:] for row in rows["PLAN"]}
    responses = {row[4]: row for row in rows["RESPONSE"] if row[4] != "0"}
    deal_events = {row[0]: row[1:] for row in rows["DEAL_EVENT"]}
    openings = defaultdict(Counter)
    refusals = defaultdict(Counter)
    for magic, msc, direction, state, attempts, retcode, order, deal, qty, entry_reason in rows["ENTRY"]:
        check(magic in expected and int(attempts) in (0, 1), "entry_scope")
        if state in ("4", "5"):
            fill = deal_by_ticket.get(int(deal))
            check(fill and fill["entry"] == "DEAL_ENTRY_IN" and fill["order"] == int(order)
                  and str(fill["owner"]) == magic and fill["type"] == ("DEAL_TYPE_BUY" if direction == "0" else "DEAL_TYPE_SELL")
                  and int(msc) == fill["msc"] and fill["steps"] <= _atoms(qty, step), "observed_fill")
            plan = plans.get((magic, msc, direction))
            response = responses.get(deal)
            check(plan and response and response[0] == magic and response[3] == order and response[6] == retcode
                  and _atoms(plan[0], step) == _atoms(qty, step) and Decimal(plan[-1]) > 0, "plan_and_mt5_response")
            observed_entries[int(deal)] += 1
        else:
            refusals[magic][entry_reason or retcode] += 1
    closed_deals = defaultdict(list)
    active, sides, owners = {}, {}, {}
    replay_peak, partials = 0, Counter()
    for deal in sorted(deals, key=lambda d: (d["msc"], d["ticket"])):
        position, magic = deal["position"], str(deal["owner"])
        order = observed_orders.get(deal["order"])
        event = deal_events.get(str(deal["ticket"]))
        check(event and event[0] == str(deal["order"]), "deal_notification")
        is_buy = deal["type"] == "DEAL_TYPE_BUY"
        check(deal["steps"] > 0 and position > 0 and magic in expected
              and deal["type"] in ("DEAL_TYPE_BUY", "DEAL_TYPE_SELL"), "owned_deal")
        check(order and order[1:3] == [magic, str(position)] and int(order[4]) == (0 if is_buy else 1)
              and order[5] == "4" and _atoms(order[6], step) >= deal["steps"] and _atoms(order[7], step) == 0,
              "order_deal_link")
        if deal["entry"] == "DEAL_ENTRY_IN":
            check(observed_entries[deal["ticket"]] == 1, "entry_request_link")
            check(deal["sl"] > 0 and (deal["sl"] < deal["price"] if is_buy else deal["sl"] > deal["price"])
                  and (deal["tp"] == 0 or (deal["tp"] > deal["price"] if is_buy else deal["tp"] < deal["price"]))
                  and order and _atoms(order[9], tick) == deal["sl"] and _atoms(order[10], tick) == deal["tp"],
                  "initial_stop_target")
            check(position not in owners or (owners[position] == magic and sides[position] == is_buy), "position_owner")
            active[position] = active.get(position, 0) + deal["steps"]
            owners[position], sides[position] = magic, is_buy
            openings[magic]["deals"] += 1
            replay_peak = max(replay_peak, len(active))
        else:
            check(deal["entry"] == "DEAL_ENTRY_OUT" and position in active and owners.get(position) == magic
                  and sides.get(position) != is_buy and active.get(position, 0) >= deal["steps"], "close_position_link")
            if position in active:
                active[position] -= deal["steps"]
                if active[position] == 0:
                    del active[position]
                else:
                    partials[magic] += 1
            closed_deals[position].append(deal)
            openings[magic][deal["cause"]] += 1
    check({d["order"] for d in deals} == observed_orders.keys(), "all_orders_accounted")
    protects, closes = defaultdict(Counter), defaultdict(Counter)
    protections = defaultdict(list)
    management = defaultdict(list)
    for record_type, row in management_order:
        magic, msc, position, kind, before, requested, after, ok, recorded, reason = row
        at, owner_position = int(msc), int(position)
        event = dict(tick_msc=at, position=owner_position, action=kind, acknowledged=ok == "1",
                     recorded=recorded == "1", reason=reason)
        if record_type == "PROTECT":
            if event["acknowledged"]:
                side = sides.get(owner_position)
                check(event["recorded"] and owners.get(owner_position) == magic and side is not None
                      and _atoms(after, tick) > 0
                      and (Decimal(after) > Decimal(before) if side else Decimal(after) < Decimal(before)),
                      "confirmed_protection")
                protects[magic][kind] += 1
                protections[owner_position].append((at, _atoms(before, tick), _atoms(after, tick)))
            event.update(previous_sl=before, requested_sl=requested, observed_sl=after)
        else:
            if event["acknowledged"]:
                reduction = _atoms(before, step) - _atoms(after, step)
                out = closed_deals[owner_position]
                check(event["recorded"] and owners.get(owner_position) == magic and reduction > 0
                      and sum(d["steps"] for d in out if d["msc"] == at) == reduction, "confirmed_management_close")
                closes[magic][kind] += 1
            event.update(previous_volume=before, requested_volume=requested, observed_volume=after)
        management[magic].append(event)
    brackets = {}
    for deal in sorted(deals, key=lambda d: (d["msc"], d["ticket"])):
        position = deal["position"]
        if deal["entry"] == "DEAL_ENTRY_IN":
            brackets.setdefault(position, (deal["sl"], deal["tp"]))
        elif position in brackets and deal["cause"] in ("DEAL_REASON_SL", "DEAL_REASON_TP"):
            original_sl, target = brackets[position]
            effective = original_sl
            for applied, before, after in protections[position]:
                if applied > deal["msc"]:
                    break
                check(effective == before, "protection_chain")
                effective = after
            check(deal["sl"] == effective and deal["tp"] == target and
                  (effective > 0 if deal["cause"] == "DEAL_REASON_SL" else target > 0), "exit_bracket")
    check(replay_peak >= int(peak) and bool(rows["PEAK"]) == (int(peak) > 0), "simultaneous_positions")
    for _, count, magics in rows["PEAK"]:
        at_peak = magics.split(",")
        check(len(set(at_peak)) == int(count) and set(at_peak) <= expected.keys(), "concurrent_owners")
    check(final["settled"] and final["open_positions"] == final["pending_orders"] == 0 and not active, "settled_portfolio")
    cash_from_deals = _atoms(deposit, cent) + sum(sum(d[k] for k in ("commission", "fee", "swap", "profit")) for d in deals)
    check(cash_from_deals == final["cash"] == final["equity"], "cash_and_equity")
    report = compare_report({"specification": {"tick_size": str(tick), "volume_step": str(step)}},
                            {"cycles": [{"deals": deals}], "platform_account": {"symbol": symbol, "cash": _atoms(deposit, cent)},
                             "platform_final": final}, tester_html)
    check(report["status"] == "report_matches", "tester_report")
    report_orders = _report_orders(tester_html, observed_orders, symbol, tick, step)
    check(report_orders["status"] == "report_matches", "tester_report_orders")
    counts_by_owner = Counter(row[1] for row in observed_orders.values())
    owners_result = [dict(key=key, magic=int(magic), evaluated_bars=decisions[magic], signal_bars=signals[magic],
                          raw_signal_bars=raw_signals[magic] if rows["RAW"] else None, orders=counts_by_owner[magic],
                          rules=rules.get(magic), entries=openings[magic].pop("deals", 0),
                          exits=dict(openings[magic]), partials=partials[magic], refused=dict(refusals[magic]),
                          protections=dict(protects[magic]), managed_closes=dict(closes[magic]), management=management[magic])
                     for magic, key in expected.items()]
    status = ("portfolio_diverged" if issues else "portfolio_matches" if int(peak) >= 2 else
              "single_owner_matches" if len(expected) == 1 and deals else "portfolio_without_overlap")
    return dict(status=status, issues=issues,
                tick_comparison=tape, owners=owners_result, portfolio_state=state_file,
                totals=dict(orders=len(observed_orders), deals=len(deals), partials=sum(partials.values())),
                peak=dict(on_tick=int(peak), chronological=replay_peak, samples=rows["PEAK"]),
                final=final, report_comparison=report, report_orders=report_orders, deals=deals, pattern_parity=pattern_result,
                economic_approval=False,
                observations=dict(plans=len(plans), responses=len(rows["RESPONSE"]), deal_events=len(deal_events),
                                  preset=rows["PRESET"], raw_decisions=len(rows["RAW"]), decision_rates=len(rows["RATE"]),
                                  boundary=rows["BOUNDARY"]),
                units=dict(tick_size=str(tick), volume_step=str(step), cash_unit="0.01 USD"))
