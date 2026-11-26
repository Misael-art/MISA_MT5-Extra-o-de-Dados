"""Controle técnico S09 sobre uma fita de mercado com bid, ask e relógio original."""
from decimal import Decimal
from itertools import groupby, zip_longest
import re
from pathlib import Path
from string import Template

import pandas as pd

from ..experiments.schema import digest
from ..strategies import get_strategy, indicators
from . import ultra_market as market
from .ultra_platform import compare_report


PROFILE = dict(fast=5, slow=12, warmup=30, stop=20, target=30, spread=5,
               lot="0.01", risk="0.005", capital=100000)
MARKER = "MT5X_ULTRA_MARKET_CYCLE|"
DEAL_FIELDS = ("ticket", "msc", "order", "position_id", "entry_code", "type_code", "reason_code",
               "price", "steps", "profit", "commission", "swap", "fee")
EPSILON = 1e-12


def export(source, destination, *, attempt):
    manifest = market.verify(source)
    instrument = manifest["contract"]["current"]
    if (manifest["symbol"] != "EURUSD" or manifest["source"]["server"] != "MetaQuotes-Demo"
            or manifest["account"]["currency"] != "USD" or instrument["trade_tick_size"] != 0.00001
            or instrument["volume_min"] != 0.01 or instrument["volume_step"] != 0.01
            or not re.fullmatch(r"[a-f0-9]{32}", attempt)):
        raise ValueError("O controle S09 exige o contrato técnico congelado da DEMO MetaQuotes-Demo EURUSD.")
    binding = digest([manifest["manifest_hash"], PROFILE, attempt])
    source_file = Path(__file__).resolve().parents[1] / "robots" / "templates" / "UltraMarketCycle.mq5.tmpl"
    content = Template(source_file.read_text(encoding="utf-8")).substitute(**PROFILE,
        manifest=manifest["manifest_hash"], binding=binding, symbol=manifest["symbol"],
        tick_size="0.00001", start=manifest["time"]["start_msc"], end=manifest["time"]["end_msc"])
    target = Path(destination)
    with target.open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(content)
    return dict(symbol=manifest["symbol"], manifest_hash=manifest["manifest_hash"], binding=binding,
                source_sha256=market._sha(target), tester_model="4", profile=PROFILE.copy())


def expected(source, manifest):
    axis = []
    ticks = enumerate(market._csv_rows(source, manifest))
    for minute, group in groupby(ticks, lambda item: item[1][0] // 60000):
        offset, first = next(group)
        last = first
        low = high = first[1]
        for _, last in group:
            low, high = min(low, last[1]), max(high, last[1])
        axis.append((minute, offset, first, last, low, high))
    if len(axis) < PROFILE["warmup"] + 1 or any(a[0] + 1 != b[0] for a, b in zip(axis, axis[1:])):
        raise ValueError("O controle exige um prefixo de pelo menos 31 minutos consecutivos.")
    frame = pd.DataFrame(dict(time=pd.to_datetime([row[0] * 60 for row in axis], unit="s"),
                              open=[row[2][1] for row in axis], close=[row[3][1] for row in axis],
                              low=[row[4] for row in axis], high=[row[5] for row in axis]))
    strategy = get_strategy("s09_reference", fast=PROFILE["fast"], slow=PROFILE["slow"],
                            warmup_bars=PROFILE["warmup"], stop_points=PROFILE["stop"],
                            target_points=PROFILE["target"])
    signals = strategy.gerar_sinais(frame)
    fast = indicators.ema(frame.close, PROFILE["fast"])
    slow = indicators.ema(frame.close, PROFILE["slow"])
    unit = Decimal(str(manifest["contract"]["current"]["trade_tick_size"]))
    bars, entry, exit_, entry_offset = [], None, None, None
    for i, (minute, offset, first, *_ohlc) in enumerate(axis[1:]):
        direction = int(signals.entry_long.iloc[i]) - int(signals.entry_short.iloc[i])
        bid = market._price_atoms(frame.close.iloc[i], unit)
        bars.append((minute * 60000, first[0], bid, fast.iloc[i], slow.iloc[i], direction))
        if direction and entry is None:
            price = market._price_atoms(first[2 if direction > 0 else 1], unit)
            if price is None:
                raise ValueError("Preço de entrada fora da grade congelada.")
            entry = dict(msc=first[0], side=direction, price=price, stop=price-direction*PROFILE["stop"],
                         target=price+direction*PROFILE["target"], steps=1)
            entry_offset = offset
    if entry:
        for i, row in enumerate(market._csv_rows(source, manifest)):
            if i <= entry_offset:
                continue
            liquidation = market._price_atoms(row[1 if entry["side"] > 0 else 2], unit)
            if liquidation is None:
                raise ValueError("Preço executável fora da grade do controle.")
            side = entry["side"]
            stop = side * (liquidation - entry["stop"]) <= 0
            target = side * (liquidation - entry["target"]) >= 0
            if stop or target:
                exit_ = dict(msc=row[0], price=liquidation, reason="SL" if stop else "TP", steps=1,
                             profit=side*(liquidation - entry["price"]))
                break
    return bars, entry, exit_


def compare(source, *, binding, agent_journal, tester_html):
    source = Path(source)
    manifest = market.verify(source)
    rows = {}
    for line in agent_journal.splitlines():
        if MARKER in line:
            kind, *values = line.partition(MARKER)[2].split("|")
            rows.setdefault(kind, []).append(values)
    begin = rows.get("BEGIN", [])
    end = rows.get("END", [])
    widths = {"BEGIN": 8, "BAR": 6, "RISK": 6, "REQUEST": 11, "POSITION": 6, "DEAL": 13, "END": 7}
    if (len(begin) != 1 or len(end) != 1
            or any(len(fields) != widths.get(kind) for kind, records in rows.items() for fields in records)
            or begin[0][:3] != [manifest["manifest_hash"], binding, "EURUSD"] or end[0][0] != binding):
        raise ValueError("O journal não vincula um ciclo do agente ao manifesto e à tentativa.")
    bars, entry, exit_ = expected(source, manifest)
    actual = rows.get("BAR", [])
    first = None

    def check(field, want, got, *, tick=None, same=None):
        nonlocal first
        if first is None and not (want == got if same is None else same):
            first = dict(field=field, expected=want, observed=got, tick_msc=tick)

    bar_fields = ("closed_at", "first_tick", "close", "ema_fast", "ema_slow", "side")
    check("s09_profile", [PROFILE[k] for k in ("fast", "slow", "warmup", "stop")], list(map(int, begin[0][4:])))
    for want, observed in zip_longest(bars, actual):
        if want is None or observed is None:
            check("m1_rows", len(bars), len(actual))
            break
        for i, (field, a, b) in enumerate(zip(bar_fields, want, observed)):
            if i not in (3, 4):
                check(field, a, int(b), tick=want[1])
            elif pd.notna(a):
                check(field, a, float(b), tick=want[1], same=abs(a-float(b)) <= EPSILON)
    deals = [dict(zip(DEAL_FIELDS, map(int, v))) for v in rows.get("DEAL", [])]
    for deal in deals:
        deal["entry"] = {0:"DEAL_ENTRY_IN", 1:"DEAL_ENTRY_OUT"}.get(deal["entry_code"], "unknown")
        deal["type"] = {0:"DEAL_TYPE_BUY", 1:"DEAL_TYPE_SELL"}.get(deal["type_code"], "unknown")
    state = rows["END"][0]
    footer = dict(bars=int(state[1]), passed_end=bool(int(state[2])), failed=bool(int(state[3])),
                  cash=int(state[4]), equity=int(state[5]), open_positions=int(state[6]))
    request, position, risk = (rows.get(key, []) for key in ("REQUEST", "POSITION", "RISK"))
    report = compare_report({"specification": {"tick_size": 0.00001, "volume_step": 0.01}},
        {"cycles": [{"deals": deals}], "platform_account": {"symbol": "EURUSD", "cash": PROFILE["capital"]},
         "platform_final": footer}, tester_html)
    end_complete = bool(footer["passed_end"] and not footer["failed"] and footer["open_positions"] == 0)
    check("coverage", True, end_complete)
    check("observed_bars", len(bars), footer["bars"])
    if entry:
        check("request.count", 1, len(request), tick=entry["msc"])
        check("position.count", 1, len(position), tick=entry["msc"])
        check("risk.result", 1, int(risk[0][-1]) if len(risk) == 1 else None, tick=entry["msc"])
        if len(risk) == 1:
            observed = list(map(int, risk[0]))
            check("risk.budget", [entry["msc"], PROFILE["stop"], 500], observed[:3], tick=entry["msc"])
            check("risk.margin_admitted", True, 0 < observed[3] <= observed[4])
        if request:
            req = list(map(int, request[0]))
            check("request.market_plan", [entry[k] for k in ("msc", "side", "price", "stop", "target")], req[:5])
            check("request.fill", 10009, req[6])
            if deals:
                check("request.tickets", req[7:9], [deals[0]["order"], deals[0]["ticket"]])
        if position and deals:
            pos = list(map(int, position[0]))
            check("position.owner_and_protection", [deals[0]["position_id"], entry["price"], entry["stop"], entry["target"]],
                  pos[1:5])
            check("position.margin_positive", True, pos[5] > 0)
            if len(risk) == 1:
                check("position.initial_margin", int(risk[0][3]), pos[5])
        check("deals.count", 2 if exit_ else 1, len(deals))
        if deals:
            check("entry.fill", [entry["msc"], entry["price"], 1, 0, 0 if entry["side"] > 0 else 1],
                  [deals[0][k] for k in ("msc", "price", "steps", "entry_code", "type_code")], tick=entry["msc"])
        if exit_ and len(deals) == 2:
            check("exit.position", deals[0]["position_id"], deals[1]["position_id"])
            check("exit.fill", [exit_["msc"], exit_["price"], 1, 1, 1 if entry["side"] > 0 else 0, exit_["profit"]],
                  [deals[1][k] for k in ("msc", "price", "steps", "entry_code", "type_code", "profit")], tick=exit_["msc"])
            check("exit.reason", 4 if exit_["reason"] == "SL" else 5, deals[1]["reason_code"])
    else:
        check("unexpected_requests", [], request)
        check("unexpected_deals", [], deals)
    expected_cash = PROFILE["capital"] + (exit_["profit"] if exit_ else 0)
    for deal in deals:
        check("deal.costs", [0, 0, 0], [deal["commission"], deal["swap"], deal["fee"]], tick=deal["msc"])
    check("final.balance_and_equity", [expected_cash]*2, [footer["cash"], footer["equity"]])
    check("tester.report", "report_matches", report["status"])
    status = "diverged" if first else "cycle_matches" if exit_ else "no_complete_cycle"
    decisions = [dict(closed_at=int(closed), first_tick=int(msc), close=int(close),
        ema_fast=float(fast) if pd.notna(fast) else None, ema_slow=float(slow) if pd.notna(slow) else None,
        side=int(side)) for closed, msc, close, fast, slow, side in bars]
    return dict(status=status, manifest_hash=manifest["manifest_hash"], build=int(begin[0][3]), binding=binding,
                bars_compared=min(len(actual), len(bars)), expected_entry=entry, expected_exit=exit_,
                signals_count=sum(row["side"] != 0 for row in decisions), decisions=decisions,
                requests=request, positions=position, risk=risk, deals=deals, final=footer,
                report_comparison=report, first_divergence=first, economic_approval=False,
                tolerance=dict(ticks=0, volume_steps=0, cents=0, ema_absolute=EPSILON))
