"""Referência independente: saldo dos fills e último encerramento total conhecido."""
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
import re
from ..robots import mql
from .session_events import EVENT as SESSION_EVENT

EVENT = re.compile(r"HDK_COOLDOWN event_at=(\d+) at_msc=(\d+) direction=(-?\d+) last_close_msc=(\d+) seconds=(\d+) allowed=([01]) reason=(\w+)")


def read_events(directory, filters, signals, deals):
    text = "\n".join(mql.read_text(p) for p in (Path(directory)/"Tester").glob("Agent-*/logs/*.log"))
    selected = [row for row in filters if row["mode"] == "cooldown_v1"]
    seconds = selected[0]["params"]["seconds"] if selected else 0
    matches = list(EVENT.finditer(text))
    if len(matches) != text.count("HDK_COOLDOWN event_at="):
        raise ValueError("Evento de cooldown truncado/inválido; preserve a sessão.")
    if not seconds:
        if matches:raise ValueError("Cooldown registrado sem política carregada.")
        return []
    expected_origins = {int(m.group(2)) for m in SESSION_EVENT.finditer(text) if m.group(1)=="check" and m.group(10)=="1"}
    by_origin = {row["event_at"]:row for row in signals}
    rows, origins = [], set()
    for match in matches:
        origin, at, direction, last, duration, allowed, reason = match.groups()
        origin, at, direction, last, duration = map(int,(origin,at,direction,last,duration))
        if origin in origins or origin not in expected_origins or direction not in (-1,1) or at < origin*1000 or duration != seconds:
            raise ValueError("Cooldown sem contexto/identidade ou repetido.")
        if rows and at < rows[-1]["at_msc"]:raise ValueError("Relógio do cooldown regrediu.")
        signal = by_origin.get(origin)
        own = next((d["position"] for d in deals if signal and d["deal"]==signal["deal_ticket"]),None)
        grouped = defaultdict(list)
        for deal in deals:
            if deal["at_msc"] <= at and deal["position"] != own:grouped[deal["position"]].append(deal)
        closed = []
        for fills in grouped.values():
            entering = sum((Decimal(str(d["volume"])) for d in fills if d["entry"]==0),Decimal(0))
            exiting = sum((Decimal(str(d["volume"])) for d in fills if d["entry"]==1),Decimal(0))
            if entering<=0 or exiting>entering:raise ValueError("Histórico de saldo indisponível para cooldown.")
            if exiting==entering:closed.append(max(d["at_msc"] for d in fills if d["entry"]==1))
        expected_last = max(closed,default=0)
        expected_reason = "cooldown" if expected_last and at < expected_last+seconds*1000 else "allowed"
        if last != expected_last or reason != expected_reason or (allowed=="1") != (reason=="allowed"):
            raise ValueError("Cooldown diverge da referência independente de fechamento total e milissegundos.")
        if (allowed=="1") != (signal is not None) or (signal is not None and signal["direction"]!=direction):
            raise ValueError("Tentativa enviada após recusa ou decisão sem tentativa correspondente.")
        rows.append(dict(event_at=origin,at_msc=at,direction=direction,last_close_msc=last,
                         seconds=seconds,allowed=allowed=="1",reason=reason))
        origins.add(origin)
    if origins != expected_origins:raise ValueError("Faltam decisões de cooldown no recibo nativo.")
    return rows
