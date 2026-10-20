"""Recibo independente de entradas/cota, confrontado com os fills observados.

Referência por datetime e conjuntos de posições; não chama o controlador/predicado
de produção e não converte o relógio do servidor em UTC real.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import re
from ..robots import mql

EVENT = re.compile(r"HDK_SESSION event=(check|fill) event_at=(\d+) at=(\d+) direction=(-?\d+) enabled=([01]) "
                   r"session_start=(\d+) weekday=(\d+) minute=(\d+) cycles=(-?\d+) allowed=([01]) reason=(\w+)")


def _context(at, anchor):
    # UTC serve somente como codificação aritmética independente do timezone do host.
    wall = datetime.fromtimestamp(at, timezone.utc).replace(tzinfo=None)
    day = (wall - timedelta(minutes=anchor)).date()
    start = datetime.combine(day, datetime.min.time()) + timedelta(minutes=anchor)
    return int(start.replace(tzinfo=timezone.utc).timestamp()), day.weekday(), wall.hour*60+wall.minute


def _reason(params, minute, weekday, cycles):
    def within(start, end):
        if start == end:
            return True
        if start < end:
            return minute in range(start, end)
        return minute >= start or minute < end
    if not (params["weekdays"] >> weekday) & 1:
        return "weekday"
    if not within(params["start_minute"], params["end_minute"]):
        return "outside_window"
    if params["exclude_start_minute"] != params["exclude_end_minute"] and within(
            params["exclude_start_minute"], params["exclude_end_minute"]):
        return "excluded_window"
    if params["max_cycles"] and cycles >= params["max_cycles"]:
        return "quota"
    return "allowed"


def read_events(directory, filters, signals, deals):
    text = "\n".join(mql.read_text(path) for path in (Path(directory)/"Tester").glob("Agent-*/logs/*.log"))
    matches = list(EVENT.finditer(text))
    if len(matches) != text.count("HDK_SESSION event="):
        raise ValueError("Evento de sessão truncado/inválido; preserve a sessão.")
    enabled = bool(filters)
    params = filters[0]["params"] if enabled else dict(anchor_minute=0)
    by_origin = {signal["event_at"]: signal for signal in signals}
    entries = [row for row in deals if row["entry"] == 0]
    rows, checks, fills = [], {}, {}
    for match in matches:
        event, origin, at, direction, active, session, weekday, minute, cycles, allowed, reason = match.groups()
        row = dict(event=event, event_at=int(origin), at=int(at), direction=int(direction), enabled=active=="1",
                   session_start=int(session), weekday=int(weekday), minute=int(minute), cycles=int(cycles),
                   allowed=allowed=="1", reason=reason)
        origin, at = row["event_at"], row["at"]
        expected_context = _context(at, params["anchor_minute"])
        if (row["enabled"] != enabled or row["direction"] not in (-1,1) or at < origin or
                (row["session_start"],row["weekday"],row["minute"]) != expected_context):
            raise ValueError("Sessão/relógio/direção do diário divergem do preset.")
        if rows and at < rows[-1]["at"]:
            raise ValueError("Relógio de sessão regrediu; não qualificar mudança de DST implicitamente.")
        if cycles is None or row["cycles"] < 0 or reason in ("history_unavailable","unsupported_history","clock_reversal"):
            raise ValueError("Contador de ciclos sem histórico/identidade verificável; aceite de sessão pendente.")
        signal = by_origin.get(origin)
        if signal is not None and signal["direction"] != row["direction"]:
            raise ValueError("Filtro e tentativa têm direções diferentes.")
        own_ticket = signal["deal_ticket"] if signal is not None and signal["opened"] else None
        if event == "check":
            if origin in checks:
                raise ValueError("Decisão de sessão repetida; não reaplicar intenção.")
            known = {entry["position"] for entry in entries if entry["at_msc"] < (at+1)*1000
                     and entry["deal"] != own_ticket and _context(entry["at_msc"]//1000,params["anchor_minute"])[0] == expected_context[0]}
            expected_cycles = len(known) if enabled else 0
            expected_reason = _reason(params,row["minute"],row["weekday"],expected_cycles) if enabled else "disabled"
            if (row["cycles"] != expected_cycles or reason != expected_reason or
                    row["allowed"] != (reason in ("allowed","disabled"))):
                raise ValueError("Dia/janela/cota divergem da referência independente e dos primeiros fills.")
            if row["allowed"] != (signal is not None):
                raise ValueError("Filtro sem tentativa correspondente ou entrada enviada após recusa de sessão.")
            checks[origin] = row
        else:
            if not enabled or origin in fills or origin not in checks or signal is None or not signal["opened"]:
                raise ValueError("Confirmação do ciclo sem entrada própria permitida.")
            own = [entry for entry in entries if entry["deal"] == own_ticket]
            if len(own) != 1 or own[0]["at_msc"]//1000 != at or not checks[origin]["allowed"]:
                raise ValueError("Ciclo não corresponde ao primeiro fill nativo.")
            known = {entry["position"] for entry in entries if entry["at_msc"] < (at+1)*1000
                     and _context(entry["at_msc"]//1000,params["anchor_minute"])[0] == expected_context[0]}
            if row["cycles"] != len(known) or not row["allowed"] or reason != "allowed":
                raise ValueError("Confirmação do contador não reconcilia com posições preenchidas.")
            if _reason(params,row["minute"],row["weekday"],row["cycles"]-1) != "allowed":
                raise ValueError("Fill cruzou fronteira/cota da sessão permitida; não aprovar por intenção anterior.")
            fills[origin] = row
        rows.append(row)
    if set(by_origin) - checks.keys() or (enabled and set(fills) != {s["event_at"] for s in signals if s["opened"]}):
        raise ValueError("Faltam decisões/primeiros fills no recibo de sessão.")
    return rows
