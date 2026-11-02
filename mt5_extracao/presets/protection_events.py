"""E14: decisões observadas → journal canônico → fills, sem executor adicional."""
from datetime import datetime, timezone
import hashlib
import math
from pathlib import Path
import re

from ..robots import mql


def _require(condition, message="Proteção/saída não reconcilia com preset, diário e fills; preserve a sessão."):
    if not condition:
        raise ValueError(message)


def _sha(text):
    return hashlib.sha256(text.encode("ascii")).hexdigest()


def _intent(context, key):
    return _sha(f"HDK_MANAGEMENT_INTENT1|{context}|{len(key)}:{key}")


def _close_intent(context, position):
    key = "bulk-"+_sha(f"HDK_MANAGEMENT_CHILD1|13:profile-close|5:close|{position}")
    return _intent(context,key)


def _number(text):
    try:
        value = float(text)
    except (ValueError, TypeError):
        raise ValueError("Número indisponível no evento de proteção.") from None
    _require(math.isfinite(value) and abs(value) != float.fromhex("0x1.fffffffffffffp+1023"))
    return value


def _integer(text):
    _require(isinstance(text,str) and re.fullmatch(r"0|[1-9]\d*",text))
    return int(text)


def _same(left, right):
    # Preços normalizados podem variar por ulps; nunca aceitar diferença de um tick.
    return math.isfinite(left) and math.isfinite(right) and abs(left-right) <= 8*max(math.ulp(left),math.ulp(right))


def _same_price(requested, observed, digits):
    # O journal retém o pedido binary64; o histórico/CSV observa o preço
    # decimal aceito. Só reconhecer a MESMA cotação, dentro do limite de ulps
    # já usado pelo leitor e estritamente abaixo de meio ponto do símbolo.
    if requested == observed:return True
    return (type(digits) is int and 0 <= digits <= 8 and _same(requested,observed)
            and abs(requested-observed) < 10.0**(-digits)/2
            and round(requested,digits) == observed == round(observed,digits))


def _reference_stop(row, inputs):
    """Conformidade binary64 do contrato, com trace17 dígitos e inputs efetivos.

    A referência racional independente está nos testes MQL do contrato; esta
    conferência de recibo precisa respeitar os mesmos limiares representáveis.
    """
    direction=row["direction"];entry=row["initial_entry"];risk=row["initial_risk"];quote=row["executable"]
    tick=row["tick"];minimum=row["minimum"];progress=(quote-entry)*direction/risk
    _require(math.isfinite(progress))
    candidate=row["before"];be=inputs["InpBreakevenTriggerR"];cause=None
    if be > 0 and progress >= be:
        _require(math.isfinite(entry/tick))
        units=math.ceil(entry/tick) if direction > 0 else math.floor(entry/tick)
        level=units*tick
        if (level-candidate)*direction > 0:candidate=level;cause="breakeven"
    mode=inputs["InpTrailingMode"] if row["sampled"] else 0
    if mode and progress >= inputs["InpTrailingTriggerR"]:
        if mode == 1:distance=inputs["InpTrailingPoints"]*row["point"]
        else:
            _require(row["atr"] is not None and row["atr"] > 0)
            distance=inputs["InpTrailingATR"]*row["atr"]
        _require(math.isfinite(distance) and distance > 0)
        level=quote-direction*max(distance,minimum)
        if (level-candidate)*direction > 0:candidate=level;cause="trailing"
    _require(math.isfinite(candidate/tick))
    candidate=(math.floor(candidate/tick) if direction > 0 else math.ceil(candidate/tick))*tick
    _require((candidate-row["before"])*direction >= tick/2 and (quote-candidate)*direction >= minimum)
    if be > 0 and progress >= be:_require((candidate-entry)*direction >= 0)
    return float(candidate),cause


def read_events(directory, inputs, journal, signals, deals, ledger, partials=(), temporal=()):
    _require(journal and journal.get("all_settled") and journal.get("reconciled"),
             "Diário final ausente ou intenções/reservas pendentes; resultado não aprovado.")
    context=journal["context_hash"];by_position={};opened=[row for row in signals if row["opened"]]
    for signal in opened:
        entries=[row for row in journal["entries"] if row["deal"] == signal["deal_ticket"]]
        _require(len(entries) == 1 and entries[0]["state"] == 6)
        entry=entries[0];position=entry["position"]
        _require(entry["order"] == signal["order_ticket"])
        fills=[row for row in deals if row["position"] == position and row["entry"] == 0]
        _require(len(fills) == 1 and position > 0 and position not in by_position)
        clock=datetime.fromtimestamp(fills[0]["at_msc"]/1000,timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        trades=[row for row in ledger if row["direction"] == signal["direction"] and row["entry_price"] == fills[0]["price"] and row["entry_time"] == clock]
        _require(len(trades) == 1)
        fill=fills[0];trade=trades[0]
        _require(_same(entry["requested_volume"],fill["volume"]) and _same(signal["filled_volume"],fill["volume"]))
        _require(_same_price(entry["initial_stop"],trade["stop_price"],fill.get("digits")) and
                 _same_price(entry["initial_target"],trade["target_price"],fill.get("digits")) and
                 entry["direction"] == trade["direction"] and abs(fill["price"]-trade["stop_price"]) > 0,
                 "SL/TP solicitados não correspondem à proteção inicial observada; preserve pedido e histórico.")
        # Mesmo com um único deal, preço*volume/volume pode diferir um ulp.
        # A gestão nativa ancora na média ponderada, não no preço cru do deal.
        anchor=fill["price"]*fill["volume"]/fill["volume"]
        _require(math.isfinite(anchor) and anchor > 0 and (anchor-trade["stop_price"])*trade["direction"] > 0)
        by_position[position]=dict(entry=entry,fill=fill,trade=trade,anchor=anchor,risk=abs(anchor-trade["stop_price"]))
    _require(len(by_position) == len(ledger))
    _require(all(row["state"] == 5 or row["position"] in by_position for row in journal["entries"]))
    actions={row["intent_hash"]:row for row in journal["actions"]};seen_deals=set();last_stop={pid:row["trade"]["stop_price"] for pid,row in by_position.items()}
    for action in journal["actions"]:
        _require(action["kind"] in (1,2) and action["position"] in by_position)
        base=by_position[action["position"]]
        _require(action["parent"] == base["entry"]["id"] and action["at"]*1000 >= base["fill"]["at_msc"])
        if action["effect"] in (1,2):
            candidates=[row for row in deals if row["entry"] == 1 and row["position"] == action["position"] and row["deal"] not in seen_deals
                        and row["at_msc"]//1000 >= action["at"] and _same(row["volume"],action["confirmed"])
                        and (not action["response_deal"] or row["deal"] == action["response_deal"])]
            _require(len(candidates) == 1,"Redução não corresponde a fill próprio inequívoco; efeito tardio/parcial continua pendente.")
            deal=candidates[0];seen_deals.add(deal["deal"])
            _require(_same(action["before"]-action["confirmed"],action["remaining"]))
        if action["effect"] == 3:
            direction=base["trade"]["direction"]
            _require((action["stop"]-last_stop[action["position"]])*direction >= 0 and
                     _same_price(action["tp"],base["trade"]["target_price"],base["fill"].get("digits")))
            last_stop[action["position"]]=action["stop"]
    text="\n".join(mql.read_text(path) for path in (Path(directory)/"Tester").glob("Agent-*/logs/*.log"))
    events=[];seen_intents=set();seen_actions=set()
    matches=list(re.finditer(r"HDK_(PROTECTION|TARGET|EXIT) ([^\r\n]*)",text))
    _require(len(matches) == sum(text.count("HDK_"+name) for name in ("PROTECTION","TARGET","EXIT")))
    for match in matches:
        kind,payload=match.groups();pairs=[part.split("=",1) for part in payload.split()]
        _require(all(len(pair) == 2 for pair in pairs))
        raw=dict(pairs);_require(len(raw) == len(pairs))
        position=_integer(raw.get("position"));at=_integer(raw.get("at"));_require(position in by_position)
        base=by_position[position];direction=base["trade"]["direction"]
        _require(base["fill"]["at_msc"]//1000 <= at <= journal["final_at"])
        row=dict(kind=kind.lower(),position=position,at=at)
        if kind == "PROTECTION":
            required="position at intent direction bar_at sampled executable point tick minimum atr initial_entry initial_risk initial_volume before requested acknowledged observed tp_before tp".split()
            _require(set(raw) == set(required) and raw["direction"] in ("1","-1") and int(raw["direction"]) == direction and raw["sampled"] in ("0","1"))
            row.update({key:_number(raw[key]) for key in "executable point tick minimum initial_entry initial_risk initial_volume before requested tp_before tp".split()})
            row.update(direction=direction,bar_at=_integer(raw["bar_at"]),sampled=raw["sampled"] == "1",atr=None if raw["atr"] == "unavailable" else _number(raw["atr"]),intent=raw["intent"])
            _require(all(row[key] > 0 for key in ("executable","point","tick","minimum","initial_risk","initial_volume","before","requested")))
            _require(row["bar_at"] <= at and (not row["sampled"] or row["bar_at"] > 0))
            key=(f"trailing-{position}-{row['bar_at']}" if row["sampled"] and inputs["InpTrailingMode"] else f"breakeven-{position}")
            _require(key == row["intent"])
            intent=_intent(context,key)
            _require(_same(row["initial_entry"],base["anchor"]) and _same(row["initial_risk"],base["risk"]) and _same(row["initial_volume"],base["fill"]["volume"]))
            _require(row["tp"] == row["tp_before"] and
                     _same_price(row["tp"],base["trade"]["target_price"],base["fill"].get("digits")) and
                     raw["acknowledged"] == raw["observed"] == "1")
            expected,cause=_reference_stop(row,inputs)
            _require((row["requested"]-row["before"])*direction > 0 and _same(row["requested"],expected) and cause is not None)
            row["cause"]=cause
        else:
            intent=_close_intent(context,position)
            _require(raw["acknowledged"] == "1")
            if kind == "TARGET":
                _require(set(raw) == set("position at direction initial_entry initial_risk closed_atr target executable acknowledged".split()) and inputs["InpTargetMode"] == 2 and raw["direction"] == str(direction))
                row.update({key:_number(raw[key]) for key in "initial_entry initial_risk closed_atr target executable".split()})
                _require(row["closed_atr"] > 0 and _same(row["initial_entry"],base["anchor"]) and _same(row["initial_risk"],base["risk"]))
                distance=max(inputs["InpDynamicTargetMinR"]*base["risk"],min(inputs["InpDynamicTargetMaxR"]*base["risk"],inputs["InpDynamicTargetATR"]*row["closed_atr"]))
                _require(_same(row["target"],base["anchor"]+direction*distance) and (row["executable"]-row["target"])*direction >= 0)
            elif raw.get("kind") == "signal":
                _require(set(raw) == set("kind position event_at at direction acknowledged".split()) and inputs["InpExitOnOpposite"] and raw["direction"] == str(-direction))
                row.update(exit_kind="signal",event_at=_integer(raw["event_at"]),direction=int(raw["direction"]))
                _require(0 < row["event_at"] <= at)
            else:
                _require(set(raw) == set("kind position at decision acknowledged".split()) and raw.get("kind") == "session" and inputs["InpExitOnSessionEnd"] and inputs["InpSessionEnabled"])
                _require(raw["decision"] in ("weekday","outside_window","excluded_window"));row.update(exit_kind="session",decision=raw["decision"])
                from .session_events import _context, _reason
                params={key:inputs[value] for key,value in dict(anchor_minute="InpSessionAnchorMinute",weekdays="InpSessionWeekdays",start_minute="InpSessionStartMinute",end_minute="InpSessionEndMinute",exclude_start_minute="InpSessionExcludeStartMinute",exclude_end_minute="InpSessionExcludeEndMinute").items()}
                params["max_cycles"]=0
                _,weekday,minute=_context(at,params["anchor_minute"])
                _require(_reason(params,minute,weekday,0) == raw["decision"])
        action=actions.get(intent)
        _require(action is not None and intent not in seen_intents and action["position"] == position and action["at"] == at)
        seen_intents.add(intent)
        seen_actions.add(action["id"])
        if kind == "PROTECTION":_require(action["kind"] == 2 and action["effect"] == 3 and action["stop"] == row["requested"] and action["tp"] == row["tp"])
        else:_require(action["kind"] == 1 and action["effect"] == 1 and action["remaining"] == 0)
        row.update(action_id=action["id"],request_id=action["request_id"],effect=action["effect"],confirmed=action["confirmed"],remaining=action["remaining"])
        events.append(row)
    # Parciais/tempo já foram conferidos com a agenda, relógios, saldo e fills.
    # Ligá-los também ao diário impede aprovar uma ação sem decisão observada.
    for partial in (row for row in partials if row["state"] == "confirmed"):
        matching=[row for row in journal["actions"] if row["id"] not in seen_actions and row["kind"] == 1 and row["effect"] == 1
                  and row["position"] == partial["position"] and row["at"] == partial["at"] and _same(row["confirmed"],partial["filled"]) and _same(row["remaining"],partial["remaining"])]
        _require(len(matching) == 1);seen_actions.add(matching[0]["id"])
        if partial["stop_before"] != partial["stop_after"]:
            matching=[row for row in journal["actions"] if row["id"] not in seen_actions and row["kind"] == 2 and row["effect"] == 3
                      and row["position"] == partial["position"] and row["at"] == partial["at"] and _same(row["stop"],partial["stop_after"])]
            _require(partial["be_confirmed"] and len(matching) == 1);seen_actions.add(matching[0]["id"])
    for closed in (row for row in temporal if row["state"] == "confirmed"):
        matching=[row for row in journal["actions"] if row["id"] not in seen_actions and row["kind"] == 1 and row["effect"] == 1
                  and row["position"] == closed["position"] and row["at"] == closed["at_msc"]//1000 and _same(row["confirmed"],closed["filled"]) and row["remaining"] == 0]
        _require(len(matching) == 1);seen_actions.add(matching[0]["id"])
    _require(seen_actions == {row["id"] for row in journal["actions"]},"Ação financeira sem decisão/confirmação correspondente; preserve o diário e os logs.")
    return events


def semantic(rows):
    # Identificadores variam no Tester; seus vínculos são obrigatórios antes desta projeção.
    return [{key:value for key,value in row.items() if key not in ("position","intent","action_id","request_id")} for row in rows]


def acceptance(rows, inputs, *, active, replay_match):
    modes=dict(trailing=inputs["InpTrailingMode"] > 0,trigger_breakeven=inputs["InpBreakevenTriggerR"] > 0,
               dynamic_target=inputs["InpTargetMode"] == 2,signal_exit=inputs["InpExitOnOpposite"],session_exit=inputs["InpExitOnSessionEnd"])
    counts=dict(trailing=sum(row.get("cause") == "trailing" for row in rows),
                trigger_breakeven=sum(row.get("cause") == "breakeven" for row in rows),
                dynamic_target=sum(row["kind"] == "target" for row in rows),
                signal_exit=sum(row.get("exit_kind") == "signal" for row in rows),
                session_exit=sum(row.get("exit_kind") == "session" for row in rows))
    return {key:dict(configured=bool(enabled),events=counts[key],exercised=counts[key] > 0,
                     replay_qualified=bool(enabled and active and replay_match is True and counts[key] > 0)) for key,enabled in modes.items()}
