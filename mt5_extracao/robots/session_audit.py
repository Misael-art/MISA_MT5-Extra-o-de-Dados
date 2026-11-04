"""Auditoria de uma sessão de Expert (Tester ou DEMO) a partir dos registros que ela própria produz.

Separada da reprodução histórica SA-REF-01 (contrato fixo de outubro): aqui não há gêmeo Python nem janela
congelada. Cada decisão elegível é seguida até a tentativa do host, o deal de entrada, a posição e a saída,
por identificadores — nunca por igualdade presumida entre ticket de ordem e identificador de posição.
Recusas, bloqueios, ambiguidades, posições órfãs e duplicadas ficam visíveis; nada é descartado.
"""
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path

from . import parity



def _epoch_minute(stamp):
    return int(datetime.strptime(stamp, "%Y.%m.%d %H:%M").replace(tzinfo=timezone.utc).timestamp())


def file_identity(path):
    path = Path(path)
    return dict(name=path.name, sha256=hashlib.sha256(path.read_bytes()).hexdigest())


def audit_session(log_text, trade_rows, *, session_id, symbol, magic, preset=None, source=None, binary=None,
                  timeframe_seconds=300, deals=(), positions=None, orders=None, observed=None):
    if not session_id or not symbol or int(magic) <= 0 or timeframe_seconds <= 0:
        raise ValueError("Identidade/TF da sessão inválidos.")
    events = parity.parse_strategy_events(log_text)
    # Histórico nativo prova deal -> ordem -> POSITION_IDENTIFIER; saídas manuais podem ter magic=0.
    deal_index = {}
    for deal in deals:
        deal_index.setdefault(str(deal.get("ticket")), []).append(deal)
    used_signals, used_refusals = set(), set()
    refusals = [dict(r, minute=r["decision"]) for r in events["refusal"]]
    magics_seen = sorted(set(re.findall(r"HDK#(\d+)", log_text)))
    by_entry_deal = {}
    for row in trade_rows:
        by_entry_deal.setdefault(str(row.get("entry_deal") or ""), []).append(row)
    chains, used_deals = [], set()
    for decision in events["decision"]:
        duplicates = [d for d in events["decision"] if d.get("decision") == decision.get("decision")]
        if decision.get("eligible") != "1" or decision.get("orders") != "1":
            chains.append(dict(decision=decision["decision"], side=decision.get("side"), state="not_armed_or_ineligible",
                               eligible=decision.get("eligible"), orders=decision.get("orders")))
            continue
        bar = str(_epoch_minute(decision["decision"]) - timeframe_seconds)
        signals = [s for s in events["signal"] if s.get("event_at") == bar and s.get("direction") == decision.get("side")]
        refused = [r for r in refusals if r["magic"] == str(magic) and r["minute"] == decision["decision"] and str(r["side"]) == decision.get("side")]
        chain = dict(decision=decision["decision"], side=decision.get("side"), sources=decision.get("_sources"))
        used_signals.update(id(s) for s in signals)
        used_refusals.update(id(r) for r in refused)
        if len(duplicates) > 1 or len(signals) > 1 or len(refused) > 1:
            chain.update(state="ambiguous", note="mais de uma tentativa do host para a mesma barra e lado")
        elif not signals:
            chain.update(state="refused" if refused else "not_attempted", refusal=refused[0]["reason"] if refused else None)
        elif (signals[0].get("execution_state") not in ("1", "2", "4") or
              (signals[0].get("opened") == "1" and signals[0].get("execution_state") != "4")):
            chain.update(state="ambiguous", order=signals[0].get("order"), deal=signals[0].get("deal"),
                         note="aceitação, parcial ou incerteza não comprovam execução integral")
        elif signals[0].get("opened") != "1":
            chain.update(state="refused" if signals[0].get("execution_state") in ("1", "2") else "ambiguous", refusal=signals[0].get("reason") or (refused[0]["reason"] if refused else None),
                         order=signals[0].get("order"), deal=signals[0].get("deal"))
        else:
            deal = signals[0].get("deal")
            matched_positions = by_entry_deal.get(str(deal), [])
            used_deals.add(str(deal))
            chain.update(order=signals[0].get("order"), deal=deal)
            native = deal_index.get(str(deal), [])
            if native:
                entry = native[0]
                pid = str(entry.get("position_id"))
                exits = [d for d in deals if str(d.get("position_id")) == pid and d.get("entry") in (1, 3)]
                chain.update(position_id=pid, entry_deals=native, exits=exits)
                compatible = (len(native) == 1 and str(entry.get("order")) == str(signals[0].get("order"))
                    and str(entry.get("magic")) == str(magic) and entry.get("symbol") == symbol
                    and entry.get("entry") == 0 and entry.get("type") == (0 if decision.get("side") == "1" else 1)
                    and pid not in ("None", "0", ""))
                # CSV fechado não é necessário; volumes e fotografia atual distinguem exposição de saída total.
                ins = [d for d in deals if str(d.get("position_id")) == pid and d.get("entry") == 0]
                remaining = sum(float(d.get("volume", 0)) for d in ins) - sum(float(d.get("volume", 0)) for d in exits)
                if any(d.get("entry") == 2 or (d.get("entry") == 0 and (str(d.get("magic")) != str(magic) or str(d.get("order")) != str(entry.get("order")))) for d in deals if str(d.get("position_id")) == pid):
                    compatible = False  # netting/reversão exige revisão, sem inventar regra de divisão
                chain.update(state="executed" if compatible and exits and abs(remaining) < 1e-8 else
                             "open_position" if compatible and remaining > 0 else "ambiguous", remaining_volume=remaining)
            elif len(matched_positions) > 1:
                chain.update(state="duplicate_position", positions=[p.get("position_id") for p in matched_positions])
            elif not matched_positions:
                chain.update(state="open_or_unexported", note="entrada aberta sem posição encerrada na exportação")
            else:
                p = matched_positions[0]
                compatible = p.get("symbol") == symbol and str(p.get("entry_order")) == str(signals[0].get("order"))
                chain.update(state="executed" if compatible else "ambiguous", position_id=p.get("position_id"), entry_order=p.get("entry_order"),
                             exit_deal=p.get("exit_deal"), exit_time=p.get("time"), exit_reason=p.get("exit_reason"),
                             result=p.get("result"), order_matches_position=str(p.get("position_id")) == str(deal) or
                             str(p.get("position_id")) == str(signals[0].get("order")))
        chains.append(chain)
    orphans = [dict(position_id=r.get("position_id"), entry_deal=r.get("entry_deal"), entry_time=r.get("entry_time"))
               for deal, rows in by_entry_deal.items() if deal not in used_deals for r in rows]
    stamps = [d["decision"] for kind in ("decision", "blocked") for d in events[kind]]
    unmatched_signals = [s for s in events["signal"] if id(s) not in used_signals]
    unmatched_refusals = [r for r in refusals if id(r) not in used_refusals]
    linked_pids = {c.get("position_id") for c in chains if c.get("position_id")}
    unmatched_deals = [d for d in deals if str(d.get("magic")) == str(magic) and str(d.get("position_id")) not in linked_pids]
    foreign_deals = [d for d in deals if str(d.get("magic")) != str(magic) and str(d.get("position_id")) not in linked_pids]
    own_positions = None if positions is None else [p for p in positions if p.get("symbol") == symbol and
        (str(p.get("magic")) == str(magic) or str(p.get("identifier")) in linked_pids)]
    own_orders = None if orders is None else [o for o in orders if o.get("symbol") == symbol and str(o.get("magic")) == str(magic)]
    states = {}
    for c in chains:
        states[c["state"]] = states.get(c["state"], 0) + 1
    identity = dict(session_id=session_id, symbol=symbol, magic=str(magic), magics_in_log=magics_seen,
                    foreign_magics=[m for m in magics_seen if m != str(magic)],
                    symbols_in_export=sorted({r.get("symbol") for r in trade_rows if r.get("symbol")}),
                    preset=file_identity(preset) if preset else None, source=file_identity(source) if source else None,
                    binary=file_identity(binary) if binary else None,
                    observed=observed, interval=dict(first_decision=min(stamps) if stamps else None, last_decision=max(stamps) if stamps else None))
    interrupt = []
    if identity["foreign_magics"] or any(s != symbol for s in identity["symbols_in_export"]):
        interrupt.append("identidade: magic ou símbolo alheio nos registros da sessão")
    if orphans:
        interrupt.append("posição sem decisão correspondente")
    if states.get("duplicate_position") or states.get("ambiguous"):
        interrupt.append("tentativa ou posição duplicada/ambígua")
    if unmatched_signals or unmatched_deals:
        interrupt.append("eventos nativos sem correspondência")
    if states.get("open_or_unexported") or states.get("open_position") or states.get("not_attempted"):
        interrupt.append("cadeia incompleta ou exposição ainda aberta")
    if own_positions or own_orders:
        interrupt.append("posição ou pendência própria ainda aberta")
    final_state = "unknown" if own_positions is None or own_orders is None else "exposed" if own_positions or own_orders else "flat_snapshot"
    # Uma fotografia vazia não substitui histórico completo nem reconciliação de entradas.
    return dict(final_state=final_state, open_positions=own_positions, pending_orders=own_orders,
                unmatched_signals=unmatched_signals, unmatched_refusals=unmatched_refusals, unmatched_deals=unmatched_deals,
                foreign_deals=foreign_deals, events=events, identity=identity, states=states, chains=chains, orphans=orphans,
                blocked=[dict(decision=b["decision"], reason=b.get("reason")) for b in events["blocked"]],
                refusals=refusals, interrupt=interrupt)


def capture_snapshot(mt5, *, session_id, symbol, magic, start, end):
    """Fotografia somente leitura pelo backend oficial; None nunca vira carteira vazia."""
    from .controls import account_identity
    account = account_identity(mt5)
    if not account["demo"]:
        raise ValueError("Auditoria operacional exige conta DEMO.")
    if start.tzinfo is None or end.tzinfo is None or start >= end:
        raise ValueError("Intervalo de histórico exige início/fim com fuso explícito.")
    result = {}
    for name, value in (("positions", mt5.positions_get()), ("orders", mt5.orders_get()),
                        ("deals", mt5.history_deals_get(start, end))):
        if value is None:
            raise ValueError(f"Leitura incompleta de {name}; estado final desconhecido.")
        result[name] = [dict(row._asdict()) for row in value]
    after = account_identity(mt5)
    if after != account:
        raise ValueError("Conta/carteira mudou durante a captura; obtenha nova fotografia.")
    return dict(result, session_id=session_id, symbol=symbol, magic=str(magic), account=account,
                observed_at=datetime.now(timezone.utc).isoformat(),
                history_interval=dict(start=start.isoformat(), end=end.isoformat(), basis="API datetime with explicit offset"))


def audit_files(*, session_id, symbol, magic, timeframe, logs, preset, source, wrapper, binary, inputs, fields,
                snapshot, trades_csv=None):
    """Audita arquivos efetivos da sessão própria, sem janela fixa ou acesso ao banco principal."""
    from dataclasses import replace
    from . import catalog, controls, mql, mt5_tools, native_inputs
    from mt5_extracao import timeframes
    tf = timeframes.parse(timeframe)
    if tf is None:
        raise ValueError("TF nativo inválido.")
    robot = catalog.robot_from_file(Path(source))
    robot = replace(robot, presets={"audit": mql.parse_set(mql.read_text(preset))})
    effective = controls.effective_values(robot, "audit")
    if str(effective.get("InpMagicNumber")) != str(magic):
        raise ValueError("Magic não corresponde ao preset efetivo.")
    proof = native_inputs.read(Path(inputs), session_id=session_id, symbol=symbol, timeframe=tf.name,
        fields=fields, expected={k: effective[k] for k in fields}, complete=set(fields) == {i.name for i in robot.inputs},
        tester=False)
    # O wrapper compilado precisa incluir a fonte arquivada na mesma pasta e o recibo desta sessão.
    wrapper_text = mql.read_text(wrapper)
    included = Path(wrapper).parent / Path(source).name
    if (f'#include "{Path(source).name}"' not in wrapper_text or
            not included.is_file() or included.read_bytes() != Path(source).read_bytes() or
            f"mt5x_session_{session_id}_inputs.csv" not in wrapper_text):
        raise ValueError("Wrapper/fonte/recibo não pertencem à composição declarada.")
    snap = json.loads(Path(snapshot).read_text(encoding="utf-8"))
    if (snap.get("session_id"), snap.get("symbol"), str(snap.get("magic"))) != (session_id, symbol, str(magic)):
        raise ValueError("Fotografia pertence a outra sessão/símbolo/magic.")
    if snap.get("account", {}).get("demo") is not True:
        raise ValueError("Fotografia sem confirmação de conta DEMO.")
    # Os logs devem vir exclusivamente do perfil próprio; nomes e hashes mantêm a proveniência.
    text = "\n".join(f"===== {Path(p).resolve()} =====\n" + mt5_tools.read_log_text(Path(p)) for p in logs)
    if not re.search(r"MT5X_NATIVE_MODE session=" + re.escape(session_id) + r" tester=0 visual=[01] demo=1(?:\s|$)", text):
        raise ValueError("Log sem confirmação nativa desta sessão DEMO.")
    account = snap["account"]
    server = str(account["server"]).replace("\\", "\\\\").replace(chr(34), "\\"+chr(34))
    if (f'ACCOUNT_LOGIN)=={int(account["login"])}' not in wrapper_text or
            'ACCOUNT_TRADE_MODE_DEMO' not in wrapper_text or
            f'AccountInfoString(ACCOUNT_SERVER)=="{server}"' not in wrapper_text):
        raise ValueError("Wrapper sem vínculo à conta DEMO da fotografia.")
    report = audit_session(text, parity.read_trade_export(trades_csv) if trades_csv else [],
        session_id=session_id, symbol=symbol, magic=magic, timeframe_seconds=tf.minutes * 60,
        preset=preset, source=source, binary=binary, deals=snap["deals"], positions=snap["positions"],
        orders=snap["orders"], observed=dict(at=snap["observed_at"], history=snap["history_interval"]))
    report["identity"].update(wrapper=file_identity(wrapper), logs=[file_identity(p) for p in logs],
        snapshot=file_identity(snapshot), inputs=proof, timeframe=tf.name, account=snap["account"],
        time_basis="decision/event_at: broker native encoded as epoch; snapshot: explicit wall clock offset")
    if not proof["preset_verified"]:
        report["interrupt"].append("recibo não cobre todos os inputs efetivos")
    if not report["events"]["decision"] and not report["events"]["blocked"]:
        report["interrupt"].append("sem observação da estratégia")
    report["operational_acceptance"] = False  # só a revisão de todos os casos autoriza esse aceite
    return report
