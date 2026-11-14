"""SA-01: prefixo observado → header fixado → intenção, diário e fills existentes.

Esta conferência não implementa ATR em Python e não autoriza negociação ao vivo.
Indisponibilidade, intenção, resposta e efeito confirmado são estados distintos.
"""
import calendar
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import math
from pathlib import Path
import re

from ..analytics import super_atr_triple
from ..composition.schema import canonical_json
from ..robots import mql, native_journal
from .. import timeframes
from . import partial_events
from .protection_events import _close_intent
from .protocol import strict_events

_N = r"([^\s]+)"
_START = re.compile(r"HDK_TRIPLE_START schema=1 at=(\d+) seed_at=(\d+)")
_BAR = re.compile(r"HDK_TRIPLE_BAR schema=1 at=(\d+) received_at=(\d+) open="+_N+r" high="+_N+r" low="+_N+r" close="+_N)
_DECISION = re.compile(r"HDK_TRIPLE_DECISION schema=1 bar_at=(\d+) (?:available=0 reason=([^\r\n]+)|"
    r"confirmed_at=(\d+) available_at=(\d+) seed_at=(\d+) available=1 consensus=(-?\d+) episode_at=(\d+) gaps=(\d+) first="+_N+r" second="+_N+r" third="+_N+r")")
_ENTRY = re.compile(r"HDK_TRIPLE_ENTRY schema=1 event_at=(\d+) episode_at=(\d+) direction=(-?\d+) opened=([01]) "
    r"state=(\d+) order=(\d+) deal=(\d+) planned_volume="+_N+r" curve_stop="+_N+r" reason=([^\r\n]*)")
_SIZING = re.compile(r"HDK_TRIPLE_SIZING schema=1 event_at=(\d+) plan_known=([01]) planned_volume="+_N+
    r" risk_money="+_N+r" risk_budget="+_N+r" stop="+_N+r" price_tick="+_N+r" margin_required="+_N+
    r" margin_free="+_N+r" volume_min="+_N+r" volume_max="+_N+r" volume_step="+_N)
_EXIT = re.compile(r"HDK_EXIT kind=signal position=(\d+) event_at=(\d+) at=(\d+) direction=(-?\d+) acknowledged=([01])")
_PLOT = re.compile(r"HDK_TRIPLE_PLOT schema=1 bar_at=(\d+) first="+_N+r" second="+_N+r" third="+_N)
_PLOT_STATE = re.compile(r"HDK_TRIPLE_PLOT_STATE schema=1 at=(\d+) enabled=([01])")


def _require(condition, detail="prefixo/decisão/risco/diário/fills divergentes"):
    if not condition:
        raise ValueError("Triplo: "+detail+"; preserve a sessão para revisão.")


def _number(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise ValueError("Triplo: número indisponível no trace.") from None
    _require(math.isfinite(number) and abs(number) != float.fromhex("0x1.fffffffffffffp+1023"))
    return number


def _same(a, b):
    return abs(a-b) <= 8*max(math.ulp(a), math.ulp(b))


def _clock(at):
    return datetime.fromtimestamp(at, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _text(directory):
    paths = sorted((Path(directory)/"Tester").glob("Agent-*/logs/*.log"))
    _require(sum(p.stat().st_size for p in paths) <= 64*1024*1024, "trace excede64MiB")
    return "\n".join(mql.read_text(p) for p in paths)


def _observations(text, params, seconds):
    seed = calendar.timegm(datetime.strptime(params["seed_start_broker"], "%Y-%m-%dT%H:%M").timetuple())
    starts = [tuple(map(int, m.groups())) for m in strict_events(text, "HDK_TRIPLE_START", _START)]
    _require(seed > 0 and starts and all(at >= seed and s == seed for at, s in starts))
    _require(starts == sorted(set(starts)), "inícios de instância repetidos/fora de ordem")
    bars = {}; last = 0
    for match in strict_events(text, "HDK_TRIPLE_BAR", _BAR):
        raw = match.groups(); at, received = map(int, raw[:2]); o, h, l, c = map(_number, raw[2:])
        _require(at >= seed and at % seconds == 0 and received >= at+seconds and l > 0 and l <= o <= h and l <= c <= h)
        row = dict(at=at, received_at=received, open=o, high=h, low=l, close=c)
        if at in bars:
            _require(all(bars[at][k] == row[k] for k in ("open", "high", "low", "close"))
                     and received >= bars[at]["received_at"], "barra já observada mudou após reinício")
        else:
            _require(at > last and len(bars) < 20000, "prefixo fora de ordem/limite")
            bars[at] = row; last = at
    ordered = list(bars.values())
    _require(not ordered or ordered[0]["at"] == seed, "primeira barra real da seed ausente")
    decisions = []; seen = set()
    for match in strict_events(text, "HDK_TRIPLE_DECISION", _DECISION):
        at, reason, confirmed, available, declared_seed, consensus, episode, gaps, *levels = match.groups()
        at = int(at)
        _require(at >= seed and at % seconds == 0 and at not in seen, "decisão repetida/fora do TF")
        seen.add(at)
        if reason is not None:
            decisions.append(dict(bar_at=at, available=False, reason=reason)); continue
        confirmed, available, declared_seed, consensus, episode, gaps = map(int, (confirmed, available, declared_seed, consensus, episode, gaps))
        _require(confirmed == at+seconds and available >= confirmed and declared_seed == seed and consensus in (-1, 0, 1))
        decisions.append(dict(bar_at=at, available=True, confirmed_at=confirmed, available_at=available,
                              consensus=consensus, episode_at=episode, gaps=gaps, levels=list(map(_number, levels))))
    _require(decisions, "nenhuma decisão conhecida ou indisponível foi registrada")
    return starts, ordered, decisions


def _reference(directory, framework_root, manifest_path, params, seconds, bars, decisions):
    artifact, factory = super_atr_triple.source(framework_root, manifest_path)
    _require(artifact["available"], "compilador do adaptador fixado indisponível")
    model = None
    try:
        model = factory(workspace=directory) if bars else None
        points = model.calculate(bars, period=params["period"], mean=params["mean"],
            deviations=tuple(params["deviation"+str(i)] for i in (1, 2, 3)), initial_side=params["initial_side"]) if model else []
    finally:
        if model is not None: model.close()
    _require(super_atr_triple.source(framework_root, manifest_path)[0] == artifact, "fonte/toolchain mudou durante a conferência")
    indices = {b["at"]:i for i, b in enumerate(bars)}
    received = []; gaps = []; latest = gap = 0
    for i, bar in enumerate(bars):
        latest = max(latest, bar["received_at"])
        if i and bar["at"]-bars[i-1]["at"] > seconds: gap += 1
        received.append(latest); gaps.append(gap)
    _require(len(points) == len(bars))
    for row in decisions:
        if not row["available"]: continue  # ausência não recebe um zero nem vira consenso misto
        i = indices.get(row["bar_at"])
        _require(i is not None and received[i] <= row["available_at"])
        point = points[i]; episode = point["episode_index"]
        _require(episode == -1 or 0 <= episode <= i)
        expected_episode = bars[episode]["at"] if episode >= 0 else 0
        _require(point["available"] and point["consensus"] == row["consensus"] and expected_episode == row["episode_at"]
                 and row["gaps"] == gaps[i] and all(_same(a, b) for a, b in zip(point["levels"], row["levels"])),
                 "primeira diferença no header Triplo fixado")
    return artifact


def _attempts(text, params, starts, decisions, signals, ledger, seconds):
    by_decision = {r["bar_at"]:r for r in decisions if r["available"]}
    by_signal = {r["event_at"]:r for r in signals}
    _require(len(by_signal) == len(signals))
    entries = {}; sizes = {}; filled_episodes = set()
    for match in strict_events(text, "HDK_TRIPLE_ENTRY", _ENTRY):
        at, episode, direction, opened, state, order, deal, volume, stop, _reason = match.groups()
        at, episode, direction, state, order, deal = map(int, (at, episode, direction, state, order, deal))
        volume, stop = map(_number, (volume, stop)); signal = by_signal.get(at); row = by_decision.get(at)
        _require(at not in entries and signal is not None and row is not None and params["entries_enabled"])
        begun = [start for start, _seed in starts if start <= signal["available_at"]]
        _require(begun and episode > 0 and episode+seconds > max(begun), "episódio anterior ao início não rearma entrada")
        _require(direction == row["consensus"] == signal["direction"] and episode == row["episode_at"]
                 and signal["confirmed_at"] == row["confirmed_at"] and signal["available_at"] >= row["available_at"]
                 and (opened == "1") == signal["opened"] and state == signal["execution_state"]
                 and order == signal["order_ticket"] and deal == signal["deal_ticket"] and _same(stop, row["levels"][0]) and volume >= 0)
        if signal["opened"]:
            _require(episode not in filled_episodes and _same(volume, signal["filled_volume"]), "episódio preenchido mais de uma vez")
            filled_episodes.add(episode)
        entries[at] = dict(episode_at=episode, curve_stop=stop, planned_volume=volume)
    for match in strict_events(text, "HDK_TRIPLE_SIZING", _SIZING):
        at, known, *raw = match.groups(); at = int(at)
        _require(at in entries and at not in sizes)
        values = [None if v == "unavailable" and i in (5, 6) else _number(v) for i, v in enumerate(raw)]
        volume, risk, budget, stop, tick, margin, free, minimum, maximum, step = values
        signal = by_signal[at]; entry = entries[at]; direction = signal["direction"]
        _require(volume >= 0 and risk >= 0 and budget >= 0 and volume == entry["planned_volume"])
        margin_known = margin is not None and free is not None
        spec_known = minimum > 0 and maximum >= minimum and step > 0
        if signal["opened"]:
            trades = [r for r in ledger if r["entry_time"] == _clock(signal["available_at"]) and r["direction"] == direction]
            _require(known == "1" and len(trades) == 1 and 0 < risk <= budget and tick > 0 and stop > 0
                     and margin_known and 0 <= margin <= free*.8+2e-8 and free > 0 and spec_known
                     and minimum <= volume <= maximum and Decimal(str(volume)) % Decimal(str(step)) == 0)
            distance = (entry["curve_stop"]-stop)*direction
            _require(distance >= -8*math.ulp(stop) and distance <= tick+8*math.ulp(stop)
                     and abs(stop/tick-round(stop/tick)) <= 1e-6 and _same(stop, trades[0]["stop_price"])
                     and trades[0]["target_price"] == 0, "SL absoluto/TP zero não corresponde ao livro")
        sizes[at] = dict(event_at=at, mode="risk_fraction", plan_known=known == "1", planned_volume=volume,
            filled_volume=signal["filled_volume"], risk_money=risk, risk_budget=budget, initial_stop=stop,
            price_tick=tick, margin_required=margin, margin_free=free, margin_known=margin_known,
            volume_min=minimum if spec_known else None, volume_max=maximum if spec_known else None,
            volume_step=step if spec_known else None, spec_known=spec_known)
    _require(set(entries) == set(sizes) == set(by_signal), "agenda de tentativas/lotes incompleta")
    return entries, list(sizes.values())


def _journal(directory, journal, symbol, magic, signals, entries, sizing, deals, ledger):
    _require(journal and journal.get("all_settled") and journal.get("reconciled"), "diário final ausente ou pendente")
    path = Path(directory)/"native_portfolio.state"
    _require(path.is_file() and not path.is_symlink() and path.stat().st_size <= 8388608)
    raw = path.read_bytes()
    actual = native_journal.read(raw, filename=journal["native_filename"], context_hash=journal["context_hash"], symbol=symbol, magic=magic)
    _require(all(journal.get(k) == v for k, v in actual.items()), "recibo não corresponde aos bytes do diário")
    context = journal["context_hash"]; positions = {}; used = set()
    for signal in [s for s in signals if s["opened"]]:
        intent_key = "triple-episode-"+str(entries[signal["event_at"]]["episode_at"])
        intent = hashlib.sha256(f"HDK_INTENT1|{context}|{len(intent_key)}:{intent_key}".encode("ascii")).hexdigest()
        candidates = [e for e in journal["entries"] if e["deal"] == signal["deal_ticket"]]
        _require(len(candidates) == 1)
        entry = candidates[0]; spec = next(s for s in sizing if s["event_at"] == signal["event_at"])
        fills = [d for d in deals if d["deal"] == signal["deal_ticket"] and d["entry"] == 0]
        _require(len(fills) == 1)
        fill = fills[0]; pid = entry["position"]
        _require(entry["state"] == 6 and entry["order"] == signal["order_ticket"] and entry["intent_hash"] == intent
                 and entry["created_at"] >= signal["available_at"] and 0 < entry["reserved_risk"] <= spec["risk_budget"]
                 and _same(entry["requested_volume"], signal["filled_volume"]) and pid > 0 and pid not in positions and fill["position"] == pid)
        _require({"order", "entry_sl", "entry_tp", "order_sl", "order_tp", "order_state", "order_position"} <= fill.keys(),
                 "SL/TP históricos lossless ausentes")
        _require(fill["order"] == entry["order"] and fill["order_position"] == pid and fill["order_state"] == 4
                 and _same(entry["initial_stop"], spec["initial_stop"]) and _same(fill["entry_sl"], spec["initial_stop"])
                 and _same(fill["order_sl"], spec["initial_stop"]) and entry["initial_target"] == fill["entry_tp"] == fill["order_tp"] == 0)
        positions[pid] = entry; used.add(entry["id"])
    _require(len(positions) == len(ledger) and all(e["id"] in used or e["state"] == 5 for e in journal["entries"]))
    used_exits = set()
    for action in journal["actions"]:
        _require(action["kind"] == 1 and action["position"] in positions and action["parent"] == positions[action["position"]]["id"],
                 "perfil sem parcial/BE/trailing contém outra mutação")
        if action["effect"] in (1, 2):
            fills = [d for d in deals if d["position"] == action["position"] and d["entry"] == 1
                     and d["at_msc"]//1000 >= action["at"] and _same(d["volume"], action["confirmed"])
                     and (not action["response_deal"] or d["deal"] == action["response_deal"])]
            _require(len(fills) == 1 and action["effect"] == 1 and action["remaining"] == 0
                     and fills[0]["deal"] not in used_exits
                     and (not action["response_order"] or fills[0].get("order") == action["response_order"]),
                     "fechamento parcial/incerto não qualifica o perfil")
            used_exits.add(fills[0]["deal"])
    return positions


def _presentation(text, decisions, *, visual):
    known = {row["bar_at"]:row for row in decisions if row["available"]}
    points = []; states = []
    # Espaço faz parte do marcador: STATE/UNAVAILABLE/CLEANUP são eventos distintos.
    for match in strict_events(text, "HDK_TRIPLE_PLOT ", _PLOT):
        at, *raw = match.groups(); at = int(at); levels = list(map(_number, raw)); row = known.get(at)
        _require(visual is True and row is not None and all(_same(a,b) for a,b in zip(levels,row["levels"])),
                 "plot difere dos pontos confirmados usados na decisão")
        points.append(dict(bar_at=at, levels=levels))
    for match in strict_events(text, "HDK_TRIPLE_PLOT_STATE ", _PLOT_STATE):
        at, enabled = map(int, match.groups())
        _require(visual is True and at > 0 and (not states or at >= states[-1]["at"]))
        states.append(dict(at=at, enabled=bool(enabled)))
    failures = re.findall(r"HDK_TRIPLE_PLOT_(?:UNAVAILABLE|CLEANUP): ([^\r\n]*)", text)
    return dict(points=points, states=states, failures=failures,
        status="unavailable" if failures else "observed" if points else "not_observed",
        native_visual_qualified=False, paired_decisions_qualified=False)


def semantic_reference(reference):
    """Apresentação é auditada, mas não participa da comparação de decisões."""
    return {key:value for key,value in reference.items() if key != "presentation"}


def validate(directory, params, signals, ledger, *, timeframe, framework_root, journal, symbol, magic, partial_plan, manifest_path=None, visual=None):
    frame = timeframes.parse(timeframe)
    _require(frame is not None and frame.minutes < 1440)
    seconds = frame.minutes*60; text = _text(directory)
    starts, bars, decisions = _observations(text, params, seconds)
    artifact = _reference(directory, framework_root, manifest_path, params, seconds, bars, decisions)
    entries, sizing = _attempts(text, params, starts, decisions, signals, ledger, seconds)
    partials, deals = partial_events.read_events(directory, partial_plan, signals, sizing, ledger)
    _require(not partials, "perfil não permite parcial")
    positions = _journal(directory, journal, symbol, magic, signals, entries, sizing, deals, ledger)
    by_decision = {r["bar_at"]:r for r in decisions if r["available"]}; exits = []
    for match in strict_events(text, "HDK_EXIT", _EXIT):
        pid, event, at, direction, ack = map(int, match.groups()); row = by_decision.get(event)
        _require(pid in positions and row and row["consensus"] == direction == -positions[pid]["direction"]
                 and at >= row["available_at"] and not any(s["event_at"] == event for s in signals))
        actions = [a for a in journal["actions"] if a["intent_hash"] == _close_intent(journal["context_hash"], pid) and a["at"] == at]
        _require(len(actions) == 1 and (not ack or actions[0]["effect"] == 1))
        exits.append(dict(event_at=event, at=at, direction=direction, acknowledged=bool(ack), effect=actions[0]["effect"]))
    # Hash sem tickets/contexto de sessão: comparação backtest/replay usa o mesmo
    # prefixo e decisões; a identidade do journal é conferida antes, separadamente.
    trace = dict(starts=starts, bars=bars, decisions=decisions)
    return (dict(strategy="super_atr_triple", contract="super_atr_triple_closed/1", source=artifact,
        contexts=len(decisions), bars=len(bars), trace_sha256=hashlib.sha256(canonical_json(trace).encode("utf-8")).hexdigest(),
        decisions=decisions, exits=exits, reference="three original MQL curves on the fixed observed closed prefix",
        source_conformity=True, external_parity=False, native_mt5_qualified=False, restart_operational_qualified=False,
        history_revision="unavailable", allows_live=False, presentation=_presentation(text,decisions,visual=visual)), sizing, deals)


def compare_presentation_pair(visible, hidden):
    """Consulta de dois recibos existentes; não executa/importa/aprova negociação."""
    from .execution import cash_difference, first_difference
    from ..composition import super_atr_triple_native as profile
    for proof, expected in ((visible, True), (hidden, False)):
        _require(proof.get("status")=="reconciled_replay" and proof.get("functional_native_activity") is True
                 and proof.get("actual_mode")==dict(tester=True, visual=True, demo=True)
                 and proof.get("inputs",{}).get("runtime_binding",{}).get("native_verified") is True,
                 "par exige dois replays DEMO ativos e conciliados")
        values = proof["inputs"]["values"]
        _require(set(values)==set(profile.PLOT_INPUT_TYPES) and values.get("InpPlotEnabled") is expected)
        source = proof.get("source", {})
        version = source.get("version")
        native = profile.source_profile(version) if isinstance(version, str) else None
        _require(native is not None and native[2] >= 2 and
                 source == dict(key=profile.ROBOT_KEY, version=version, fingerprint=native[0]))
        reference = proof["strategy_reference"]
        _require(reference.get("source_conformity") is True and reference.get("contract")=="super_atr_triple_closed/1")
        _require(reference.get("source",{}).get("framework_commit")==native[3]
                 and reference["source"].get("qualification")=="PINNED_TRIPLE_HEADER_CPP_ADAPTER")
        presentation = reference.get("presentation",{})
        _require(not presentation.get("failures"), "par contém falha visual")
        _require(all(row["enabled"] is expected for row in presentation.get("states",[])), "par alternou a apresentação durante o ensaio")
        if expected: _require(presentation.get("points"), "par ligado não observou curvas")
        else: _require(not presentation.get("points"), "par desligado observou plot")
    for key in ("source", "context", "test_context", "tester_model"):
        _require(visible.get(key)==hidden.get(key), "par usa fontes/contextos/modelos diferentes")
    omitted = {"InpPlotEnabled", "InpPresetId", "InpConfigHash"}
    a = {k:v for k,v in visible["inputs"]["values"].items() if k not in omitted}
    b = {k:v for k,v in hidden["inputs"]["values"].items() if k not in omitted}
    _require(a==b, "par mudou parâmetros operacionais")
    difference = first_difference(hidden,visible["ledger"],visible["signals"],visible.get("sizing"),
                                  deals=visible.get("deals"))
    if difference is None and semantic_reference(visible["strategy_reference"]) != semantic_reference(hidden["strategy_reference"]):
        difference=dict(section="strategy_reference",field="observed_context")
    if difference is None:
        difference = cash_difference(hidden, visible)
    return dict(schema_version=1,visible_run=visible["run_id"],hidden_run=hidden["run_id"],
        same_observed_decisions_and_closed_fills=difference is None, first_difference=difference,
        scope="observed fixed closed prefix and reconciled fills; not latency, display appearance or full tick history",
        native_visual_qualified=False, operational_acceptance=False)
