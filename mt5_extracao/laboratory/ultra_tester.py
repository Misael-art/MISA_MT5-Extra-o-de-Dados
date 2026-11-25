"""Livro MQL5 concorrente para duas estratégias em ticks sintéticos de um pacote verificado."""
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
import re
from string import Template
from tempfile import TemporaryDirectory

from ..experiments.schema import digest

from ..presets.compiler import ultra_research_ledger, verify_ultra_research
from . import ultra_preparation

TEMPLATES = Path(__file__).resolve().parents[1] / "robots" / "templates"
FIELDS = ("time_msc", "seq", "bid", "ask", "donchian", "s09_reference", "available_msc", "reference")
MARKERS = {"strategy_tester": "MT5X_ULTRA_B1|", "terminal_history": "MT5X_ULTRA_HISTORY_B1|",
           "terminal_restart": "MT5X_ULTRA_RESTART_B1|"}
PLATFORM = "strategy_tester_orders"
MARKERS[PLATFORM] = "MT5X_ULTRA_PLATFORM_BOOK1|"
RESTART_CONTROL = "MT5X_ULTRA_RESTART_CTL|"
BALANCE = ("cash", "equity", "risk", "margin", "pending_entry_cost", "exposure_steps", "cost", "spread_cost_twice")
EVENT = ("time_msc", "seq", "owner_slot", "cycle_available_msc", "event", "reason", "steps", "price", "stop",
         "target", "remaining", "fee", "net", "requested")
MODULE = {"size": "steps", "stop_distance": "stop", "target_distance": "target", "breakeven": "breakeven",
          "invalidate": "invalidate", "ttl": "ttl_ms", "max_losses": "max_losses", "goal": "daily_goal"}
NATIVE_TEMPLATES = ("UltraRuntime.mqh", "UltraSignals.mqh", "UltraBook.mqh", "UltraConcurrent.mq5",
                    "UltraTerminalProbe.mq5", "UltraImport.mq5")


def capabilities():
    return {"scope": "two_owner_modeled_book", "strategies": ["donchian", "s09_reference"],
            "origins": list(MARKERS), "feed": "synthetic_bid_ticks_m1_utc",
            "max_ticks": 10000, "tester_requires_connected_demo": True,
            "broker_orders": False, "simulated_platform_orders": {
                "origin": PLATFORM, "portfolio": "independent_signal_per_owner",
                "strategies": ["donchian", "s09_reference", "pattern_definition"],
                "pattern_scope": "M1 OHLC/prior_close, on-grid comparisons, and/or/not, cancel_before_entry", "orders": "limit_with_sl_tp", "management": ["cancel", "breakeven"],
                "hedging": "per_cycle_magic_and_position", "netting": "exclusive_cycle_independent_protection",
                "requires": "Strategy Tester on prepared custom symbol", "acceptance": "requires_agent_evidence"}, "economic_approval": False,
            "tester_primer_m1_bars": ultra_preparation.PRIMER_MINUTES}


def _templates(platform=False):
    templates = {name: (TEMPLATES / (name + ".tmpl")).read_text(encoding="utf-8") for name in NATIVE_TEMPLATES}
    hook = ("      if(owner>=0) PlatformIntent(msc,seq,owner,opened,kind,cycle[owner].side,cycle[owner].limit,qty,stop,target);\n"
            if platform else "")
    templates["UltraSignals.mqh"] = Template(templates["UltraSignals.mqh"]).substitute(closed_cross=(
        "         s09=HDKS09Cross(prev_fast,prev_slow,fast,slow,bars+1,SLOW_N);" if platform else
        "         if(fast>slow && prev_fast<=prev_slow) s09=1;\n         if(fast<slow && prev_fast>=prev_slow) s09=-1;"))
    templates["UltraBook.mqh"] = Template(templates["UltraBook.mqh"]).substitute(
        platform_intent=hook, owners="OWNER_COUNT" if platform else "2",
        owner_storage="OWNER_STORAGE" if platform else "2",
        owner_inputs="(long)MathRound(observed.refs[i]/TICK_SIZE),observed.times[i],observed.verdicts[i]" if platform else "reference,observed.available",
        verdict_param=",const int verdict" if platform else "",
        entry_verdict=('      if(verdict!=1) { event(i,available,(verdict==0 ? "invalidated" : "veto"),'
                       '(verdict==0 ? "pattern_invalidation" : "unknown_pattern_condition"));return; }\n' if platform else ""),
        side_for_owner="observed.sides[i]" if platform else "(i==DON_SLOT ? observed.donchian : observed.s09)")
    return templates


def _platform_source():
    return (TEMPLATES / "UltraPlatformOrders.mq5.tmpl").read_text(encoding="utf-8")


def _kernel_constants(don, s09):
    # O lado sem consumidor usa anéis mínimos. Seu sinal é descartado.
    return {"ENTRY_N": don.get("entry_period", 1), "ATR_AVG": don.get("atr_avg", 1), "ADX_MIN": don.get("adx_min", 0),
            "FAST_N": s09.get("fast", 1), "SLOW_N": s09.get("slow", 2), "WARM_N": s09.get("warmup_bars", 2)}


def _platform_signals(slots, tick):
    from . import ultra_pattern_native
    templates = {False: _templates()["UltraSignals.mqh"], True: _templates(platform=True)["UltraSignals.mqh"]}
    kernels, declarations, observations = [], [], []
    seen = set()
    for i, module in enumerate(slots):
        kind = module["strategy"]
        params = module["params"]
        if kind == "pattern_definition":
            pattern = (TEMPLATES / "UltraPattern.mqh.tmpl").read_text(encoding="utf-8")
            kernels.append(ultra_pattern_native.kernel(pattern, module, i, tick))
            signal, verdict = "direction", f"kernel{i}.verdict"
        else:
            values = _kernel_constants(params if kind == "donchian" else {}, params if kind == "s09_reference" else {})
            if max(values["ENTRY_N"], values["ATR_AVG"]) > 4096:
                raise ValueError("O indicador excede o limite verificado dos anéis do Expert.")
            names = dict(values, UltraSignals=f"UltraKernel{i}")
            kernels.append(re.sub(r"\b("+"|".join(names)+r")\b", lambda match: str(names[match[0]]),
                                  templates[kind == "s09_reference"]))
            signal, verdict = ("donchian" if kind == "donchian" else "s09"), "1"
        declarations.append(f"   UltraKernel{i} kernel{i};")
        observations.extend((f"      if(minute<0) kernel{i}.minute=-1;", f"      kernel{i}.observe(msc,bid);",
                             f"      sides[{i}]=kernel{i}.{signal};verdicts[{i}]={verdict};",
                             f"      times[{i}]=kernel{i}.available;refs[{i}]=kernel{i}.reference;",
                             f"      if(sides[{i}]!=0) {{",
                             f'         PrintFormat("MT5X_ULTRA_SIGNAL1|{i}|%I64d|%I64d|%d|%I64d",msc,times[{i}],'
                             f'sides[{i}],(long)MathRound(refs[{i}]/TICK_SIZE));',
                             f"         if(available==0) {{ available=times[{i}];reference=refs[{i}]; }}",
                             "      }"))
        if kind != "pattern_definition" and kind not in seen:
            observations.append(f"      {signal}=sides[{i}];")
            seen.add(kind)
    return "\n".join([*kernels, "struct UltraSignals {", *declarations,
        "   long minute,available,times[OWNER_STORAGE]; double reference,refs[OWNER_STORAGE];",
        "   int donchian,s09,sides[OWNER_STORAGE],verdicts[OWNER_STORAGE];",
        "   void observe(const long msc,const double bid) {",
        "      available=0;reference=0;donchian=0;s09=0;", *observations, "      minute=msc/60000;", "   }", "};"])


def _platform_revision(slots, tick):
    from ..robots.ultra_hadouken import HEADERS
    return digest([_templates(platform=True), _platform_source(), _platform_signals(slots, tick),
                   HEADERS if any(m["strategy"]=="s09_reference" for m in slots) else {}])


def _context(db, package, *, platform=False):
    verify_ultra_research(db, package)
    policy = package["policy"]
    modules = {}
    for module in policy["modules"]:
        modules.setdefault(module["strategy"], module)
    if (set(modules).difference((*FIELDS[4:6], "pattern_definition")) or
            (not platform and (len(policy["modules"]) != 2 or set(modules) != set(FIELDS[4:6])))
            or package["sample_kind"] != "synthetic" or package["feed"].get("derivation") != "bid_ticks_m1"
            or package["time"].get("broker_utc_offset_hours") != 0):
        raise ValueError("O Expert de ordens aceita Donchian, S09 e hipóteses direcionais de preço em ticks bid M1 sintéticos UTC. O livro histórico requer um dono de cada.")
    run = ultra_research_ledger(db, package["receipt"])
    if not 0 < run["progress"] <= 10000:
        raise ValueError("O journal de pesquisa admite de 1 a 10000 cotações.")
    return modules, run["timeline"]


def _restart_template():
    return (TEMPLATES / "UltraRestartProbe.mq5.tmpl").read_text(encoding="utf-8")


def _build(db, package, restart_id, prepare_tester):
    if restart_id is not None and (not isinstance(restart_id, str) or not re.fullmatch(r"[a-f0-9]{32}", restart_id)):
        raise ValueError("O identificador da retomada deve conter 32 dígitos hexadecimais.")
    modules, frames = _context(db, package, platform=prepare_tester and restart_id is None)
    key = package["package_hash"]
    symbol = "UX_" + key[:20]
    csv = symbol + ".csv"
    spec = package["specification"]
    unit = Decimal(str(spec["tick_size"]))
    don, s09 = (modules.get(k, {}).get("params", {}) for k in FIELDS[4:6])
    constants = dict(_kernel_constants(don, s09), TICK_SIZE=unit,
                     FIRST_MSC=frames[0]["mark"]["key"][0], LAST_MSC=frames[-1]["mark"]["key"][0])
    policy = package["policy"]
    slots = policy["modules"]
    legacy = len(slots) == 2 and set(modules) == set(FIELDS[4:6])
    constants.update(TICK_CASH=policy["tick_cash"], INITIAL_CASH=policy["cash"], MARGIN_STEP=policy["margin"],
                     FEE=policy["fee"], RISK_LIMIT=policy["risk_limit"], FILL_CAPACITY=policy["fill_steps_per_tick"],
                     DON_SLOT=slots.index(modules["donchian"]) if don else -1)
    highest = max(frame["mark"]["ask"] for frame in frames) + max((max(m["stop"], m["target"]) for m in slots), default=0)
    quantity = sum(m["steps"] for m in slots)
    worst = quantity * (highest * policy["tick_cash"] + policy["margin"] + 2 * policy["fee"])
    if (policy["cash"] + 4 * len(frames) * worst >= 2**63
            or constants["LAST_MSC"] * max(2, len(slots)) + len(slots) >= 2**63
            or max(constants["ENTRY_N"], constants["ATR_AVG"]) > 4096 or highest > 2**53
            or constants["FIRST_MSC"] < 60000):
        raise ValueError("A campanha excede os limites inteiros ou de memória do Expert.")
    defines = "\n".join(f"#define {name} {value}" for name, value in constants.items())
    assignments = "\n   ".join(f"book.module[{i}].{name}={m[field]};"
                               for i, m in enumerate(slots) for name, field in MODULE.items())
    templates = _templates()
    revision = digest(templates)
    def program(name, kind, restart=None, binding=None, instrument=symbol):
        physical = kind == PLATFORM
        build_revision = _platform_revision(slots, unit) if physical else digest([revision, restart]) if restart else revision
        runtime_template = templates["UltraRuntime.mqh"]
        source_defines = defines
        if physical:
            source_defines += f"\n#define OWNER_COUNT {len(slots)}\n#define OWNER_STORAGE {max(1, len(slots))}\n#define MAGIC_STRIDE {max(2,len(slots))}"
            runtime_template = runtime_template.replace("(signals.donchian!=0 || signals.s09!=0)", "(signals.available!=0)")
        runtime = Template(runtime_template).substitute(
            defines=source_defines, core=_platform_signals(slots, unit) if physical else templates["UltraSignals.mqh"], book=_templates(platform=True)["UltraBook.mqh"] if physical else templates["UltraBook.mqh"],
            modules=assignments, package=key, manifest=package["source_manifest"], revision=build_revision,
            marker=MARKERS[kind])
        if physical:
            return Template(_platform_source()).substitute(runtime=runtime, symbol=instrument, package=key,
                manifest=package["source_manifest"], revision=build_revision, volume_step=Decimal(str(spec["volume_step"])),
                signal_includes='#include "HADOUKEN/Strategies/HDK_S09Reference.mqh"' if s09 else "",
                currency=policy["currency"])
        if restart:
            return Template(restart).substitute(runtime=runtime, symbol=symbol, binding=binding)
        return Template(templates[name]).substitute(runtime=runtime, symbol=instrument)
    importer = Template(templates["UltraImport.mq5"]).substitute(
        symbol=symbol, csv=csv, count=len(frames), digits=spec["digits"], tick_size=unit, package=key)
    def quote(frame):
        mark = frame["mark"]
        return f'{mark["key"][0]},{mark["bid"] * unit:f},{mark["ask"] * unit:f}\r\n'
    contents = {"UltraImport.mq5": importer, csv: "".join(map(quote, frames))}
    if legacy:
        contents["UltraConcurrent.mq5"] = program("UltraConcurrent.mq5", "strategy_tester")
        contents["UltraTerminalProbe.mq5"] = program("UltraTerminalProbe.mq5", "terminal_history")
    metadata = {}
    if restart_id is not None:
        restart = _restart_template()
        binding = digest([key, package["source_manifest"], revision, restart, restart_id])
        contents["UltraRestartProbe.mq5"] = program("UltraRestartProbe.mq5", "terminal_restart", restart, binding)
        metadata.update(restart_id=restart_id, checkpoint_binding=binding)
    if prepare_tester:
        prepared, info = ultra_preparation.render((TEMPLATES / "UltraTesterContract.mq5.tmpl").read_text(encoding="utf-8"),
                                                  package, symbol, frames)
        contents["UltraTesterContract.mq5"] = prepared
        if legacy:
            contents["UltraPreparedTester.mq5"] = program("UltraConcurrent.mq5", "strategy_tester", instrument=info["symbol"])
        contents["UltraPlatformOrders.mq5"] = program(None, PLATFORM, instrument=info["symbol"])
        metadata["tester_contract"] = info
        if s09:
            from ..robots.ultra_hadouken import HEADERS
            metadata["hadouken_headers"] = dict(HEADERS)
    metadata.update(capability="two_owner_modeled_book" if legacy else "multi_owner_strategy_tester", package_hash=key, symbol=symbol, ticks=len(frames),
                    sha256={name: sha256(content.encode("utf-8")).hexdigest() for name, content in contents.items()})
    return contents, metadata


def package_capabilities(db, package):
    verify_ultra_research(db, package)
    result = {"package_hash": package["package_hash"], "source_manifest": package["source_manifest"],
              "symbol": package["symbol"], "sample_kind": package["sample_kind"], "research": package["capabilities"],
              "owners": [m["owner"] for m in package["policy"]["modules"]],
              "owner_signals": {m["owner"]: m["strategy"] for m in package["policy"]["modules"]}, "native_order_parity": "pending"}
    try:
        _, metadata = _build(db, package, None, True)
    except ValueError as exc:
        return dict(result, status="capability_absent", reason=str(exc))
    origins = list(MARKERS) if "UltraConcurrent.mq5" in metadata["sha256"] else [PLATFORM]
    return dict(result, status="exportable", supported_origins=origins, tester=capabilities(), tester_contract=metadata["tester_contract"],
                sources=metadata["sha256"])


def export(db, package, destination, *, restart_id=None, prepare_tester=False):
    target = Path(destination)
    if target.exists():
        raise FileExistsError(f"Escolha uma pasta nova para o Expert: {target}")
    contents, metadata = _build(db, package, restart_id, prepare_tester)
    with TemporaryDirectory(dir=target.parent) as temporary:
        stage = Path(temporary) / "bundle"
        stage.mkdir()
        for name, content in contents.items():
            (stage / name).write_bytes(content.encode("utf-8"))
        stage.rename(target)
    return metadata


def _expected(frame, owners):
    mark = frame["mark"]
    armed = {e["owner"]: e for e in frame["events"] if e["event"] == "armed"}
    signal = next(iter(armed.values()), None)
    return (*mark["key"], mark["bid"], mark["ask"], *(armed.get(owner, {}).get("side", 0) for owner in owners),
            signal["available_msc"] if signal else 0, signal["reference"] if signal else 0)


def _expected_rows(frames, modules, policy):
    owners = [modules.get(k, {}).get("owner") for k in FIELDS[4:6]]
    slots = {m["owner"]: i for i, m in enumerate(policy["modules"])}
    opened = {None: 0}
    for ordinal, frame in enumerate(frames):
        key = frame["mark"]["key"]
        for e in frame["events"]:
            if e["event"] == "armed":
                opened[e["cycle_id"]] = e["available_msc"]
            values = (*key, slots.get(e["owner"], -1), opened[e["cycle_id"]], e["event"], e["reason"],
                      e.get("steps", e.get("cancelled_steps", 0)), e.get("price", 0), e.get("stop", 0),
                      e.get("target", 0), e.get("remaining", 0), e.get("fee", 0), e.get("net", 0),
                      e.get("requested_margin", e.get("requested_risk", 0)))
            yield ordinal, key, "EVENT", EVENT, values
            yield ordinal, key, "MARK", BALANCE, tuple(e[k] for k in BALANCE)
        yield ordinal, key, "FRAME", FIELDS, _expected(frame, owners)
        yield ordinal, key, "MARK", BALANCE, tuple(frame["mark"][k] for k in BALANCE)


def _restart_proof(lines, marker, frames, policy):
    controls = []
    count = 0
    last = None
    for line in lines:
        if marker in line:
            row = line.partition(marker)[2].split("|")
            if row[0] == "FRAME":
                count += 1
                last = row[1:3]
        elif RESTART_CONTROL in line:
            row = line.partition(RESTART_CONTROL)[2].split("|")
            if (len(row) != 9 or not re.fullmatch(r"[a-f0-9]{64}", row[1]) or
                    row[2] != str(count) or row[3:5] != last or not 0 < count <= len(frames)):
                raise ValueError("Cursor de retomada incompatível com a sequência do journal.")
            controls.append(row)
    if (len(controls) != 2 or [r[0] for r in controls] != ["SAVED", "LOADED"]
            or controls[0][1:] != controls[1][1:]):
        raise ValueError("A gravação e a restauração do checkpoint precisam coincidir.")
    _, binding, count, msc, seq, owner, opened, filled, pending = controls[0]
    owners = {module["owner"]: slot for slot, module in enumerate(policy["modules"])}
    openings = {}
    expected = None
    for ordinal, frame in enumerate(frames, start=1):
        for event in frame["events"]:
            if event["event"] == "armed":
                openings[event["cycle_id"]] = event["available_msc"]
            if event["event"] == "fill" and event["reason"] == "partial":
                expected = [ordinal, *event["key"], owners[event["owner"]], openings[event["cycle_id"]],
                            event["steps"], event["remaining"]]
                break
        if expected:
            break
    if expected is None or controls[0][2:] != list(map(str, expected)):
        raise ValueError("O checkpoint diverge do primeiro fill parcial do livro congelado.")
    return {"checkpoint": {"binding": binding, "frames_before_restart": int(count),
                           "key": [int(msc), int(seq)], "owner_slot": int(owner),
                           "opened_msc": int(opened), "filled": int(filled), "pending": int(pending)}}


def compare(db, package, journal, *, origin):
    """O chamador escolhe a origem que pretende auditar. Cada executável tem seu próprio marcador."""
    modules, frames = _context(db, package, platform=origin == PLATFORM)
    marker = MARKERS[origin]
    encoding = "utf-16" if journal.startswith((b"\xff\xfe", b"\xfe\xff")) else "utf-8-sig"
    lines = journal.decode(encoding).splitlines()
    rows = [line.partition(marker)[2].split("|") for line in lines if marker in line]
    revision = digest(_templates())
    if origin == "terminal_restart":
        revision = digest([revision, _restart_template()])
    if origin == PLATFORM:
        revision = _platform_revision(package["policy"]["modules"], Decimal(str(package["specification"]["tick_size"])))
    begin = ["BEGIN", package["package_hash"], package["source_manifest"], revision]
    count = sum(row[0] == "FRAME" for row in rows)
    if (len(rows) < 2 or rows[0][:-1] != begin or not rows[0][-1].isdigit() or int(rows[0][-1]) <= 0
            or rows[-1] != ["END", str(count), "0"] or any(r[0] in ("BEGIN", "END") for r in rows[1:-1])):
        raise ValueError("Journal ausente, interrompido ou incompatível com a origem, o código ou o pacote.")
    result = {"origin": origin, "scope": "two_owner_modeled_book", "native_order_parity": "pending",
              "package_hash": package["package_hash"], "terminal_build": int(rows[0][-1]),
              "journal_sha256": sha256(journal).hexdigest(), "frames_compared": 0, "events_compared": 0,
              "first_divergence": None}
    def physical(modeled):
        from .ultra_platform import reconcile
        _, contract = ultra_preparation.render((TEMPLATES / "UltraTesterContract.mq5.tmpl").read_text(encoding="utf-8"),
                                               package, "UX_" + package["package_hash"][:20], frames)
        try:
            proof = reconcile(package, frames, lines, contract["symbol"], revision, modeled["terminal_build"])
        except ValueError as exc:
            proof = {"status": "platform_journal_incomplete", "native_order_parity": "pending", "platform_error": str(exc)}
        from . import ultra_pattern_native
        checks = ultra_pattern_native.audit(db, package, frames, lines)
        proof["predicate_audit"] = checks
        signals = ultra_pattern_native.audit_signals(package, frames, lines)
        proof["signal_audit"] = signals
        by_magic = {c["magic"]: c for c in proof.get("cycles", [])}
        stride = max(2, len(package["policy"]["modules"]))
        for slot, msc, available, index, value in checks["observed"]:
            cycle = by_magic.get(available * stride + slot)
            if cycle is not None:
                cycle.setdefault("predicate_values", []).append({"msc": msc, "rule_index": index, "value": value})
        for slot, msc, available, side, reference in signals["observed"]:
            cycle = by_magic.get(available * stride + slot)
            if cycle is not None:
                cycle["observed_signal"] = {"msc": msc, "available_msc": available, "side": side, "reference_ticks": reference}
        def earliest(*differences):
            candidates = [d for d in differences if d]
            return min(candidates, key=lambda d: tuple(d.get("key") or (float("inf"),)), default=None)
        first = earliest(modeled["first_divergence"], signals["first_divergence"], checks["first_divergence"])
        if first:
            modeled = dict(modeled, status="diverged", first_divergence=first)
        combined = dict(modeled, **proof)
        combined.update(scope="strategy_tester_platform_orders", modeled_status=modeled["status"],
                        modeled_first_divergence=first, platform_first_divergence=proof.get("first_divergence"))
        if first:
            combined.update(status="diverged", first_divergence=earliest(first, proof.get("first_divergence")))
        return combined

    def difference(index, key, field, expected, observed):
        modeled = dict(result, status="diverged", first_divergence={
            "ordinal": index, "key": key, "field": field, "expected": expected, "observed": observed})
        return physical(modeled) if origin == PLATFORM else modeled
    actual = iter(rows[1:-1])
    last = None
    for index, key, kind, fields, values in _expected_rows(frames, modules, package["policy"]):
        row = next(actual, [])
        if not row or row[0] != kind:
            return difference(index, key, "record", kind, row[0] if row else "END")
        if len(row) != len(fields)+1:
            return difference(index, key, kind + ".length", len(fields), len(row)-1)
        for field, expected, value in zip(fields, values, row[1:]):
            if str(expected) != value:
                return difference(index, key, kind + "." + field, expected, value)
        if kind == "MARK":
            result["frames_compared" if last == "FRAME" else "events_compared"] += 1
        last = kind
    extra = next(actual, None)
    if extra:
        return difference(len(frames), None, "record", "END", extra[0])
    if origin == "terminal_restart":
        result.update(_restart_proof(lines, marker, frames, package["policy"]))
    result.update(status="modeled_book_matches", end_equity=frames[-1]["mark"]["equity"])
    return physical(result) if origin == PLATFORM else result
