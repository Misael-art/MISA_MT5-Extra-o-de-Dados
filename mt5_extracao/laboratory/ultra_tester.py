"""Livro MQL5 concorrente para duas estratégias em ticks sintéticos de um pacote verificado."""
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from string import Template
from tempfile import TemporaryDirectory

from ..experiments.schema import digest

from ..presets.compiler import verify_ultra_research
from .ultra import inspect_receipt

TEMPLATES = Path(__file__).resolve().parents[1] / "robots" / "templates"
FIELDS = ("time_msc", "seq", "bid", "ask", "donchian", "s09_reference", "available_msc", "reference")
MARKERS = {"strategy_tester": "MT5X_ULTRA_B1|", "terminal_history": "MT5X_ULTRA_HISTORY_B1|"}
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
            "broker_orders": False, "economic_approval": False}


def _templates():
    return {name: (TEMPLATES / (name + ".tmpl")).read_text(encoding="utf-8") for name in NATIVE_TEMPLATES}



def _context(db, package):
    verify_ultra_research(db, package)
    policy = package["policy"]
    modules = {m["strategy"]: m for m in policy["modules"]}
    if (len(policy["modules"]) != 2 or set(modules) != set(FIELDS[4:6])
            or package["sample_kind"] != "synthetic" or package["feed"].get("derivation") != "bid_ticks_m1"
            or package["time"].get("broker_utc_offset_hours") != 0):
        raise ValueError("O livro MQL5 requer Donchian e S09 em ticks bid M1 sintéticos com offset UTC zero.")
    run = inspect_receipt(db, package["receipt"])
    if not 0 < run["progress"] <= 10000:
        raise ValueError("O journal de pesquisa admite de 1 a 10000 cotações.")
    frames = inspect_receipt(db, package["receipt"], timeline=True)["timeline"]
    return modules, frames


def export(db, package, destination):
    modules, frames = _context(db, package)
    target = Path(destination)
    if target.exists():
        raise FileExistsError(f"Escolha uma pasta nova para o Expert: {target}")
    key = package["package_hash"]
    symbol = "UX_" + key[:20]
    csv = symbol + ".csv"
    spec = package["specification"]
    unit = Decimal(str(spec["tick_size"]))
    don, s09 = (modules[k]["params"] for k in FIELDS[4:6])
    constants = {"ENTRY_N": don["entry_period"], "ATR_AVG": don["atr_avg"], "ADX_MIN": don["adx_min"],
                 "FAST_N": s09["fast"], "SLOW_N": s09["slow"], "WARM_N": s09["warmup_bars"],
                 "TICK_SIZE": unit, "FIRST_MSC": frames[0]["mark"]["key"][0], "LAST_MSC": frames[-1]["mark"]["key"][0]}
    policy = package["policy"]
    slots = policy["modules"]
    constants.update(TICK_CASH=policy["tick_cash"], INITIAL_CASH=policy["cash"], MARGIN_STEP=policy["margin"],
                     FEE=policy["fee"], RISK_LIMIT=policy["risk_limit"], FILL_CAPACITY=policy["fill_steps_per_tick"],
                     DON_SLOT=slots.index(modules["donchian"]))
    highest = max(frame["mark"]["ask"] for frame in frames) + max(max(m["stop"], m["target"]) for m in slots)
    quantity = sum(m["steps"] for m in slots)
    worst = quantity * (highest * policy["tick_cash"] + policy["margin"] + 2 * policy["fee"])
    if (policy["cash"] + 4 * len(frames) * worst >= 2**63
            or max(don["entry_period"], don["atr_avg"]) > 4096 or highest > 2**53
            or constants["FIRST_MSC"] < 60000):
        raise ValueError("A campanha excede os limites inteiros ou de memória do Expert.")
    defines = "\n".join(f"#define {name} {value}" for name, value in constants.items())
    assignments = "\n   ".join(f"book.module[{i}].{name}={m[field]};"
                               for i, m in enumerate(slots) for name, field in MODULE.items())
    templates = _templates()
    revision = digest(templates)
    def program(name, kind):
        runtime = Template(templates["UltraRuntime.mqh"]).substitute(
            defines=defines, core=templates["UltraSignals.mqh"], book=templates["UltraBook.mqh"],
            modules=assignments, package=key, manifest=package["source_manifest"], revision=revision,
            marker=MARKERS[kind])
        return Template(templates[name]).substitute(runtime=runtime, symbol=symbol)
    importer = Template(templates["UltraImport.mq5"]).substitute(
        symbol=symbol, csv=csv, count=len(frames), digits=spec["digits"], tick_size=unit, package=key)
    def quote(frame):
        mark = frame["mark"]
        return f'{mark["key"][0]},{mark["bid"] * unit:f},{mark["ask"] * unit:f}\r\n'
    contents = {"UltraConcurrent.mq5": program("UltraConcurrent.mq5", "strategy_tester"),
                "UltraTerminalProbe.mq5": program("UltraTerminalProbe.mq5", "terminal_history"),
                "UltraImport.mq5": importer, csv: "".join(map(quote, frames))}
    with TemporaryDirectory(dir=target.parent) as temporary:
        stage = Path(temporary) / "bundle"
        stage.mkdir()
        for name, content in contents.items():
            (stage / name).write_bytes(content.encode("utf-8"))
        stage.rename(target)
    return {"capability": "two_owner_modeled_book", "package_hash": key, "symbol": symbol, "ticks": len(frames),
            "sha256": {name: sha256(content.encode("utf-8")).hexdigest() for name, content in contents.items()}}


def _expected(frame, owners):
    mark = frame["mark"]
    armed = {e["owner"]: e for e in frame["events"] if e["event"] == "armed"}
    signal = next(iter(armed.values()), None)
    return (*mark["key"], mark["bid"], mark["ask"], *(armed.get(owner, {}).get("side", 0) for owner in owners),
            signal["available_msc"] if signal else 0, signal["reference"] if signal else 0)


def _expected_rows(frames, modules, policy):
    owners = [modules[k]["owner"] for k in FIELDS[4:6]]
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


def compare(db, package, journal, *, origin):
    """O chamador escolhe a origem que pretende auditar. Cada executável tem seu próprio marcador."""
    modules, frames = _context(db, package)
    marker = MARKERS[origin]
    encoding = "utf-16" if journal.startswith((b"\xff\xfe", b"\xfe\xff")) else "utf-8-sig"
    rows = [line.partition(marker)[2].split("|") for line in journal.decode(encoding).splitlines() if marker in line]
    begin = ["BEGIN", package["package_hash"], package["source_manifest"], digest(_templates())]
    count = sum(row[0] == "FRAME" for row in rows)
    if (len(rows) < 2 or rows[0][:-1] != begin or not rows[0][-1].isdigit() or int(rows[0][-1]) <= 0
            or rows[-1] != ["END", str(count), "0"] or any(r[0] in ("BEGIN", "END") for r in rows[1:-1])):
        raise ValueError("Journal ausente, interrompido ou incompatível com a origem, o código ou o pacote.")
    result = {"origin": origin, "scope": "two_owner_modeled_book", "native_order_parity": "pending",
              "package_hash": package["package_hash"], "terminal_build": int(rows[0][-1]),
              "journal_sha256": sha256(journal).hexdigest(), "frames_compared": 0, "events_compared": 0,
              "first_divergence": None}
    def difference(index, key, field, expected, observed):
        return dict(result, status="diverged", first_divergence={
            "ordinal": index, "key": key, "field": field, "expected": expected, "observed": observed})
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
    return dict(result, status="modeled_book_matches", end_equity=frames[-1]["mark"]["equity"])
