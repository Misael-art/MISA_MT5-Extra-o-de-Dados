"""Leitura do journal canônico HADOUKEN; nunca envia, reserva ou recupera ordens."""
import hashlib
import math
from pathlib import Path
import re

from . import mql

FINAL = re.compile(r"HDK_JOURNAL at=(\d+) reconciled=([01]) file=(HDKPR2\.[0-9a-f]{32}_RESERVATIONS\.state) sha256=([0-9a-f]{64}) context=([0-9a-f]{64})(?=\s|$)")
REQUEST = "Q P L T K A C G N F Y J H b".split()
ACTION = "V S K C N U Q P L T H j y h l r a f p g e u n i c".split()
IMMUTABLE = "V K C U Q P L T H j y h l".split()
DIGESTS = {f"{prefix}{half}{i}" for prefix in "BIWx" for half in "ab" for i in range(4)}
ENTRY_FIELDS = set("R E V S X0 X1 X2 X3 z_h z_l u r".split()) | set(REQUEST) | DIGESTS | {f"{p}_{s}" for p in "MODv" for s in ("h", "l")}
ACTION_FIELDS = set(ACTION) | DIGESTS | {f"{p}_{s}" for p in "EOZYDv" for s in ("h", "l")}


def _sha(data):
    return hashlib.sha256(data).hexdigest()


def _order(key):
    # StringCompare ordena alfabeticamente: comparação primária sem caixa e
    # desempate com minúsculas antes de maiúsculas. Chaves só contêm ASCII.
    return key.lower(), tuple(character.isupper() for character in key)


def _require(condition, message="Diário nativo inválido; preserve a sessão e confira a conciliação."):
    if not condition:
        raise ValueError(message)


def _integer(value, maximum=9007199254739999):
    _require(math.isfinite(value) and 0 <= value <= maximum and value == int(value))
    return int(value)


def _id(row, field):
    return (_integer(row[field+"_h"], 2**32-1) << 32) | _integer(row[field+"_l"], 2**32-1)


def _digest(row, field):
    return "".join(f"{_integer(row[f'{field}{half}{i}'], 2**32-1):08x}" for half in "ab" for i in range(4))


def read(data, *, filename, context_hash, symbol, magic):
    """Confere envelope, payloads imutáveis e identidades sem interpretar bool como fill."""
    _require(isinstance(data, bytes) and 0 < len(data) <= 8388608)
    _require(re.fullmatch(r"HDKPR2\.[0-9a-f]{32}_RESERVATIONS\.state", filename))
    _require(re.fullmatch(r"[0-9a-f]{64}", context_hash) and isinstance(symbol, str) and symbol)
    _require(type(magic) is int and 0 < magic < 2**63)
    try:
        lines = data.decode("ascii").split("\n")
    except UnicodeDecodeError:
        raise ValueError("Diário deve conter somente o contrato ASCII.") from None
    _require(len(lines) >= 5 and lines[0] in ("HDK_RESERVATIONS_2", "HDK_RESERVATIONS_3") and lines[-1] == "")
    _require(re.fullmatch(r"[0-9a-f]{64}", lines[1]) and re.fullmatch(r"0|[1-9]\d*", lines[2]))
    count = int(lines[2]);root = filename.removesuffix("_RESERVATIONS.state")
    _require(count <= 65536 and len(lines) == count+5)
    _require(lines[-2] == "checksum="+_sha(("\n".join(lines[:-2])+"\n").encode("ascii")))
    records = {"R": {}, "A": {}};raw_records = {"R": {}, "A": {}};previous = "";sequence = None
    for line in lines[3:-2]:
        parts = line.split("|");_require(len(parts) == 2)
        key, text = parts;_require((not previous or _order(previous) < _order(key)) and len(key) <= 63);previous = key
        try:
            number = float(text)
        except ValueError:
            raise ValueError("Número inválido no diário nativo.") from None
        _require(math.isfinite(number) and abs(number) != float.fromhex("0x1.fffffffffffffp+1023") and f"{number:.16e}" == text)
        if key == root+"_SEQ":
            sequence = _integer(number);continue
        match = re.fullmatch(re.escape(root)+r"_([RA])_([1-9]\d*)\.([A-Za-z0-9_]+)", key)
        _require(match is not None)
        kind, identity, field = match.groups();identity = int(identity)
        _require(0 < identity < 9007199254740000 and field in (ENTRY_FIELDS if kind == "R" else ACTION_FIELDS))
        _require(kind != "A" or lines[0] == "HDK_RESERVATIONS_3")
        records[kind].setdefault(identity, {})[field] = number
        raw_records[kind].setdefault(identity, {})[field] = text
    _require(sequence is not None and not (records["R"].keys() & records["A"].keys()))
    _require(all(identity <= sequence for group in records.values() for identity in group))
    entries = [];actions = [];intents = set();symbol_hash = _sha(symbol.encode("utf-8"))
    for identity, row in sorted(records["R"].items()):
        required = ENTRY_FIELDS - {"z_h", "z_l", "u", "r"}
        _require(required <= row.keys() and row["V"] == 2 and row["R"] > 0 and row["E"] > 0)
        optional={"z_h","z_l","u","r"} & row.keys();_require(not optional or optional == {"z_h","z_l","u","r"})
        _require(_digest(row, "B") == context_hash and _digest(row, "x") == symbol_hash and _id(row, "M") == magic)
        _require("".join(f"{_integer(row[f'X{i}'],2**32-1):08x}" for i in range(4)) == symbol_hash[:32])
        deviation = _id(row, "v");_require(deviation <= 2**31-1)
        payload = "REQ1|"+symbol_hash+f"|{magic}|{deviation}|"+"|".join(raw_records["R"][identity][f] for f in REQUEST)
        _require(_sha(payload.encode("ascii")) == _digest(row, "W"))
        state = _integer(row["S"], 7);_require(state >= 1)
        _require(row["Q"] > 0 and row["P"] > 0 and row["L"] > 0 and row["T"] >= 0 and row["K"] in range(8))
        _require(all(row[f] == int(row[f]) for f in ("K","A","C","G","N","F","Y")))
        _require(row["A"] in (1,5) and (row["K"] <= 1 if row["A"] == 1 else row["K"] >= 2))
        _require(0 < row["G"] <= row["C"] and row["F"] in (0,1,2) and row["Y"] in (0,1,2,3))
        _require((row["Y"] in (0,1) and row["N"] == 0) or (row["Y"] in (2,3) and row["N"] > row["C"]))
        _require(row["J"] >= 0 and row["H"] >= 0 and 0 <= row["b"] < 100 and ((row["K"] in (6,7)) == (row["H"] > 0)))
        direction = 1 if int(row["K"]) in (0,2,4,6) else -1
        entry_price=row["H"] if row["K"] in (6,7) else row["P"]
        _require((entry_price-row["L"])*direction > 0 and (row["T"] == 0 or (row["T"]-entry_price)*direction > 0))
        intent = _digest(row,"I");_require(intent not in intents);intents.add(intent)
        entries.append(dict(id=identity,state=state,order=_id(row,"O"),deal=_id(row,"D"),
                            position=_id(row,"z") if "z_h" in row and "z_l" in row else 0,
                            requested_volume=row["Q"],requested_price=row["P"],initial_stop=row["L"],initial_target=row["T"],
                            reserved_risk=row["R"],created_at=row["C"],direction=direction,intent_hash=intent))
    for identity, row in sorted(records["A"].items()):
        _require(set(row) == ACTION_FIELDS and row["V"] == 1)
        parent = _id(row,"E");target = _id(row,"O");position = _id(row,"Z");deviation = _id(row,"v")
        _require(parent in records["R"] and target > 0 and deviation <= 2**31-1)
        _require(_digest(row,"B") == context_hash and _digest(row,"x") == symbol_hash)
        intent = _digest(row,"I");_require(intent not in intents);intents.add(intent)
        payload = f"HDK_MANAGEMENT1|{context_hash}|{intent}|{symbol_hash}|{parent}|{target}|{position}|{deviation}|"+"|".join(raw_records["A"][identity][f] for f in IMMUTABLE)
        _require(_sha(payload.encode("ascii")) == _digest(row,"W"))
        state = _integer(row["S"],6);kind = _integer(row["K"],3);effect = _integer(row["e"],10)
        _require(state >= 1 and kind >= 1 and row["C"] > 0 and row["C"] == int(row["C"]) and row["N"] >= 0 and row["N"] == int(row["N"]))
        _require(row["N"] == 0 or row["N"] >= row["C"])
        _require(row["U"] > 0 and 0 <= row["Q"] <= row["U"] and 0 <= row["u"] <= row["Q"] and 0 <= row["n"] <= row["U"])
        _require(all(row[f] >= 0 for f in ("P","L","T","f","p")) and row["a"] in (0,1) and row["g"] in (0,1))
        for f in ("r","i"):_integer(row[f],2**32-1)
        _require(-(2**31) <= row["c"] < 2**31 and row["c"] == int(row["c"]))
        _require(row["H"] in (0,1) and row["j"] in (0,1,2) and all(row[f] == 0 for f in ("y","h","l")))
        if kind == 1:_require(position > 0 and row["Q"] > 0 and row["P"] > 0 and row["L"] == row["T"] == 0)
        elif kind == 2:_require(position > 0 and row["Q"] == row["P"] == 0 and row["L"] > 0 and row["H"] == row["j"] == deviation == 0)
        else:_require(row["Q"] == row["P"] == row["L"] == row["T"] == row["H"] == row["j"] == deviation == 0)
        if row["N"] == 0:_require(all(row[f] == 0 for f in ("r","a","f","p","g","i","c")) and _id(row,"Y") == _id(row,"D") == 0)
        if row["g"] == 0:_require(row["f"] == row["p"] == 0)
        if state < 5:_require(effect == row["u"] == row["n"] == 0)
        if state == 1:_require(row["N"] == 0)
        if state == 3:_require(row["N"] > 0 and row["g"] == 1)
        if state == 5:_require(effect == 8 and row["u"] == 0 and row["N"] > 0)
        if state == 6:_require(effect in (1,2,3,4,5,6,7,9,10))
        if effect in (1,2):_require(kind == 1 and row["u"] > 0 and (math.isclose(row["u"],row["Q"],rel_tol=1e-12,abs_tol=1e-12) if effect == 1 else row["u"] < row["Q"]))
        if effect == 3:_require(kind == 2 and row["g"] == row["a"] == 1 and row["r"] in (10009,10025))
        if effect in (4,5,6):_require(kind == 3 and row["u"] == row["n"] == 0)
        if effect == 7:_require(row["N"] == 0)
        if effect == 10:_require(kind != 3 and row["u"] == row["n"] == 0)
        actions.append(dict(id=identity,parent=parent,target=target,position=position,kind=kind,state=state,effect=effect,
                            at=int(row["C"]),response_at=int(row["N"]),before=row["U"],requested=row["Q"],price=row["P"],stop=row["L"],tp=row["T"],
                            retcode=int(row["r"]),sent=bool(row["a"]),response_known=bool(row["g"]),response_volume=row["f"],response_price=row["p"],
                            response_order=_id(row,"Y"),response_deal=_id(row,"D"),request_id=int(row["i"]),external_retcode=int(row["c"]),
                            confirmed=row["u"],remaining=row["n"],intent_hash=intent))
    return dict(schema_version=1,sha256=_sha(data),envelope_version=int(lines[0][-1]),identity_hash=lines[1],
                context_hash=context_hash,rows=count,sequence=sequence,entries=entries,actions=actions,
                all_settled=all(row["state"] in (5,6) for row in entries+actions),operational_acceptance=False)


def capture(common, directory, binding, values):
    """Copia após o encerramento do processo; recibo final identifica os bytes exatos."""
    directory = Path(directory)
    text = "\n".join(mql.read_text(path) for path in (directory/"Tester").glob("Agent-*/logs/*.log"))
    matches = list(FINAL.finditer(text))
    if "HDK_JOURNAL" not in text:
        return None  # Perfis anteriores não produzem esse contrato; não qualificar por ausência.
    _require(len(matches) == 1 and text.count("HDK_JOURNAL") == 1,"Recibo final do diário ausente/duplicado; preserve a sessão.")
    at, reconciled, name, digest, context = matches[0].groups()
    _require(context == binding["context_hash"] and int(at) > 0)
    source = Path(common)/"mt5x_sessions"/binding["session_id"]/name
    _require(source.is_file() and not source.is_symlink() and source.stat().st_size <= 8388608)
    data = source.read_bytes();_require(_sha(data) == digest,"Diário mudou após o recibo final; preserve a divergência.")
    result = read(data, filename=name, context_hash=context, symbol=binding["symbol"],magic=values["InpMagic"])
    with (directory/"native_portfolio.state").open("xb") as target:
        target.write(data)
    result.update(final_at=int(at),reconciled=reconciled == "1",artifact="native_portfolio.state",native_filename=name)
    return result
