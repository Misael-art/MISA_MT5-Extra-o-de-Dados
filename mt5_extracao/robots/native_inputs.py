"""Recibo opt-in dos inputs públicos lidos pelo Expert no Tester ou sessão DEMO.

A lista explícita pertence ao perfil revisado: não descobrir/gravar segredos de
Experts arbitrários. Este recibo não certifica negócios nem aceite operacional.
"""
import csv
import hashlib
import io
import json
import math
import re
from pathlib import Path

from . import mql

SCHEMA_VERSION = 1
KINDS = {"int": {"int", "uint", "long", "ulong", "short", "ushort", "char", "uchar", "ENUM_TIMEFRAMES"},
         "number": {"double", "float"}, "bool": {"bool"}, "token": {"string"}, "datetime": {"datetime"}}


def fields_for(robot, fields):
    if not isinstance(fields, dict) or not fields:
        raise ValueError("Informe a lista explícita de inputs públicos do perfil revisado.")
    inputs = {item.name: item.type for item in robot.inputs}
    if len(inputs) != len(robot.inputs):
        raise ValueError("Inputs duplicados na fonte; corrija o perfil antes de executar.")
    for name, kind in fields.items():
        if (not isinstance(name, str) or not re.fullmatch(r"[A-Za-z_]\w*", name, re.ASCII)
                or kind not in KINDS or inputs.get(name) not in KINDS[kind]):
            raise ValueError(f"Input público ausente ou tipo não suportado: {name}.")
    return dict(fields)


def scalar(value, kind):
    if kind == "int":
        if isinstance(value, bool) or not re.fullmatch(r"[+-]?\d+", str(value), re.ASCII):
            raise ValueError("Input inteiro inválido.")
        return int(value)
    if kind == "number":
        if isinstance(value, bool):
            raise ValueError("Input numérico inválido.")
        result = float(value)
        if not math.isfinite(result):
            raise ValueError("Input numérico deve ser finito.")
        return result
    if kind == "bool":
        text = str(value).lower()
        if text not in ("true", "false", "0", "1"):
            raise ValueError("Input booleano inválido.")
        return text in ("true", "1")
    if kind == "datetime":
        from datetime import datetime
        text = str(value).strip().removeprefix("D'").removesuffix("'")
        try:
            parsed = datetime.strptime(text, "%Y.%m.%d %H:%M")
        except ValueError:
            raise ValueError("Semente exige data broker YYYY.MM.DD HH:MM.") from None
        if parsed.strftime("%Y.%m.%d %H:%M") != text:
            raise ValueError("Data broker não canônica.")
        return text
    if kind == "token" and isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_.-]*", value):
        return value
    raise ValueError("Input textual exige identificador público portátil.")


def expected_values(expected, fields):
    if expected is None:
        return None
    if not isinstance(expected, dict) or set(expected) != set(fields):
        raise ValueError("Os inputs esperados devem corresponder à lista pública completa do perfil.")
    return {name: scalar(expected[name].split("||", 1)[0] if isinstance(expected[name], str) else expected[name], kind) for name, kind in fields.items()}


def _binding_number(value):
    """Binary64 exato, sem depender de locale ou arredondamento decimal MQL."""
    number = scalar(value, "number")
    if abs(number) == float.fromhex("0x1.fffffffffffffp+1023"):
        raise ValueError("EMPTY_VALUE não representa parâmetro efetivo conhecido.")
    if number == 0:
        return "0"
    mantissa, exponent = math.frexp(abs(number))
    return ("-" if number < 0 else "") + str(int(math.ldexp(mantissa, 53))) + "p" + str(exponent - 53)


def binding_payload(robot, values, fields):
    """Os inputs públicos completos do perfil, ordenados e tipados para o vínculo."""
    fields = fields_for(robot, fields)
    if set(fields) != {item.name for item in robot.inputs}:
        raise ValueError("Vínculo exige todos os inputs públicos do perfil; não omitir parâmetros.")
    typed = expected_values(values, fields)
    if typed is None:
        raise ValueError("Vínculo exige parâmetros efetivos explícitos.")
    payload = f"HDK_EFFECTIVE_INPUTS1|{len(fields)}|"
    for name, kind in sorted(fields.items()):
        value = typed[name]
        if kind == "number":
            value = _binding_number(value)
        elif kind == "bool":
            value = "1" if value else "0"
        elif kind == "int":
            if not -(2**63) <= value < 2**63:
                raise ValueError("Inteiro do vínculo fora do intervalo de 64 bits com sinal.")
            value = str(value)
        for part in (name, kind, value):
            payload += str(len(part)) + ":" + part
    return payload


def binding_source(robot, fields):
    """Lê variáveis input reais; inclui só o perfil público previamente revisado."""
    fields = fields_for(robot, fields)
    if set(fields) != {item.name for item in robot.inputs}:
        raise ValueError("Vínculo nativo exige lista pública completa.")
    lines = []
    for name, kind in sorted(fields.items()):
        if kind == "number":
            lines.append(f'if(!MT5X_BindingNumber({name},value)) return "";')
        elif kind == "int":
            if next(item.type for item in robot.inputs if item.name == name) == "ulong":
                lines.append(f'if({name}>9223372036854775807) return "";')
            lines.append(f"value=IntegerToString((long){name});")
        elif kind == "bool":
            lines.append(f'value={name} ? "1" : "0";')
        elif kind == "datetime":
            lines.append(f'if((long){name}%60!=0) return "";')
            lines.append(f"value=TimeToString({name},TIME_DATE|TIME_MINUTES);")
        else:
            lines.append(f'if(!MT5X_InputToken({name})) return "";value={name};')
        lines.append(f'payload+=MT5X_BindingFrame("{name}")+MT5X_BindingFrame("{kind}")+MT5X_BindingFrame(value);')
    body = "\n  ".join(lines)
    return '''string MT5X_BindingFrame(const string value) {return IntegerToString(StringLen(value))+":"+value;}
bool MT5X_BindingNumber(const double number,string &value) {
  value="";if(!MathIsValidNumber(number) || number==EMPTY_VALUE || number==-EMPTY_VALUE) return false;
  if(number==0) {value="0";return true;}
  double normalized=MathAbs(number);int exponent=0;
  while(normalized>=2) {normalized/=2;exponent++;}
  while(normalized<1) {normalized*=2;exponent--;}
  value=(number<0 ? "-" : "")+IntegerToString((long)(normalized*4503599627370496.0))+"p"+IntegerToString(exponent-52);
  return true;
}
string MT5X_RuntimeInputs() {
''' + f'  string payload="HDK_EFFECTIVE_INPUTS1|{len(fields)}|",value="";\n  {body}\n  return payload;\n}}\n'


def runtime_request(request):
    """Metadados públicos do pacote, sem aceitar hashes de execução presumidos."""
    if not isinstance(request, dict) or set(request) != {"preset_hash", "revision", "strategy", "actor", "source_hash", "symbol", "timeframe"}:
        raise ValueError("Vínculo exige preset/revisão/estratégia/ator explícitos do pacote.")
    if (any(not isinstance(request[k], str) or not re.fullmatch(r"[0-9a-f]{64}", request[k]) for k in ("preset_hash", "source_hash"))
            or type(request["revision"]) is not int or not 0 < request["revision"] < 2**63):
        raise ValueError("Hash/revisão do pacote inválidos.")
    for name, maximum in (("strategy", 128), ("actor", 64)):
        value = request[name]
        if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1," + str(maximum) + "}", value):
            raise ValueError("Estratégia/ator do vínculo exigem identificadores públicos portáteis.")
    from .. import timeframes
    frame = timeframes.parse(request["timeframe"]) if isinstance(request["timeframe"],str) else None
    if (not isinstance(request["symbol"],str) or not re.fullmatch(r"[!-~]{1,128}",request["symbol"])
            or frame is None or frame.name!=request["timeframe"]):
        raise ValueError("Vínculo inicial exige ativo ASCII e timeframe nativo canônico.")
    return dict(request)


def project_sources(robot):
    """Grafo público no layout de instalação; bibliotecas padrão são do compilador."""
    from . import mt5_tools
    return {item.target.relative_to(Path("MQL5")).as_posix():hashlib.sha256(item.source.read_bytes()).hexdigest()
            for item in mt5_tools.install_plan(robot,Path(".")) if item.source.suffix.lower() in (".mq5", ".mqh")}


def project_sources_hash(sources):
    return hashlib.sha256(json.dumps(sources,sort_keys=True,separators=(",", ":")).encode("utf-8")).hexdigest()


def _runtime_document(lines):
    text = "\n".join(lines) + "\n"
    return (text + "checksum=" + hashlib.sha256(text.encode("ascii")).hexdigest() + "\n").encode("ascii")


def runtime_manifest(request, *, session_id, source_hash, binary_hash, wrapper_hash, inputs_hash):
    request = runtime_request(request)
    if source_hash != request["source_hash"]:
        raise ValueError("Grafo de fontes mudou após compilar o pacote; abrir uma revisão nova.")
    if not isinstance(session_id, str) or not re.fullmatch(r"[0-9a-f]{32}", session_id):
        raise ValueError("Vínculo exige a identidade UUID da sessão atual.")
    hashes = dict(source_hash=source_hash, binary_hash=binary_hash, wrapper_hash=wrapper_hash, inputs_hash=inputs_hash)
    if any(not isinstance(v, str) or not re.fullmatch(r"[0-9a-f]{64}", v) for v in hashes.values()):
        raise ValueError("Vínculo exige SHA256 completos dos artefatos efetivos.")
    from .. import timeframes
    lines = ["HDK_RUNTIME_BINDING_1", session_id, request["symbol"], str(timeframes.parse(request["timeframe"]).value),
             request["strategy"], request["actor"], request["preset_hash"],
             str(request["revision"]), source_hash, binary_hash, inputs_hash, wrapper_hash]
    data = _runtime_document(lines)
    manifest = dict(request, schema_version=1, session_id=session_id, **hashes,
                    manifest_sha256=hashlib.sha256(data).hexdigest(), operational_acceptance=False)
    return manifest, data


def read_runtime_receipt(path, manifest):
    """Confirma o ack nativo; não transforma vínculo em aceite de negociação."""
    data = path.read_bytes()
    if len(data) > 2048:
        raise ValueError("Recibo de vínculo excede o contrato.")
    try:
        lines = data.decode("ascii").splitlines()
    except UnicodeDecodeError:
        raise ValueError("Recibo de vínculo não é ASCII canônico.") from None
    if len(lines) != 13 or data != _runtime_document(lines[:12]) or lines[0] != "HDK_RUNTIME_BOUND_1":
        raise ValueError("Recibo de vínculo truncado, corrompido ou de versão desconhecida.")
    from .. import timeframes
    expected = [manifest["session_id"], manifest["manifest_sha256"], None, manifest["inputs_hash"],
                manifest["source_hash"], manifest["binary_hash"], manifest["preset_hash"],
                str(manifest["revision"]), manifest["wrapper_hash"],manifest["symbol"],str(timeframes.parse(manifest["timeframe"]).value)]
    if (not re.fullmatch(r"[0-9a-f]{64}", lines[3]) or
            any(value is not None and value != lines[i+1] for i, value in enumerate(expected))):
        raise ValueError("Vínculo lido pelo Expert diverge do pacote/artefatos/sessão; nenhum aceite concedido.")
    return dict(manifest, context_hash=lines[3], native_verified=True,
                receipt_sha256=hashlib.sha256(data).hexdigest(), operational_acceptance=False)


def source(robot, filename, session_id, fields, *, tester=True):
    fields = fields_for(robot, fields)
    if not re.fullmatch(r"[0-9a-f]{32}", session_id) or filename != f"mt5x_session_{session_id}_inputs.csv":
        raise ValueError("Nome do recibo deve pertencer à sessão atual.")
    lines = []
    for name, kind in fields.items():
        if kind == "number":
            lines.append(f"if(!MathIsValidNumber({name})) {{ FileClose(f); return false; }}")
            value = f"DoubleToString({name},16)"
        elif kind == "datetime":
            value = f"TimeToString({name},TIME_DATE|TIME_MINUTES)"
        elif kind == "token":
            lines.append(f"if(!MT5X_InputToken({name})) {{ FileClose(f); return false; }}")
            value = name
        else:
            value = f"IntegerToString((long){name})"
        lines.append(f'FileWrite(f,"input","{name}","{kind}",{value});')
    body = "\n  ".join(lines)
    return f'''bool MT5X_InputToken(const string value) {{
  for(int i=0;i<StringLen(value);i++) {{
    ushort c=StringGetCharacter(value,i);
    if(!((c>=65 && c<=90)||(c>=97 && c<=122)||(c>=48 && c<=57)||c==95||c==46||c==45)) return false;
  }}
  return true;
}}
bool MT5X_RecordInputs() {{
  if(MQLInfoInteger(MQL_TESTER)!={str(tester).lower()} || AccountInfoInteger(ACCOUNT_TRADE_MODE)!=ACCOUNT_TRADE_MODE_DEMO) return false;
  int f=FileOpen("{filename}",FILE_WRITE|FILE_CSV|FILE_UNICODE|FILE_COMMON,';');
  if(f==INVALID_HANDLE) return false;
  FileWrite(f,"mt5x-native-inputs","1","{session_id}",_Symbol,EnumToString(_Period),
            IntegerToString(MQLInfoInteger(MQL_TESTER)),IntegerToString(AccountInfoInteger(ACCOUNT_TRADE_MODE)),
            IntegerToString((long)TimeCurrent()),"{len(fields)}");
  {body}
  FileWrite(f,"end","{len(fields)}"); FileFlush(f); FileClose(f);
  Print("MT5X_NATIVE_MODE session={session_id} tester=",MQLInfoInteger(MQL_TESTER),
        " visual=",MQLInfoInteger(MQL_VISUAL_MODE)," demo=",
        AccountInfoInteger(ACCOUNT_TRADE_MODE)==ACCOUNT_TRADE_MODE_DEMO ? 1 : 0);
  return true;
}}
'''


def read(path, *, session_id, symbol, timeframe, fields, expected=None, complete=False, tester=True):
    rows = list(csv.reader(io.StringIO(mql.read_text(path)), delimiter=";"))
    if len(rows) < 3 or len(rows[0]) != 9:
        raise ValueError("Recibo de inputs ausente/incompleto; confira OnInit e os logs desta sessão.")
    header = rows[0]
    if (header[:5] != ["mt5x-native-inputs", "1", session_id, symbol, "PERIOD_"+timeframe]
            or header[5:7] != ["1" if tester else "0", "0"]):
        raise ValueError("Recibo incompatível: confira versão, sessão, ativo/TF e Tester DEMO.")
    try:
        if int(header[7]) <= 0 or int(header[8]) != len(fields):
            raise ValueError
    except ValueError:
        raise ValueError("Recibo com instante/quantidade de inputs inválidos.") from None
    if rows[-1] != ["end", str(len(fields))] or len(rows) != len(fields)+2:
        raise ValueError("Recibo de inputs incompleto; confira a inicialização do Expert.")
    actual = {}
    for row in rows[1:-1]:
        if len(row) != 4 or row[0] != "input" or row[1] not in fields or row[1] in actual or row[2] != fields[row[1]]:
            raise ValueError("Recibo com input desconhecido, duplicado ou tipo incompatível.")
        actual[row[1]] = scalar(row[3], row[2])
    wanted = expected_values(expected, fields)
    if wanted is not None and actual != wanted:
        names = ", ".join(name for name in fields if actual.get(name) != wanted[name])
        raise ValueError(f"Preset não corresponde aos inputs carregados: {names}. Confira o arquivo .set e o perfil.")
    return {"schema_version": SCHEMA_VERSION, "session": session_id, "symbol": symbol,
            "timeframe": timeframe, "tester": tester, "demo": True, "known_at": int(header[7]),
            "values": actual, "complete": bool(complete), "compared": wanted is not None,
            "preset_verified": bool(complete and wanted is not None), "operational_acceptance": False,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
