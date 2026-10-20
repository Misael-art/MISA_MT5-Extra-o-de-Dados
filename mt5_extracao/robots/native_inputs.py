"""Recibo opt-in dos inputs públicos lidos pelo Expert no Tester DEMO.

A lista explícita pertence ao perfil revisado: não descobrir/gravar segredos de
Experts arbitrários. Este recibo não certifica negócios nem aceite operacional.
"""
import csv
import hashlib
import io
import math
import re

from . import mql

SCHEMA_VERSION = 1
KINDS = {"int": {"int", "uint", "long", "ulong", "short", "ushort", "char", "uchar"},
         "number": {"double", "float"}, "bool": {"bool"}, "token": {"string"}}


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
    if kind == "token" and isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_.-]*", value):
        return value
    raise ValueError("Input textual exige identificador público portátil.")


def expected_values(expected, fields):
    if expected is None:
        return None
    if not isinstance(expected, dict) or set(expected) != set(fields):
        raise ValueError("Os inputs esperados devem corresponder à lista pública completa do perfil.")
    return {name: scalar(expected[name].split("||", 1)[0] if isinstance(expected[name], str) else expected[name], kind) for name, kind in fields.items()}


def source(robot, filename, session_id, fields):
    fields = fields_for(robot, fields)
    if not re.fullmatch(r"[0-9a-f]{32}", session_id) or filename != f"mt5x_session_{session_id}_inputs.csv":
        raise ValueError("Nome do recibo deve pertencer à sessão atual.")
    lines = []
    for name, kind in fields.items():
        if kind == "number":
            lines.append(f"if(!MathIsValidNumber({name})) {{ FileClose(f); return false; }}")
            value = f"DoubleToString({name},16)"
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
  if(!MQLInfoInteger(MQL_TESTER) || AccountInfoInteger(ACCOUNT_TRADE_MODE)!=ACCOUNT_TRADE_MODE_DEMO) return false;
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


def read(path, *, session_id, symbol, timeframe, fields, expected=None, complete=False):
    rows = list(csv.reader(io.StringIO(mql.read_text(path)), delimiter=";"))
    if len(rows) < 3 or len(rows[0]) != 9:
        raise ValueError("Recibo de inputs ausente/incompleto; confira OnInit e os logs desta sessão.")
    header = rows[0]
    if (header[:5] != ["mt5x-native-inputs", "1", session_id, symbol, "PERIOD_"+timeframe]
            or header[5:7] != ["1", "0"]):
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
            "timeframe": timeframe, "tester": True, "demo": True, "known_at": int(header[7]),
            "values": actual, "complete": bool(complete), "compared": wanted is not None,
            "preset_verified": bool(complete and wanted is not None), "operational_acceptance": False,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
