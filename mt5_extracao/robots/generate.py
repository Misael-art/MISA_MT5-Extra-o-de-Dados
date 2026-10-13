"""
Geração do sinal MQL5 a partir de uma estratégia declarada (`strategies/declared.py`).

    files = create(spec, "MeuRobo", "~/robos", robots, config)     # o mesmo que `mt5x robots generate`

Cria o robô com `scaffold.create` e preenche os blocos marcados do esqueleto. O MQL5 gerado só calcula o sinal
sobre barras fechadas; quem envia a ordem é o esqueleto, pela biblioteca de segurança. A prova de que a geração
está certa é a paridade (`mt5x robots parity`), não a leitura do código.
"""
import json
import re
from pathlib import Path
from typing import Dict, Iterable, List

from mt5_extracao.strategies.declared import PRICE, SignalSpec

from . import mql, scaffold

SPEC_SUFFIX = ".signal.json"
DECLARED = "declared"              # valor de "parity_with" no robot.json de um robô gerado
_KIND_LABELS = {"sma": "média simples", "ema": "média exponencial", "rsi": "RSI"}


def input_name(indicator: str) -> str:
    """rapida -> InpRapidaPeriod; media_longa -> InpMediaLongaPeriod."""
    return "Inp" + "".join(part.capitalize() for part in indicator.split("_")) + "Period"


def _operand(operand, index: int) -> str:
    if isinstance(operand, str):
        return f"v_{operand}[{index}]"
    return repr(float(operand))


def _condition(condition) -> str:
    """Índice 1 = última barra fechada; índice 0 = a anterior (vetores na ordem do tempo)."""
    operator, a, b = condition
    if operator == "above":
        return f"({_operand(a, 1)} > {_operand(b, 1)})"
    if operator == "below":
        return f"({_operand(a, 1)} < {_operand(b, 1)})"
    if operator == "cross_above":
        return f"({_operand(a, 1)} > {_operand(b, 1)} && {_operand(a, 0)} <= {_operand(b, 0)})"
    return f"({_operand(a, 1)} < {_operand(b, 1)} && {_operand(a, 0)} >= {_operand(b, 0)})"


def sections(spec: SignalSpec) -> Dict[str, str]:
    """O conteúdo de cada bloco marcado do esqueleto (PARAMETROS, GLOBAIS, INIT, SINAL)."""
    names = list(spec.indicators)
    params = ['input group "5. Estratégia"'] + [
        f"input int    {input_name(n)} = {spec.indicators[n].period};   // Período de {n} "
        f"({_KIND_LABELS[spec.indicators[n].kind]})" for n in names]
    globals_ = [f"int g_{n} = INVALID_HANDLE;" for n in names]
    init = []
    for n in names:
        kind = spec.indicators[n].kind
        call = f"iRSI(_Symbol, PERIOD_CURRENT, {input_name(n)}, PRICE_CLOSE)" if kind == "rsi" else \
            f"iMA(_Symbol, PERIOD_CURRENT, {input_name(n)}, 0, {'MODE_SMA' if kind == 'sma' else 'MODE_EMA'}, PRICE_CLOSE)"
        init.append(f"   g_{n} = {call};")
    init.append("   return (" + " && ".join(f"g_{n} != INVALID_HANDLE" for n in names) + ");")
    used = {o for c in spec.long + spec.short for o in c[1:] if isinstance(o, str)}
    signal = [f"   double v_{n}[2];\n   if(CopyBuffer(g_{n}, 0, 1, 2, v_{n}) != 2) return 0;" for n in names if n in used]
    if PRICE in used:
        signal.append(f"   double v_{PRICE}[2];\n   if(CopyClose(_Symbol, PERIOD_CURRENT, 1, 2, v_{PRICE}) != 2) return 0;")
    for variable, conditions in (("buy", spec.long), ("sell", spec.short)):
        signal.append(f"   bool {variable} = " + (" && ".join(_condition(c) for c in conditions) or "false") + ";")
    signal += ["   if(buy && !sell) return 1;", "   if(sell && !buy) return -1;", "   return 0;"]
    return {"PARAMETROS": "\n".join(params), "GLOBAIS": "\n".join(globals_), "INIT": "\n".join(init),
            "SINAL": "\n".join(signal)}


def _set_default(source: str, name: str, value: str) -> str:
    pattern = re.compile(r"^(input\s+\w+\s+%s\s*=\s*)([^;]+)(;)" % re.escape(name), re.M)
    if not pattern.search(source):
        raise ValueError(f"O esqueleto não tem o parâmetro {name}.")
    return pattern.sub(lambda m: m.group(1) + value + m.group(3), source, count=1)


def to_mql5(spec: SignalSpec, skeleton: str) -> str:
    """O fonte do esqueleto com o sinal da declaração e os padrões de stop, alvo e ATR."""
    source = skeleton
    for section, body in sections(spec).items():
        source = scaffold.set_section(source, section, body)
    source = _set_default(source, "InpAtrPeriod", str(int(spec.atr_period)))
    source = _set_default(source, "InpStopAtrMult", f"{float(spec.stop_atr):g}")
    return _set_default(source, "InpTakeProfitR", f"{float(spec.target_r):g}")


def create(spec: SignalSpec, name: str, folder, robots: Iterable = (), config=None, magic=None) -> List[Path]:
    """Cria o robô a partir da declaração. Devolve os arquivos (o esqueleto preenchido, o robot.json, ...)."""
    files = scaffold.create(name, folder, robots, config, magic)
    source_file, manifest_file, preset_file = files[0], files[1], files[3]
    source_file.write_text(to_mql5(spec, mql.read_text(source_file)), encoding="utf-8")
    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    manifest.update(description=f"{spec.name} (sinal gerado a partir da declaração {spec.key}).",
                    tunable=[input_name(n) for n in spec.indicators] + ["InpStopAtrMult", "InpTakeProfitR"],
                    parity_with=DECLARED)
    manifest_file.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    preset = preset_file.read_text(encoding="utf-8")
    preset = re.sub(r"^InpStopAtrMult=.*$", f"InpStopAtrMult={float(spec.stop_atr):g}", preset, flags=re.M)
    preset = re.sub(r"^InpTakeProfitR=.*$", f"InpTakeProfitR={float(spec.target_r):g}", preset, flags=re.M)
    preset_file.write_text(preset, encoding="utf-8")
    spec_file = source_file.with_name(source_file.stem + SPEC_SUFFIX)
    spec_file.write_text(json.dumps(spec.to_dict(), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return files + [spec_file]


def load_spec(robot) -> SignalSpec:
    """A declaração gravada ao lado de um robô gerado (<Nome>.signal.json)."""
    path = Path(robot.path).with_name(Path(robot.path).stem + SPEC_SUFFIX)
    if not path.is_file():
        raise FileNotFoundError(f"{robot.name} não tem declaração ao lado ({path.name}): ele não foi gerado por "
                                "'mt5x robots generate'.")
    return SignalSpec.from_dict(json.loads(path.read_text(encoding="utf-8")))


def read_spec_file(path) -> SignalSpec:
    path = Path(path).expanduser()
    if not path.is_file():
        raise FileNotFoundError(f"Declaração não encontrada: {path}")
    try:
        return SignalSpec.from_dict(json.loads(path.read_text(encoding="utf-8-sig")))
    except json.JSONDecodeError as e:
        raise ValueError(f"{path.name} não é um JSON válido: {e}") from None
