"""
Leitura de arquivos do MetaTrader 5 sem precisar do MT5: fonte MQL5 (.mq5/.mqh) e presets (.set).

Não compila nem executa nada: só extrai o que o catálogo e a classificação de risco precisam
(versão, descrição, parâmetros de entrada, enums, includes e importações de DLL).
"""
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

_INPUT = re.compile(r"^\s*(s?input)\s+(?!group\b)([A-Za-z_]\w*)\s+([A-Za-z_]\w*)\s*(?:=\s*(.*?))?\s*;\s*(?://\s*(.*))?$")
_GROUP = re.compile(r'^\s*s?input\s+group\s+"(.*)"')
_PROPERTY = re.compile(r'^\s*#property\s+(\w+)\s+(.*?)\s*$')
_INCLUDE = re.compile(r'^\s*#include\s+([<"])(.+?)[>"]')
_IMPORT = re.compile(r'^\s*#import\s+"(.+?)"')
_ENUM = re.compile(r"\benum\s+(\w+)\s*\{(.*?)\}", re.S)
_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.S)
# Envio de ordem em MQL5: a função nativa ou os métodos de abertura da classe CTrade
ORDER_CALL = re.compile(r"\b(OrderSend|OrderSendAsync)\s*\(|"
                        r"\.\s*(Buy|Sell|PositionOpen|BuyLimit|SellLimit|BuyStop|SellStop)\s*\(")


@dataclass(frozen=True)
class MqlInput:
    name: str
    type: str
    default: str
    comment: str = ""
    group: str = ""
    hidden: bool = False      # sinput: não entra na otimização


@dataclass
class MqlSource:
    properties: Dict[str, str] = field(default_factory=dict)
    descriptions: List[str] = field(default_factory=list)
    inputs: List[MqlInput] = field(default_factory=list)
    includes: List[Tuple[str, bool]] = field(default_factory=list)   # (caminho, True se entre <>)
    dll_imports: List[str] = field(default_factory=list)
    enums: Dict[str, Dict[str, int]] = field(default_factory=dict)
    indicator: bool = False
    script: bool = False

    @property
    def version(self) -> str:
        return self.properties.get("version", "")

    @property
    def groups(self) -> List[str]:
        seen = []
        for item in self.inputs:
            if item.group and item.group not in seen:
                seen.append(item.group)
        return seen


def read_text(path) -> str:
    """Lê UTF-8 (com ou sem BOM) e UTF-16, que é como o MetaEditor grava arquivos antigos e logs."""
    data = Path(path).read_bytes()
    if data[:2] in (b"\xff\xfe", b"\xfe\xff"):
        return data.decode("utf-16", errors="replace")
    if data[:3] == b"\xef\xbb\xbf":
        return data[3:].decode("utf-8", errors="replace")
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return data.decode("cp1252", errors="replace")


def _unquote(value: str) -> str:
    value = value.strip()
    return value[1:-1] if len(value) >= 2 and value[0] == value[-1] == '"' else value


def parse_enums(text: str) -> Dict[str, Dict[str, int]]:
    enums = {}
    for name, body in _ENUM.findall(_BLOCK_COMMENT.sub("", text)):
        members, value = {}, -1
        for part in re.sub(r"//[^\n]*", "", body).split(","):
            part = part.strip()
            if not part:
                continue
            member, _, explicit = part.partition("=")
            try:
                value = int(explicit.strip(), 0) if explicit.strip() else value + 1
            except ValueError:
                value += 1
            members[member.strip()] = value
        enums[name] = members
    return enums


def parse_source(text: str) -> MqlSource:
    src = MqlSource(enums=parse_enums(text))
    group = ""
    for line in _BLOCK_COMMENT.sub("", text).splitlines():
        if line.lstrip().startswith("//"):
            continue
        m = _PROPERTY.match(line)
        if m:
            key, value = m.group(1), _unquote(m.group(2).split("//")[0])
            if key == "description":
                src.descriptions.append(value)
            elif key.startswith("indicator_"):
                src.indicator = True
            elif key.startswith("script_"):
                src.script = True
            else:
                src.properties[key] = value
            continue
        m = _GROUP.match(line)
        if m:
            group = m.group(1)
            continue
        m = _INPUT.match(line)
        if m:
            src.inputs.append(MqlInput(name=m.group(3), type=m.group(2), default=_unquote(m.group(4) or ""),
                                       comment=(m.group(5) or "").strip(), group=group,
                                       hidden=m.group(1) == "sinput"))
            continue
        m = _INCLUDE.match(line)
        if m:
            src.includes.append((m.group(2).replace("\\", "/"), m.group(1) == "<"))
            continue
        m = _IMPORT.match(line)
        if m and m.group(1).lower().endswith(".dll"):
            src.dll_imports.append(m.group(1))
    if re.search(r"\bvoid\s+OnStart\s*\(", text):
        src.script = True
    return src


def is_expert(src: MqlSource, text: str) -> bool:
    return (not src.indicator and not src.script
            and re.search(r"\b(OnTick|OnTimer|OnTradeTransaction)\s*\(", text) is not None)


def parse_set(text: str) -> Dict[str, str]:
    """Preset do MT5: 'chave=valor' por linha; ';' comenta; a otimização acrescenta '||início||passo||fim||Y'."""
    values = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith(";") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.split("||")[0].strip()
    return values


def resolve_includes(path: Path, include_root: Optional[Path]):
    """
    Segue os #include a partir de `path`. Devolve (arquivos encontrados, includes não encontrados).
    Os não encontrados costumam ser da biblioteca padrão do MT5 (<Trade/Trade.mqh>), que não é versionada.
    """
    found, missing, queue = [], [], [Path(path)]
    seen = {Path(path).resolve()}
    while queue:
        current = queue.pop()
        for name, angled in parse_source(read_text(current)).includes:
            bases = ([include_root] if include_root else []) if angled else [current.parent, include_root]
            target = next((b / name for b in bases if b and (b / name).is_file()), None)
            if target is None:
                if name not in missing:
                    missing.append(name)
            elif target.resolve() not in seen:
                seen.add(target.resolve())
                found.append(target)
                queue.append(target)
    return sorted(found), missing


def enum_member(enums: Dict[str, Dict[str, int]], enum_name: str, value: str) -> str:
    """Nome do membro para o valor de um parâmetro (o fonte guarda o nome; o preset guarda o número)."""
    members = enums.get(enum_name, {})
    if value in members:
        return value
    try:
        number = int(value)
    except (TypeError, ValueError):
        return ""
    return next((name for name, n in members.items() if n == number), "")
