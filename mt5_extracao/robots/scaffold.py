"""
Ponto de partida de um robô novo: `mt5x robots new <Nome> --dir <pasta>`.

Cria, no layout do MT5, o esqueleto de um Expert Advisor sobre a biblioteca de segurança, o robot.json do
contrato, um preset conservador e um README. O esqueleto não tem estratégia: sem sinal, ele não opera.

Os modelos ficam em robots/templates e usam marcadores simples ({{NAME}}, {{MAGIC}}, {{SAFETY}}, {{SLUG}}).
"""
import random
import re
from pathlib import Path
from typing import Iterable, List, Optional

from . import battery, catalog, safety_lib

TEMPLATES = Path(__file__).parent / "templates"
DEFAULT_SAFETY_VERSION = "1.4.0"
_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_]{2,39}$")
# blocos do modelo que quem desenvolve (ou a geração de código) preenche
SECTIONS = ("PARAMETROS", "GLOBAIS", "INIT", "SINAL")


def render(template: str, **values) -> str:
    text = (TEMPLATES / template).read_text(encoding="utf-8")
    for key, value in values.items():
        text = text.replace("{{" + key + "}}", str(value))
    return text


def new_magic(robots: Iterable = (), rng: Optional[random.Random] = None) -> int:
    """Magic number de seis dígitos que nenhum robô do catálogo usa."""
    used = {battery.magic_number(r) for r in robots if r.kind == catalog.MQL5 and r.has_source}
    rng = rng or random.Random()
    for _ in range(1000):
        magic = rng.randint(100000, 999999)
        if str(magic) not in used:
            return magic
    raise RuntimeError("Não foi possível sortear um magic number livre.")


def safety_version(config) -> str:
    """Versão da biblioteca no clone configurado; sem clone, a versão padrão do SDK."""
    folder = safety_lib.lib_dir(config)
    if folder is not None:
        return str(safety_lib.read_manifest(folder)["version"])
    return DEFAULT_SAFETY_VERSION


def set_section(source: str, section: str, body: str) -> str:
    """Troca o conteúdo entre os marcadores MT5X:<section>:INICIO e :FIM de um fonte criado pelo modelo."""
    pattern = re.compile(r"(//--- MT5X:%s:INICIO[^\n]*\n)(.*?)(//--- MT5X:%s:FIM)" % (section, section), re.S)
    if section not in SECTIONS or not pattern.search(source):
        raise ValueError(f"Marcador MT5X:{section} não encontrado no fonte.")
    body = body if body.endswith("\n") or not body else body + "\n"
    return pattern.sub(lambda m: m.group(1) + body + m.group(3), source, count=1)


def create(name: str, folder, robots: Iterable = (), config=None, magic: Optional[int] = None) -> List[Path]:
    """Cria o robô em `folder` (layout Experts/ Presets/) e devolve os arquivos criados. Não sobrescreve."""
    if not _NAME.match(name or ""):
        raise ValueError("Nome inválido: use de 3 a 40 letras, números ou '_', começando por letra (ex.: MeuRobo).")
    robots = list(robots)
    slug = catalog._slug(name)
    if any(r.key == f"{catalog.MQL5}:{slug}" for r in robots):
        raise ValueError(f"Já existe um robô chamado {name} no catálogo. Escolha outro nome.")
    root = Path(folder).expanduser()
    target = root / "Experts" / name
    if target.exists():
        raise FileExistsError(f"A pasta {target} já existe: nada foi alterado. Escolha outro nome ou outra pasta.")
    values = {"NAME": name, "SLUG": slug, "MAGIC": magic or new_magic(robots), "SAFETY": safety_version(config)}
    files = {target / f"{name}.mq5": "Robot.mq5.tmpl", target / f"{name}.robot.json": "robot.json.tmpl",
             target / "README.md": "README.md.tmpl",
             root / "Presets" / f"{name}_Conservador.set": "Conservador.set.tmpl"}
    preset = root / "Presets" / f"{name}_Conservador.set"
    if preset.exists():
        raise FileExistsError(f"O preset {preset} já existe: nada foi alterado.")
    for path, template in files.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(render(template, **values), encoding="utf-8")
    return list(files)
