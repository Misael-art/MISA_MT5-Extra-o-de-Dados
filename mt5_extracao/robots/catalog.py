"""
Catálogo de robôs: estratégias Python do projeto e Expert Advisors MQL5 lidos de pastas.

    robots = load_catalog(config, db)      # lista de Robot, pronta para a CLI e para a aba "Robôs"

Nada aqui fala com o MT5 nem envia ordens: é leitura de arquivos e do banco.
"""
import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from . import mql, store
from . import risk as risk_mod

log = logging.getLogger(__name__)

PYTHON, MQL5 = "python", "mql5"
KIND_LABELS = {PYTHON: "Python", MQL5: "MQL5"}

# Ciclo de vida. Cada estágio só é alcançado pela tarefa que o implementa (veja docs/robos.md).
IMPORTED, COMPILED, TESTED, APPROVED, DEMO, LIVE = ("importado", "compilado", "testado", "bateria", "demo", "real")
STAGES = (IMPORTED, COMPILED, TESTED, APPROVED, DEMO, LIVE)
STAGE_LABELS = {IMPORTED: "Importado", COMPILED: "Compilado", TESTED: "Testado", APPROVED: "Bateria",
                DEMO: "Demo", LIVE: "Real"}


@dataclass
class Robot:
    key: str
    kind: str
    name: str
    version: str = ""
    description: str = ""
    regime: str = ""                       # tendência | lateral | rompimento | compressão | "" (não informado)
    timeframes: Tuple[str, ...] = ()
    markets: str = ""
    path: str = ""                         # arquivo do robô (MQL5) ou módulo (Python)
    files: List[str] = field(default_factory=list)            # fonte + includes do projeto + presets
    missing_includes: List[str] = field(default_factory=list)  # em geral, biblioteca padrão do MT5
    inputs: List[mql.MqlInput] = field(default_factory=list)
    presets: Dict[str, Dict[str, str]] = field(default_factory=dict)
    preset_risks: Dict[str, risk_mod.RiskRating] = field(default_factory=dict)
    risk: risk_mod.RiskRating = field(default_factory=lambda: risk_mod.RiskRating(risk_mod.UNKNOWN))
    has_source: bool = True
    dll_imports: List[str] = field(default_factory=list)
    stage: str = IMPORTED
    notes: List[str] = field(default_factory=list)             # avisos para o usuário (ex.: "não compilado")

    @property
    def kind_label(self) -> str:
        return KIND_LABELS[self.kind]

    @property
    def stage_label(self) -> str:
        return STAGE_LABELS.get(self.stage, self.stage)

    @property
    def groups(self) -> List[str]:
        seen = []
        for item in self.inputs:
            if item.group and item.group not in seen:
                seen.append(item.group)
        return seen


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def library_dirs(config) -> List[Path]:
    raw = config.get("ROBOTS", "library_dirs", fallback="") if config is not None else ""
    return [Path(p.strip()).expanduser() for p in raw.split(";") if p.strip()]


# --- Python ---------------------------------------------------------------------------

def python_robots(config=None) -> List[Robot]:
    from mt5_extracao.strategies import REGISTRY
    risk_per_trade = config.getfloat("STRATEGY", "risk_per_trade", fallback=0.01) if config is not None else 0.01
    robots = []
    for key, cls in REGISTRY.items():
        info = cls.info
        inputs = [mql.MqlInput(name=k, type=type(v).__name__, default=f"{v:g}" if isinstance(v, float) else str(v))
                  for k, v in cls.default_params.items()]
        robots.append(Robot(
            key=f"{PYTHON}:{key}", kind=PYTHON, name=info.name, description=info.summary, regime=info.regime,
            timeframes=tuple(info.timeframes), markets=info.markets, path=cls.__module__, inputs=inputs,
            risk=risk_mod.rate_python(risk_per_trade),
            notes=["Estratégia de pesquisa: faz backtest com a sua base, não opera no MT5."]))
    return robots


# --- MQL5 -----------------------------------------------------------------------------

def _library_root(path: Path) -> Optional[Path]:
    """Pasta que contém 'Experts/' (é onde ficam também 'Include/' e 'Presets/')."""
    for parent in path.parents:
        if parent.name.lower() == "experts":
            return parent.parent
    return None


def _manifest(path: Path) -> dict:
    """robot.json opcional ao lado do robô (<nome>.robot.json ou robot.json): tem prioridade sobre a heurística."""
    for candidate in (path.with_suffix(".robot.json"), path.parent / "robot.json"):
        if candidate.is_file():
            try:
                data = json.loads(mql.read_text(candidate))
                if isinstance(data, dict):
                    return data
            except ValueError as e:
                log.warning(f"robot.json inválido em {candidate}: {e}")
    return {}


def _presets_for(path: Path, root: Optional[Path], input_names) -> Dict[str, Dict[str, str]]:
    """Presets (.set) ao lado do robô e em <raiz>/Presets cujas chaves são, na maioria, parâmetros deste robô."""
    folders = [path.parent] + ([root / "Presets"] if root else [])
    presets = {}
    for folder in folders:
        if not folder.is_dir():
            continue
        for file in sorted(folder.glob("*.set")):
            values = mql.parse_set(mql.read_text(file))
            if values and sum(k in input_names for k in values) * 2 >= len(values):
                presets.setdefault(file.stem, values)
                presets[file.stem]["__file__"] = str(file)
    return presets


def robot_from_file(path) -> Robot:
    """Lê um robô MQL5 a partir do .mq5 (com fonte) ou do .ex5 (só o compilado)."""
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")
    suffix = path.suffix.lower()
    if suffix not in (".mq5", ".ex5"):
        raise ValueError(f"Não é um robô do MetaTrader 5: {path.name}. Escolha um arquivo .mq5 ou .ex5.")
    root = _library_root(path)
    manifest = _manifest(path)
    key = f"{MQL5}:{_slug(path.stem)}"
    if suffix == ".ex5":
        return Robot(key=key, kind=MQL5, name=manifest.get("name", path.stem), path=str(path), files=[str(path)],
                     regime=manifest.get("regime", ""), timeframes=tuple(manifest.get("timeframes", ())),
                     markets=manifest.get("markets", ""), has_source=False, stage=COMPILED,
                     risk=risk_mod.rate([], has_source=False, manifest=manifest.get("risk")),
                     notes=["Sem código-fonte: parâmetros e proteções não podem ser verificados."])

    text = mql.read_text(path)
    src = mql.parse_source(text)
    if not mql.is_expert(src, text):
        raise ValueError(f"{path.name} não é um robô (Expert Advisor): parece ser um indicador ou script.")
    includes, missing = mql.resolve_includes(path, root / "Include" if root else None)
    enums = dict(src.enums)
    for include in includes:
        enums.update(mql.parse_enums(mql.read_text(include)))
    presets = _presets_for(path, root, {i.name for i in src.inputs})
    preset_files = [values.pop("__file__") for values in presets.values()]
    risk_manifest = manifest.get("risk")
    compiled = path.with_suffix(".ex5")
    is_compiled = compiled.is_file() and compiled.stat().st_mtime >= path.stat().st_mtime
    notes = [] if is_compiled else ["Ainda não compilado: compile no MetaEditor (F7) antes de testar ou operar."]
    if src.dll_imports:
        notes.append("Usa DLL externa (" + ", ".join(src.dll_imports) + "): só rode se confiar na origem.")
    return Robot(
        key=key, kind=MQL5, name=manifest.get("name", path.stem), version=src.version,
        description=manifest.get("description", " ".join(src.descriptions)),
        regime=manifest.get("regime", ""), timeframes=tuple(manifest.get("timeframes", ())),
        markets=manifest.get("markets", ""), path=str(path),
        files=[str(path)] + [str(p) for p in includes] + preset_files, missing_includes=missing,
        inputs=src.inputs, presets=presets,
        preset_risks={name: risk_mod.rate(src.inputs, values, enums, manifest=risk_manifest)
                      for name, values in presets.items()},
        risk=risk_mod.rate(src.inputs, {}, enums, manifest=risk_manifest),
        dll_imports=src.dll_imports, stage=COMPILED if is_compiled else IMPORTED, notes=notes)


def scan_library(folder) -> List[Robot]:
    """Robôs de uma pasta: usa 'Experts/' se existir (layout do MQL5), senão a própria pasta."""
    folder = Path(folder)
    if not folder.is_dir():
        log.warning(f"Pasta de robôs não encontrada: {folder}")
        return []
    base = next((p for p in (folder / "Experts", folder / "MQL5" / "Experts") if p.is_dir()), folder)
    robots, sources = [], set()
    for path in sorted(base.rglob("*.mq5")):
        try:
            robots.append(robot_from_file(path))
            sources.add(path.with_suffix("").resolve())
        except ValueError:
            continue                      # indicador ou script
        except OSError as e:
            log.warning(f"Não foi possível ler {path}: {e}")
    for path in sorted(base.rglob("*.ex5")):
        if path.with_suffix("").resolve() not in sources and base is not folder:
            robots.append(robot_from_file(path))
    return robots


# --- catálogo -------------------------------------------------------------------------

def load_catalog(config=None, db=None) -> List[Robot]:
    """Python + pastas de [ROBOTS] library_dirs + robôs importados um a um (tabela _robots)."""
    robots = {r.key: r for r in python_robots(config)}
    for folder in library_dirs(config):
        for robot in scan_library(folder):
            robots.setdefault(robot.key, robot)
    records = store.load_records(db)
    for key, record in records.items():
        if key in robots or record["kind"] != MQL5:
            continue
        try:
            robots[key] = robot_from_file(record["path"])
        except (OSError, ValueError) as e:
            robots[key] = Robot(key=key, kind=MQL5, name=record["name"] or key, path=record["path"] or "",
                                has_source=False, risk=risk_mod.RiskRating(risk_mod.UNKNOWN, ["arquivo não encontrado"]),
                                notes=[f"O arquivo importado não está mais disponível: {e}"])
    for key, record in records.items():      # estágio gravado só avança, nunca regride o que foi lido do disco
        robot = robots.get(key)
        if robot and record.get("stage") in STAGES and STAGES.index(record["stage"]) > STAGES.index(robot.stage):
            robot.stage = record["stage"]
    return list(robots.values())


def import_robot(db, path) -> Robot:
    """Registra um robô avulso (fora das pastas de library_dirs) para aparecer no catálogo."""
    robot = robot_from_file(Path(path).expanduser().resolve())
    store.save_record(db, robot.key, robot.kind, robot.name, robot.path, robot.stage)
    return robot


def get_robot(robots: List[Robot], key: str) -> Robot:
    wanted = key.strip().lower()
    for robot in robots:
        if wanted in (robot.key, robot.key.split(":", 1)[1]):
            return robot
    raise ValueError(f"Robô desconhecido: '{key}'. Use 'mt5x robots list' para ver as chaves.")
