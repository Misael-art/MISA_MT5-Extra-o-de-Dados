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
LIBRARY = "library"      # registro de uma pasta de robôs adicionada pela interface (não é um robô)
KIND_LABELS = {PYTHON: "Python", MQL5: "MQL5"}

# Ciclo de vida. Cada estágio só é alcançado pela tarefa que o implementa (veja docs/robos.md).
IMPORTED, COMPILED, TESTED, APPROVED, DEMO, LIVE = ("importado", "compilado", "testado", "bateria", "demo", "real")
STAGES = (IMPORTED, COMPILED, TESTED, APPROVED, DEMO, LIVE)
STAGE_LABELS = {IMPORTED: "Importado", COMPILED: "Compilado", TESTED: "Testado", APPROVED: "Bateria",
                DEMO: "Demo", LIVE: "Real"}
# O que falta para chegar ao estágio (texto do card)
STAGE_NEXT = {COMPILED: "compilar no MetaEditor", TESTED: "rodar um backtest",
              APPROVED: "passar na bateria de segurança", DEMO: "rodar em conta demo", LIVE: "operar em conta real"}

NOT_COMPILED = "Ainda não compilado"       # início do aviso que some quando a compilação é registrada


def stages_for(kind: str) -> Tuple[str, ...]:
    """Estratégia Python não compila nem opera: o caminho dela termina na bateria."""
    return (IMPORTED, TESTED, APPROVED) if kind == PYTHON else STAGES


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
    enums: Dict[str, Dict[str, int]] = field(default_factory=dict)   # para mostrar o nome do valor de um preset
    presets: Dict[str, Dict[str, str]] = field(default_factory=dict)
    preset_risks: Dict[str, risk_mod.RiskRating] = field(default_factory=dict)
    risk: risk_mod.RiskRating = field(default_factory=lambda: risk_mod.RiskRating(risk_mod.UNKNOWN))
    has_source: bool = True
    dll_imports: List[str] = field(default_factory=list)
    stage: str = IMPORTED
    notes: List[str] = field(default_factory=list)             # avisos para o usuário (ex.: "não compilado")
    results: List[dict] = field(default_factory=list)          # validações gravadas, a melhor primeiro
    runs: List[dict] = field(default_factory=list)             # backtests gravados, o mais recente primeiro
    checks: List[dict] = field(default_factory=list)           # baterias de segurança, a mais recente primeiro

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

    def display_value(self, item: mql.MqlInput, value: Optional[str] = None) -> str:
        """Valor de um parâmetro como o usuário lê: presets guardam enums como número ('1' -> nome do membro)."""
        value = item.default if value is None else value
        if item.type in self.enums:
            member = mql.enum_member(self.enums, item.type, value)
            if member and member != value:
                return f"{member} ({value})"
        return value

    @property
    def stages(self) -> Tuple[str, ...]:
        return stages_for(self.kind)

    @property
    def next_step(self) -> str:
        """O que falta para o próximo estágio ('' se já está no último)."""
        stages = self.stages
        position = stages.index(self.stage) if self.stage in stages else 0
        return STAGE_NEXT[stages[position + 1]] if position + 1 < len(stages) else ""

    @property
    def result(self) -> Optional[dict]:
        return self.results[0] if self.results else None

    @property
    def headline(self) -> Optional[dict]:
        """
        Números do card: a melhor validação (mais rigorosa: fora da amostra) ou, sem ela, o último backtest.
        'approved' é None quando o número vem de um backtest isolado, que não aprova nem reprova nada.
        """
        if self.results:
            r = self.results[0]
            return {"kind": "validação", "symbol": r["symbol"], "timeframe": r["timeframe"],
                    "profit_factor": r["profit_factor"], "trades": r["trades"], "max_drawdown": r["max_drawdown"],
                    "extra": r["positive_windows"], "extra_label": "janelas com lucro", "approved": r["approved"],
                    "count": len(self.results)}
        if self.runs:
            r, m = self.runs[0], self.runs[0]["metrics"]
            return {"kind": "backtest", "symbol": r["symbol"], "timeframe": r["timeframe"],
                    "profit_factor": m["profit_factor"], "trades": m["trades"], "max_drawdown": m["max_drawdown_pct"],
                    "extra": m["win_rate"], "extra_label": "acerto", "approved": None, "count": len(self.runs)}
        return None


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
    notes = [] if is_compiled else [NOT_COMPILED + ": use 'mt5x robots compile' (ou F7 no MetaEditor) antes de "
                                    "testar ou operar."]
    if src.dll_imports:
        notes.append("Usa DLL externa (" + ", ".join(src.dll_imports) + "): só rode se confiar na origem.")
    return Robot(
        key=key, kind=MQL5, name=manifest.get("name", path.stem), version=src.version,
        description=manifest.get("description", " ".join(src.descriptions)),
        regime=manifest.get("regime", ""), timeframes=tuple(manifest.get("timeframes", ())),
        markets=manifest.get("markets", ""), path=str(path),
        files=[str(path)] + [str(p) for p in includes] + preset_files, missing_includes=missing,
        inputs=src.inputs, enums=enums, presets=presets,
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
    for record in records.values():
        if record["kind"] == LIBRARY:
            for robot in scan_library(record["path"]):
                robots.setdefault(robot.key, robot)
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
            robot.notes = [n for n in robot.notes if not n.startswith(NOT_COMPILED)]
    if db is not None and getattr(db, "engine", None) is not None:
        from mt5_extracao.strategies.validation import load_validations
        attach_validations(list(robots.values()), load_validations(db))
        attach_runs(list(robots.values()), store.load_runs(db))
        attach_checks(list(robots.values()), store.load_checks(db))
    return list(robots.values())


def attach_validations(robots: List[Robot], validations) -> None:
    """
    Liga às estratégias Python os vereditos já gravados por 'mt5x validate' (Filtro C), o melhor primeiro:
    aprovados antes, depois o maior profit factor fora da amostra. Quem tem teste passa a "Testado".
    """
    if validations is None or len(validations) == 0:
        return
    for robot in robots:
        if robot.kind != PYTHON:
            continue
        rows = validations[validations["strategy"] == robot.key.split(":", 1)[1]]
        robot.results = sorted((
            {"symbol": r.symbol, "timeframe": r.timeframe, "approved": bool(r.approved), "status": r.status,
             "profit_factor": float(r.profit_factor_oos), "trades": int(r.trades_oos),
             "positive_windows": float(r.positive_windows), "robust": bool(r.robust),
             "max_drawdown": float(r.mc_dd), "when": r.created_at,
             "source": "validação fora da amostra (walk-forward)"} for r in rows.itertuples()),
            key=lambda x: (not x["approved"], -x["profit_factor"]))
        if robot.results and robot.stage == IMPORTED:
            robot.stage = TESTED


def attach_runs(robots: List[Robot], runs: List[dict]) -> None:
    """Liga a cada robô os backtests gravados (mais recente primeiro). Quem tem teste passa a "Testado"."""
    by_key = {}
    for run in runs:
        by_key.setdefault(run["robot_key"], []).append(run)
    for robot in robots:
        robot.runs = by_key.get(robot.key, [])
        if robot.runs and robot.stage in robot.stages and \
                robot.stages.index(robot.stage) < robot.stages.index(TESTED):
            robot.stage = TESTED


def attach_checks(robots: List[Robot], checks: List[dict]) -> None:
    """
    Liga a cada robô as baterias gravadas. Vale a mais recente: aprovada, o robô passa a "Bateria"; reprovada,
    ele não passa (o estágio da bateria não fica gravado em `_robots`, para poder ser revogado).
    """
    by_key = {}
    for check in checks:
        by_key.setdefault(check["robot_key"], []).append(check)
    for robot in robots:
        robot.checks = by_key.get(robot.key, [])
        if robot.checks and robot.checks[0]["approved"] and robot.stage in robot.stages and \
                robot.stages.index(robot.stage) < robot.stages.index(APPROVED):
            robot.stage = APPROVED


def import_library(db, folder) -> List[Robot]:
    """Registra uma pasta de robôs (layout do MT5 ou pasta simples) para entrar no catálogo."""
    folder = Path(folder).expanduser().resolve()
    if not folder.is_dir():
        raise FileNotFoundError(f"Pasta não encontrada: {folder}")
    found = scan_library(folder)
    if not found:
        raise ValueError(f"Nenhum robô (.mq5 ou .ex5 de Expert Advisor) encontrado em {folder}.")
    store.save_record(db, f"{LIBRARY}:{_slug(str(folder))}", LIBRARY, folder.name, str(folder))
    return found


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
