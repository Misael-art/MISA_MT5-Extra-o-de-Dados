"""
Ferramentas do MT5 para robôs MQL5: achar a pasta de dados e o MetaEditor, copiar o robô (com includes e
presets) para `MQL5/` e compilar, lendo o log do MetaEditor em erros e avisos.

    items, result = compile_robot(config, robot)     # instala e compila; result.ok diz se gerou o .ex5

Nada aqui envia ordens nem inicia o robô. Os executáveis entram por função injetada (`run`), para testar sem MT5.

Validado à mão no Wine (2026-10-12, Wine 11.17, T8.4): o MetaEditor só entende caminhos com espaço quando as
aspas ficam depois dos dois-pontos (/compile:"C:\\..."), e o `wine` não repassa argumentos assim. Por isso, no
Linux, a linha de comando vai dentro de um .bat executado por `wine cmd /c`. O código de saída do MetaEditor é
1 quando compila e 0 quando falha: quem decide o resultado é o log, nunca o código de saída.
"""
import logging
import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, List, Optional, Tuple

from mt5_extracao.bootstrap import paths

from . import mql
from .catalog import MQL5, Robot

log = logging.getLogger(__name__)

METAEDITOR_EXE = "MetaEditor64.exe"
COMPILE_TIMEOUT = 300      # segundos
COPIED, SAME, KEPT = "copiado", "igual", "mantido"

_MESSAGE = re.compile(r"^(?P<file>.*?)\((?P<line>\d+),(?P<column>\d+)\)\s*:\s*(?P<kind>error|warning)\s+"
                      r"(?P<code>\d+)\s*:\s*(?P<text>.*)$")
_RESULT = re.compile(r"Result:\s*(\d+)\s+errors?,\s*(\d+)\s+warnings?", re.I)


# --- onde está o MT5 ------------------------------------------------------------------

def _get(config, section: str, key: str) -> str:
    return config.get(section, key, fallback="").strip() if config is not None else ""


def wine_prefix(config) -> Optional[Path]:
    """Prefixo Wine do MT5 (None no Windows)."""
    if paths.is_windows():
        return None
    value = _get(config, "MT5", "wine_prefix")
    return Path(value).expanduser() if value else None


def install_dir(config) -> Path:
    value = _get(config, "MT5", "path")
    if not value:
        raise FileNotFoundError("Caminho do MT5 não configurado. Rode: python -m mt5_extracao.bootstrap configure")
    path = Path(value).expanduser()
    return path.parent if path.name.lower() == paths.MT5_TERMINAL_EXE else path


def _appdata_roots(config) -> List[Path]:
    prefix = wine_prefix(config)
    if prefix is not None:
        return sorted((prefix / "drive_c" / "users").glob("*/AppData/Roaming"))
    return [Path(os.environ["APPDATA"])] if os.environ.get("APPDATA") else []


def data_dir(config, mt5=None) -> Path:
    """
    Pasta de dados do terminal (a que contém `MQL5/`). Com o MT5 conectado usa `terminal_info().data_path`;
    sem ele, procura a pasta do usuário que aponta para esta instalação e, por fim, a própria instalação
    (modo portátil, que é como o terminal roda no Wine).
    """
    prefix = wine_prefix(config)
    if mt5 is not None:
        try:
            raw = str(getattr(mt5.terminal_info(), "data_path", "") or "")
        except Exception as e:
            log.debug(f"terminal_info() falhou ao buscar a pasta de dados: {e}")
            raw = ""
        if raw:
            try:
                path = paths.wine_to_linux_path(raw, prefix) if prefix is not None else Path(raw)
            except ValueError:
                path = None
            if path is not None and (path / "MQL5").is_dir():
                return path
    base = install_dir(config)
    windows_base = paths.linux_to_wine_path(base, prefix) if prefix is not None else str(base)
    for root in _appdata_roots(config):
        for origin in sorted((root / "MetaQuotes" / "Terminal").glob("*/origin.txt")):
            try:
                same = mql.read_text(origin).strip().rstrip("\\").lower() == windows_base.rstrip("\\").lower()
            except OSError:
                continue
            if same and (origin.parent / "MQL5").is_dir():
                return origin.parent
    if (base / "MQL5").is_dir():
        return base
    raise FileNotFoundError(f"Pasta de dados do MT5 não encontrada (procurei 'MQL5' em {base}). "
                            "Abra o MetaTrader 5 uma vez para ele criar a pasta e tente de novo.")


def find_metaeditor(config) -> Path:
    """`[ROBOTS] metaeditor_path`, se preenchido; senão o MetaEditor64.exe da instalação do MT5."""
    configured = _get(config, "ROBOTS", "metaeditor_path")
    candidate = Path(configured).expanduser() if configured else install_dir(config) / METAEDITOR_EXE
    if not candidate.is_file():
        raise FileNotFoundError(f"MetaEditor não encontrado em {candidate}. Confira a instalação do MT5 ou "
                                "preencha [ROBOTS] metaeditor_path em config/config.ini.")
    return candidate


# --- instalar arquivos ----------------------------------------------------------------

@dataclass
class InstallItem:
    source: Path
    target: Path
    status: str = ""       # copiado | igual | mantido (preset que o usuário alterou no MT5)


def _experts_dir(path: Path) -> Optional[Path]:
    return next((p for p in path.parents if p.name.lower() == "experts"), None)


def _inside(path: Path, folder: Optional[Path]) -> Optional[Path]:
    """Caminho relativo de `path` dentro de `folder` (None se estiver fora)."""
    if folder is None:
        return None
    try:
        return path.resolve().relative_to(folder.resolve())
    except ValueError:
        return None


def install_plan(robot: Robot, data: Path) -> List[InstallItem]:
    """De onde para onde vai cada arquivo do robô, espelhando o layout Experts/ Include/ Presets/ do MT5."""
    if robot.kind != MQL5:
        raise ValueError(f"{robot.name} é uma estratégia Python: não há o que instalar no MT5.")
    if not robot.path or not Path(robot.path).is_file():
        raise FileNotFoundError(f"Arquivo do robô não encontrado: {robot.path or '(sem caminho)'}")
    main = Path(robot.path)
    experts = _experts_dir(main)
    include = experts.parent / "Include" if experts is not None else None
    mql5 = Path(data) / "MQL5"
    main_target = mql5 / "Experts" / (_inside(main, experts) or Path(main.name))
    plan = [InstallItem(main, main_target)]
    for name in robot.files:
        file = Path(name)
        if file == main:
            continue
        if file.suffix.lower() == ".set":
            plan.append(InstallItem(file, mql5 / "Presets" / file.name))
            continue
        for folder, base in ((include, mql5 / "Include"), (experts, mql5 / "Experts"),
                             (main.parent, main_target.parent)):
            relative = _inside(file, folder)
            if relative is not None:
                plan.append(InstallItem(file, base / relative))
                break
        else:
            raise ValueError(f"O include {file} fica fora da pasta do robô e de 'Include/': copie-o para "
                             f"'{mql5 / 'Include'}' à mão ou mova-o para junto do robô.")
    return plan


def install(robot: Robot, data: Path, overwrite_presets: bool = False) -> List[InstallItem]:
    """
    Copia o robô, seus includes e presets para a pasta de dados. Rodar de novo não muda nada. Fonte e includes
    acompanham a biblioteca; um preset que já existe com outro conteúdo é mantido (o usuário pode tê-lo ajustado
    no MT5), a não ser com `overwrite_presets`.
    """
    plan = install_plan(robot, data)
    for item in plan:
        if item.target.is_file() and (item.target.resolve() == item.source.resolve()
                                      or item.target.read_bytes() == item.source.read_bytes()):
            item.status = SAME
        elif item.target.is_file() and item.source.suffix.lower() == ".set" and not overwrite_presets:
            item.status = KEPT
        else:
            item.target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(item.source, item.target)
            item.status = COPIED
    return plan


# --- compilar -------------------------------------------------------------------------

@dataclass
class CompileMessage:
    kind: str              # error | warning
    file: str
    line: int
    column: int
    code: int
    text: str

    def __str__(self) -> str:
        label = "erro" if self.kind == "error" else "aviso"
        return f"{self.file}({self.line},{self.column}): {label} {self.code}: {self.text}"


@dataclass
class CompileResult:
    ok: bool
    errors: int = 0
    warnings: int = 0
    messages: List[CompileMessage] = field(default_factory=list)
    source: str = ""
    compiled: str = ""     # caminho do .ex5, quando gerado
    log_path: str = ""
    summary: str = ""

    def to_dict(self) -> dict:
        return {"ok": self.ok, "errors": self.errors, "warnings": self.warnings, "source": self.source,
                "compiled": self.compiled, "log": self.log_path, "summary": self.summary,
                "messages": [vars(m) for m in self.messages]}


def parse_compile_log(text: str) -> Tuple[Optional[int], Optional[int], List[CompileMessage]]:
    """(erros, avisos, mensagens) do log do MetaEditor. Erros e avisos são None se faltar a linha 'Result:'."""
    messages, seen = [], set()
    errors = warnings = None
    for line in text.splitlines():
        line = line.strip()
        found = _MESSAGE.match(line)
        if found:
            if line not in seen:           # o MetaEditor repete algumas linhas de erro
                seen.add(line)
                messages.append(CompileMessage(found["kind"], found["file"].strip(), int(found["line"]),
                                               int(found["column"]), int(found["code"]), found["text"].strip()))
            continue
        result = _RESULT.search(line)
        if result:
            errors, warnings = int(result.group(1)), int(result.group(2))
    return errors, warnings, messages


def _windows_path(path: Path, prefix: Optional[Path]) -> str:
    if prefix is None:
        return str(path)
    try:
        return paths.linux_to_wine_path(Path(path).resolve(), Path(prefix).resolve())
    except ValueError:
        raise ValueError(f"{path} fica fora do prefixo Wine do MT5 ({prefix}): o MetaEditor não o alcança.") from None


def run_windows_command(command_line: str, cwd: Path, prefix: Optional[Path] = None,
                        timeout: int = COMPILE_TIMEOUT) -> int:
    """Executa uma linha de comando do Windows: direto no Windows; no Linux, por um .bat dentro do Wine."""
    if paths.is_windows():
        return subprocess.run(command_line, cwd=str(cwd), timeout=timeout).returncode
    if prefix is None:
        raise RuntimeError("[MT5] wine_prefix vazio em config/config.ini. Execute ./install.sh")
    if shutil.which("wine") is None:
        raise RuntimeError("Wine não encontrado. Execute ./install.sh")
    temp = Path(prefix) / "drive_c" / "windows" / "temp"
    temp.mkdir(parents=True, exist_ok=True)
    handle, name = tempfile.mkstemp(suffix=".bat", prefix="mt5x_", dir=str(temp))
    bat = Path(name)
    try:
        script = "@echo off\r\n" + command_line.replace("%", "%%") + "\r\nexit /b %errorlevel%\r\n"
        try:
            data = script.encode("cp1252")
        except UnicodeEncodeError:
            raise ValueError("O caminho do robô tem caracteres que o Wine não repassa ao MetaEditor: "
                             "renomeie a pasta ou o arquivo usando só letras comuns.") from None
        with os.fdopen(handle, "wb") as f:
            handle = None
            f.write(data)
        env = dict(os.environ, WINEPREFIX=str(prefix), WINEDEBUG="-all")
        return subprocess.run(["wine", "cmd", "/c", _windows_path(bat, prefix)], cwd=str(cwd), env=env,
                              timeout=timeout, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode
    finally:
        if handle is not None:
            os.close(handle)
        bat.unlink(missing_ok=True)


def compile_source(source, metaeditor, prefix: Optional[Path] = None, run: Optional[Callable] = None,
                   timeout: int = COMPILE_TIMEOUT) -> CompileResult:
    """
    Compila um .mq5 que já está na pasta de dados do MT5. O log fica ao lado do fonte (<nome>.log).
    `run(linha_de_comando, cwd, prefix, timeout)` é quem executa; o padrão é `run_windows_command`.
    """
    source, metaeditor = Path(source), Path(metaeditor)
    if not source.is_file():
        raise FileNotFoundError(f"Fonte não encontrado: {source}")
    log_path, compiled = source.with_suffix(".log"), source.with_suffix(".ex5")
    log_path.unlink(missing_ok=True)
    command_line = '"{}" /compile:"{}" /log:"{}"'.format(_windows_path(metaeditor, prefix),
                                                         _windows_path(source, prefix),
                                                         _windows_path(log_path, prefix))
    result = CompileResult(False, source=str(source), log_path=str(log_path))
    try:
        code = (run or run_windows_command)(command_line, metaeditor.parent, prefix, timeout)
    except subprocess.TimeoutExpired:
        result.summary = (f"O MetaEditor não terminou em {timeout} s. Feche janelas do MetaEditor que estejam "
                          "abertas e tente de novo.")
        return result
    log.debug(f"MetaEditor terminou com código {code} para {source.name}")
    if not log_path.is_file():
        result.summary = ("O MetaEditor não gerou o log de compilação. Abra o MetaEditor uma vez à mão para "
                          "conferir se ele inicia e tente de novo.")
        return result
    errors, warnings, result.messages = parse_compile_log(mql.read_text(log_path))
    result.errors = sum(m.kind == "error" for m in result.messages) if errors is None else errors
    result.warnings = sum(m.kind == "warning" for m in result.messages) if warnings is None else warnings
    if errors is None:
        result.summary = f"Log de compilação incompleto (sem a linha de resultado): veja {log_path}."
    elif result.errors:
        result.summary = f"{result.errors} erro(s) e {result.warnings} aviso(s): corrija o fonte e compile de novo."
    elif not compiled.is_file():
        result.summary = f"Sem erros no log, mas o arquivo {compiled.name} não foi gerado: veja {log_path}."
    else:
        result.ok, result.compiled = True, str(compiled)
        result.summary = f"Compilado sem erros ({result.warnings} aviso(s)): {compiled.name}"
    return result


def compile_robot(config, robot: Robot, mt5=None, run: Optional[Callable] = None,
                  overwrite_presets: bool = False) -> Tuple[List[InstallItem], CompileResult]:
    """Copia o robô para a pasta de dados do MT5 e compila lá. Devolve (arquivos instalados, resultado)."""
    if robot.kind == MQL5 and not robot.has_source:
        raise ValueError(f"{robot.name} não tem código-fonte (.mq5): não há o que compilar.")
    items = install(robot, data_dir(config, mt5), overwrite_presets)
    return items, compile_source(items[0].target, find_metaeditor(config), wine_prefix(config), run)
