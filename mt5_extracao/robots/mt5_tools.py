"""
Ferramentas do MT5 para robôs MQL5: achar a pasta de dados e o MetaEditor, copiar o robô (com includes e
presets) para `MQL5/` e compilar, lendo o log do MetaEditor em erros e avisos.

    items, result = compile_robot(config, robot)     # instala e compila; result.ok diz se gerou o .ex5

    report = run_tester(config, data, tester_ini(...), "nome")    # roda o Strategy Tester e devolve o relatório

Nada aqui envia ordens nem inicia o robô numa conta: o Strategy Tester só simula. Os executáveis entram por
função injetada (`run`), para testar sem MT5.

Validado à mão no Wine (2026-10-12, Wine 11.17, T8.4): o MetaEditor só entende caminhos com espaço quando as
aspas ficam depois dos dois-pontos (/compile:"C:\\..."), e o `wine` não repassa argumentos assim. Por isso, no
Linux, a linha de comando vai dentro de um .bat executado por `wine cmd /c`. O código de saída do MetaEditor é
1 quando compila e 0 quando falha: quem decide o resultado é o log, nunca o código de saída.
O terminal aceita `/config:"C:\\x.ini"` com `[Tester]` e `ShutdownTerminal=1`: roda o teste, grava o relatório na
pasta de dados e fecha sozinho. No Wine ele precisa de `/portable` (sem isso abre uma pasta de dados vazia).
"""
import logging
import os
import re
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, List, Optional, Tuple

from mt5_extracao.bootstrap import display, paths

from . import mql, safety_lib
from .catalog import MQL5, Robot

log = logging.getLogger(__name__)

# Configuração em uso, para o modo oculto ([MT5] hidden). Quem carrega a configuração (CLI, interface) chama
# use_config uma vez; sem isso, os processos do Wine herdam o ambiente como está.
_ACTIVE_CONFIG = None


def use_config(config) -> None:
    global _ACTIVE_CONFIG
    _ACTIVE_CONFIG = config


def _wine_env(prefix) -> dict:
    env = display.child_env(_ACTIVE_CONFIG)
    env.update(WINEPREFIX=str(prefix), WINEDEBUG="-all")
    return env


METAEDITOR_EXE = "MetaEditor64.exe"
COMPILE_TIMEOUT = 300      # segundos
TESTER_TIMEOUT = 3600
TESTER_MODELS = {"0": "cada tick", "1": "OHLC de 1 minuto", "2": "só preços de abertura",
                 "4": "cada tick com ticks reais"}
# Medido no host (T10.10): no modelo "OHLC de 1 minuto" um stop atravessado por gap de fim de semana é preenchido
# no preço do stop (-1,00R); com "cada tick" e com ticks reais, no preço do gap (-1,66R), como numa conta. Por
# isso só os modelos por tick valem para a bateria, e o padrão é o de ticks reais.
DEFAULT_TESTER_MODEL = "4"
TICK_MODELS = ("0", "4")
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
        startup = display.windows_startup(display.enabled(_ACTIVE_CONFIG))
        return subprocess.run(command_line, cwd=str(cwd), timeout=timeout,
                              **({"startupinfo": startup} if startup else {})).returncode
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
        return subprocess.run(["wine", "cmd", "/c", _windows_path(bat, prefix)], cwd=str(cwd), env=_wine_env(prefix),
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


def install_robot(config, robot: Robot, mt5=None, overwrite_presets: bool = False,
                  upgrade_safety: bool = False) -> List[InstallItem]:
    """Instala o robô e, se ele a declara no robot.json, a biblioteca de segurança na versão pedida."""
    data = data_dir(config, mt5)
    if robot.kind == MQL5:
        safety_lib.ensure(config, robot, data, upgrade_safety)
    return install(robot, data, overwrite_presets)


def compile_robot(config, robot: Robot, mt5=None, run: Optional[Callable] = None,
                  overwrite_presets: bool = False, upgrade_safety: bool = False
                  ) -> Tuple[List[InstallItem], CompileResult]:
    """Copia o robô para a pasta de dados do MT5 e compila lá. Devolve (arquivos instalados, resultado)."""
    if robot.kind == MQL5 and not robot.has_source:
        raise ValueError(f"{robot.name} não tem código-fonte (.mq5): não há o que compilar.")
    items = install_robot(config, robot, mt5, overwrite_presets, upgrade_safety)
    return items, compile_source(items[0].target, find_metaeditor(config), wine_prefix(config), run)


# --- Strategy Tester ------------------------------------------------------------------

def common_files_dir(config) -> Optional[Path]:
    """`Common\\Files` do MetaTrader (onde o HADOUKEN grava HDK_trades_<magic>.csv). None se não existir."""
    for root in _appdata_roots(config):
        folder = root / "MetaQuotes" / "Terminal" / "Common" / "Files"
        if folder.is_dir():
            return folder
    return None


OURS, OTHER, UNKNOWN = "desta instalação", "de outra instalação", "não identificado"


def _owner(proc, config) -> str:
    """De qual instalação é este terminal. Sem `config`, todo terminal conta como nosso (comportamento antigo)."""
    if config is None:
        return OURS
    try:
        exe = install_dir(config) / paths.MT5_TERMINAL_EXE
    except FileNotFoundError:
        exe = None
    return {paths.OURS: OURS, paths.OTHER: OTHER}.get(paths.terminal_owner(proc, exe, wine_prefix(config)), UNKNOWN)


def terminal_processes(config=None) -> List[tuple]:
    """[(processo, dono)] de cada terminal64.exe em execução; dono = OURS, OTHER ou UNKNOWN."""
    import psutil
    found = []
    for proc in psutil.process_iter(["name", "cmdline"]):
        try:
            if paths.is_terminal_process(proc.info.get("name"), proc.info.get("cmdline")):
                found.append((proc, _owner(proc, config)))
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
    return found


def terminal_running(config=None) -> bool:
    """
    O terminal DESTA instalação está aberto? (O MT5 não abre duas vezes a mesma instalação.) Um terminal de outra
    instalação não conta. Um que não dá para identificar conta como aberto: na dúvida, o teste não roda.
    """
    return any(owner != OTHER for _, owner in terminal_processes(config))


def expert_name(compiled: Path, data: Path) -> str:
    """Nome do robô como o Tester espera: relativo a MQL5\\Experts, sem extensão, com barras do Windows."""
    relative = _inside(Path(compiled), Path(data) / "MQL5" / "Experts")
    if relative is None:
        raise ValueError(f"{compiled} não está em MQL5/Experts da pasta de dados do MT5.")
    return "\\".join(relative.with_suffix("").parts)


def tester_model(config) -> str:
    value = _get(config, "ROBOTS", "tester_model") or DEFAULT_TESTER_MODEL
    if value not in TESTER_MODELS:
        raise ValueError(f"[ROBOTS] tester_model = {value} é inválido. Use " +
                         ", ".join(f"{k} ({v})" for k, v in TESTER_MODELS.items()) + ".")
    return value


def tester_ini(expert: str, symbol: str, period: str, date_from, date_to, deposit: float, report: str,
               model: str = "1", parameters: str = "", visual: Optional[bool] = None,
               port: Optional[int] = None) -> str:
    """Texto do .ini de um teste único (sem otimização) que fecha o terminal ao terminar."""
    lines = ["[Tester]", f"Expert={expert}"]
    if parameters:
        lines.append(f"ExpertParameters={parameters}")
    lines += [f"Symbol={symbol}", f"Period={period}", "Optimization=0", f"Model={model}",
              f"FromDate={date_from:%Y.%m.%d}", f"ToDate={date_to:%Y.%m.%d}", "ForwardMode=0",
              f"Deposit={deposit:.2f}", f"Report={report}", "ReplaceReport=1", "ShutdownTerminal=1"]
    if visual is not None:
        lines += [f"Visual={int(visual)}", "UseLocal=1", "UseRemote=0", "UseCloud=0"]
    if port is not None:
        if isinstance(port, bool) or not isinstance(port, int) or not 1024 <= port <= 65535:
            raise ValueError("Porta do agente local inválida.")
        lines.append(f"Port={port}")
    return "\r\n".join(lines) + "\r\n"


def tester_run_log(data: Path, run_id: str) -> Path:
    """Caminho do log isolado que o mt5x guarda para uma execução do Strategy Tester."""
    if not re.fullmatch(r"[A-Za-z0-9_-]+", str(run_id)):
        raise ValueError("Identificador inválido para o log do Strategy Tester.")
    return Path(data) / "Tester" / "mt5x_runs" / f"{run_id}.log"


def _tester_log_snapshot(data: Path) -> dict:
    """Tamanho, data de alteração e cauda dos logs diários do terminal e de seus agentes."""
    root = Path(data) / "Tester"
    result = {}
    for path in root.glob("**/logs/*.log"):
        try:
            stat = path.stat()
            with path.open("rb") as handle:
                handle.seek(max(0, stat.st_size - 256))
                tail = handle.read()
            result[path] = {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns, "tail": tail}
        except OSError as e:
            log.debug(f"Não foi possível inspecionar o log do Tester {path.name}: {e}")
    return result


def _capture_tester_log(data: Path, before: dict, run_id: str) -> Optional[Path]:
    """Copia para um arquivo por execução somente os bytes novos nos logs do Tester."""
    try:
        after = _tester_log_snapshot(data)
        chunks = []
        for path in sorted(after, key=lambda p: str(p)):
            current = after[path]
            previous = before.get(path)
            if previous is None:
                start = 0
            elif current["mtime_ns"] == previous["mtime_ns"] and current["size"] == previous["size"]:
                continue
            elif current["size"] > previous["size"]:
                tail_start = max(0, previous["size"] - len(previous["tail"]))
                with path.open("rb") as handle:
                    handle.seek(tail_start)
                    same_tail = handle.read(len(previous["tail"])) == previous["tail"]
                start = previous["size"] if same_tail else 0
            else:
                start = 0
            with path.open("rb") as handle:
                handle.seek(start)
                content = handle.read()
            if content:
                relative = path.relative_to(Path(data))
                chunks.append((relative, content))
        if not chunks:
            return None
        target = tester_run_log(data, run_id)
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("wb") as handle:
            for relative, content in chunks:
                handle.write(f"\n===== {relative.as_posix()} =====\n".encode("utf-8"))
                handle.write(content)
                if not content.endswith(b"\n"):
                    handle.write(b"\n")
        return target
    except (OSError, ValueError) as e:
        log.warning(f"Não foi possível guardar um log isolado do Strategy Tester: {e}")
        return None


def write_tester_set(data: Path, name: str, values: dict) -> str:
    """Grava os parâmetros do teste em MQL5/Profiles/Tester/<name>.set e devolve o nome do arquivo."""
    folder = Path(data) / "MQL5" / "Profiles" / "Tester"
    folder.mkdir(parents=True, exist_ok=True)
    text = "".join(f"{key}={value}\r\n" for key, value in values.items())
    (folder / f"{name}.set").write_bytes(text.encode("utf-16"))
    return f"{name}.set"


def run_tester(config, data: Path, ini_text: str, name: str, run: Optional[Callable] = None,
               timeout: int = TESTER_TIMEOUT, is_running: Optional[Callable] = None,
               run_id: Optional[str] = None) -> Path:
    """
    Roda o Strategy Tester com o .ini dado e devolve o caminho do relatório (.htm). O terminal precisa estar
    fechado. `name` é o nome do relatório (o mesmo passado em `Report=`), sem extensão.
    """
    data = Path(data)
    if is_running() if is_running else terminal_running(config):
        raise RuntimeError("O MetaTrader 5 está aberto e não roda o teste em uma segunda janela. Feche o MT5 "
                           "(e pare a ponte: ./scripts/mt5-bridge.sh stop) ou repita com --close-terminal.")
    prefix = wine_prefix(config)
    terminal = install_dir(config) / paths.MT5_TERMINAL_EXE
    if not terminal.is_file():
        raise FileNotFoundError(f"Terminal não encontrado em {terminal}. Confira [MT5] path em config/config.ini.")
    report = data / f"{name}.htm"
    report.unlink(missing_ok=True)
    ini = data / f"{name}.ini"
    ini.write_bytes(ini_text.encode("utf-16"))
    logs_before = _tester_log_snapshot(data) if run_id else {}
    portable = prefix is not None or data.resolve() == terminal.parent.resolve()
    command_line = '"{}"{} /config:"{}"'.format(_windows_path(terminal, prefix), " /portable" if portable else "",
                                                _windows_path(ini, prefix))
    try:
        (run or run_windows_command)(command_line, terminal.parent, prefix, timeout)
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"O teste não terminou em {timeout // 60} min. Feche o MetaTrader 5 que ficou aberto e "
                           "use um período menor ou [ROBOTS] tester_model = 1.") from None
    finally:
        ini.unlink(missing_ok=True)
        if run_id:
            _capture_tester_log(data, logs_before, run_id)
    if not report.is_file():
        if is_running() if is_running else terminal_running(config):
            # Visto no host: com uma atualização pendente, o terminal chama o LiveUpdate, sai sem testar e é
            # reaberto pelo atualizador (já sem o nosso .ini). A segunda execução funciona.
            raise RuntimeError("O Strategy Tester não gerou o relatório e o MetaTrader 5 ficou aberto: ele "
                               "provavelmente acabou de se atualizar e reabriu sozinho. Feche o MT5 (ou use "
                               "--close-terminal) e rode de novo.")
        raise RuntimeError("O Strategy Tester não gerou o relatório. Causas comuns: o terminal sem conta conectada, "
                           "o ativo não existe na corretora ou falta histórico no período. Veja o log em "
                           f"{data / 'Tester' / 'logs'}.")
    return report


def ask_terminal_to_close(config=None) -> bool:
    """
    Pede ao terminal para fechar normalmente (`taskkill` sem /F, que manda o mesmo aviso do botão de fechar).
    Devolve False se não deu para pedir; quem chama cai no encerramento à força.
    """
    command = ["taskkill", "/IM", paths.MT5_TERMINAL_EXE]
    try:
        if paths.is_windows():
            return subprocess.run(command, capture_output=True, timeout=15).returncode == 0
        prefix = wine_prefix(config)
        if prefix is None or shutil.which("wine") is None:
            return False
        return subprocess.run(["wine"] + command, env=_wine_env(prefix), capture_output=True, timeout=30).returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False


def close_terminal(timeout: float = 20.0, config=None) -> bool:
    """
    Encerra o terminal DESTA instalação. Primeiro pede para ele fechar (como o botão de fechar da janela), que é
    quando ele grava a configuração e o perfil de gráficos; se não sair em `timeout` s, encerra à força (terminate
    e kill). Encerrar à força de saída fazia o terminal perder o arranjo e o robô que estava no gráfico (T8.7).
    True se nenhum ficou aberto. Nunca encerra o terminal de outra instalação (pode haver um robô operando nele) nem um que não dá para
    identificar: nesse caso para e pede que a pessoa feche.
    """
    import psutil
    found = terminal_processes(config)
    if any(owner == UNKNOWN for _, owner in found):
        raise RuntimeError("Há um MetaTrader 5 aberto que não dá para saber se é o desta instalação. Não vou "
                           "fechá-lo: feche à mão o terminal que usa esta instalação e tente de novo.")
    ours = [proc for proc, owner in found if owner == OURS]
    if ours and ask_terminal_to_close(config):
        _, ours = psutil.wait_procs(ours, timeout=timeout)       # deu tempo de gravar e sair?
    procs = []
    for proc in ours:
        try:
            proc.terminate()
            procs.append(proc)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
    _, alive = psutil.wait_procs(procs, timeout=timeout)
    for proc in alive:
        try:
            proc.kill()
        except psutil.NoSuchProcess:
            pass
    return not terminal_running(config)


def _bridge(action: str) -> int:
    script = paths.PROJECT_ROOT / "scripts" / "mt5-bridge.sh"
    if paths.is_windows() or not script.is_file():
        return 3                # sem ponte: no Windows o app fala direto com o MT5
    env = dict(os.environ)
    if display.enabled(_ACTIVE_CONFIG):
        env[display.ENV_SWITCH] = "1"        # a ponte lê config/config.ini; isto cobre o modo ligado só nesta execução
    return subprocess.run([str(script), action], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                          env=env).returncode


def release_terminal(config=None) -> bool:
    """Para a ponte (Linux) e fecha o terminal, para o Tester poder abrir. Devolve True se a ponte estava ativa."""
    if any(owner == UNKNOWN for _, owner in terminal_processes(config)):
        close_terminal(config=config)          # levanta o erro antes de mexer na ponte
    bridge_was_running = _bridge("status") == 0
    if bridge_was_running:
        _bridge("stop")
    if not close_terminal(config=config):
        raise RuntimeError("Não foi possível fechar o MetaTrader 5. Feche-o à mão e tente de novo.")
    return bridge_was_running


def restart_bridge() -> None:
    if _bridge("start") != 0:
        log.warning("A ponte não voltou a subir depois do teste. Rode: ./scripts/mt5-bridge.sh start")


# --- iniciar um robô no terminal (T8.7) -----------------------------------------------
# Atenção: este trecho NÃO foi validado com o terminal real (só com processos simulados). As chaves de
# [StartUp] e [Experts] seguem a documentação do MT5 ("iniciar a plataforma com configuração própria").

KILL_FILE = "HDK_KILL.txt"


def kill_file(config) -> Path:
    """Arquivo que a camada de segurança do HADOUKEN lê a cada ciclo: enquanto existir, nenhum robô opera."""
    folder = common_files_dir(config)
    if folder is None:
        roots = _appdata_roots(config)
        if not roots:
            raise FileNotFoundError("Pasta Common\\Files do MetaTrader não encontrada. Abra o MT5 uma vez.")
        folder = roots[0] / "MetaQuotes" / "Terminal" / "Common" / "Files"
    return folder / KILL_FILE


def startup_ini(expert: str, symbol: str, period: str, login, server: str, parameters: str = "",
                live_trading: bool = True, protect_context: bool = False) -> str:
    """
    .ini que abre o terminal na conta conferida e coloca o robô num gráfico. Fixar Login e Server impede que o
    robô suba em outra conta que não a verificada como demo. DLL fica desligada. Com `live_trading=False` o robô
    entra no gráfico com a negociação desligada (é como a visão mostra um painel que não passou na bateria).
    """
    lines = ["[Common]", f"Login={login}", f"Server={server}",
             "[Experts]", "Enabled=1", f"AllowLiveTrading={1 if live_trading else 0}", "AllowDllImport=0",
             "[StartUp]", f"Expert={expert}", f"Symbol={symbol}", f"Period={period}"]
    if parameters:
        lines.append(f"ExpertParameters={parameters}")
    if protect_context:
        # As chaves pertencem a Experts; desligam EAs quando muda conta ou perfil.
        lines[lines.index("[StartUp]"):lines.index("[StartUp]")] = ["Account=1", "Profile=1"]
    return "\r\n".join(lines) + "\r\n"


def confirm_startup(data: Path, expert: str, since: datetime, timeout: float = 60.0, sleep=time.sleep) -> None:
    """
    Confere no log do terminal que ele carregou o arquivo de início e pôs o robô no gráfico. Sem isso o programa
    dizia "iniciado" mesmo quando o terminal abria sem o robô (T8.7). `expert` é o nome do robô, sem pasta;
    `since`, o instante em que o terminal foi aberto. Levanta RuntimeError, com o que fazer, se o terminal recusar
    o arquivo ou se o robô não aparecer em `timeout` segundos.
    """
    log_file = Path(data) / "logs" / (since.strftime("%Y%m%d") + ".log")
    stamp = since.strftime("%H:%M:%S")
    name = Path(expert.replace("\\", "/")).name.lower()
    deadline = time.time() + timeout
    while True:
        lines = []
        if log_file.is_file():
            try:
                lines = [l for l in read_log_text(log_file).splitlines() if len(l.split("\t")) >= 5 and l.split("\t")[2][:8] >= stamp]
            except OSError:
                lines = []
        if any("cannot load config" in l for l in lines):
            raise RuntimeError("O MetaTrader 5 abriu, mas não conseguiu ler o arquivo de início: o robô NÃO está no gráfico. "
                               f"Veja {log_file} e tente de novo; se repetir, rode: mt5x doctor")
        if any("expert " + name in l.lower() and "loaded successfully" in l for l in lines):
            return
        if time.time() >= deadline:
            raise RuntimeError(f"O MetaTrader 5 abriu, mas em {int(timeout)} s o log não mostrou o robô {Path(expert).name} "
                               f"carregado: ele pode NÃO estar no gráfico. Confira no terminal e em {log_file}.")
        sleep(2.0)


def read_log_text(path: Path) -> str:
    """Log do terminal (UTF-16; alguns builds gravam UTF-8)."""
    data = Path(path).read_bytes()
    if data[:2] in (b"\xff\xfe", b"\xfe\xff"):
        return data.decode("utf-16", errors="replace")
    return data.decode("utf-8", errors="replace")


SPAWN_BAT = "mt5x_spawn.bat"          # .bat que abre o terminal sem esperar (Wine)


def spawn_windows_command(command_line: str, cwd: Path, prefix: Optional[Path] = None) -> None:
    """Como run_windows_command, mas sem esperar o programa terminar (o terminal fica aberto)."""
    if paths.is_windows():
        startup = display.windows_startup(display.enabled(_ACTIVE_CONFIG))
        subprocess.Popen(command_line, cwd=str(cwd), **({"startupinfo": startup} if startup else {}))
        return
    # No Wine a linha vai num .bat, sem `start`: o `start` do cmd reescreve as aspas de /config:"C:\...ini" e o
    # terminal recebia o caminho com uma aspa sobrando ("cannot load config"), abrindo sem o robô (visto no host
    # na T8.7). Quem não espera é o Popen. O .bat tem nome fixo porque precisa existir enquanto o cmd o lê.
    if prefix is None:
        raise RuntimeError("[MT5] wine_prefix vazio em config/config.ini. Execute ./install.sh")
    if shutil.which("wine") is None:
        raise RuntimeError("Wine não encontrado. Execute ./install.sh")
    temp = Path(prefix) / "drive_c" / "windows" / "temp"
    temp.mkdir(parents=True, exist_ok=True)
    bat = temp / SPAWN_BAT
    try:
        bat.write_bytes(("@echo off\r\n" + command_line.replace("%", "%%") + "\r\n").encode("cp1252"))
    except UnicodeEncodeError:
        raise ValueError("O caminho do MetaTrader 5 tem caracteres que o Wine não repassa: use uma pasta só com "
                         "letras comuns.") from None
    subprocess.Popen(["wine", "cmd", "/c", _windows_path(bat, prefix)], cwd=str(cwd), env=_wine_env(prefix),
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)


def start_terminal(config, data: Path, ini_text: str, name: str, spawn: Optional[Callable] = None,
                   is_running: Optional[Callable] = None) -> Path:
    """
    Abre o terminal com o .ini dado (gravado como <name>.ini na pasta de dados) e devolve o caminho do .ini. Com
    `ini_text` vazio o terminal abre sem /config, como pelo atalho (é o que "Abrir visão MT5" usa sem perfil próprio).
    """
    data = Path(data)
    if is_running() if is_running else terminal_running(config):
        raise RuntimeError("O MetaTrader 5 está aberto: o robô só entra no gráfico quando o terminal é iniciado "
                           "pelo programa. Feche o MT5 ou repita com --close-terminal.")
    prefix = wine_prefix(config)
    terminal = install_dir(config) / paths.MT5_TERMINAL_EXE
    if not terminal.is_file():
        raise FileNotFoundError(f"Terminal não encontrado em {terminal}. Confira [MT5] path em config/config.ini.")
    ini = data / f"{name}.ini"
    ini.write_bytes(ini_text.encode("utf-16"))
    portable = prefix is not None or data.resolve() == terminal.parent.resolve()
    command_line = '"{}"{}'.format(_windows_path(terminal, prefix), " /portable" if portable else "")
    if ini_text:
        command_line += ' /config:"{}"'.format(_windows_path(ini, prefix))
    (spawn or spawn_windows_command)(command_line, terminal.parent, prefix)
    return ini
