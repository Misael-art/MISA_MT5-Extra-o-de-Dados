"""
Diagnóstico do ambiente ("doctor").

Cada verificação retorna um Check com status OK / AVISO / FALHA e uma dica
de correção em português. O código de saída do comando é 1 se houver FALHA.
"""
import configparser
import importlib
import os
import shutil
import socket
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Optional

from . import paths
from .mt5_locator import has_terminal

OK, WARN, FAIL = "OK", "AVISO", "FALHA"

# Módulos obrigatórios para o app (nome de import, nome do pacote pip)
REQUIRED_MODULES = [
    ("pandas", "pandas"),
    ("numpy", "numpy"),
    ("sqlalchemy", "SQLAlchemy"),
    ("psutil", "psutil"),
    ("dotenv", "python-dotenv"),
    ("cryptography", "cryptography"),
    ("openpyxl", "openpyxl"),
    ("matplotlib", "matplotlib"),
    ("scipy", "scipy"),
    ("pytz", "pytz"),
]
OPTIONAL_MODULES = [("pandas_ta", "pandas-ta")]

MIN_PYTHON = (3, 9)
MAX_TESTED_PYTHON = (3, 13)


@dataclass
class Check:
    name: str
    status: str
    detail: str
    hint: str = ""


def check_python() -> Check:
    v = sys.version_info[:3]
    text = ".".join(map(str, v))
    if v[:2] < MIN_PYTHON:
        return Check("Python", FAIL, f"Python {text}",
                     f"Instale Python {MIN_PYTHON[0]}.{MIN_PYTHON[1]}+ (recomendado 3.11) e rode o instalador novamente.")
    if v[:2] > MAX_TESTED_PYTHON:
        return Check("Python", WARN, f"Python {text} (não testado)", "Recomendado: Python 3.11.")
    return Check("Python", OK, f"Python {text} em {sys.executable}")


def check_venv() -> Check:
    in_venv = sys.prefix != getattr(sys, "base_prefix", sys.prefix)
    if in_venv:
        return Check("Ambiente virtual", OK, sys.prefix)
    return Check("Ambiente virtual", WARN, "Executando fora de um ambiente virtual",
                 "Use os lançadores run.bat / run.sh, que ativam o .venv do projeto.")


def check_module(module: str, package: str, required: bool = True) -> Check:
    try:
        mod = importlib.import_module(module)
        version = getattr(mod, "__version__", "")
        return Check(f"Módulo {package}", OK, version or "importado")
    except Exception as e:  # ImportError ou erro na importação
        status = FAIL if required else WARN
        return Check(f"Módulo {package}", status, f"{type(e).__name__}: {e}",
                     f"Execute o instalador novamente ou: pip install {package}")


def check_tkinter() -> Check:
    try:
        importlib.import_module("tkinter")
        return Check("Interface gráfica (tkinter)", OK, "disponível")
    except Exception as e:
        hint = ("Linux: sudo apt install python3-tk (Debian/Ubuntu), sudo dnf install python3-tkinter "
                "(Fedora), sudo pacman -S tk (Arch)." if not paths.is_windows()
                else "Reinstale o Python marcando a opção 'tcl/tk and IDLE'.")
        return Check("Interface gráfica (tkinter)", FAIL, f"{type(e).__name__}: {e}", hint)


def _read_config(root: Path) -> Optional[configparser.ConfigParser]:
    path = paths.config_path(root)
    if not path.exists():
        return None
    cfg = configparser.ConfigParser(interpolation=None)
    cfg.read(path, encoding="utf-8")
    return cfg


def check_config(root: Path) -> List[Check]:
    checks = []
    path = paths.config_path(root)
    cfg = _read_config(root)
    if cfg is None:
        return [Check("Arquivo de configuração", FAIL, f"{path} não existe",
                      "Execute: python -m mt5_extracao.bootstrap configure")]
    checks.append(Check("Arquivo de configuração", OK, str(path)))

    mt5_path = cfg.get("MT5", "path", fallback="").strip()
    if not mt5_path:
        checks.append(Check("Caminho do MT5", FAIL, "[MT5] path vazio",
                            "Instale o MT5 pelo instalador ou informe --mt5-path ao configurar."))
    elif not has_terminal(Path(mt5_path)):
        checks.append(Check("Caminho do MT5", FAIL, f"terminal64.exe não encontrado em {mt5_path}",
                            "Reinstale o MT5 ou corrija [MT5] path em config/config.ini."))
    else:
        checks.append(Check("Caminho do MT5", OK, mt5_path))

    db_path = cfg.get("DATABASE", "path", fallback="database/mt5_data.db")
    db_dir = (Path(root) / db_path).parent if not Path(db_path).is_absolute() else Path(db_path).parent
    if db_dir.is_dir() and os.access(db_dir, os.W_OK):
        checks.append(Check("Pasta do banco de dados", OK, str(db_dir)))
    else:
        checks.append(Check("Pasta do banco de dados", FAIL, f"{db_dir} inexistente ou sem permissão de escrita",
                            "Execute: python -m mt5_extracao.bootstrap configure"))
    return checks


def check_mt5_module_windows() -> Check:
    return check_module("MetaTrader5", "MetaTrader5", required=True)


def check_linux_bridge(root: Path, probe: bool = True) -> List[Check]:
    checks = []
    cfg = _read_config(root)
    wine = shutil.which("wine")
    if wine:
        checks.append(Check("Wine", OK, wine))
    else:
        checks.append(Check("Wine", FAIL, "comando 'wine' não encontrado",
                            "Execute ./install.sh (instala o Wine automaticamente)."))

    if cfg is None:
        return checks
    prefix = cfg.get("MT5", "wine_prefix", fallback="").strip()
    wine_python = cfg.get("BRIDGE", "wine_python", fallback="").strip()
    if prefix and wine_python:
        try:
            host_py = paths.wine_to_linux_path(wine_python, Path(prefix))
            if host_py.is_file():
                checks.append(Check("Python do Windows (Wine)", OK, str(host_py)))
            else:
                checks.append(Check("Python do Windows (Wine)", FAIL, f"{host_py} não existe",
                                    "Execute ./install.sh novamente."))
        except ValueError as e:
            checks.append(Check("Python do Windows (Wine)", FAIL, str(e), "Corrija [BRIDGE] wine_python."))
    else:
        checks.append(Check("Python do Windows (Wine)", WARN, "não configurado",
                            "Execute ./install.sh para instalar o Python do Windows no Wine."))

    if not probe:
        return checks
    host = cfg.get("BRIDGE", "host", fallback=paths.DEFAULT_BRIDGE_HOST)
    port = cfg.getint("BRIDGE", "port", fallback=paths.DEFAULT_BRIDGE_PORT)
    if not _port_open(host, port):
        checks.append(Check("Ponte MT5 (RPyC)", WARN, f"nada escutando em {host}:{port}",
                            "Inicie a ponte: ./scripts/mt5-bridge.sh start"))
        return checks
    checks.append(probe_bridge(host, port))
    return checks


def _port_open(host: str, port: int, timeout: float = 1.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def probe_bridge(host: str, port: int) -> Check:
    """Conecta na ponte e pede a versão do módulo MetaTrader5 remoto."""
    try:
        import rpyc  # type: ignore
    except ImportError:
        return Check("Ponte MT5 (RPyC)", FAIL, "módulo rpyc ausente no Python do Linux",
                     "pip install -r requirements.txt")
    try:
        conn = rpyc.classic.connect(host, port)
        try:
            remote_mt5 = conn.modules["MetaTrader5"]
            version = str(getattr(remote_mt5, "__version__", "?"))
        finally:
            conn.close()
        return Check("Ponte MT5 (RPyC)", OK, f"MetaTrader5 {version} via {host}:{port}")
    except Exception as e:
        return Check("Ponte MT5 (RPyC)", FAIL, f"{type(e).__name__}: {e}",
                     "Veja logs/mt5_bridge.log e reinicie: ./scripts/mt5-bridge.sh restart")


def _bridge_enabled(root: Path) -> bool:
    cfg = _read_config(root)
    if cfg is None:
        return True  # sem config: mostra o que falta para a ponte
    return cfg.getboolean("BRIDGE", "enabled", fallback=True)


def run_checks(root: Path = paths.PROJECT_ROOT, probe_bridge_conn: bool = True) -> List[Check]:
    root = Path(root)
    checks = [check_python(), check_venv()]
    checks += [check_module(m, p) for m, p in REQUIRED_MODULES]
    checks += [check_module(m, p, required=False) for m, p in OPTIONAL_MODULES]
    checks.append(check_tkinter())
    checks += check_config(root)
    if paths.is_windows():
        checks.append(check_mt5_module_windows())
    elif _bridge_enabled(root):
        checks += check_linux_bridge(root, probe=probe_bridge_conn)
    else:
        checks.append(Check("Ponte MT5 (Wine)", WARN, "desativada ([BRIDGE] enabled = false)",
                            "Para usar o MT5 no Linux rode ./install.sh sem --no-mt5."))
    return checks


def format_report(checks: List[Check], color: bool = False) -> str:
    colors = {OK: "\033[32m", WARN: "\033[33m", FAIL: "\033[31m"}
    reset = "\033[0m"
    lines = []
    for c in checks:
        tag = f"[{c.status:^5}]"
        if color:
            tag = colors[c.status] + tag + reset
        lines.append(f"{tag} {c.name}: {c.detail}")
        if c.hint and c.status != OK:
            lines.append(f"        -> {c.hint}")
    fails = sum(1 for c in checks if c.status == FAIL)
    warns = sum(1 for c in checks if c.status == WARN)
    lines.append("")
    lines.append(f"Resumo: {len(checks) - fails - warns} OK, {warns} aviso(s), {fails} falha(s).")
    return "\n".join(lines)


def checks_to_dicts(checks: List[Check]) -> list:
    return [asdict(c) for c in checks]
