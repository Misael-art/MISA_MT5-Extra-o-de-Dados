"""Caminhos padrão do projeto e do ambiente Wine (somente biblioteca padrão)."""
import os
import sys
from pathlib import Path

# Raiz do repositório (…/mt5_extracao/bootstrap/paths.py -> …/)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

CONFIG_DIR_NAME = "config"
CONFIG_FILE_NAME = "config.ini"
SYMBOLS_FILE_NAME = "default_symbols.json"
ENV_FILE_NAME = ".env"

# Diretórios de trabalho criados na configuração inicial (relativos à raiz)
WORK_DIRS = ("config", "database", "logs", "exports")

# Executável do terminal MT5 (64 bits)
MT5_TERMINAL_EXE = "terminal64.exe"

# Porta padrão da ponte RPyC (Linux/Wine)
DEFAULT_BRIDGE_HOST = "127.0.0.1"
DEFAULT_BRIDGE_PORT = 18812

# Python do Windows instalado dentro do prefixo Wine pelo instalador Linux
WINE_PYTHON_WINDOWS_DIR = r"C:\Python311"


def is_windows() -> bool:
    return os.name == "nt" or sys.platform == "win32"


def config_path(root: Path = PROJECT_ROOT) -> Path:
    return Path(root) / CONFIG_DIR_NAME / CONFIG_FILE_NAME


def env_path(root: Path = PROJECT_ROOT) -> Path:
    return Path(root) / ENV_FILE_NAME


def symbols_path(root: Path = PROJECT_ROOT) -> Path:
    return Path(root) / CONFIG_DIR_NAME / SYMBOLS_FILE_NAME


def default_wine_prefix(env=None) -> Path:
    """
    Prefixo Wine dedicado usado pelo instalador Linux.

    Ordem: $MT5X_WINEPREFIX > $XDG_DATA_HOME/mt5-extracao/wine >
    ~/.local/share/mt5-extracao/wine. Um prefixo dedicado evita interferir
    no ~/.wine do usuário.
    """
    env = os.environ if env is None else env
    if env.get("MT5X_WINEPREFIX"):
        return Path(env["MT5X_WINEPREFIX"]).expanduser()
    data_home = env.get("XDG_DATA_HOME") or str(Path.home() / ".local" / "share")
    return Path(data_home).expanduser() / "mt5-extracao" / "wine"


def wine_prefix_candidates(env=None) -> list:
    """Prefixos Wine onde o MT5 costuma estar instalado (sem duplicatas)."""
    env = os.environ if env is None else env
    candidates = [default_wine_prefix(env)]
    if env.get("WINEPREFIX"):
        candidates.append(Path(env["WINEPREFIX"]).expanduser())
    home = Path.home()
    # ~/.mt5 é o prefixo usado pelo script oficial da MetaQuotes (mt5linux.sh)
    candidates += [home / ".wine", home / ".mt5"]
    unique = []
    for c in candidates:
        if c not in unique:
            unique.append(c)
    return unique


def linux_to_wine_path(path: Path, prefix: Path) -> str:
    """
    Converte um caminho dentro de <prefix>/drive_c para o formato Windows.

    Ex.: ~/.wine/drive_c/Program Files/MetaTrader 5 -> C:\\Program Files\\MetaTrader 5
    Lança ValueError se o caminho não estiver dentro de drive_c.
    """
    rel = Path(path).relative_to(Path(prefix) / "drive_c")
    parts = [p for p in rel.parts if p]
    return "C:\\" + "\\".join(parts)


def wine_to_linux_path(win_path: str, prefix: Path) -> Path:
    """Converte C:\\xxx\\yyy para <prefix>/drive_c/xxx/yyy."""
    p = win_path.replace("/", "\\")
    if len(p) < 2 or p[1] != ":" or p[0].upper() != "C":
        raise ValueError(f"Somente caminhos da unidade C: são suportados: {win_path}")
    rest = [part for part in p[2:].split("\\") if part]
    return Path(prefix, "drive_c", *rest)
