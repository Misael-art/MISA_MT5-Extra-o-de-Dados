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


# Ambiente Python do projeto. Uma composição pode ser aberta por dois sistemas no mesmo disco
# (dual boot / WSL): o `.venv` pertence ao sistema que o criou e nunca é reaproveitado nem
# sobrescrito pelo outro. Esse outro usa um único diretório próprio, com configuração própria.
VENV_DIR_NAME = ".venv"
PLATFORM_VENV_DIR_NAMES = {"windows": ".venv-windows", "posix": ".venv-posix"}
WINDOWS, POSIX, MIXED = "windows", "posix", "mixed"


def host_platform(windows=None) -> str:
    return WINDOWS if (is_windows() if windows is None else windows) else POSIX


def venv_platform(directory: Path):
    """Sistema dono de um ambiente virtual: WINDOWS, POSIX, MIXED (as duas árvores) ou None (ausente).

    Lê só a estrutura: um `bin/python` Linux pode ser link quebrado quando visto do Windows."""
    directory = Path(directory)
    if not directory.is_dir():
        return None
    has_windows = (directory / "Scripts" / "python.exe").is_file()
    posix_python = directory / "bin" / "python"
    has_posix = posix_python.is_symlink() or posix_python.exists()
    if has_windows and has_posix:
        return MIXED
    if has_windows:
        return WINDOWS
    if has_posix:
        return POSIX
    cfg = directory / "pyvenv.cfg"
    if cfg.is_file():
        for line in cfg.read_text(encoding="utf-8", errors="replace").splitlines():
            key, _, value = line.partition("=")
            if key.strip() == "home":
                value = value.strip()
                return WINDOWS if len(value) > 1 and value[1] == ":" else POSIX
    return None


def environment_dir(root: Path = PROJECT_ROOT, windows=None) -> Path:
    """Único ambiente do projeto para este sistema.

    1. diretório próprio do sistema (`.venv-windows`/`.venv-posix`) se já contém ambiente dele;
    2. `.venv` se é deste sistema ou ainda não existe (instalação comum, inalterada);
    3. diretório próprio quando `.venv` é de outro sistema ou está misturado (preservado)."""
    root = Path(root)
    own = host_platform(windows)
    dedicated = root / PLATFORM_VENV_DIR_NAMES[own]
    if venv_platform(dedicated) == own:
        return dedicated
    primary = root / VENV_DIR_NAME
    if venv_platform(primary) in (own, None):
        return primary
    return dedicated


def venv_python(directory: Path, windows=None) -> Path:
    windows = is_windows() if windows is None else windows
    return Path(directory) / ("Scripts/python.exe" if windows else "bin/python")


def foreign_environment(root: Path = PROJECT_ROOT, windows=None):
    """`.venv` de outro sistema preservado ao lado do ambiente atual (ou None)."""
    primary = Path(root) / VENV_DIR_NAME
    if environment_dir(root, windows) == primary:
        return None
    return primary


def config_file_name(root: Path = PROJECT_ROOT, windows=None) -> str:
    """`config.ini` pertence ao dono do `.venv`; o outro sistema usa `config.<sistema>.ini`.

    Caminhos de terminal, Wine e bibliotecas diferem entre sistemas: compartilhar o arquivo
    faria cada um sobrescrever a configuração do outro."""
    if foreign_environment(root, windows) is None:
        return CONFIG_FILE_NAME
    return "config.%s.ini" % host_platform(windows)


def config_path(root: Path = PROJECT_ROOT, windows=None) -> Path:
    return Path(root) / CONFIG_DIR_NAME / config_file_name(root, windows)


def shared_config_path(root: Path = PROJECT_ROOT) -> Path:
    """`config.ini` do dono do `.venv` (semente somente leitura para o outro sistema)."""
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
    rel = Path(path).resolve().relative_to((Path(prefix) / "drive_c").resolve())
    parts = [p for p in rel.parts if p]
    return "C:\\" + "\\".join(parts)


def wine_to_linux_path(win_path: str, prefix: Path) -> Path:
    """Converte C:\\xxx\\yyy para <prefix>/drive_c/xxx/yyy."""
    p = win_path.replace("/", "\\")
    if len(p) < 2 or p[1] != ":" or p[0].upper() != "C":
        raise ValueError(f"Somente caminhos da unidade C: são suportados: {win_path}")
    rest = [part for part in p[2:].split("\\") if part]
    return Path(prefix, "drive_c", *rest)


def is_terminal_process(name, cmdline=None) -> bool:
    """
    O processo é o terminal do MT5? No Windows o nome do processo é terminal64.exe. No Wine o processo aparece
    com outro nome (ex.: "main") e o executável é o wine-preloader: só o primeiro item da linha de comando traz
    o caminho do terminal64.exe (conferido com Wine 11.17).
    """
    if (name or "").lower() == MT5_TERMINAL_EXE:
        return True
    first = cmdline[0] if cmdline else ""
    return str(first).replace("\\", "/").rsplit("/", 1)[-1].lower() == MT5_TERMINAL_EXE


OURS, OTHER, UNKNOWN = "ours", "other", "unknown"


def _same_path(a, b) -> bool:
    return str(a).replace("\\", "/").rstrip("/").lower() == str(b).replace("\\", "/").rstrip("/").lower()


def terminal_owner(proc, terminal_exe=None, wine_prefix=None) -> str:
    """
    De qual instalação é este processo de terminal: OURS, OTHER ou UNKNOWN. `proc` é um processo do psutil.
    No Windows compara o caminho do executável com `terminal_exe`. No Wine o caminho é o mesmo em todo prefixo
    (C:\\...), então compara a variável WINEPREFIX do processo com `wine_prefix` (sem a variável, o Wine usa
    ~/.wine). UNKNOWN quando não dá para ler o processo ou falta a referência.
    """
    try:
        if is_windows():
            if not terminal_exe:
                return UNKNOWN
            return OURS if _same_path(proc.exe(), terminal_exe) else OTHER
        if not wine_prefix:
            return UNKNOWN
        theirs = (proc.environ() or {}).get("WINEPREFIX") or str(Path.home() / ".wine")
        return OURS if _same_path(Path(theirs).expanduser(), Path(wine_prefix).expanduser()) else OTHER
    except Exception:           # acesso negado, processo que sumiu...
        return UNKNOWN


def state_dir(env=None) -> Path:
    """Estado local por usuário, comum às cópias do app e independente do Wine/TMPDIR."""
    env = os.environ if env is None else env
    if is_windows():
        root = env.get("LOCALAPPDATA") or env.get("APPDATA")
        root = Path(root) if root else Path.home() / "AppData" / "Local"
    else:
        root = Path(env["XDG_STATE_HOME"]) if env.get("XDG_STATE_HOME") else Path.home() / ".local" / "state"
    return root.expanduser() / "mt5-extracao"
