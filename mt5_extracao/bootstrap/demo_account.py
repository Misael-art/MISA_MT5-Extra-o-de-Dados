"""
Conta de demonstração gratuita do MetaTrader 5 (somente biblioteca padrão).

O app usa a conta em que o terminal estiver logado; login/senha no .env são
opcionais. Um MT5 recém-instalado não tem conta e abre um assistente: aqui
detectamos esse estado e preenchemos o assistente pelo usuário
(scripts/mt5_demo_account.py, executado no Python do Windows / Wine).
"""
import configparser
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Callable, Optional

from . import paths

# Dados do projeto usados no cadastro (não são dados pessoais do usuário).
# O MT5 recusa telefones com dígitos repetidos; o dia é igual ao mês para a data
# valer em dd/mm/aaaa e mm/dd/aaaa.
DEMO_DEFAULTS = {
    "first": "Projeto",
    "last": "Extracao",
    "email": "mt5-extracao@example.com",
    "phone": "11912345678",
    "birth": "1990-01-01",
}
TERMS_NOTICE = ("Ao abrir a conta você concorda com os termos e condições e com a política de "
                "proteção de dados da MetaQuotes, exibidos no assistente do MT5.")

ACCOUNTS_FILE = "accounts.dat"
SCRIPT_REL = Path("scripts") / "mt5_demo_account.py"
EXIT_REJECTED = 4


def _child_ci(directory: Path, name: str) -> Optional[Path]:
    """Filho de `directory` com esse nome, sem diferenciar maiúsculas (Config/config)."""
    try:
        for entry in directory.iterdir():
            if entry.name.lower() == name.lower():
                return entry
    except OSError:
        pass
    return None


def _has_accounts_file(data_dir: Path) -> bool:
    config_dir = _child_ci(data_dir, "config")
    return bool(config_dir and _child_ci(config_dir, ACCOUNTS_FILE))


def _roaming_data_dirs(mt5_dir: Path, appdata_roots) -> list:
    """Pastas de dados em AppData\\Roaming\\MetaQuotes\\Terminal\\<id> desta instalação."""
    found = []
    for appdata in appdata_roots:
        base = Path(appdata) / "MetaQuotes" / "Terminal"
        if not base.is_dir():
            continue
        for data_dir in base.iterdir():
            origin = data_dir / "origin.txt"
            try:
                text = origin.read_bytes().decode("utf-16", errors="ignore").strip().lower()
            except OSError:
                continue
            if text.replace("/", "\\").rstrip("\\").endswith(mt5_dir.name.lower()):
                found.append(data_dir)
    return found


def has_account(mt5_dir, wine_prefix=None, env=None) -> bool:
    """
    True se o terminal já tem alguma conta cadastrada (arquivo Config/accounts.dat,
    criado pelo MT5 quando a primeira conta é aberta).
    """
    env = os.environ if env is None else env
    mt5_dir = Path(mt5_dir)
    if _has_accounts_file(mt5_dir):
        return True
    appdata_roots = []
    if wine_prefix:
        users = Path(wine_prefix) / "drive_c" / "users"
        if users.is_dir():
            appdata_roots = [u / "AppData" / "Roaming" for u in users.iterdir()]
    elif env.get("APPDATA"):
        appdata_roots = [Path(env["APPDATA"])]
    return any(_has_accounts_file(d) for d in _roaming_data_dirs(mt5_dir, appdata_roots))


def _terminal_running() -> bool:
    try:
        if paths.is_windows():
            out = subprocess.run(["tasklist", "/FI", f"IMAGENAME eq {paths.MT5_TERMINAL_EXE}"],
                                 capture_output=True, text=True, timeout=20).stdout
            return paths.MT5_TERMINAL_EXE.lower() in out.lower()
        # -x: nome exato do processo (com -f, qualquer linha de comando que cite o nome casaria)
        return subprocess.run(["pgrep", "-x", paths.MT5_TERMINAL_EXE],
                              capture_output=True, timeout=20).returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False


def _linux_to_z(path: Path) -> str:
    """Caminho do Linux visto pelo Wine (unidade Z:)."""
    return "Z:" + str(Path(path).resolve()).replace("/", "\\")


def read_settings(root: Path) -> dict:
    cfg = configparser.ConfigParser(interpolation=None)
    cfg.read(paths.config_path(root), encoding="utf-8")
    return {
        "mt5_dir": cfg.get("MT5", "path", fallback="").strip(),
        "windows_path": cfg.get("MT5", "windows_path", fallback="").strip(),
        "wine_prefix": cfg.get("MT5", "wine_prefix", fallback="").strip(),
        "wine_python": cfg.get("BRIDGE", "wine_python",
                               fallback=paths.WINE_PYTHON_WINDOWS_DIR + "\\python.exe").strip(),
    }


def open_demo_account(root: Path, data: Optional[dict] = None,
                      log: Callable[[str], None] = print, wait_account: int = 60) -> int:
    """
    Abre o terminal (se preciso) e preenche o assistente de conta demo.
    Retorna 0 se a conta existir ao final; outro valor é o código do script.
    """
    root = Path(root)
    data = {**DEMO_DEFAULTS, **{k: v for k, v in (data or {}).items() if v}}
    cfg = read_settings(root)
    if not cfg["mt5_dir"] or not Path(cfg["mt5_dir"]).is_dir():
        log("MetaTrader 5 não configurado. Execute o instalador primeiro.")
        return 1
    prefix = cfg["wine_prefix"] or None
    if has_account(cfg["mt5_dir"], prefix):
        log("O MetaTrader 5 já tem uma conta cadastrada. Nada a fazer.")
        return 0

    script = root / SCRIPT_REL
    args = ["--first", data["first"], "--last", data["last"], "--email", data["email"],
            "--phone", data["phone"], "--birth", data["birth"]]
    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    if paths.is_windows():
        terminal = [str(Path(cfg["mt5_dir"]) / paths.MT5_TERMINAL_EXE)]
        runner = [sys.executable, str(script)]
    else:
        if not prefix:
            log("[MT5] wine_prefix vazio em config/config.ini. Execute ./install.sh")
            return 1
        env.update({"WINEPREFIX": prefix, "WINEDEBUG": "-all"})
        win_dir = cfg["windows_path"] or paths.linux_to_wine_path(Path(cfg["mt5_dir"]), Path(prefix))
        terminal = ["wine", win_dir + "\\" + paths.MT5_TERMINAL_EXE]
        runner = ["wine", cfg["wine_python"], _linux_to_z(script)]

    if not _terminal_running():
        log("Abrindo o MetaTrader 5...")
        subprocess.Popen(terminal, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                         start_new_session=not paths.is_windows())
    log("Abrindo a conta de demonstração no assistente do MT5 (leva cerca de 1 minuto)...")
    code = subprocess.run(runner + args, env=env).returncode
    if code != 0:
        return code
    end = time.time() + wait_account
    while time.time() < end:
        if has_account(cfg["mt5_dir"], prefix):
            log("Conta de demonstração pronta. O MT5 passa a entrar nela sozinho.")
            return 0
        time.sleep(2)
    log("O assistente terminou, mas a conta não apareceu no terminal.")
    return 1
