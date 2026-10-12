"""
Atalhos do aplicativo na área de trabalho e no menu de aplicativos / menu Iniciar
(somente biblioteca padrão). Usado pelo instalador, pelo comando
`python -m mt5_extracao.bootstrap shortcuts` e pela tela de Configurações.
"""
import os
import subprocess
from pathlib import Path
from typing import List, Optional, Tuple

from . import paths

APP_NAME = "MT5 Extração de Dados"
APP_COMMENT = "Extração de dados do MetaTrader 5"
LINUX_FILE = "mt5-extracao.desktop"
WINDOWS_FILE = "MT5 Extração.lnk"   # mesmo nome usado pelo instalador do Windows
ASSETS = Path(__file__).resolve().parents[1] / "assets"


def icon_path() -> Path:
    return ASSETS / ("icon.ico" if paths.is_windows() else "icon.png")


def desktop_dir(env=None) -> Path:
    """Pasta da área de trabalho do usuário (respeita o nome traduzido, ex.: 'Área de trabalho')."""
    env = os.environ if env is None else env
    if paths.is_windows():
        return Path(env.get("USERPROFILE", str(Path.home()))) / "Desktop"
    try:
        out = subprocess.run(["xdg-user-dir", "DESKTOP"], capture_output=True, text=True, timeout=10).stdout.strip()
        if out and Path(out) != Path.home():
            return Path(out)
    except (OSError, subprocess.SubprocessError):
        pass
    return Path.home() / "Desktop"


def menu_dir(env=None) -> Path:
    """Pasta do menu de aplicativos (Linux) ou do menu Iniciar (Windows) do usuário."""
    env = os.environ if env is None else env
    if paths.is_windows():
        appdata = env.get("APPDATA") or str(Path.home() / "AppData" / "Roaming")
        return Path(appdata) / "Microsoft" / "Windows" / "Start Menu" / "Programs"
    data_home = env.get("XDG_DATA_HOME") or str(Path.home() / ".local" / "share")
    return Path(data_home).expanduser() / "applications"


def targets(desktop: bool = True, menu: bool = True, env=None) -> List[Path]:
    name = WINDOWS_FILE if paths.is_windows() else LINUX_FILE
    found = []
    if desktop:
        found.append(desktop_dir(env) / name)
    if menu:
        found.append(menu_dir(env) / name)
    return found


def desktop_entry(root: Path) -> str:
    """Conteúdo do arquivo .desktop (Linux)."""
    root = Path(root)
    return "\n".join([
        "[Desktop Entry]",
        "Type=Application",
        f"Name={APP_NAME}",
        f"Comment={APP_COMMENT}",
        f'Exec="{root / "run.sh"}"',
        f"Path={root}",
        f"Icon={icon_path()}",
        "Terminal=false",
        "Categories=Office;Finance;",
        "StartupNotify=true",
        "",
    ])


def _write_linux(target: Path, root: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(desktop_entry(root), encoding="utf-8")
    target.chmod(0o755)  # na área de trabalho, o ambiente gráfico só confia em atalhos executáveis
    # GNOME também exige marcar o atalho como confiável; em outros ambientes o comando só não faz nada
    try:
        subprocess.run(["gio", "set", str(target), "metadata::trusted", "true"],
                       capture_output=True, timeout=10)
    except (OSError, subprocess.SubprocessError):
        pass


def _write_windows(target: Path, root: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    values = {"MT5X_LNK": str(target), "MT5X_TARGET": str(Path(root) / "run.bat"),
              "MT5X_DIR": str(root), "MT5X_ICON": str(icon_path()), "MT5X_DESC": APP_COMMENT}
    # Os caminhos vão por variáveis de ambiente: nada de aspas/acentos montados na linha de comando
    script = ("$s = (New-Object -ComObject WScript.Shell).CreateShortcut($env:MT5X_LNK); "
              "$s.TargetPath = $env:MT5X_TARGET; $s.WorkingDirectory = $env:MT5X_DIR; "
              "$s.IconLocation = $env:MT5X_ICON; $s.Description = $env:MT5X_DESC; $s.Save()")
    subprocess.run(["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", script],
                   env={**os.environ, **values}, check=True, capture_output=True, timeout=60)


def create(root: Path = paths.PROJECT_ROOT, desktop: bool = True, menu: bool = True,
           env=None) -> List[Tuple[Path, Optional[str]]]:
    """
    Cria (ou atualiza) os atalhos pedidos. Retorna [(caminho, erro ou None)]: um atalho que
    falha não impede o outro. Pode ser chamado várias vezes.
    """
    results = []
    for target in targets(desktop, menu, env):
        try:
            (_write_windows if paths.is_windows() else _write_linux)(target, Path(root))
            results.append((target, None))
        except Exception as e:
            results.append((target, str(e)))
    return results


def remove(desktop: bool = True, menu: bool = True, env=None) -> List[Path]:
    """Apaga os atalhos criados pelo aplicativo. Retorna os que existiam."""
    removed = []
    for target in targets(desktop, menu, env):
        if target.is_file():
            target.unlink()
            removed.append(target)
    return removed


def existing(env=None) -> List[Path]:
    return [t for t in targets(env=env) if t.is_file()]
