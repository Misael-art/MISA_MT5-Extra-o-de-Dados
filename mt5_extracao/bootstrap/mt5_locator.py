"""
Localiza instalações do MetaTrader 5 no Windows e em prefixos Wine (Linux).

Somente biblioteca padrão. Nenhuma função aqui inicia processos.
"""
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, List, Optional

from . import paths


@dataclass
class MT5Installation:
    """Uma instalação encontrada do terminal MT5."""
    directory: Path                  # diretório no sistema de arquivos do host
    windows_path: str                # caminho no formato Windows (igual a directory no Windows)
    wine_prefix: Optional[Path] = None  # prefixo Wine, se for instalação no Linux
    source: str = "scan"             # "registry", "scan", "wine" ou "manual"

    @property
    def terminal_exe(self) -> Path:
        return self.directory / paths.MT5_TERMINAL_EXE

    def to_dict(self) -> dict:
        return {
            "directory": str(self.directory),
            "windows_path": self.windows_path,
            "wine_prefix": str(self.wine_prefix) if self.wine_prefix else None,
            "source": self.source,
        }


def has_terminal(directory: Path) -> bool:
    try:
        return (Path(directory) / paths.MT5_TERMINAL_EXE).is_file()
    except OSError:
        return False


def _scan_program_dirs(base_dirs: Iterable[Path]) -> List[Path]:
    """Procura <base>/<qualquer>/terminal64.exe (corretoras usam nomes próprios)."""
    found = []
    for base in base_dirs:
        base = Path(base)
        if not base.is_dir():
            continue
        if has_terminal(base):
            found.append(base)
        try:
            children = sorted(base.iterdir())
        except OSError:
            continue
        for child in children:
            if child.is_dir() and has_terminal(child):
                found.append(child)
    return found


def _windows_registry_dirs() -> List[Path]:
    """Lê as chaves de desinstalação do Windows procurando o MetaTrader."""
    try:
        import winreg  # type: ignore
    except ImportError:
        return []
    result = []
    roots = [
        (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall"),
        (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall"),
        (winreg.HKEY_CURRENT_USER, r"SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall"),
    ]
    for hive, key_path in roots:
        try:
            key = winreg.OpenKey(hive, key_path)
        except OSError:
            continue
        with key:
            i = 0
            while True:
                try:
                    sub_name = winreg.EnumKey(key, i)
                except OSError:
                    break
                i += 1
                try:
                    with winreg.OpenKey(key, sub_name) as sub:
                        name = str(winreg.QueryValueEx(sub, "DisplayName")[0])
                        if "metatrader" not in name.lower():
                            continue
                        location = str(winreg.QueryValueEx(sub, "InstallLocation")[0])
                        if location:
                            result.append(Path(location))
                except OSError:
                    continue
    return result


def find_windows_installations(env=None) -> List[MT5Installation]:
    env = os.environ if env is None else env
    found: List[MT5Installation] = []
    for d in _windows_registry_dirs():
        if has_terminal(d):
            found.append(MT5Installation(d, str(d), source="registry"))
    bases = []
    for var in ("ProgramFiles", "ProgramFiles(x86)", "ProgramW6432"):
        if env.get(var):
            bases.append(Path(env[var]))
    bases += [Path(r"C:\Program Files"), Path(r"C:\Program Files (x86)")]
    if env.get("APPDATA"):
        # Instalações "portáteis" feitas por algumas corretoras
        bases.append(Path(env["APPDATA"]) / "MetaQuotes")
    for d in _scan_program_dirs(bases):
        found.append(MT5Installation(d, str(d), source="scan"))
    return _dedupe(found)


def find_wine_installations(prefixes: Optional[Iterable[Path]] = None, env=None) -> List[MT5Installation]:
    prefixes = list(prefixes) if prefixes is not None else paths.wine_prefix_candidates(env)
    found: List[MT5Installation] = []
    for prefix in prefixes:
        prefix = Path(prefix)
        drive_c = prefix / "drive_c"
        if not drive_c.is_dir():
            continue
        bases = [drive_c / "Program Files", drive_c / "Program Files (x86)"]
        for d in _scan_program_dirs(bases):
            found.append(MT5Installation(d, paths.linux_to_wine_path(d, prefix), prefix, source="wine"))
    return _dedupe(found)


def find_installations(env=None, wine_prefixes: Optional[Iterable[Path]] = None) -> List[MT5Installation]:
    """Todas as instalações encontradas, a instalação padrão "MetaTrader 5" primeiro."""
    if paths.is_windows():
        found = find_windows_installations(env)
    else:
        found = find_wine_installations(wine_prefixes, env)
    return sorted(found, key=_preference)


def _preference(inst: MT5Installation):
    # 0 = pasta padrão da MetaQuotes; depois ordem alfabética
    return (0 if inst.directory.name.lower() == "metatrader 5" else 1, str(inst.directory).lower())


def _dedupe(items: List[MT5Installation]) -> List[MT5Installation]:
    seen = set()
    out = []
    for inst in items:
        try:
            key = str(inst.directory.resolve()).lower()
        except OSError:
            key = str(inst.directory).lower()
        if key not in seen:
            seen.add(key)
            out.append(inst)
    return out


def installation_from_path(path, wine_prefix: Optional[Path] = None) -> Optional[MT5Installation]:
    """
    Cria uma MT5Installation a partir de um caminho informado pelo usuário.
    Aceita o diretório ou o próprio terminal64.exe. Retorna None se inválido.
    """
    p = Path(path).expanduser()
    if p.name.lower() == paths.MT5_TERMINAL_EXE:
        p = p.parent
    if not has_terminal(p):
        return None
    if paths.is_windows():
        return MT5Installation(p, str(p), source="manual")
    prefix = Path(wine_prefix) if wine_prefix else _guess_prefix(p)
    if prefix is None:
        return None
    return MT5Installation(p, paths.linux_to_wine_path(p, prefix), prefix, source="manual")


def _guess_prefix(directory: Path) -> Optional[Path]:
    for parent in directory.parents:
        if parent.name == "drive_c":
            return parent.parent
    return None
