"""
Localiza instalações do MetaTrader 5 no Windows e em prefixos Wine (Linux).

Somente biblioteca padrão. Nenhuma função aqui inicia processos.
"""
import os
import struct
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


def terminal_executable(configured_path) -> Path:
    """Aceita a pasta da instalação ou o próprio terminal64.exe."""
    path = Path(configured_path).expanduser()
    return path if path.name.lower() == paths.MT5_TERMINAL_EXE else path / paths.MT5_TERMINAL_EXE


class InvalidWindowsImage(RuntimeError):
    """O arquivo existe, mas não pode ser usado como executável Windows."""


def validate_windows_image(path: Path) -> None:
    """Verifica a estrutura PE lendo só os cabeçalhos, sem iniciar o executável."""
    def invalid(reason):
        return InvalidWindowsImage(f"{path}: executável Windows inválido ({reason}).")

    with path.open("rb") as image:
        size = os.fstat(image.fileno()).st_size
        dos = image.read(64)
        if len(dos) != 64 or dos[:2] != b"MZ":
            raise invalid("cabeçalho MZ ausente ou incompleto")
        offset, = struct.unpack_from("<I", dos, 60)
        if not 64 <= offset <= size - 24:
            raise invalid("cabeçalho PE fora do arquivo")
        image.seek(offset)
        header = image.read(24)
        if len(header) != 24 or header[:4] != b"PE\0\0":
            raise invalid("assinatura PE ausente ou incompleta")
        machine, section_count, _, _, _, optional_size, flags = struct.unpack_from("<HHIIIHH", header, 4)
        if not (flags & 0x0002) or flags & 0x2000 or not 1 <= section_count <= 96:
            raise invalid("imagem não executável ou tabela de seções inválida")

        optional = image.read(optional_size)
        if len(optional) != optional_size or len(optional) < 2:
            raise invalid("cabeçalho opcional truncado")
        magic, = struct.unpack_from("<H", optional)
        directory_start = {0x010B: 96, 0x020B: 112}.get(magic)
        if directory_start is None or len(optional) < directory_start:
            raise invalid("formato PE32 ou PE32+ ausente ou incompleto")
        if (machine, magic) not in {(0x014C, 0x010B), (0x8664, 0x020B),
                                   (0xAA64, 0x020B), (0xA641, 0x020B), (0xA64E, 0x020B)}:
            raise invalid(f"arquitetura PE incompatível (0x{machine:04X})")

        sections = image.read(section_count * 40)
        if len(sections) != section_count * 40:
            raise invalid("tabela de seções truncada")
        for index in range(section_count):
            data_size, data_offset = struct.unpack_from("<II", sections, index * 40 + 16)
            if data_size and (not data_offset or data_offset + data_size > size):
                raise invalid(f"dados da seção {index + 1} ultrapassam o fim do arquivo")
        directory_count, = struct.unpack_from("<I", optional, directory_start - 4)
        if directory_count >= 5:
            if len(optional) < directory_start + 40:
                raise invalid("entrada da tabela de certificados truncada")
            certificate, certificate_size = struct.unpack_from("<II", optional, directory_start + 32)
            if certificate_size and (not certificate or certificate + certificate_size > size):
                raise invalid("tabela de certificados ultrapassa o fim do arquivo")


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
