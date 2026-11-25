"""Contrato puro de cruzamento usado pelo Expert S09 do HADOUKEN auditado."""
from hashlib import sha256
from pathlib import Path

from .catalog import library_dirs

HEADERS = {
    "Strategies/HDK_S09Reference.mqh": "efbb1ab0488ce1b49509d496de961ad394e85ace1ffb4b3b8bdc7612413382c3",
    "Contracts/HDK_MarketContext.mqh": "fd1b0ea9e3a0f53b8e96fe4e03428f986a9a50a4f249e98be2c775890b3ac924",
}


def headers(config):
    for library in library_dirs(config):
        root = Path(library) / "Include/Framework/HADOUKEN"
        paths = {name: root / name for name in HEADERS}
        if all(p.is_file() and sha256(p.read_bytes().replace(b"\r\n", b"\n")).hexdigest() == HEADERS[n]
               for n, p in paths.items()):
            return paths
    raise ValueError("Contrato S09 do HADOUKEN incompatível ou ausente nas bibliotecas configuradas.")
