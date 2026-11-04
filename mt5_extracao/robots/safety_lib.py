"""
Biblioteca de segurança MQL5 (mt5x-safety) como dependência com versão.

    ensure(config, robot, data_dir)      # instala a versão que o robô declara em "safety_lib" (robot.json)

A biblioteca mora em repositório próprio (https://github.com/Misael-art/mt5x-safety) e não é versionada aqui.
`[ROBOTS] safety_lib_dir` aponta para um clone local; `safety.json`, na raiz dele, diz a versão, a pasta de include
e os arquivos. Nada é baixado automaticamente e uma versão já instalada nunca é trocada em silêncio.
"""
import hashlib
import json
import shutil
from pathlib import Path
from typing import List, Optional

MANIFEST_NAME = "safety.json"
MARKER_NAME = "mt5x-safety.json"       # gravado em MQL5/Include: qual versão está instalada na pasta do MT5
REPOSITORY = "https://github.com/Misael-art/mt5x-safety"


class SafetyLibError(RuntimeError):
    """Problema com a biblioteca de segurança; a mensagem traz a ação de correção."""


def lib_dir(config) -> Optional[Path]:
    value = config.get("ROBOTS", "safety_lib_dir", fallback="").strip() if config is not None else ""
    return Path(value).expanduser() if value else None


def read_manifest(folder) -> dict:
    """safety.json da biblioteca (na pasta do clone)."""
    path = Path(folder) / MANIFEST_NAME
    if not path.is_file():
        raise SafetyLibError(f"{path} não encontrado: [ROBOTS] safety_lib_dir deve apontar para um clone de {REPOSITORY}.")
    try:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
    except ValueError as e:
        raise SafetyLibError(f"{path} inválido: {e}") from None
    missing = [k for k in ("version", "include_dir", "files") if not data.get(k)]
    if missing or not isinstance(data.get("files"), list):
        raise SafetyLibError(f"{path} incompleto: faltam " + ", ".join(missing or ["files"]) + ".")
    return data


def installed(data_dir) -> Optional[dict]:
    """O que está instalado na pasta de dados do MT5 (None se a biblioteca nunca foi instalada pelo mt5x)."""
    marker = Path(data_dir) / "MQL5" / "Include" / MARKER_NAME
    if not marker.is_file():
        return None
    try:
        return json.loads(marker.read_text(encoding="utf-8"))
    except ValueError:
        return None


def installed_version(data_dir) -> str:
    return str((installed(data_dir) or {}).get("version", ""))


def required_version(robot) -> str:
    """Versão que o robô declara em "safety_lib" no robot.json ('' se não declara)."""
    return str((getattr(robot, "manifest", None) or {}).get("safety_lib", "") or "")


def indicator_assets(folder, manifest=None):
    """Ativos gráficos opcionais revisados; validar tudo antes de copiar headers."""
    root = Path(folder).resolve()
    manifest = read_manifest(root) if manifest is None else manifest
    records = manifest.get("indicators", [])
    if not isinstance(records, list):
        raise SafetyLibError("Manifesto de indicadores inválido: restaure a biblioteca completa.")
    assets, seen = [], set()
    for record in records:
        if not isinstance(record, dict) or not isinstance(record.get("source"), str):
            raise SafetyLibError("Ativo gráfico inválido no manifesto da biblioteca.")
        relative = Path(record["source"])
        if (relative.is_absolute() or ".." in relative.parts or "\\" in record["source"] or
                len(relative.parts) < 3 or relative.parts[0] != "Indicators" or relative.suffix.lower() != ".mq5" or
                relative in seen):
            raise SafetyLibError("Caminho de indicador inválido no manifesto da biblioteca.")
        source = (root / relative).resolve()
        if not source.is_relative_to(root) or not source.is_file():
            raise SafetyLibError(f"Indicador da biblioteca ausente: {relative}. Restaure a distribuição completa.")
        content = source.read_bytes()
        if hashlib.sha256(content).hexdigest() != record.get("sha256"):
            raise SafetyLibError(f"Indicador {relative} diverge do hash: restaure a distribuição completa.")
        seen.add(relative)
        assets.append((source, relative, content))
    return assets


def install(folder, data_dir, upgrade: bool = False) -> List[Path]:
    """
    Copia a biblioteca para MQL5/Include/<include_dir> e grava o marcador de versão. Rodar de novo não muda nada.
    Com outra versão já instalada, só troca se `upgrade` (outros robôs podem depender da versão atual).
    """
    manifest = read_manifest(folder)
    include = Path(data_dir) / "MQL5" / "Include"
    current = installed_version(data_dir)
    if current and current != manifest["version"] and not upgrade:
        raise SafetyLibError(f"A pasta do MT5 tem a biblioteca de segurança {current} e o clone em {folder} é a "
                             f"{manifest['version']}. Para trocar, repita com --upgrade-safety (os outros robôs "
                             "passam a compilar com a versão nova).")
    source_dir = Path(folder) / "Include" / manifest["include_dir"]
    target_dir = include / manifest["include_dir"]
    hashes = manifest.get("sha256")
    if hashes is not None and (not isinstance(hashes, dict) or set(hashes) != set(manifest["files"])):
        raise SafetyLibError(f"O manifesto em {folder} tem hashes incompletos: restaure a distribuição "
                             f"mt5x-safety {manifest['version']} antes de instalar.")
    # Conferir o conjunto inteiro antes de alterar a instalação. Um último header
    # ausente/divergente não pode deixar metade do novo painel sobre a versão antiga.
    sources = []
    for name in manifest["files"]:
        source = source_dir / name
        if not source.is_file():
            raise SafetyLibError(f"A biblioteca em {folder} está incompleta: falta {source}.")
        content = source.read_bytes()
        if hashes is not None and hashlib.sha256(content).hexdigest() != hashes[name]:
            raise SafetyLibError(f"O arquivo {source} diverge do hash do manifesto: restaure a distribuição "
                                 f"mt5x-safety {manifest['version']} antes de instalar.")
        sources.append((source, name, content))
    assets = indicator_assets(folder, manifest)
    copied = []
    for source, name, content in sources:
        target = target_dir / name
        if not target.is_file() or target.read_bytes() != content:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
            shutil.copystat(source, target)
            copied.append(target)
    for source, relative, content in assets:
        target = Path(data_dir) / "MQL5" / relative
        if not target.is_file() or target.read_bytes() != content:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
            shutil.copystat(source, target)
            copied.append(target)
    marker = {"name": manifest.get("name", "mt5x-safety"), "version": manifest["version"],
              "include_dir": manifest["include_dir"], "files": manifest["files"]}
    if assets:
        marker["indicators"] = manifest["indicators"]
    if hashes is not None:
        marker["sha256"] = hashes
    include.mkdir(parents=True, exist_ok=True)
    (include / MARKER_NAME).write_text(json.dumps(marker, ensure_ascii=False, indent=2), encoding="utf-8")
    return copied


def ensure(config, robot, data_dir, upgrade: bool = False) -> str:
    """
    Garante, na pasta do MT5, a versão da biblioteca que o robô declara. Devolve a versão ('' se o robô não
    declara nenhuma). Erros dizem o que fazer.
    """
    wanted = required_version(robot)
    if not wanted:
        return ""
    folder = lib_dir(config)
    if folder is None:
        raise SafetyLibError(f"{robot.name} usa a biblioteca de segurança {wanted}, mas [ROBOTS] safety_lib_dir está "
                             f"vazio. Clone {REPOSITORY} e aponte a chave para a pasta do clone.")
    available = read_manifest(folder)["version"]
    if available != wanted:
        raise SafetyLibError(f"{robot.name} pede a biblioteca de segurança {wanted} e o clone em {folder} está na "
                             f"{available}. No clone: git checkout v{wanted} (ou atualize \"safety_lib\" no robot.json).")
    install(folder, data_dir, upgrade)
    return wanted
