#!/usr/bin/env python3
"""Conferência somente leitura de um pacote privado antes/depois da reinstalação.

Usa apenas a biblioteca padrão. Não extrai arquivos, instala dependências,
consulta MT5 ou declara aceite operacional/autorização para formatar.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath, PureWindowsPath
import re


REQUIRED_GROUPS = frozenset((
    "database", "project_state", "git_and_context", "native_evidence", "toolchain",
))


class VerificationError(ValueError):
    """Pacote incompleto ou divergente; preservar a origem e corrigir a cópia."""


def sha256(path):
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def verify(backup_dir, manifest):
    root = Path(backup_dir).resolve(strict=True)
    if not root.is_dir():
        raise VerificationError("O destino informado não é uma pasta de backup.")
    if (not isinstance(manifest, dict) or type(manifest.get("schema_version")) is not int
            or manifest.get("schema_version") != 1):
        raise VerificationError("Manifesto ausente/inválido; use schema_version=1.")
    if manifest.get("complete") is not True:
        raise VerificationError("O manifesto não registra um pacote completo.")
    files = manifest.get("files")
    if not isinstance(files, list) or not files:
        raise VerificationError("O manifesto precisa listar todos os arquivos do pacote.")
    groups, destinations, verified = set(), set(), []
    # Validar TODO o inventário antes de ler o conteúdo. Hashes não provam que
    # uma categoria foi capturada corretamente: isso exige o ensaio de restore.
    planned = []
    for item in files:
        if not isinstance(item, dict):
            raise VerificationError("Entrada de arquivo inválida no manifesto.")
        name, expected, size, group = (item.get(k) for k in ("path", "sha256", "bytes", "group"))
        if (not isinstance(name, str) or not name or "\\" in name
                or PurePosixPath(name).is_absolute() or PureWindowsPath(name).drive
                or any(part in ("..", ".") for part in name.split("/"))):
            raise VerificationError("O manifesto contém caminho inválido ou fora do backup.")
        if not isinstance(expected, str) or not re.fullmatch(r"[0-9a-f]{64}", expected):
            raise VerificationError(f"Hash SHA-256 inválido: {name}.")
        if isinstance(size, bool) or not isinstance(size, int) or size <= 0:
            raise VerificationError(f"Tamanho inválido: {name}.")
        if not isinstance(group, str) or not group:
            raise VerificationError(f"Categoria ausente: {name}.")
        candidate = root / name
        target = candidate.resolve(strict=True)
        if not target.is_relative_to(root) or not target.is_file() or candidate.is_symlink():
            raise VerificationError(f"O arquivo não é uma cópia regular dentro do backup: {name}.")
        key = os.path.normcase(str(target))
        if key in destinations:
            raise VerificationError(f"Arquivo repetido no manifesto: {name}.")
        destinations.add(key)
        groups.add(group)
        planned.append((target, name, expected, size))
    if not REQUIRED_GROUPS.issubset(groups):
        missing = ", ".join(sorted(REQUIRED_GROUPS - groups))
        raise VerificationError(f"Faltam categorias obrigatórias no backup: {missing}.")
    for target, name, expected, size in planned:
        before = target.stat()
        if before.st_size != size or sha256(target) != expected:
            raise VerificationError(f"Conteúdo/tamanho divergente: {name}. Preserve a origem.")
        after = target.stat()
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
                after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
            raise VerificationError(f"Arquivo alterado durante a conferência: {name}.")
        verified.append(name)
    return {"files_verified": verified, "groups_verified": sorted(groups),
            "archive_integrity_verified": True,
            "external_storage_location_verified": False,
            "restore_exercised_by_this_command": False,
            "operational_acceptance": False,
            "next": "Conferir cópia em dispositivo externo e ensaio de restauração conforme o roteiro."}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backup-dir", required=True, type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
        result = verify(args.backup_dir, manifest)
    except (OSError, ValueError) as error:
        print(json.dumps({"ok": False, "error": str(error), "preserve_source": True}, ensure_ascii=False))
        return 1
    print(json.dumps({"ok": True, **result}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
