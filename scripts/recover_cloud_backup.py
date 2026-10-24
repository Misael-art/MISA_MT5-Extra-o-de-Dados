#!/usr/bin/env python3
"""Recompõe partes privadas baixadas da Library; stdlib, sem credenciais/rede.

O transporte é feito pela conta autenticada do proprietário. Este comando confere
TODAS as partes antes de publicar o ZIP e só extrai para uma pasta nova.
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path, PurePosixPath, PureWindowsPath
import re
import shutil
import sqlite3
import stat
import tempfile
import zipfile


class RecoveryError(ValueError):
    """Pacote divergente: preservar origem e não restaurar por cima de dados."""


def digest(data):
    return hashlib.sha256(data).hexdigest()


def safe_name(name):
    return (isinstance(name, str) and bool(name) and '\\' not in name
            and not PurePosixPath(name).is_absolute() and not PureWindowsPath(name).drive
            and not any(p in ('', '.', '..') for p in name.split('/')))


def decoded_part(root, part):
    name = part.get('name')
    if not safe_name(name) or '/' in name:
        raise RecoveryError('Nome de parte inválido.')
    path = root / name
    if not path.exists() and part.get('download_name'):
        downloaded = part['download_name']
        if not safe_name(downloaded) or '/' in downloaded:
            raise RecoveryError('Nome de download inválido.')
        path = root / downloaded
    if path.is_symlink() or not path.is_file():
        raise RecoveryError('Parte ausente ou link: %s.' % name)
    if path.stat().st_size > 1024 * 1024:
        raise RecoveryError('Parte excede o limite de 1 MiB: %s.' % name)
    encoded = path.read_bytes()
    # Só tolera um LF do editor se recuperar EXATAMENTE o hash da fonte.
    if encoded.endswith(b'\n') and digest(encoded) != part.get('sha256'):
        candidate = encoded[:-1]
        if digest(candidate) == part.get('sha256'):
            encoded = candidate
    if len(encoded) > 1024 * 1024 or digest(encoded) != part.get('sha256'):
        raise RecoveryError('Hash/tamanho de parte divergente: %s.' % name)
    try:
        raw = base64.b64decode(encoded, validate=True)
    except ValueError as error:
        raise RecoveryError('Base64 inválido: %s.' % name) from error
    if (type(part.get('decoded_bytes')) is not int or len(raw) != part['decoded_bytes']
            or not raw or digest(raw) != part.get('decoded_sha256')):
        raise RecoveryError('Conteúdo decodificado divergente: %s.' % name)
    return raw


def reassemble(root, manifest, output):
    root = Path(root).resolve(strict=True)
    output = Path(output)
    if output.exists() or output.is_symlink():
        raise RecoveryError('O ZIP de destino já existe; escolha outro caminho.')
    parts = manifest.get('parts')
    if (type(manifest.get('schema_version')) is not int or manifest['schema_version'] != 1
            or type(manifest.get('archive_bytes')) is not int or manifest['archive_bytes'] <= 0
            or manifest.get('encoding') != 'base64'
            or not isinstance(parts, list) or not parts
            or any(not isinstance(p, dict) for p in parts)):
        raise RecoveryError('Manifesto de recuperação inválido.')
    names = [p.get('name') for p in parts]
    if not all(isinstance(n, str) for n in names) or len(set(names)) != len(names):
        raise RecoveryError('Parte duplicada.')
    if not re.fullmatch('[a-f0-9]{64}', str(manifest.get('archive_sha256', ''))):
        raise RecoveryError('Hash do ZIP inválido.')
    total = 0
    checksum = hashlib.sha256()
    for part in parts:
        raw = decoded_part(root, part)
        total += len(raw)
        checksum.update(raw)
    if total != manifest.get('archive_bytes') or checksum.hexdigest() != manifest['archive_sha256']:
        raise RecoveryError('Ordem, tamanho ou hash do ZIP divergente.')
    if shutil.disk_usage(output.parent).free < total + 16 * 1024 * 1024:
        raise RecoveryError('Espaço insuficiente para recompor o ZIP.')
    fd, temporary = tempfile.mkstemp(prefix='.mt5x-recovery-', dir=output.parent)
    try:
        with os.fdopen(fd, 'wb') as handle:
            for part in parts:
                handle.write(decoded_part(root, part))
            handle.flush()
            os.fsync(handle.fileno())
        # Criação atômica sem sobrescrever destino que tenha surgido durante a leitura.
        os.link(temporary, output)
    finally:
        Path(temporary).unlink(missing_ok=True)
    return {'archive': str(output), 'sha256': checksum.hexdigest(), 'parts_verified': len(parts)}


def extract_new(archive, target, max_mib=2048):
    target = Path(target)
    if target.exists() or target.is_symlink():
        raise RecoveryError('A restauração exige uma pasta nova e vazia.')
    with zipfile.ZipFile(archive) as source:
        infos = source.infolist()
        names = [i.filename.rstrip('/') for i in infos]
        if len(set(names)) != len(names):
            raise RecoveryError('Nome repetido no ZIP.')
        for info, name in zip(infos, names):
            if (not safe_name(name) or stat.S_ISLNK(info.external_attr >> 16)
                    or info.flag_bits & 1):
                raise RecoveryError('Caminho/link/arquivo criptografado não aceito no ZIP.')
        total = sum(i.file_size for i in infos)
        if total > max_mib * 1024 * 1024 or shutil.disk_usage(target.parent).free < total + 16 * 1024 * 1024:
            raise RecoveryError('Limite ou espaço de extração insuficiente.')
        inventory = json.loads(source.read('CONTENTS.json'))
        expected = inventory['files']
        if set(expected) != set(names) - {'CONTENTS.json'}:
            raise RecoveryError('Inventário interno difere do conteúdo do ZIP.')
        # Preflight de TODO o conteúdo antes de criar a pasta de destino.
        for name, sha in expected.items():
            with source.open(name) as handle:
                check = hashlib.sha256()
                for block in iter(lambda: handle.read(1024 * 1024), b''):
                    check.update(block)
            if check.hexdigest() != sha:
                raise RecoveryError('Hash interno divergente: %s.' % name)
        target.mkdir(mode=0o700)
        for info in infos:
            path = target / info.filename
            if info.is_dir():
                path.mkdir(parents=True, exist_ok=True)
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            with source.open(info) as reader, path.open('xb') as writer:
                shutil.copyfileobj(reader, writer)
            if os.name != 'nt':
                path.chmod(0o600)
    database = target / 'database' / 'owner-database.sqlite'
    if database.exists():
        with sqlite3.connect(database.as_uri() + '?mode=ro', uri=True) as connection:
            result = connection.execute('PRAGMA quick_check').fetchone()[0]
        if result != 'ok':
            raise RecoveryError('SQLite restaurado reprovou quick_check; preserve o ensaio.')
    return {'extracted_to': str(target), 'files_verified': len(expected),
            'database_quick_check': 'ok' if database.exists() else 'not_present',
            'active_configuration_changed': False, 'operational_acceptance': False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--parts-dir', required=True, type=Path)
    parser.add_argument('--manifest', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--extract-to', type=Path)
    args = parser.parse_args(argv)
    try:
        manifest = json.loads(args.manifest.read_text(encoding='utf-8'))
        result = reassemble(args.parts_dir, manifest, args.output)
        if args.extract_to:
            result.update(extract_new(args.output, args.extract_to.resolve()))
    except (ValueError, OSError, KeyError, TypeError, zipfile.BadZipFile, sqlite3.Error) as error:
        print(json.dumps({'ok': False, 'error': str(error), 'preserve_source': True}, ensure_ascii=False))
        return 1
    print(json.dumps({'ok': True, **result}, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
