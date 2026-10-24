"""O backup remoto só é publicado/extradído após verificação completa."""
import base64
import importlib.util
import json
from pathlib import Path
import sqlite3
import stat
import zipfile

import pytest


SOURCE = Path(__file__).resolve().parents[1] / 'scripts' / 'recover_cloud_backup.py'
SPEC = importlib.util.spec_from_file_location('recover_cloud_backup', SOURCE)
cloud = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cloud)


@pytest.fixture
def parts(tmp_path):
    payload = b'conteudo completo da recuperacao' * 8
    manifest = {'schema_version': 1, 'encoding': 'base64', 'archive_bytes': len(payload),
                'archive_sha256': cloud.digest(payload), 'parts': []}
    for index, start in enumerate(range(0, len(payload), 90)):
        raw = payload[start:start + 90]
        encoded = base64.b64encode(raw)
        part = {'name': '%04d.txt' % index, 'sha256': cloud.digest(encoded),
                'decoded_bytes': len(raw), 'decoded_sha256': cloud.digest(raw)}
        (tmp_path / part['name']).write_bytes(encoded)
        manifest['parts'].append(part)
    return tmp_path, manifest, payload


def test_all_parts_reassemble_and_existing_output_remains_untouched(parts):
    root, manifest, payload = parts
    target = root / 'restore.zip'
    result = cloud.reassemble(root, manifest, target)
    assert target.read_bytes() == payload and result['parts_verified'] == len(manifest['parts'])
    with pytest.raises(cloud.RecoveryError): cloud.reassemble(root, manifest, target)
    assert target.read_bytes() == payload


def test_library_download_names_are_supported_without_renaming(parts):
    root, manifest, payload = parts
    for part in manifest['parts']:
        part['download_name'] = part['name'] + '-library.txt'
        (root / part['name']).rename(root / part['download_name'])
    cloud.reassemble(root, manifest, root / 'restored.zip')
    assert (root / 'restored.zip').read_bytes() == payload


def test_executor_final_newline_requires_exact_source_hash(parts):
    root, manifest, payload = parts
    first = root / manifest['parts'][0]['name']
    first.write_bytes(first.read_bytes() + b'\n')
    cloud.reassemble(root, manifest, root / 'correct.zip')
    assert (root / 'correct.zip').read_bytes() == payload
    first.write_bytes(first.read_bytes() + b'\n')
    with pytest.raises(cloud.RecoveryError): cloud.reassemble(root, manifest, root / 'bad.zip')
    assert not (root / 'bad.zip').exists()


def test_oversized_part_is_rejected_before_loading_it_in_memory(parts, monkeypatch):
    root, manifest, _ = parts
    first = root / manifest['parts'][0]['name']
    first.write_bytes(b'x' * (1024 * 1024 + 1))
    def forbidden_read(path):
        pytest.fail('Parte acima do limite não deveria ser carregada em memória')
    monkeypatch.setattr(Path, 'read_bytes', forbidden_read)
    with pytest.raises(cloud.RecoveryError, match='1 MiB'):
        cloud.decoded_part(root, manifest['parts'][0])


@pytest.mark.parametrize('mutation', ['missing_last', 'tampered_last', 'reordered', 'duplicate', 'version_bool', 'size_bool', 'wrong_decoded_hash'])
def test_rejected_package_never_publishes_partial_archive(parts, mutation):
    root, manifest, _ = parts
    last = root / manifest['parts'][-1]['name']
    if mutation == 'missing_last': last.unlink()
    if mutation == 'tampered_last': last.write_bytes(b'other')
    if mutation == 'reordered': manifest['parts'].reverse()
    if mutation == 'duplicate': manifest['parts'].append(manifest['parts'][-1])
    if mutation == 'version_bool': manifest['schema_version'] = True
    if mutation == 'size_bool': manifest['archive_bytes'] = True
    if mutation == 'wrong_decoded_hash': manifest['parts'][-1]['decoded_sha256'] = '0' * 64
    with pytest.raises(cloud.RecoveryError): cloud.reassemble(root, manifest, root / 'never.zip')
    assert not (root / 'never.zip').exists()


@pytest.mark.parametrize('path', ['../escape', '/absolute', 'C:/outside', 'a\\b', '', './part'])
def test_part_path_cannot_escape(parts, path):
    root, manifest, _ = parts
    manifest['parts'][-1]['name'] = path
    with pytest.raises(cloud.RecoveryError): cloud.reassemble(root, manifest, root / 'never.zip')
    assert not (root / 'never.zip').exists()


def make_archive(path, files, symlink=False, wrong_hash=False):
    with zipfile.ZipFile(path, 'w') as z:
        for name, value in files.items():
            info = zipfile.ZipInfo(name)
            if symlink: info.external_attr = (stat.S_IFLNK | 0o777) << 16
            z.writestr(info, value)
        hashes = {name: cloud.digest(value) for name, value in files.items()}
        if wrong_hash: hashes[next(iter(hashes))] = '0' * 64
        z.writestr('CONTENTS.json', json.dumps({'files': hashes}))


def test_extract_checks_database_and_preserves_active_configuration(tmp_path):
    database = tmp_path / 'input.sqlite'
    with sqlite3.connect(database) as conn:
        conn.execute('CREATE TABLE owner_state(value TEXT)')
        conn.execute("INSERT INTO owner_state VALUES('persistido')")
    archive = tmp_path / 'source.zip'
    make_archive(archive, {'database/owner-database.sqlite': database.read_bytes(), 'READ_ME.md': b'private'})
    target = tmp_path / 'restauracao nova'
    result = cloud.extract_new(archive, target)
    assert result['database_quick_check'] == 'ok'
    assert not result['active_configuration_changed'] and not result['operational_acceptance']
    assert (target / 'database/owner-database.sqlite').read_bytes() == database.read_bytes()
    with pytest.raises(cloud.RecoveryError): cloud.extract_new(archive, target)


@pytest.mark.parametrize('mutation', ['traversal', 'symlink', 'hash', 'limit'])
def test_zip_preflight_rejects_before_creating_destination(tmp_path, mutation):
    archive = tmp_path / 'source.zip'
    name = '../outside' if mutation == 'traversal' else 'ok.txt'
    make_archive(archive, {name: b'owner'}, symlink=mutation == 'symlink', wrong_hash=mutation == 'hash')
    target = tmp_path / 'new'
    with pytest.raises(cloud.RecoveryError): cloud.extract_new(archive, target, max_mib=0 if mutation == 'limit' else 2048)
    assert not target.exists() and not (tmp_path.parent / 'outside').exists()
