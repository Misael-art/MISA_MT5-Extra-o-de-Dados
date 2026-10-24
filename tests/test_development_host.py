"""Retomada recusa WIP/identidades erradas e não cria ambientes no modo plano."""
import importlib.util
import json
from pathlib import Path
import subprocess

import pytest


SOURCE = Path(__file__).resolve().parents[1] / 'scripts' / 'setup_development.py'
SPEC = importlib.util.spec_from_file_location('setup_development', SOURCE)
host = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(host)


def git(path, *args):
    return subprocess.check_output(['git', '-C', str(path), *args], text=True, encoding='utf-8').strip()


@pytest.fixture
def repository(tmp_path):
    path = tmp_path / 'fonte com espaço'
    path.mkdir()
    git(path, 'init')
    git(path, 'config', 'user.name', 'QA')
    git(path, 'config', 'user.email', 'qa@example.invalid')
    (path / 'source.txt').write_text('source original')
    git(path, 'add', 'source.txt')
    git(path, 'commit', '-m', 'fixture')
    git(path, 'remote', 'add', 'origin', 'https://github.com/Misael-art/fixture.git')
    return path, git(path, 'rev-parse', 'HEAD')


def test_plan_needs_no_runtime_or_private_data_and_writes_nothing(tmp_path):
    destination = tmp_path / 'não existe'
    result = host.prepare(destination, plan=True)
    assert result['mode'] == 'plan'
    assert not result['private_database_required']
    assert set(result['sources']) == {'app', 'hadouken', 'safety'}
    assert not destination.exists()


@pytest.mark.parametrize('windows,ending', [(False, '.venv/bin/python'), (True, '.venv/Scripts/python.exe')])
def test_environment_path_for_both_platforms(tmp_path, windows, ending):
    assert host.venv_python(tmp_path, windows).as_posix().endswith(ending)


def test_valid_existing_checkout_is_reused_without_checkout_or_reset(repository):
    path, sha = repository
    commands = []
    def run(args, cwd):
        commands.append(args)
        return host.command(args, cwd)
    assert host.ensure_checkout(path, {'url': 'https://github.com/Misael-art/fixture.git', 'commit': sha}, run) == sha
    assert all(c[1] not in ('clone', 'checkout', 'reset') for c in commands)
    assert (path / 'source.txt').read_text() == 'source original'


@pytest.mark.parametrize('change', ['wip', 'untracked', 'wrong_origin', 'wrong_commit'])
def test_existing_work_is_never_replaced(repository, change):
    path, sha = repository
    if change == 'wip': (path / 'source.txt').write_text('WIP do proprietário')
    if change == 'untracked': (path / 'novo.txt').write_text('fonte novo')
    if change == 'wrong_origin': git(path, 'remote', 'set-url', 'origin', 'https://example.invalid/other.git')
    expected = '0' * 40 if change == 'wrong_commit' else sha
    before = {p.name: p.read_bytes() for p in path.iterdir() if p.is_file()}
    with pytest.raises(host.SetupError):
        host.ensure_checkout(path, {'url': 'https://github.com/Misael-art/fixture.git', 'commit': expected})
    assert before == {p.name: p.read_bytes() for p in path.iterdir() if p.is_file()}
    assert git(path, 'rev-parse', 'HEAD') == sha


def test_new_clone_is_pinned_and_second_call_does_not_clone_again(repository, tmp_path):
    source, sha = repository
    destination = tmp_path / 'repos' / 'copy'
    spec = {'url': str(source), 'commit': sha}
    assert host.ensure_checkout(destination, spec) == sha
    assert host.ensure_checkout(destination, spec) == sha
    assert git(destination, 'status', '--porcelain') == ''
    assert git(destination, 'config', '--get', 'core.autocrlf') == 'false'


def test_non_repository_destination_is_preserved(tmp_path):
    (tmp_path / 'owner.txt').write_text('preservar')
    with pytest.raises(host.SetupError): host.inspect_checkout(tmp_path, 'https://example.invalid')
    assert (tmp_path / 'owner.txt').read_text() == 'preservar'


@pytest.mark.parametrize('change', ['sha', 'url', 'version', 'repo_missing'])
def test_manifest_rejects_unpinned_or_external_sources(tmp_path, change):
    data = host.load_manifest()
    if change == 'sha': data['repositories']['hadouken']['commit'] = 'master'
    if change == 'url': data['repositories']['hadouken']['url'] = 'https://example.invalid/code.git'
    if change == 'version': data['schema_version'] = 2
    if change == 'repo_missing': del data['repositories']['safety']
    target = tmp_path / 'manifest.json'
    target.write_text(json.dumps(data))
    with pytest.raises(host.SetupError): host.load_manifest(target)


def test_library_defaults_fill_only_empty_values_and_preserve_other_settings(tmp_path):
    config = tmp_path / 'config' / 'config.ini'
    config.parent.mkdir()
    config.write_text('# comentário do usuário\n[ROBOTS]\nlibrary_dirs = minha-biblioteca\nsafety_lib_dir =\nallow_real = false\n', encoding='utf-8')
    host.configure_libraries(tmp_path, tmp_path / 'workspace')
    text = config.read_text(encoding='utf-8')
    assert '# comentário do usuário' in text
    assert 'library_dirs = minha-biblioteca' in text
    assert str(tmp_path / 'workspace/repos/safety') in text
    assert 'allow_real = false' in text
    before = config.read_bytes()
    host.configure_libraries(tmp_path, tmp_path / 'outra-workspace')
    assert config.read_bytes() == before


def test_setup_wrong_checkout_does_not_run_commands_or_create_state(tmp_path):
    called = []
    with pytest.raises(host.SetupError):
        host.prepare(tmp_path, run=lambda *a: called.append(a))
    assert not called and not (tmp_path / 'state').exists()


@pytest.mark.parametrize('fail_install', [False, True])
def test_real_checkout_orchestration_preserves_data_and_records_failure(repository, tmp_path, monkeypatch, fail_install):
    source, sha = repository
    workspace = tmp_path / 'workspace'
    app = workspace / 'repos' / 'app'
    spec = {'url': str(source), 'commit': sha}
    host.ensure_checkout(app, spec)
    git(app, 'remote', 'set-url', 'origin', 'https://github.com/Misael-art/fixture.git')
    (app / '.git/info/exclude').write_text('.venv/\nconfig/\ndatabase/\n')
    (app / '.venv').mkdir()
    (app / 'config').mkdir()
    (app / 'config/config.ini').write_text('[ROBOTS]\nlibrary_dirs =\nsafety_lib_dir =\nallow_real = false\n')
    (app / 'database').mkdir()
    data = app / 'database/owner.db'
    data.write_bytes(b'nao substituir a base')
    manifest = {'repositories': {'app': {'url': 'https://github.com/Misael-art/fixture.git', 'commit': sha},
                                 'hadouken': spec, 'safety': spec}}
    monkeypatch.setattr(host, 'ROOT', app)
    monkeypatch.setattr(host.sys, 'prefix', str(app / '.venv'))
    monkeypatch.setattr(host, 'load_manifest', lambda: manifest)
    monkeypatch.setattr(host.shutil, 'which', lambda name: 'compiler' if name == 'g++' else None)
    calls = []
    def run(args, cwd, log=None):
        calls.append([str(a) for a in args])
        if str(args[0]) == 'git': return host.command(args, cwd, log)
        if fail_install and 'install' in args: raise host.SetupError('falha de instalação simulada')
        if log: log.write_text('simulação da plataforma, sem instalar pacotes')
        return ''
    if fail_install:
        with pytest.raises(host.SetupError): host.prepare(workspace, run=run)
    else:
        assert host.prepare(workspace, run=run)['status'] == 'development_ready'
    receipt = json.loads((workspace / 'state/setup.json').read_text())
    assert receipt['status'] == ('failed' if fail_install else 'development_ready')
    assert not receipt['database_restored'] and not receipt['native_qualified']
    assert data.read_bytes() == b'nao substituir a base'
    assert any('shortcuts' in c for c in calls) is not fail_install
    assert git(app, 'rev-parse', 'HEAD') == sha
