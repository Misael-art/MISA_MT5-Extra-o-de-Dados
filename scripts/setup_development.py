#!/usr/bin/env python3
"""Compõe fontes e ambiente de desenvolvimento; não substitui os instaladores.

Somente stdlib neste orquestrador. Rodar pelo .venv criado pelo instalador oficial.
--plan é somente leitura, sem exigir dependências, banco ou terminal.
"""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / 'docs' / 'repasse' / 'NOVO_HOST_MANIFESTO_20261024.json'


class SetupError(ValueError):
    """Pré-condição recusada sem destruir estado existente."""


def venv_python(app, windows=None):
    windows = os.name == 'nt' if windows is None else windows
    return app / '.venv' / ('Scripts/python.exe' if windows else 'bin/python')


def command(args, cwd, log=None):
    result = subprocess.run([str(x) for x in args], cwd=str(cwd), text=True, encoding='utf-8',
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if log:
        log.parent.mkdir(parents=True, exist_ok=True)
        log.write_text(result.stdout, encoding='utf-8')
    if result.returncode:
        raise SetupError('Comando falhou (código %s): %s; consulte o recibo/log local.' %
                         (result.returncode, args[0]))
    return result.stdout.strip()


def same_origin(actual, expected):
    return actual.removesuffix('.git').rstrip('/') == expected.removesuffix('.git').rstrip('/')


def inspect_checkout(path, url, commit=None, run=command):
    if not path.exists():
        return None
    if path.is_symlink() or not (path / '.git').exists():
        raise SetupError('Destino existente não é um clone próprio: %s. Escolha outra pasta.' % path)
    if Path(run(['git', 'rev-parse', '--show-toplevel'], path)).resolve() != path.resolve():
        raise SetupError('Destino não é a raiz do clone: %s.' % path)
    if not same_origin(run(['git', 'remote', 'get-url', 'origin'], path), url):
        raise SetupError('Origem diferente em %s; nada foi substituído.' % path)
    if run(['git', 'status', '--porcelain'], path):
        raise SetupError('WIP em %s; preserve-o e use outra pasta.' % path)
    head = run(['git', 'rev-parse', 'HEAD'], path)
    if commit and head != commit:
        raise SetupError('Commit diferente em %s; não será feito checkout/reset automático.' % path)
    return head


def ensure_checkout(path, spec, run=command):
    head = inspect_checkout(path, spec['url'], spec['commit'], run)
    if head:
        return head
    path.parent.mkdir(parents=True, exist_ok=True)
    run(['git', 'clone', '--config', 'core.autocrlf=false', '--no-checkout', spec['url'], str(path)], path.parent)
    run(['git', 'checkout', '--detach', spec['commit']], path)
    return inspect_checkout(path, spec['url'], spec['commit'], run)


def load_manifest(path=MANIFEST):
    data = json.loads(path.read_text(encoding='utf-8'))
    if (not isinstance(data, dict) or type(data.get('schema_version')) is not int
            or data['schema_version'] != 1 or set(data['repositories']) != {'app', 'hadouken', 'safety'}):
        raise SetupError('Manifesto de fontes inválido.')
    for spec in data['repositories'].values():
        if not re.fullmatch(r'[0-9a-f]{40}', spec['commit']):
            raise SetupError('Commit precisa ser SHA completo.')
        if not re.fullmatch(r'https://github\.com/Misael-art/[A-Za-z0-9_-]+\.git', spec['url']):
            raise SetupError('URL de fonte não autorizada no manifesto.')
    return data


def configure_libraries(app, workspace):
    # Cliente do configurador canônico; conserva escolhas anteriores e comentários.
    if str(app) not in sys.path:
        sys.path.insert(0, str(app))
    from mt5_extracao.bootstrap.config_builder import load_config, set_config_value
    target = app / 'config' / 'config.ini'
    if not target.is_file():
        raise SetupError('Configuração ausente; execute o instalador oficial primeiro.')
    config = load_config(target)
    for key, folder in [('library_dirs', 'hadouken'), ('safety_lib_dir', 'safety')]:
        if not config.get('ROBOTS', key, fallback='').strip():
            set_config_value(target, 'ROBOTS', key, str(workspace / 'repos' / folder))


def verify_readiness(app, python, state, run=command):
    run([python, '-m', 'pip', 'check'], app, state / 'pip-check.log')
    run([python, '-c', 'import tkinter,pytest,pandas,numpy,sqlalchemy,matplotlib'], app,
        state / 'imports.log')
    if not shutil.which('g++'):
        raise SetupError('g++ ausente: use bootstrap-dev do seu SO para instalar a toolchain.')
    run([python, '-m', 'mt5_extracao.bootstrap', '--root', app,
         'doctor', '--no-probe', '--json'], app, state / 'doctor.json')


def prepare(workspace, plan=False, checks=True, run=command):
    workspace = workspace.expanduser().resolve()
    app = workspace / 'repos' / 'app'
    manifest = load_manifest()
    if plan:
        return {'mode': 'plan', 'workspace': str(workspace), 'sources': manifest['repositories'],
                'environment': str(app / '.venv'), 'private_database_required': False,
                'native_installation': 'somente com --with-mt5 / -WithMT5 no launcher'}
    if app.resolve() != ROOT:
        raise SetupError('Execute o wrapper do clone em WORKSPACE/repos/app; não use a árvore do proprietário.')
    if Path(sys.prefix).resolve() != (app / '.venv').resolve():
        raise SetupError('Execute pelo Python do .venv criado pelo instalador oficial.')
    app_head = inspect_checkout(app, manifest['repositories']['app']['url'], run=run)
    run(['git', 'merge-base', '--is-ancestor', manifest['repositories']['app']['commit'], app_head], app)
    # Validar destinos existentes ANTES de pip/config/atalhos.
    for key in ('hadouken', 'safety'):
        inspect_checkout(workspace / 'repos' / key, manifest['repositories'][key]['url'],
                         manifest['repositories'][key]['commit'], run)
    state = workspace / 'state'
    state.mkdir(parents=True, exist_ok=True)
    if os.name != 'nt':
        state.chmod(0o700)
    receipt = {'status': 'preparing', 'app': app_head, 'checks_requested': checks,
               'native_qualified': False, 'database_restored': False, 'repositories': {}}
    receipt_path = state / 'setup.json'
    try:
        for key in ('hadouken', 'safety'):
            receipt['repositories'][key] = ensure_checkout(workspace / 'repos' / key,
                                                          manifest['repositories'][key], run)
        python = venv_python(app)
        run([python, '-m', 'pip', 'install', '--disable-pip-version-check', '-r',
             app / 'requirements-dev.txt'], app, state / 'dependencies.log')
        configure_libraries(app, workspace)
        verify_readiness(app, python, state, run)
        if checks:
            run([python, '-m', 'pytest', '-q', 'tests/test_recovery_verification.py',
                 'tests/test_packaging.py', 'tests/test_development_host.py'], app, state / 'tests.log')
            run([python, 'Tools/check_repo.py'], workspace / 'repos' / 'hadouken', state / 'hadouken-check.log')
            run([python, '-m', 'unittest', 'discover', '-s', 'Tools/tests', '-p', 'test_super_atr.py'],
                workspace / 'repos' / 'hadouken', state / 'super-atr-tests.log')
        # O mesmo criador de atalhos usado pelo instalador: desktop e menu coerentes.
        run([python, '-m', 'mt5_extracao.bootstrap', '--root', app, 'shortcuts'], app, state / 'shortcuts.log')
        for folder in ('worktrees', 'studies', 'downloads'):
            (workspace / folder).mkdir(exist_ok=True)
        receipt['status'] = 'development_ready' if checks else 'environment_only_checks_pending'
    except Exception as error:
        receipt['status'] = 'failed'
        receipt['error'] = str(error)
        raise
    finally:
        receipt_path.write_text(json.dumps(receipt, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    return receipt


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workspace', type=Path, required=True)
    parser.add_argument('--plan', action='store_true')
    parser.add_argument('--no-checks', action='store_true', help='Só ambiente; não declara pronto para desenvolvimento')
    args = parser.parse_args(argv)
    try:
        result = prepare(args.workspace, args.plan, not args.no_checks)
    except (SetupError, OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        print(json.dumps({'ok': False, 'error': str(error), 'preserve_existing': True}, ensure_ascii=False))
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    sys.exit(main())
