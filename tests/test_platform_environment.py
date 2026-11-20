"""Composição aberta por dois sistemas: cada um usa o próprio ambiente e configuração, sem tocar no outro."""
from pathlib import Path
import os
import shutil
import subprocess

import pytest

from mt5_extracao.bootstrap import config_builder, paths

ROOT = Path(__file__).resolve().parents[1]


def linux_venv(root, name='.venv'):
    env = root / name
    (env / 'bin').mkdir(parents=True)
    (env / 'lib' / 'python3.11').mkdir(parents=True)
    (env / 'pyvenv.cfg').write_text('home = /home/u/.local/bin\nversion = 3.11.15\n', encoding='utf-8')
    target = env / 'bin' / 'python'
    try:
        target.symlink_to('/home/u/.local/bin/python3.11')   # quebrado quando visto do Windows
    except OSError:
        target.write_text('fixture')
    return env


def windows_venv(root, name='.venv'):
    env = root / name
    (env / 'Scripts').mkdir(parents=True)
    (env / 'Scripts' / 'python.exe').write_bytes(b'fixture, never executed')
    (env / 'pyvenv.cfg').write_text('home = C:\\Python311\nversion = 3.11.9\n', encoding='utf-8')
    return env


def test_platform_of_environment_is_read_from_structure(tmp_path):
    assert paths.venv_platform(tmp_path / 'ausente') is None
    assert paths.venv_platform(linux_venv(tmp_path, 'a')) == paths.POSIX
    assert paths.venv_platform(windows_venv(tmp_path, 'b')) == paths.WINDOWS
    mixed = windows_venv(tmp_path, 'c')
    (mixed / 'bin').mkdir()
    (mixed / 'bin' / 'python').write_text('fixture')
    assert paths.venv_platform(mixed) == paths.MIXED
    only_cfg = tmp_path / 'd'
    only_cfg.mkdir()
    (only_cfg / 'pyvenv.cfg').write_text('home = D:\\Py\n', encoding='utf-8')
    assert paths.venv_platform(only_cfg) == paths.WINDOWS


@pytest.mark.parametrize('windows', [True, False])
def test_single_system_installation_keeps_venv_and_config_ini(tmp_path, windows):
    assert paths.environment_dir(tmp_path, windows) == tmp_path / '.venv'
    (windows_venv if windows else linux_venv)(tmp_path)
    assert paths.environment_dir(tmp_path, windows) == tmp_path / '.venv'
    assert paths.foreign_environment(tmp_path, windows) is None
    assert paths.config_path(tmp_path, windows) == tmp_path / 'config' / 'config.ini'


def test_windows_never_reuses_linux_venv_and_gets_own_config(tmp_path):
    linux = linux_venv(tmp_path)
    before = sorted(p.relative_to(tmp_path).as_posix() for p in linux.rglob('*'))
    assert paths.environment_dir(tmp_path, True) == tmp_path / '.venv-windows'
    assert paths.foreign_environment(tmp_path, True) == linux
    assert paths.config_path(tmp_path, True) == tmp_path / 'config' / 'config.windows.ini'
    windows_venv(tmp_path, '.venv-windows')
    assert paths.environment_dir(tmp_path, True) == tmp_path / '.venv-windows'
    # O Linux continua dono do .venv e do config.ini.
    assert paths.environment_dir(tmp_path, False) == linux
    assert paths.config_path(tmp_path, False) == tmp_path / 'config' / 'config.ini'
    assert sorted(p.relative_to(tmp_path).as_posix() for p in linux.rglob('*')) == before


def test_registry_translates_only_the_same_composition(tmp_path, monkeypatch):
    import json

    workspace = tmp_path / "MT5X"
    monkeypatch.setattr(paths, "PROJECT_ROOT", workspace / "repos" / "app")
    registry = tmp_path / ".workspace" / "projects.json"
    registry.parent.mkdir()
    foreign = ("/outra-maquina/Projetos/MT5X" if paths.is_windows() else r"Z:\Projetos\MT5X")
    project = {"id": "MT5X", "path": foreign, "windows_host": {"path": str(workspace)}}
    if not paths.is_windows():
        project.update(path=str(workspace), windows_host={"path": foreign})
    registry.write_text(json.dumps({"projects": [project]}), encoding="utf-8")
    alias = paths.registered_composition_alias()
    assert alias is not None
    artifact = foreign + "/state/qa-tmp/biblioteca/Experts/Ref.mq5"
    assert paths.resolve_composition_path(artifact, alias) == workspace / "state/qa-tmp/biblioteca/Experts/Ref.mq5"
    outside = foreign + "/../outro/Experts/Ref.mq5"
    assert paths.resolve_composition_path(outside, alias) == Path(outside)
    unrelated = foreign + "-backup/Experts/Ref.mq5"
    assert paths.resolve_composition_path(unrelated, alias) == Path(unrelated)


def test_mixed_venv_is_not_selected(tmp_path):
    mixed = windows_venv(tmp_path)
    (mixed / 'bin').mkdir()
    (mixed / 'bin' / 'python').write_text('fixture')
    assert paths.environment_dir(tmp_path, True) == tmp_path / '.venv-windows'
    assert paths.environment_dir(tmp_path, False) == tmp_path / '.venv-posix'


def test_platform_seed_keeps_preferences_and_drops_foreign_paths(tmp_path):
    source = tmp_path / 'config.ini'
    source.write_text(
        '[MT5]\npath = /home/u/.local/share/mt5-extracao/wine/drive_c/Program Files/MetaTrader 5\n'
        'windows_path = C:\\Program Files\\MetaTrader 5\nwine_prefix = /home/u/wine\nhidden = true\n'
        '[BRIDGE]\nenabled = true\nport = 18812\n'
        '[DATABASE]\npath = database/mt5_data.db\n'
        '[ROBOTS]\nlibrary_dirs = /home/u/MT5X/repos/hadouken\nallow_real = false\n'
        '[RESOURCES]\narchive_dir = /mnt/arquivo\nkeep_days = 30\n', encoding='utf-8')
    before = source.read_bytes()
    seed = config_builder.platform_seed(source, windows=True)
    assert source.read_bytes() == before
    assert not seed.has_section('BRIDGE')
    assert dict(seed['MT5']) == {'hidden': 'true'}
    assert seed['DATABASE']['path'] == 'database/mt5_data.db'
    assert dict(seed['ROBOTS']) == {'allow_real': 'false'}
    assert dict(seed['RESOURCES']) == {'keep_days': '30'}
    windows_source = tmp_path / 'windows.ini'
    windows_source.write_text('[RESOURCES]\narchive_dir = D:\\arquivo\nbackup_dir = \\\\nas\\bk\n', encoding='utf-8')
    assert dict(config_builder.platform_seed(windows_source, windows=False)['RESOURCES']) == {}
    assert config_builder.platform_seed(tmp_path / 'ausente.ini') is None


def test_configure_seeds_platform_config_without_touching_shared_ini(tmp_path, monkeypatch):
    from mt5_extracao.bootstrap import __main__ as cli
    linux_venv(tmp_path)
    shared = tmp_path / 'config' / 'config.ini'
    shared.parent.mkdir()
    shared.write_text('# do Linux\n[MT5]\npath = /home/u/mt5\n[STRATEGY]\nrisk_per_trade = 0.5\n', encoding='utf-8')
    before = shared.read_bytes()
    monkeypatch.setattr(paths, 'is_windows', lambda: True)
    monkeypatch.setattr(cli.mt5_locator, 'find_installations', lambda **kw: [])
    assert cli.main(['--root', str(tmp_path), 'configure', '--non-interactive', '--no-doctor',
                     '--skip-credentials']) == 0
    target = tmp_path / 'config' / 'config.windows.ini'
    written = config_builder.load_config(target)
    assert written['STRATEGY']['risk_per_trade'] == '0.5'
    assert not written.get('MT5', 'path', fallback='').startswith('/')
    assert shared.read_bytes() == before
    # Segunda execução preserva o arquivo do Windows (idempotente).
    config_builder.set_config_value(target, 'STRATEGY', 'risk_per_trade', '0.25')
    assert cli.main(['--root', str(tmp_path), 'configure', '--non-interactive', '--no-doctor',
                     '--skip-credentials']) == 0
    assert config_builder.load_config(target)['STRATEGY']['risk_per_trade'] == '0.25'
    assert shared.read_bytes() == before


def _cmd():
    return shutil.which('cmd') if os.name == 'nt' else None


def _unc(path):
    """Mesmo diretório visto por UNC (compartilhamento administrativo local), se disponível."""
    drive, rest = os.path.splitdrive(str(path))
    candidate = Path(r'\\localhost\%s$%s' % (drive.rstrip(':'), rest))
    try:
        return candidate if candidate.is_dir() else None
    except OSError:
        return None


@pytest.mark.skipif(_cmd() is None, reason='lançador .bat exige cmd.exe')
@pytest.mark.parametrize('folder,unc', [('com espaço', False), ('açúcar ção', False), ('com espaço', True)])
def test_run_bat_selects_windows_environment_in_spaces_accents_and_unc(tmp_path, folder, unc):
    """run.bat real com ambiente leve: escolhe .venv-windows, preserva o .venv Linux, aceita UNC."""
    import sys
    app = tmp_path / folder / 'app'
    app.mkdir(parents=True)
    shutil.copyfile(ROOT / 'run.bat', app / 'run.bat')
    linux = linux_venv(app)
    subprocess.run([sys.executable, '-m', 'venv', '--without-pip', str(app / '.venv-windows')],
                   check=True, capture_output=True, timeout=120)
    (app / 'config').mkdir()
    (app / 'config' / 'config.windows.ini').write_text('[APP]\n', encoding='utf-8')
    (app / 'app.py').write_text('import os, sys\nprint("CWD=" + os.getcwd())\nprint("EXE=" + sys.executable)\n',
                                encoding='utf-8')
    package = app / 'mt5_extracao' / 'bootstrap'
    package.mkdir(parents=True)
    (app / 'mt5_extracao' / '__init__.py').write_text('', encoding='utf-8')
    (package / '__init__.py').write_text('', encoding='utf-8')
    shutil.copyfile(ROOT / 'mt5_extracao' / 'bootstrap' / 'paths.py', package / 'paths.py')
    launcher = app / 'run.bat'
    if unc:
        launcher = _unc(launcher.parent)
        if launcher is None:
            pytest.skip('compartilhamento UNC local indisponível neste host')
        launcher = launcher / 'run.bat'
    result = subprocess.run(['cmd', '/d', '/c', str(launcher)], capture_output=True, timeout=120,
                            cwd=str(tmp_path), stdin=subprocess.DEVNULL)
    out = result.stdout.decode('utf-8', 'replace') + result.stderr.decode('utf-8', 'replace')
    assert result.returncode == 0, out
    assert 'EXE=' in out and '.venv-windows' in out
    assert 'UNC' not in out.split('CWD=')[0]
    assert (linux / 'pyvenv.cfg').read_text(encoding='utf-8').startswith('home = /home/u')
    assert not (linux / 'Scripts').exists()


def _powershell():
    return shutil.which('powershell') if os.name == 'nt' else None


def _git(path, *args):
    return subprocess.check_output(['git', '-C', str(path), *args], text=True, encoding='utf-8').strip()


@pytest.mark.skipif(_powershell() is None or shutil.which('git') is None,
                    reason='wrapper PowerShell exige Windows e Git')
@pytest.mark.parametrize('codepage', ['850', '65001'])
def test_bootstrap_dev_reuses_own_composition_and_preserves_wip(tmp_path, codepage):
    """bootstrap-dev.ps1 real: deriva a Workspace da própria pasta, aceita WIP sem alterá-lo,
    recusa outra Workspace e chama o orquestrador com o Python do ambiente Windows.

    Console novo em OEM 850 (padrão pt-BR) e em UTF-8: a saída do git com acentos precisa coincidir."""
    import sys
    workspace = tmp_path / 'Composição com espaço'
    app = workspace / 'repos' / 'app'
    app.mkdir(parents=True)
    _git(app, 'init', '-q')
    _git(app, 'config', 'user.name', 'QA')
    _git(app, 'config', 'user.email', 'qa@example.invalid')
    (app / 'source.txt').write_text('original', encoding='utf-8')
    _git(app, 'add', 'source.txt')
    _git(app, 'commit', '-q', '-m', 'fixture')
    _git(app, 'remote', 'add', 'origin', 'https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados.git')
    shutil.copyfile(ROOT / 'bootstrap-dev.ps1', app / 'bootstrap-dev.ps1')
    (app / 'source.txt').write_text('WIP do proprietário', encoding='utf-8')
    linux_venv(app)
    subprocess.run([sys.executable, '-m', 'venv', '--without-pip', str(app / '.venv-windows')],
                   check=True, capture_output=True, timeout=120)
    record = tmp_path / 'setup-args.txt'
    (app / 'scripts').mkdir()
    (app / 'scripts' / 'setup_development.py').write_text(
        'import pathlib, sys\n'
        'pathlib.Path(%r).write_text(chr(10).join([sys.executable] + sys.argv[1:]), encoding="utf-8")\n'
        % str(record), encoding='utf-8')
    tools = tmp_path / 'tools'
    tools.mkdir()
    (tools / 'g++.cmd').write_text('@echo off\r\nexit /b 0\r\n', encoding='ascii')
    status_before = _git(app, 'status', '--porcelain')
    index_before = (app / '.git' / 'index').read_bytes()
    env = {**os.environ, 'PATH': str(tools) + os.pathsep + os.environ['PATH']}
    wrapper = ['powershell', '-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', str(app / 'bootstrap-dev.ps1')]

    def run(extra, environment):
        command = 'chcp %s >nul & %s' % (codepage, subprocess.list2cmdline(wrapper + extra))
        # Linha pronta: o cmd não entende aspas escapadas com barra invertida (list2cmdline externo).
        return subprocess.run('cmd /d /s /c "%s"' % command, capture_output=True, timeout=180,
                              env=environment, stdin=subprocess.DEVNULL,
                              creationflags=getattr(subprocess, 'CREATE_NEW_CONSOLE', 0))
    result = run(['-Yes'], env)
    out = (result.stdout + result.stderr).decode('utf-8', 'replace')
    assert result.returncode == 0, out
    args = record.read_text(encoding='utf-8').splitlines()
    assert '.venv-windows' in args[0]
    assert args[-2] == '--workspace' and Path(args[-1]) == workspace
    assert _git(app, 'status', '--porcelain') == status_before
    assert (app / '.git' / 'index').read_bytes() == index_before
    assert (app / 'source.txt').read_text(encoding='utf-8') == 'WIP do proprietário'
    other = tmp_path / 'Outra'
    refused = run(['-Yes', '-Workspace', str(other)], env)
    assert refused.returncode != 0
    assert not other.exists()
    # Sem g++: prepara a instrução (pacote >70 MB) e não baixa nada.
    # Sem g++ no PATH e sem pacote do winget: o wrapper também procura em %LOCALAPPDATA%\Microsoft\WinGet.
    no_tools = {**os.environ, 'LOCALAPPDATA': str(tmp_path / 'sem-winget'),
                'PATH': os.pathsep.join(p for p in os.environ['PATH'].split(os.pathsep)
                                        if not (Path(p) / 'g++.exe').exists())}
    record.unlink()
    manual = run(['-Yes'], no_tools)
    if shutil.which('g++', path=no_tools['PATH']) is None:
        assert manual.returncode == 3, (manual.stdout + manual.stderr).decode('utf-8', 'replace')
        assert b'winget install --id BrechtSanders.WinLibs' in manual.stdout
        assert not record.exists()


def test_default_config_follows_the_effective_project_root(tmp_path, monkeypatch):
    """Regressão: o padrão não pode ficar congelado na raiz deste checkout (achado no Windows físico)."""
    from mt5_extracao import services
    monkeypatch.setattr(paths, 'is_windows', lambda: True)
    dual, single = tmp_path / 'dupla', tmp_path / 'simples'
    linux_venv(dual)
    for root, name in ((dual, 'config.windows.ini'), (single, 'config.ini')):
        (root / 'config').mkdir(parents=True)
        (root / 'config' / name).write_text('[DATABASE]\n', encoding='utf-8')
    elsewhere = tmp_path / 'outra pasta'
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    for root, name in ((dual, 'config.windows.ini'), (single, 'config.ini')):
        monkeypatch.setattr(services, 'project_root', lambda root=root: str(root))
        assert Path(services.resolve_config_path()) == root / 'config' / name
        assert Path(services.resolve_config_path(services.DEFAULT_CONFIG_PATH)) == root / 'config' / name
    explicit = tmp_path / 'qa.ini'
    explicit.write_text('[APP]\n', encoding='utf-8')
    assert services.resolve_config_path(str(explicit)) == str(explicit)


def test_default_config_is_read_from_the_current_folder_first(tmp_path, monkeypatch):
    """Regressão do Windows físico: app/CLI iniciados numa pasta com config/config.ini própria a usam,
    mesmo quando a raiz do código tem .venv de outro sistema (config.windows.ini)."""
    from mt5_extracao import services
    monkeypatch.setattr(paths, 'is_windows', lambda: True)
    code_root = tmp_path / 'checkout'
    linux_venv(code_root)
    (code_root / 'config').mkdir(parents=True)
    (code_root / 'config' / 'config.windows.ini').write_text('[DATABASE]\n', encoding='utf-8')
    monkeypatch.setattr(services, 'project_root', lambda: str(code_root))
    project = tmp_path / 'projeto do usuário'
    (project / 'config').mkdir(parents=True)
    (project / 'config' / 'config.ini').write_text('[DATABASE]\n', encoding='utf-8')
    monkeypatch.chdir(project)
    assert Path(services.resolve_config_path()) == Path('config') / 'config.ini'
    monkeypatch.chdir(code_root)
    assert Path(services.resolve_config_path()) == Path('config') / 'config.windows.ini'
