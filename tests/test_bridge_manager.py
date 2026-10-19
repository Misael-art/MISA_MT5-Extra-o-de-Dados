"""Gerenciador real com executáveis Wine falsos e a ponte restrita existente."""
import configparser
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time

import pytest

from mt5_extracao.bootstrap import bridge

REPO = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.skipif(sys.platform != 'linux', reason='gerenciador de processos Linux/Wine')


@pytest.fixture
def managed(tmp_path, monkeypatch):
    pytest.importorskip('rpyc')
    root = tmp_path / 'instância com espaços'
    (root / 'scripts').mkdir(parents=True)
    (root / 'config').mkdir()
    (root / 'prefix').mkdir()
    (root / '.venv').symlink_to(sys.prefix, target_is_directory=True)
    for name in ('mt5-bridge.sh', 'mt5_bridge_server.py'):
        target = root / 'scripts' / name
        target.write_bytes((REPO / 'scripts' / name).read_bytes())
        target.chmod(0o755)
    binaries = root / 'bin'
    binaries.mkdir()
    wine = binaries / 'wine'
    wine.write_text('#!' + sys.executable + '\nimport os,sys\nos.execv(sys.argv[1], sys.argv[1:])\n')
    wine.chmod(0o755)
    converter = binaries / 'winepath'
    converter.write_text('#!' + sys.executable + '\nimport sys\nprint(sys.argv[2])\n')
    converter.chmod(0o755)
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    cfg = configparser.ConfigParser(interpolation=None)
    cfg['MT5'] = {'wine_prefix': str(root / 'prefix'), 'hidden': 'false'}
    cfg['BRIDGE'] = {'enabled': 'true', 'host': '127.0.0.1', 'port': str(port),
                     'wine_python': sys.executable}
    with (root / 'config/config.ini').open('w') as handle:
        cfg.write(handle)
    monkeypatch.setenv('PATH', str(binaries) + os.pathsep + os.environ['PATH'])
    monkeypatch.setenv('PYTHONPATH', os.pathsep.join([str(REPO), str(REPO / 'tests/fake_mt5_package')]))
    monkeypatch.setenv('DISPLAY', os.environ.get('DISPLAY') or ':fake-test')
    monkeypatch.setenv('MT5X_HIDDEN', '0')
    manager = bridge.Bridge(root, timeout=5)
    yield manager
    record = manager.record()
    if manager.owned(record):
        manager.stop()


def run(manager, action):
    return subprocess.run([str(manager.root / 'scripts/mt5-bridge.sh'), action],
                          capture_output=True, text=True, timeout=12)


def test_start_survives_initiator_group_exit_status_repeat_stop(managed):
    # Simula o encerramento do grupo do comando iniciador, que nohup não
    # protege de SIGTERM. Não sinaliza nenhum grupo externo ao teste.
    launcher = subprocess.Popen([sys.executable, '-c',
        'import os,signal,subprocess,sys; '
        'code=subprocess.call([sys.argv[1],"start"]); '
        'sys.stdout.flush(); '
        'sys.exit(code) if code else os.killpg(os.getpgrp(),signal.SIGTERM)',
        str(managed.root / 'scripts/mt5-bridge.sh')],
        start_new_session=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    stdout, stderr = launcher.communicate(timeout=12)
    assert launcher.returncode == -signal.SIGTERM, stdout + stderr
    first = managed.record()
    assert managed.owned(first) and managed.port_open()
    assert os.getsid(first['identity']['pid']) != launcher.pid
    assert run(managed, 'status').returncode == 0
    repeat = run(managed, 'start')
    assert repeat.returncode == 0 and 'já está em execução' in repeat.stdout
    assert managed.record() == first
    # A aplicação consulta o provedor pela mesma ponte restrita, sem MT5 real.
    from mt5_extracao.mt5_backend import RemoteMT5
    from datetime import datetime
    remote = RemoteMT5(port=managed.port)
    try:
        bars = remote.copy_rates_range('WIN$N', remote.TIMEFRAME_M1,
                                      datetime(2024, 1, 2, 10), datetime(2024, 1, 2, 10, 5))
        assert len(bars) > 0
        with pytest.raises(Exception):
            remote.order_send({})
    finally:
        remote._conn.close()
    assert run(managed, 'stop').returncode == 0
    assert run(managed, 'status').returncode == 3
    assert run(managed, 'stop').returncode == 0
    assert not managed.pid_file.exists() and not managed.record_file.exists()
    assert 'sessão própria' in managed.log_file.read_text()


def test_concurrent_start_cannot_duplicate_server(managed):
    command = [str(managed.root / 'scripts/mt5-bridge.sh'), 'start']
    one = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    two = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    outputs = [one.communicate(timeout=12), two.communicate(timeout=12)]
    assert 0 in (one.returncode, two.returncode), outputs
    assert all(p.returncode in (0, 1) for p in (one, two))
    assert managed.owned(managed.record()) and managed.port_open()
    assert managed.log_file.read_text().count('iniciando ponte em sessão própria') == 1


def test_foreign_listener_is_never_adopted_or_stopped(managed):
    with socket.socket() as sock:
        sock.bind((managed.host, managed.port))
        sock.listen()
        for action in ('start', 'status', 'stop'):
            result = run(managed, action)
            assert result.returncode == 1
            assert 'comprovada' in result.stderr
            assert sock.fileno() >= 0
        assert not managed.record_file.exists()


@pytest.mark.parametrize('kind', ['legacy', 'reused', 'token_changed'])
def test_unrelated_pid_is_preserved(managed, kind):
    other = subprocess.Popen([sys.executable, '-c', 'import time;time.sleep(30)'])
    try:
        managed.pid_file.write_text(str(other.pid))
        if kind != 'legacy':
            record = managed._write(bridge.identity(other.pid), 'a' * 32)
            if kind == 'reused':
                record['identity']['start_ticks'] = '0'
                managed.record_file.write_text(json.dumps(record))
        result = run(managed, 'stop')
        assert result.returncode == 1 and 'Nenhum processo foi encerrado' in result.stderr
        assert other.poll() is None
        assert run(managed, 'start').returncode == 1
    finally:
        other.terminate()
        other.wait(timeout=5)


def test_failed_start_records_error_and_action(managed):
    managed.server.write_text('raise RuntimeError("dependência de teste ausente")\n')
    result = run(managed, 'start')
    assert result.returncode == 1
    assert 'logs/mt5_bridge.log' in result.stderr and './install.sh' in result.stderr
    assert 'dependência de teste ausente' in managed.log_file.read_text()
    assert not managed.pid_file.exists()


def test_stale_own_record_is_cleaned_without_signals(managed):
    assert run(managed, 'start').returncode == 0
    record = managed.record()
    managed._signal(record, signal.SIGTERM)
    until = time.monotonic() + 3
    while bridge.identity(record['identity']['pid']) is not None and time.monotonic() < until:
        time.sleep(.05)
    assert run(managed, 'stop').returncode == 0
    assert not managed.record_file.exists()


def test_disabled_and_invalid_config_are_actionable(managed):
    cfg = managed.config
    cfg.set('BRIDGE', 'enabled', 'false')
    with (managed.root / 'config/config.ini').open('w') as handle:
        cfg.write(handle)
    assert run(managed, 'start').returncode == 0
    assert not managed.pid_file.exists()
    cfg.set('BRIDGE', 'enabled', 'true')
    cfg.set('MT5', 'wine_prefix', '')
    with (managed.root / 'config/config.ini').open('w') as handle:
        cfg.write(handle)
    result = run(managed, 'start')
    assert result.returncode == 1 and 'wine_prefix' in result.stderr and './install.sh' in result.stderr


def test_busy_manager_does_not_spawn(managed):
    with managed.locked():
        result = run(managed, 'start')
    assert result.returncode == 1 and 'Aguarde' in result.stderr
    assert not managed.pid_file.exists()


def test_launcher_uses_its_own_checkout_from_another_checkout(managed, tmp_path):
    other = tmp_path / 'outro checkout'
    package = other / 'mt5_extracao'
    package.mkdir(parents=True)
    (package / '__init__.py').write_text('')
    result = subprocess.run([str(managed.root / 'scripts/mt5-bridge.sh'), 'status'],
                            cwd=other, capture_output=True, text=True, timeout=10)
    assert result.returncode == 3 and 'PARADA' in result.stdout
    assert 'No module named' not in result.stderr


def test_windows_path_reports_native_connection(monkeypatch, capsys):
    monkeypatch.setattr(bridge.paths, 'is_windows', lambda: True)
    assert bridge.main(['start']) == 1
    assert 'conexão MT5 nativa' in capsys.readouterr().err


def test_reused_pid_between_handle_and_signal_never_receives_signal(managed, monkeypatch):
    assert run(managed, 'start').returncode == 0
    record = managed.record()
    sent = []
    with monkeypatch.context() as patch:
        patch.setattr(managed, 'owned', lambda record: False)
        patch.setattr(bridge, 'pidfd_send_signal', lambda *args: sent.append(args))
        with pytest.raises(bridge.BridgeError, match='identidade'):
            managed._signal(record, signal.SIGTERM)
    assert not sent and managed.owned(record)


def test_pidfd_uses_libc_when_python_does_not_expose_it(managed, monkeypatch):
    assert run(managed, 'start').returncode == 0
    record = managed.record()
    monkeypatch.delattr(os, 'pidfd_open', raising=False)
    monkeypatch.delattr(signal, 'pidfd_send_signal', raising=False)
    assert managed.stop() == 0
    assert bridge.identity(record['identity']['pid']) is None


def test_stop_waits_for_identity_exit_after_environment_is_unmapped(tmp_path, monkeypatch):
    manager = bridge.Bridge(tmp_path)
    target = {"pid": 12345, "start_ticks": "1", "boot_id": "test", "uid": os.getuid()}
    manager._write(target, "a" * 32)
    state = {"alive": True, "owned": True, "waits": 0}
    sent = []
    monkeypatch.setattr(bridge, "identity", lambda pid: target if state["alive"] else None)
    monkeypatch.setattr(manager, "owned", lambda record: state["owned"])
    def send(record, sig):
        sent.append(sig)
        state["owned"] = False  # /proc/environ desaparece antes do fim da identidade.
    def wait(_seconds):
        assert manager.record() is not None
        state["waits"] += 1
        state["alive"] = False
    monkeypatch.setattr(manager, "_signal", send)
    monkeypatch.setattr(bridge.time, "sleep", wait)
    assert manager.stop() == 0
    assert state["waits"] == 1 and not state["alive"]
    assert sent == [signal.SIGTERM]
    assert not manager.record_file.exists()


def test_stop_does_not_signal_replacement_identity_during_wait(tmp_path, monkeypatch):
    manager = bridge.Bridge(tmp_path)
    target = {"pid": 12345, "start_ticks": "1", "boot_id": "test", "uid": os.getuid()}
    manager._write(target, "b" * 32)
    replacement = dict(target, start_ticks="2")
    state = {"identity": target}
    sent = []
    monkeypatch.setattr(bridge, "identity", lambda pid: state["identity"])
    monkeypatch.setattr(manager, "owned", lambda record: state["identity"] == target)
    monkeypatch.setattr(manager, "_signal", lambda record, sig: sent.append(sig))
    monkeypatch.setattr(bridge.time, "sleep", lambda seconds: state.update(identity=replacement))
    assert manager.stop() == 0
    assert state["identity"] == replacement and sent == [signal.SIGTERM]


def test_stop_preserves_record_without_escalating_if_token_is_lost(tmp_path, monkeypatch):
    manager = bridge.Bridge(tmp_path)
    target = {"pid": 12345, "start_ticks": "1", "boot_id": "test", "uid": os.getuid()}
    manager._write(target, "c" * 32)
    state = {"owned": True, "clock": 0}
    sent = []
    monkeypatch.setattr(bridge, "identity", lambda pid: target)
    monkeypatch.setattr(manager, "owned", lambda record: state["owned"])
    monkeypatch.setattr(bridge, "pidfd_open", lambda pid: os.open(os.devnull, os.O_RDONLY))
    def send(fd, sig):
        sent.append(sig)
        state["owned"] = False
    def clock():
        state["clock"] += 6
        return state["clock"]
    monkeypatch.setattr(bridge, "pidfd_send_signal", send)
    monkeypatch.setattr(bridge.time, "monotonic", clock)
    with pytest.raises(bridge.BridgeError, match="identidade"):
        manager.stop()
    assert sent == [signal.SIGTERM] and manager.record_file.exists()
