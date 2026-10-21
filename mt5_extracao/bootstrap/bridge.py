"""Ciclo de vida do scripts/mt5-bridge.sh; somente biblioteca padrão.

O processo da ponte tem sessão própria. O arquivo PID permanece compatível;
o registro adjacente comprova nascimento e posse antes de enviar sinais.
Não encerra Wine, terminal ou servidor de display.
"""
import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import configparser
import ctypes
import json
import os
from pathlib import Path, PureWindowsPath
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import time

from . import display, paths


class BridgeError(RuntimeError):
    pass


def _libc_call(name, args, types):
    # Alguns builds de CPython não expõem pidfd apesar do suporte da libc.
    # A API C mantém o sinal vinculado ao processo, sem selecionar outro PID.
    try:
        fn = getattr(ctypes.CDLL(None, use_errno=True), name)
    except AttributeError:
        raise BridgeError('Este Linux não oferece pidfd para encerramento seguro. Nenhum sinal enviado; '
                          'atualize o Linux ou pare a instância original.') from None
    fn.argtypes, fn.restype = types, ctypes.c_int
    result = fn(*args)
    if result < 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    return result


def pidfd_open(pid):
    if hasattr(os, 'pidfd_open'):
        return os.pidfd_open(pid)
    return _libc_call('pidfd_open', (pid, 0), (ctypes.c_int, ctypes.c_uint))


def pidfd_send_signal(fd, sig):
    if hasattr(signal, 'pidfd_send_signal'):
        return signal.pidfd_send_signal(fd, sig)
    return _libc_call('pidfd_send_signal', (fd, sig, None, 0),
                      (ctypes.c_int, ctypes.c_int, ctypes.c_void_p, ctypes.c_uint))


def identity(pid):
    """Identidade Linux independente de nome/argv mutáveis durante o exec do Wine."""
    try:
        folder = Path('/proc') / str(int(pid))
        fields = (folder / 'stat').read_text().rsplit(')', 1)[1].split()
        if fields[0] == 'Z':
            return None
        return {'pid': int(pid), 'start_ticks': fields[19],
                'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                'uid': folder.stat().st_uid}
    except (OSError, ValueError, IndexError):
        return None


class Bridge:
    def __init__(self, root, *, timeout=60):
        self.root = Path(root).resolve()
        self.config = configparser.ConfigParser(interpolation=None)
        self.config.read(self.root / 'config/config.ini', encoding='utf-8')
        self.folder = self.root / 'logs'
        self.folder.mkdir(parents=True, exist_ok=True)
        self.pid_file = self.folder / 'mt5_bridge.pid'
        self.record_file = self.folder / 'mt5_bridge_process.json'
        self.log_file = self.folder / 'mt5_bridge.log'
        self.server = self.root / 'scripts/mt5_bridge_server.py'
        self.host = self.config.get('BRIDGE', 'host', fallback='127.0.0.1')
        self.port = self.config.getint('BRIDGE', 'port', fallback=18812)
        self.timeout = timeout

    @contextmanager
    def locked(self):
        import fcntl
        with (self.folder / 'mt5_bridge.lock').open('a') as handle:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise BridgeError('Outra ação da ponte está em andamento. Aguarde seu término e tente novamente.') from None
            yield

    def port_open(self):
        try:
            with socket.create_connection((self.host, self.port), timeout=.3):
                return True
        except OSError:
            return False

    def record(self):
        try:
            record = json.loads(self.record_file.read_text())
            if (record['root'] != str(self.root) or
                    record['identity']['pid'] != int(self.pid_file.read_text().strip()) or
                    not isinstance(record['token'], str) or len(record['token']) != 32):
                return None
            return record
        except (OSError, ValueError, KeyError, TypeError):
            return None

    def owned(self, record):
        if not record or record['identity'] != identity(record['identity']['pid']):
            return False
        if record['identity']['uid'] != os.getuid():
            return False
        try:
            env = (Path('/proc') / str(record['identity']['pid']) / 'environ').read_bytes().split(b'\0')
            return ('MT5X_BRIDGE_TOKEN=' + record['token']).encode() in env
        except OSError:
            return False

    def _write(self, process_identity, token):
        record = {'version': 1, 'root': str(self.root), 'identity': process_identity, 'token': token}
        temp = self.record_file.with_suffix('.tmp')
        temp.write_text(json.dumps(record), encoding='utf-8')
        temp.chmod(0o600)
        temp.replace(self.record_file)
        self.pid_file.write_text(str(process_identity['pid']) + '\n')
        return record

    def _clear(self):
        self.pid_file.unlink(missing_ok=True)
        self.record_file.unlink(missing_ok=True)

    def _command(self):
        prefix = self.config.get('MT5', 'wine_prefix', fallback='').strip()
        if not prefix or not Path(prefix).expanduser().is_dir():
            raise BridgeError('Prefixo Wine ausente/inválido. Confira [MT5] wine_prefix ou execute ./install.sh.')
        wine = shutil.which('wine')
        winepath = shutil.which('winepath')
        if not wine or not winepath:
            raise BridgeError('Wine/winepath não encontrado. Instale o Wine com ./install.sh e tente novamente.')
        env = display.child_env(self.config)
        env.update(WINEPREFIX=str(Path(prefix).expanduser()), WINEDEBUG='-all')
        if not env.get('DISPLAY'):
            env['DISPLAY'] = display.ensure(display.DEFAULT_DISPLAY)
        try:
            converted = subprocess.run([winepath, '-w', str(self.server)], env=env, capture_output=True,
                                       text=True, timeout=15, check=True).stdout.strip()
        except (OSError, subprocess.SubprocessError):
            raise BridgeError('Não foi possível converter o caminho da ponte. Confira Wine/prefixo e execute ./install.sh.') from None
        if not converted:
            raise BridgeError('winepath não devolveu o caminho da ponte. Confira [MT5] wine_prefix.')
        python = self.config.get('BRIDGE', 'wine_python',
                                 fallback=str(PureWindowsPath(paths.WINE_PYTHON_WINDOWS_DIR) / 'python.exe'))
        return [wine, python, converted, '--host', self.host, '--port', str(self.port)], env

    def start(self):
        if not self.config.getboolean('BRIDGE', 'enabled', fallback=False):
            print('Ponte desativada ([BRIDGE] enabled != true). Nada a fazer.')
            return 0
        record = self.record()
        if self.owned(record):
            if self.port_open():
                print(f"Ponte já está em execução (PID {record['identity']['pid']}) em {self.host}:{self.port}.")
                return 0
            raise BridgeError('O processo próprio está ativo, mas a porta não responde. Veja logs/mt5_bridge.log e use stop antes de start.')
        if self.port_open():
            raise BridgeError('A porta da ponte está ocupada sem posse comprovada. Nenhum processo foi alterado; confira host/porta e o serviço existente.')
        if self.pid_file.exists() and (record is None or identity(record['identity']['pid']) is not None):
            raise BridgeError('PID antigo ou sem identidade comprovada. Nenhum processo foi alterado; confira logs/mt5_bridge.pid e pare a instância original antes de iniciar.')
        command, env = self._command()
        token = secrets.token_hex(16)
        env['MT5X_BRIDGE_TOKEN'] = token
        with self.log_file.open('a', encoding='utf-8') as log:
            log.write(f'===== {datetime.now(timezone.utc).isoformat()} iniciando ponte em sessão própria =====\n')
            log.flush()
            child = subprocess.Popen(command, env=env, stdin=subprocess.DEVNULL,
                                     stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        process_identity = identity(child.pid)
        if process_identity is None:
            child.wait(timeout=2)
            raise BridgeError('A ponte encerrou durante a inicialização. Veja logs/mt5_bridge.log e confira rpyc/MetaTrader5 no Python do Wine com ./install.sh.')
        record = self._write(process_identity, token)
        deadline = time.monotonic() + self.timeout
        while time.monotonic() < deadline:
            if child.poll() is not None:
                self._clear()
                raise BridgeError('A ponte encerrou durante a inicialização. Veja logs/mt5_bridge.log e confira rpyc/MetaTrader5 no Python do Wine com ./install.sh.')
            if self.owned(record) and self.port_open():
                print(f'Ponte MT5 iniciada em {self.host}:{self.port} (PID {child.pid}, sessão própria).')
                return 0
            time.sleep(.1)
        self.stop()
        raise BridgeError('A ponte não respondeu no prazo. Veja logs/mt5_bridge.log; confira host/porta e Python do Wine antes de iniciar novamente.')

    def _signal(self, record, sig):
        try:
            fd = pidfd_open(record['identity']['pid'])
        except ProcessLookupError:
            return
        try:
            if not self.owned(record):
                raise BridgeError('A identidade do processo mudou. Nenhum sinal enviado; confira a instância original.')
            pidfd_send_signal(fd, sig)
        except ProcessLookupError:
            pass
        finally:
            os.close(fd)

    def stop(self):
        record = self.record()
        if record and identity(record['identity']['pid']) is None:
            self._clear()
            print('Ponte já encerrada; registro próprio removido.')
            return 0
        if not self.owned(record):
            if self.pid_file.exists() or self.record_file.exists() or self.port_open():
                raise BridgeError('Posse da ponte não comprovada. Nenhum processo foi encerrado; confira o registro em logs e pare a instância original.')
            print('Ponte MT5 já está parada.')
            return 0
        self._signal(record, signal.SIGTERM)
        target = record['identity']
        # O ambiente pode ser desmontado antes de /proc/stat marcar a saída.
        # Perder o token não confirma término; identidade nova jamais recebe sinal.
        deadline = time.monotonic() + 5
        while identity(target['pid']) == target and time.monotonic() < deadline:
            time.sleep(.1)
        if identity(target['pid']) == target:
            self._signal(record, signal.SIGKILL)  # Revalida posse antes de sinalizar.
            deadline = time.monotonic() + 2
            while identity(target['pid']) == target and time.monotonic() < deadline:
                time.sleep(.1)
        if identity(target['pid']) == target:
            raise BridgeError('A ponte própria não encerrou. Confira logs/mt5_bridge.log; terminal e demais processos foram preservados.')
        self._clear()
        print('Ponte MT5 parada; terminal, Wine e display preservados.')
        return 0

    def status(self):
        if self.owned(self.record()) and self.port_open():
            print(f'Ponte MT5: ATIVA em {self.host}:{self.port} (processo próprio).')
            return 0
        if self.port_open():
            raise BridgeError('Porta acessível, mas posse da ponte não comprovada. Confira o serviço existente; nenhum processo foi alterado.')
        print('Ponte MT5: PARADA. Para iniciar: ./scripts/mt5-bridge.sh start')
        return 3

    def foreground(self):
        if self.pid_file.exists() or self.port_open():
            raise BridgeError('Já existe registro/serviço da ponte. Use status e stop antes de foreground.')
        command, env = self._command()
        token = secrets.token_hex(16)
        env['MT5X_BRIDGE_TOKEN'] = token
        self._write(identity(os.getpid()), token)
        os.execvpe(command[0], command, env)


def main(argv=None):
    parser = argparse.ArgumentParser(description='Gerenciador da ponte MT5 (Linux/Wine).')
    parser.add_argument('--root', default=str(paths.PROJECT_ROOT))
    parser.add_argument('action', choices=('start', 'stop', 'restart', 'status', 'foreground'))
    args = parser.parse_args(argv)
    if paths.is_windows() or not sys.platform.startswith('linux'):
        print('Este gerenciador é para Linux/Wine. No Windows use a conexão MT5 nativa.', file=sys.stderr)
        return 1
    try:
        manager = Bridge(args.root)
        with manager.locked():
            if args.action == 'restart':
                manager.stop()
                return manager.start()
            return getattr(manager, args.action)()
    except (BridgeError, RuntimeError) as error:
        print(f'Ponte MT5: {error}', file=sys.stderr)
        return 1
    except (OSError, ValueError, configparser.Error) as error:
        print(f'Ponte MT5: {error}. Confira config/config.ini, permissões de logs/ '
              'e a instalação com ./install.sh antes de tentar novamente.', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
