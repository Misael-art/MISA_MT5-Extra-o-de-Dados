"""Identidade local de processo; dúvida ou outro host nunca comprovam morte."""
import math
import os
import socket
import hashlib
import uuid
from pathlib import Path


def _scope_parts():
    """Lê os componentes da identidade local; ausência mantém o resultado indeterminado."""
    try:
        if os.name == "nt":
            import winreg
            with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Microsoft\Cryptography",
                                0, winreg.KEY_READ | winreg.KEY_WOW64_64KEY) as key:
                machine = winreg.QueryValueEx(key, "MachineGuid")[0]
            # boot_time() do psutil pode variar entre processos no Windows.
            # MachineGuid fixa a máquina; create_time distingue gerações do PID.
            parts = [machine]
        else:
            parts = [Path("/etc/machine-id").read_text().strip(),
                     Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
                     str(Path("/proc/self/ns/pid").stat().st_ino)]
        if not all(isinstance(value, str) and value for value in parts):
            return None
        return parts
    except (OSError, ValueError, ImportError):
        return None


def _scope_hash(parts):
    return hashlib.sha256("\0".join(parts).encode()).hexdigest()


def _scope():
    """Mesmo digest de máquina, boot e namespace usado pelas identidades legadas."""
    parts = _scope_parts()
    return _scope_hash(parts) if parts else None


def current():
    import psutil
    try:
        owner = dict(host=socket.gethostname(), pid=os.getpid(), started=psutil.Process().create_time(), host_scope=_scope())
        parts = _scope_parts()
        if parts and len(parts) == 3 and owner["host_scope"] == _scope_hash(parts):
            owner.update(boot_id=parts[1], pid_namespace=parts[2])
        return owner
    except (psutil.Error, OSError):
        return None


def _verified_previous_boot(owner):
    """Prova a mesma máquina e um boot encerrado, com digest e cronologia consistentes."""
    import psutil
    parts = _scope_parts()
    if not parts or len(parts) != 3:
        return False
    boot, namespace = owner.get("boot_id"), owner.get("pid_namespace")
    if (not isinstance(boot, str) or not isinstance(namespace, str)
            or len(namespace) > 20 or not namespace.isdecimal()):
        return False
    try:
        if not 0 < int(namespace) <= 2**64 - 1:
            return False
        if str(uuid.UUID(boot)) != boot or str(uuid.UUID(parts[1])) != parts[1] or boot == parts[1]:
            return False
        boot_time = psutil.boot_time()
        if (not isinstance(boot_time, (int, float)) or isinstance(boot_time, bool)
                or not math.isfinite(boot_time) or boot_time <= 0 or owner["started"] >= boot_time):
            return False
        return owner["host_scope"] == _scope_hash([parts[0], boot, namespace])
    except (ValueError, TypeError, OSError, psutil.Error):
        return False


def is_alive(owner):
    """False somente quando a identidade local já não pode estar executando."""
    import psutil
    if not isinstance(owner, dict) or not isinstance(owner.get("host"), str) or not owner["host"]:
        return True
    if owner["host"] != socket.gethostname():
        return True
    pid, started = owner.get("pid"), owner.get("started")
    if (not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0 or
            not isinstance(started, (int, float)) or isinstance(started, bool) or
            not math.isfinite(started) or started < 0):
        return True
    local_scope = _scope()
    if not owner.get("host_scope") or not local_scope:
        return True
    if owner["host_scope"] != local_scope:
        return not _verified_previous_boot(owner)
    try:
        return psutil.Process(pid).create_time() == started
    except psutil.NoSuchProcess:
        return False
    except (psutil.Error, OSError):
        return True
