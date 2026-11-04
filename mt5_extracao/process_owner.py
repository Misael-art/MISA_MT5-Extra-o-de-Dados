"""Identidade local de processo; dúvida ou outro host nunca comprovam morte."""
import math
import os
import socket
import hashlib
from pathlib import Path


def _scope():
    """Distingue máquinas, boots e namespaces sem persistir os identificadores brutos."""
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
        return hashlib.sha256("\0".join(parts).encode()).hexdigest()
    except (OSError, ValueError, ImportError):
        return None


def current():
    import psutil
    try:
        return dict(host=socket.gethostname(), pid=os.getpid(), started=psutil.Process().create_time(), host_scope=_scope())
    except (psutil.Error, OSError):
        return None


def is_alive(owner):
    """False somente quando a identidade local já não pode estar executando."""
    import psutil
    if not isinstance(owner, dict) or not isinstance(owner.get("host"), str) or not owner["host"]:
        return True
    if owner["host"] != socket.gethostname():
        return True
    if "host_scope" in owner and (not owner["host_scope"] or owner["host_scope"] != _scope()):
        return True
    pid, started = owner.get("pid"), owner.get("started")
    if (not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0 or
            not isinstance(started, (int, float)) or isinstance(started, bool) or
            not math.isfinite(started) or started < 0):
        return True
    try:
        return psutil.Process(pid).create_time() == started
    except psutil.NoSuchProcess:
        return False
    except (psutil.Error, OSError):
        return True
