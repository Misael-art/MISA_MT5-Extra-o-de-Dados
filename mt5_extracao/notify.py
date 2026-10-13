"""
Avisos em pop-up no computador (notificações do sistema) para as ações executadas.

    notify.send(config, "Extração concluída", "EURUSD M1: 2 ok, 0 com falha")

Liga-se com `[APP] notifications = true`. No Linux usa `notify-send`; no Windows, um balão pelo PowerShell (não
validado). Sem ferramenta, sem sessão gráfica (cron, servidor) ou com o modo desligado, o aviso vira só uma linha
de log. `send` NUNCA levanta erro: um aviso que falha não pode derrubar uma extração nem um teste.

Não coloque em um aviso senha, saldo nem o número inteiro da conta: use `mask_account`.
Somente biblioteca padrão.
"""
import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Callable, Optional

log = logging.getLogger(__name__)

APP_NAME = "MT5 Extração"
INFO, WARNING, CRITICAL = "normal", "aviso", "crítico"
_URGENCY = {INFO: "normal", WARNING: "normal", CRITICAL: "critical"}
_TIMEOUT_MS = {INFO: "8000", WARNING: "12000", CRITICAL: "0"}       # 0 = fica até a pessoa fechar
_ICON = Path(__file__).parent / "assets" / "icon.png"
MAX_TEXT = 300


def enabled(config) -> bool:
    return config is not None and config.get("APP", "notifications", fallback="false").strip().lower() == "true"


def mask_account(login) -> str:
    """Só os quatro últimos dígitos: o número inteiro da conta não vai para um pop-up."""
    text = str(login or "")
    return ("•••" + text[-4:]) if len(text) > 4 else text


def _is_windows() -> bool:
    return os.name == "nt" or sys.platform == "win32"


def _command(title: str, text: str, level: str) -> Optional[list]:
    if _is_windows():
        if shutil.which("powershell") is None:
            return None
        script = ("Add-Type -AssemblyName System.Windows.Forms; "
                  "$n = New-Object System.Windows.Forms.NotifyIcon; "
                  "$n.Icon = [System.Drawing.SystemIcons]::Information; $n.Visible = $true; "
                  "$n.ShowBalloonTip(8000, $env:MT5X_NOTIFY_TITLE, $env:MT5X_NOTIFY_TEXT, 'Info'); "
                  "Start-Sleep -Seconds 9; $n.Dispose()")
        return ["powershell", "-NoProfile", "-WindowStyle", "Hidden", "-Command", script]
    if shutil.which("notify-send") is None:
        return None
    command = ["notify-send", "-a", APP_NAME, "-u", _URGENCY.get(level, "normal"), "-t", _TIMEOUT_MS.get(level, "8000")]
    if _ICON.is_file():
        command += ["-i", str(_ICON)]
    return command + ["--", title, text]


def send(config, title: str, text: str = "", level: str = INFO, runner: Optional[Callable] = None) -> bool:
    """Mostra o aviso. Devolve True se ele foi entregue ao sistema; False se virou só log. Nunca levanta erro."""
    try:
        title, text = str(title)[:80], str(text)[:MAX_TEXT]
        log.log(logging.WARNING if level == CRITICAL else logging.INFO, f"Aviso: {title}: {text}")
        if not enabled(config):
            return False
        command = _command(title, text, level)
        if command is None:
            log.debug("Sem ferramenta de notificação (notify-send / PowerShell): aviso só no log.")
            return False
        env = dict(os.environ, MT5X_NOTIFY_TITLE=title, MT5X_NOTIFY_TEXT=text)
        if _is_windows():       # o balão fica alguns segundos na tela: não espera por ele
            (runner or subprocess.Popen)(command, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return True
        done = (runner or subprocess.run)(command, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                          timeout=5)
        return getattr(done, "returncode", 0) == 0
    except Exception as e:          # sessão sem D-Bus, tempo esgotado, ferramenta quebrada...
        log.debug(f"O aviso não pôde ser mostrado: {e}")
        return False
