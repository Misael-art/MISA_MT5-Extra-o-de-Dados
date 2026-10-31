"""Abertura no navegador do host; nunca inicia Wine ou instala outro navegador."""
from dataclasses import dataclass
from pathlib import Path
import os
import shutil
import webbrowser

from mt5_extracao.bootstrap import paths


@dataclass(frozen=True)
class OpenResult:
    opened: bool
    url: str
    message: str


def _controller(name):
    if not name:
        return webbrowser.get()
    executable = Path(name).expanduser()
    if executable.is_file():
        return webbrowser.BackgroundBrowser([str(executable), "%s"])
    # Uma escolha explícita inválida não é trocada silenciosamente pelo padrão.
    choices = {"edge": ("msedge", "msedge.exe"), "chrome": ("google-chrome", "chrome.exe"),
               "chromium": ("chromium", "chromium-browser"), "firefox": ("firefox", "firefox.exe")}
    if name not in choices:
        raise webbrowser.Error("Escolha um navegador ou seu executável em [DASHBOARD] browser.")
    for command in choices[name]:
        found = shutil.which(command)
        if found:
            return webbrowser.BackgroundBrowser([found, "%s"])
    if paths.is_windows():
        relative = {"edge": "Microsoft/Edge/Application/msedge.exe",
                    "chrome": "Google/Chrome/Application/chrome.exe",
                    "firefox": "Mozilla Firefox/firefox.exe"}.get(name)
        if relative:
            for folder in ("PROGRAMFILES", "PROGRAMFILES(X86)", "LOCALAPPDATA"):
                root = os.environ.get(folder)
                if root and (Path(root) / relative).is_file():
                    return webbrowser.BackgroundBrowser([str(Path(root) / relative), "%s"])
    raise webbrowser.Error("Navegador configurado não encontrado.")


def open_dashboard(server, config=None):
    """Só abre após prontidão; falha conserva servidor e URL para abertura manual."""
    server.start()
    name = config.get("DASHBOARD", "browser", fallback="").strip() if config is not None else ""
    try:
        opened = bool(_controller(name).open(server.url, new=2, autoraise=True))
    except (webbrowser.Error, OSError):
        opened = False
    message = ("Dashboard aberto no navegador." if opened else
               "Não foi possível abrir o navegador. Copie a URL abaixo ou configure [DASHBOARD] browser.")
    return OpenResult(opened, server.url, message)
