"""
MT5 oculto (Linux): o terminal, o MetaEditor e a ponte rodam em uma tela virtual (Xvfb), fora da tela real.

    env = child_env(config)        # ambiente para iniciar o Wine: DISPLAY da tela virtual quando o modo está ligado

Liga-se com `[MT5] hidden = true` em config/config.ini ou, só para uma execução, com a variável de ambiente
MT5X_HIDDEN=1 (MT5X_HIDDEN=0 desliga). A tela virtual é fixa (`[MT5] hidden_display`, padrão :99) para que a
ponte e os comandos usem a mesma. No Windows não há tela virtual: o terminal é iniciado minimizado.

Somente biblioteca padrão (este pacote roda antes das dependências existirem).
"""
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Optional

from . import paths

DEFAULT_DISPLAY = ":99"
SCREEN = "1280x800x24"
ENV_SWITCH = "MT5X_HIDDEN"


def _get(config, key: str, fallback: str = "") -> str:
    return config.get("MT5", key, fallback=fallback).strip() if config is not None else fallback


def enabled(config, env=None) -> bool:
    """O modo oculto está ligado? A variável MT5X_HIDDEN, se presente, vale mais que a configuração."""
    env = os.environ if env is None else env
    switch = str(env.get(ENV_SWITCH, "")).strip().lower()
    if switch in ("1", "true", "sim", "yes"):
        return True
    if switch in ("0", "false", "nao", "não", "no"):
        return False
    return _get(config, "hidden", "false").lower() == "true"


def display_name(config) -> str:
    name = _get(config, "hidden_display", DEFAULT_DISPLAY) or DEFAULT_DISPLAY
    return name if name.startswith(":") else ":" + name


def _number(display: str) -> str:
    return display.lstrip(":").split(".")[0]


def is_running(display: str) -> bool:
    """Há um servidor X atendendo nesse display? (o soquete existe e o processo do arquivo de trava está vivo)"""
    number = _number(display)
    if not Path(f"/tmp/.X11-unix/X{number}").exists():
        return False
    lock = Path(f"/tmp/.X{number}-lock")
    try:
        pid = int(lock.read_text().strip())
        os.kill(pid, 0)
        return True
    except (OSError, ValueError):
        return not lock.exists()        # sem arquivo de trava legível: confia no soquete


def ensure(display: str = DEFAULT_DISPLAY, timeout: float = 10.0, popen=None) -> str:
    """Garante a tela virtual: inicia o Xvfb se preciso, espera ficar pronta e devolve o DISPLAY."""
    if is_running(display):
        return display
    if shutil.which("Xvfb") is None:
        raise RuntimeError("O modo oculto precisa do Xvfb, que não está instalado. Instale o pacote "
                           "(xorg-server-xvfb no Arch/Manjaro, xvfb no Debian/Ubuntu) ou desligue [MT5] hidden.")
    (popen or subprocess.Popen)(["Xvfb", display, "-screen", "0", SCREEN, "-nolisten", "tcp"],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, stdin=subprocess.DEVNULL,
                                start_new_session=True)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if is_running(display):
            return display
        time.sleep(0.1)
    raise RuntimeError(f"A tela virtual {display} não ficou pronta em {timeout:g} s. Confira se outro programa usa "
                       "esse número ou troque [MT5] hidden_display.")


def child_env(config, env=None, start: bool = True) -> dict:
    """
    Ambiente para os processos do Wine. Com o modo oculto ligado (e fora do Windows), aponta DISPLAY para a tela
    virtual e tira WAYLAND_DISPLAY, para nenhuma janela ir para a tela real. Desligado, devolve o ambiente como está.
    """
    result = dict(os.environ if env is None else env)
    if paths.is_windows() or not enabled(config, result):
        return result
    display = display_name(config)
    result["DISPLAY"] = ensure(display) if start else display
    result.pop("WAYLAND_DISPLAY", None)
    return result


def windows_startup(hidden: bool):
    """No Windows, com o modo oculto: STARTUPINFO que abre a janela minimizada e sem tomar o foco. Senão, None."""
    if not hidden or not paths.is_windows() or not hasattr(subprocess, "STARTUPINFO"):
        return None
    info = subprocess.STARTUPINFO()
    info.dwFlags |= getattr(subprocess, "STARTF_USESHOWWINDOW", 1)
    info.wShowWindow = 7        # SW_SHOWMINNOACTIVE
    return info


def screenshot(display: str, path) -> Path:
    """Captura a tela virtual em um arquivo PNG (para ver o que o MT5 oculto está mostrando)."""
    path = Path(path)
    if not is_running(display):
        raise RuntimeError(f"A tela virtual {display} não está ativa: o MT5 oculto não está em execução.")
    if shutil.which("import") is None:
        raise RuntimeError("Para a captura é preciso o ImageMagick (comando 'import'). Instale o pacote imagemagick.")
    path.parent.mkdir(parents=True, exist_ok=True)
    done = subprocess.run(["import", "-display", display, "-window", "root", str(path)],
                          capture_output=True, text=True, timeout=30)
    if done.returncode != 0 or not path.is_file():
        raise RuntimeError("Não foi possível capturar a tela virtual: " + (done.stderr.strip() or "erro desconhecido"))
    return path


def main(argv=None) -> int:
    """`python -m mt5_extracao.bootstrap.display <config.ini>`: imprime o DISPLAY da tela virtual, ou nada."""
    import configparser
    import sys
    args = sys.argv[1:] if argv is None else argv
    config = configparser.ConfigParser(interpolation=None)
    if args:
        config.read(args[0], encoding="utf-8")
    if paths.is_windows() or not enabled(config):
        return 0
    try:
        print(ensure(display_name(config)))
    except RuntimeError as e:
        print(str(e), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
