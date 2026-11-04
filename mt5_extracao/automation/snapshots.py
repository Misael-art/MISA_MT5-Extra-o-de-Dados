"""Captura declarada da janela própria (Tester não fornece ChartScreenShot confiável)."""
from pathlib import Path
import shutil
import subprocess
import uuid

from .protocol import fields, number
from ..bootstrap import paths
from ..robots import sessions, session_window


def _windows(window, target):
    import ctypes as C
    from ctypes import wintypes as W
    from PIL import Image
    user, gdi = C.WinDLL("user32", use_last_error=True), C.WinDLL("gdi32", use_last_error=True)
    user.GetClientRect.argtypes = [W.HWND, C.POINTER(W.RECT)]
    user.GetDC.argtypes = [W.HWND]; user.GetDC.restype = W.HDC
    user.ReleaseDC.argtypes = [W.HWND, W.HDC]
    user.PrintWindow.argtypes = [W.HWND, W.HDC, W.UINT]
    gdi.CreateCompatibleDC.argtypes = [W.HDC]; gdi.CreateCompatibleDC.restype = W.HDC
    gdi.DeleteDC.argtypes = [W.HDC]
    gdi.SelectObject.argtypes = [W.HDC, W.HANDLE]; gdi.SelectObject.restype = W.HANDLE
    gdi.DeleteObject.argtypes = [W.HANDLE]
    class Header(C.Structure):
        _fields_ = [("size", W.DWORD), ("width", W.LONG), ("height", W.LONG), ("planes", W.WORD),
                    ("bits", W.WORD), ("compression", W.DWORD), ("image_size", W.DWORD),
                    ("xppm", W.LONG), ("yppm", W.LONG), ("used", W.DWORD), ("important", W.DWORD)]
    class Info(C.Structure):
        _fields_ = [("header", Header), ("colors", W.DWORD * 3)]
    gdi.CreateDIBSection.argtypes = [W.HDC, C.POINTER(Info), W.UINT, C.POINTER(C.c_void_p), W.HANDLE, W.DWORD]
    gdi.CreateDIBSection.restype = W.HANDLE
    rect = W.RECT()
    if not user.GetClientRect(window, C.byref(rect)):
        raise RuntimeError("Dimensões da janela própria indisponíveis.")
    width, height = rect.right - rect.left, rect.bottom - rect.top
    if not 1 <= width <= 8192 or not 1 <= height <= 8192:
        raise ValueError("Janela própria fora do limite8192px.")
    screen = user.GetDC(window)
    memory = gdi.CreateCompatibleDC(screen)
    bitmap, previous = None, None
    try:
        info = Info(Header(C.sizeof(Header), width, -height, 1, 32, 0, width*height*4, 0, 0, 0, 0))
        bits = C.c_void_p()
        bitmap = gdi.CreateDIBSection(screen, C.byref(info), 0, C.byref(bits), None, 0)
        if not screen or not memory or not bitmap or not bits:
            raise RuntimeError("Não foi possível preparar a captura da janela própria.")
        previous = gdi.SelectObject(memory, bitmap)
        if not user.PrintWindow(window, memory, 3):
            raise RuntimeError("Windows recusou PrintWindow; não há captura qualificada.")
        Image.frombytes("RGB", (width, height), C.string_at(bits, width*height*4), "raw", "BGRX").save(target)
    finally:
        if previous: gdi.SelectObject(memory, previous)
        if bitmap: gdi.DeleteObject(bitmap)
        if memory: gdi.DeleteDC(memory)
        if screen: user.ReleaseDC(window, screen)


def capture(job, params, *, timeout=20):
    number(timeout, "timeout", .1, 120)
    fields(params, (), ("chart",))
    if params.get("chart", "0") != "0":
        raise ValueError("owned_window captura a janela principal; omita chart ou use0.")
    if job.state != "running" or not job.visible or job.child_config is None:
        raise ValueError("A sessão visual própria ainda não está rodando.")
    executables = ("metatester64.exe",) if job.mode == "replay" else ("terminal64.exe",)
    owned = sessions.owned_processes(job.child_config, executables, strict=True, launchers=True)
    pids = {process.pid for process in owned}
    if not pids:
        raise ValueError("Nenhum processo visual próprio foi comprovado; captura recusada.")
    backend = session_window._Windows() if paths.is_windows() else session_window._X11()
    target = None
    try:
        windows = backend.windows(pids)
        if len(windows) != 1:
            raise ValueError("Janela visual própria ausente/ambígua; aguarde seu mapeamento e consulte novamente.")
        window, pid = windows[0]
        if pid not in pids or not isinstance(window, int) or window <= 0:
            raise ValueError("Janela sem identidade própria comprovada; captura recusada.")
        directory = Path(job.directory) / "captures"
        if not directory.resolve().is_relative_to(Path(job.directory).resolve()):
            raise ValueError("Pasta de captura fora da sessão própria.")
        directory.mkdir(exist_ok=True)
        target = directory / ("window_" + uuid.uuid4().hex + ".png")
        if paths.is_windows():
            _windows(window, target)
        else:
            tool = shutil.which("import")
            if tool is None:
                raise RuntimeError("Captura X11 exige ImageMagick (import), já usado pelo diagnóstico de display.")
            result = subprocess.run([tool, "-silent", "-window", str(window), str(target)],
                                    capture_output=True, text=True, timeout=timeout)
            if result.returncode:
                raise RuntimeError("Captura X11 da janela própria recusada: " + result.stderr.strip())
        # Não entregar pixels se houve perda/troca da janela durante a leitura.
        if (window, pid) not in backend.windows(pids) or job.state != "running":
            raise RuntimeError("A janela mudou durante a captura; imagem recusada.")
        from PIL import Image
        with Image.open(target) as picture:
            picture.verify()
        with Image.open(target) as picture:
            width, height = picture.size
        if not 1 <= width <= 8192 or not 1 <= height <= 8192:
            raise ValueError("Captura da janela própria excede8192px.")
        return dict(path=str(target), backend="owned_window", width=width, height=height,
                    scope="janela principal própria, inclui controles nativos; não é captura só do chart")
    except Exception:
        if target is not None:
            target.unlink(missing_ok=True)
        raise
    finally:
        backend.close()
