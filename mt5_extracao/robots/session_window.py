"""Posicionamento das janelas próprias da sessão no monitor de lançamento.

Coordenadas do sistema de janelas (X11/XWayland ou Win32), não a resolução
física do painel nem o tamanho da tela virtual usada pelo backtest. Sem Tk,
alteração de resolução, registro Wine ou manipulação de janelas de terceiros.
"""
import ctypes as C
from ctypes.util import find_library
from dataclasses import dataclass
import logging
import threading

from mt5_extracao.bootstrap import paths

log = logging.getLogger(__name__)
_X_ERROR_LOCK = threading.RLock()


@dataclass(frozen=True)
class Monitor:
    x: int
    y: int
    width: int
    height: int

    def contains(self, point):
        x, y = point
        return self.x <= x < self.x + self.width and self.y <= y < self.y + self.height


def select_monitor(monitors, point):
    """Inclui monitores à esquerda/acima e não soma as larguras das telas."""
    if not monitors:
        raise OSError("Nenhum monitor disponível para exibir a sessão.")
    for monitor in monitors:
        if monitor.contains(point):
            return monitor
    # Janela lançadora pode ter ficado fora da área após retirar um monitor.
    return min(monitors, key=lambda m: max(m.x-point[0], 0, point[0]-m.x-m.width)**2 +
               max(m.y-point[1], 0, point[1]-m.y-m.height)**2)


class _MonitorInfo(C.Structure):
    _fields_ = [("name", C.c_ulong), ("primary", C.c_int), ("automatic", C.c_int),
                ("noutput", C.c_int), ("x", C.c_int), ("y", C.c_int),
                ("width", C.c_int), ("height", C.c_int), ("mwidth", C.c_int),
                ("mheight", C.c_int), ("outputs", C.POINTER(C.c_ulong))]


class _MessageData(C.Union):
    _fields_ = [("l", C.c_long * 5), ("b", C.c_char * 20)]


class _Message(C.Structure):
    _fields_ = [("type", C.c_int), ("serial", C.c_ulong), ("send_event", C.c_int),
                ("display", C.c_void_p), ("window", C.c_ulong), ("message_type", C.c_ulong),
                ("format", C.c_int), ("data", _MessageData)]


class _Event(C.Union):
    _fields_ = [("message", _Message), ("padding", C.c_long * 24)]


class _XError(C.Structure):
    _fields_ = [("type", C.c_int), ("display", C.c_void_p), ("resource", C.c_ulong),
                ("serial", C.c_ulong), ("code", C.c_ubyte), ("request", C.c_ubyte),
                ("minor", C.c_ubyte)]


_X_ERROR_HANDLER = C.CFUNCTYPE(C.c_int, C.c_void_p, C.POINTER(_XError))


class _X11:
    def __init__(self):
        self.x = C.CDLL(find_library("X11") or "libX11.so.6")
        self.x.XOpenDisplay.argtypes = [C.c_char_p]
        self.x.XOpenDisplay.restype = C.c_void_p
        self.d = self.x.XOpenDisplay(None)
        if not self.d:
            raise OSError("DISPLAY indisponível para a janela da sessão.")
        self.x.XDefaultRootWindow.argtypes = [C.c_void_p]
        self.x.XDefaultRootWindow.restype = C.c_ulong
        self.root = self.x.XDefaultRootWindow(self.d)
        self.x.XInternAtom.argtypes = [C.c_void_p, C.c_char_p, C.c_int]
        self.x.XInternAtom.restype = C.c_ulong
        self.x.XGetWindowProperty.argtypes = [C.c_void_p, C.c_ulong, C.c_ulong, C.c_long,
            C.c_long, C.c_int, C.c_ulong, C.POINTER(C.c_ulong), C.POINTER(C.c_int),
            C.POINTER(C.c_ulong), C.POINTER(C.c_ulong), C.POINTER(C.POINTER(C.c_ubyte))]
        self.x.XFree.argtypes = [C.c_void_p]
        self.x.XSendEvent.argtypes = [C.c_void_p, C.c_ulong, C.c_int, C.c_long, C.POINTER(_Event)]
        self.x.XFlush.argtypes = [C.c_void_p]
        self.x.XCloseDisplay.argtypes = [C.c_void_p]
        self.x.XSync.argtypes = [C.c_void_p, C.c_int]
        self.x.XSetErrorHandler.argtypes = [C.c_void_p]
        self.x.XSetErrorHandler.restype = C.c_void_p

    def atom(self, name):
        return self.x.XInternAtom(self.d, name.encode("ascii"), 0)

    def values(self, window, name):
        # XGetWindowProperty pode emitir BadWindow entre a listagem e a leitura.
        # O handler padrão Xlib termina o processo inteiro, incluindo a App/Tk.
        # Limitar a exceção a esta conexão, drenar os erros e restaurar o handler
        # anterior; nunca substituir permanentemente o tratamento do Tk.
        with _X_ERROR_LOCK:
            previous = None

            @_X_ERROR_HANDLER
            def handle(display, error):
                if display == self.d and error.contents.code == 3:  # BadWindow
                    return 0
                return _X_ERROR_HANDLER(previous)(display, error) if previous else 0

            previous = self.x.XSetErrorHandler(C.cast(handle, C.c_void_p))
            try:
                return self._values(window, name)
            finally:
                try:
                    self.x.XSync(self.d, 0)
                finally:
                    self.x.XSetErrorHandler(previous)

    def _values(self, window, name):
        kind, fmt, count, rest = C.c_ulong(), C.c_int(), C.c_ulong(), C.c_ulong()
        data = C.POINTER(C.c_ubyte)()
        status = self.x.XGetWindowProperty(self.d, window, self.atom(name), 0, 4096, 0, 0,
            C.byref(kind), C.byref(fmt), C.byref(count), C.byref(rest), C.byref(data))
        try:
            if status != 0 or not data or fmt.value != 32:
                return []
            return list(C.cast(data, C.POINTER(C.c_ulong))[:count.value])
        finally:
            if data:
                self.x.XFree(data)

    def monitors(self):
        randr = C.CDLL(find_library("Xrandr") or "libXrandr.so.2")
        randr.XRRGetMonitors.argtypes = [C.c_void_p, C.c_ulong, C.c_int, C.POINTER(C.c_int)]
        randr.XRRGetMonitors.restype = C.POINTER(_MonitorInfo)
        randr.XRRFreeMonitors.argtypes = [C.POINTER(_MonitorInfo)]
        count = C.c_int()
        data = randr.XRRGetMonitors(self.d, self.root, 1, C.byref(count))
        try:
            return [Monitor(m.x, m.y, m.width, m.height) for m in data[:count.value]
                    if m.width > 0 and m.height > 0] if data else []
        finally:
            if data:
                randr.XRRFreeMonitors(data)

    def pointer(self):
        root, child, rx, ry, wx, wy, mask = (C.c_ulong(), C.c_ulong(), C.c_int(), C.c_int(),
                                            C.c_int(), C.c_int(), C.c_uint())
        self.x.XQueryPointer.argtypes = [C.c_void_p, C.c_ulong, C.POINTER(C.c_ulong),
            C.POINTER(C.c_ulong), C.POINTER(C.c_int), C.POINTER(C.c_int),
            C.POINTER(C.c_int), C.POINTER(C.c_int), C.POINTER(C.c_uint)]
        self.x.XQueryPointer(self.d, self.root, C.byref(root), C.byref(child), C.byref(rx),
                            C.byref(ry), C.byref(wx), C.byref(wy), C.byref(mask))
        return rx.value, ry.value

    def windows(self, pids):
        normal = self.atom("_NET_WM_WINDOW_TYPE_NORMAL")
        result = []
        for window in self.values(self.root, "_NET_CLIENT_LIST"):
            pid = self.values(window, "_NET_WM_PID")
            types = self.values(window, "_NET_WM_WINDOW_TYPE")
            if pid and pid[0] in pids and normal in types:
                result.append((window, pid[0]))
        return result

    def message(self, window, name, values):
        event = _Event()
        event.message = _Message(type=33, send_event=1, display=self.d, window=window,
                                 message_type=self.atom(name), format=32)
        for i, value in enumerate(values):
            event.message.data.l[i] = value
        self.x.XSendEvent(self.d, self.root, 0, (1 << 19) | (1 << 20), C.byref(event))
        self.x.XFlush(self.d)

    def place(self, window, monitor):
        # Deixar o WM calcular decoração, barra de tarefas e escala. Não usar
        # wmctrl -lG: no XWayland com escala mista ele pode dobrar a origem.
        self.message(window, "_NET_WM_STATE", [0, self.atom("_NET_WM_STATE_MAXIMIZED_VERT"),
                     self.atom("_NET_WM_STATE_MAXIMIZED_HORZ"), 1])
        self.message(window, "_NET_MOVERESIZE_WINDOW", [10 | (15 << 8) | (1 << 12),
                     monitor.x, monitor.y, monitor.width, monitor.height])
        self.message(window, "_NET_WM_STATE", [1, self.atom("_NET_WM_STATE_MAXIMIZED_VERT"),
                     self.atom("_NET_WM_STATE_MAXIMIZED_HORZ"), 1])

    def close(self):
        self.x.XCloseDisplay(self.d)


class _Windows:
    def __init__(self):
        from ctypes import wintypes as W
        self.W = W
        self.api = C.WinDLL("user32", use_last_error=True)
        self.api.MonitorFromPoint.argtypes = [W.POINT, W.DWORD]
        self.api.MonitorFromPoint.restype = W.HANDLE
        self.api.GetMonitorInfoW.argtypes = [W.HANDLE, C.c_void_p]
        self.api.GetWindowThreadProcessId.argtypes = [W.HWND, C.POINTER(W.DWORD)]
        self.api.GetWindow.argtypes = [W.HWND, W.UINT]
        self.api.GetWindow.restype = W.HWND
        self.api.IsWindowVisible.argtypes = [W.HWND]
        self.api.MoveWindow.argtypes = [W.HWND, C.c_int, C.c_int, C.c_int, C.c_int, W.BOOL]
        self.api.ShowWindow.argtypes = [W.HWND, C.c_int]
        self.callback = C.WINFUNCTYPE(W.BOOL, W.HWND, W.LPARAM)
        self.api.EnumWindows.argtypes = [self.callback, W.LPARAM]

    def pointer(self):
        point = self.W.POINT()
        if not self.api.GetCursorPos(C.byref(point)):
            raise OSError("Posição do ponteiro indisponível.")
        return point.x, point.y

    def monitor(self, point):
        class Info(C.Structure):
            _fields_ = [("size", self.W.DWORD), ("monitor", self.W.RECT),
                        ("work", self.W.RECT), ("flags", self.W.DWORD)]
        info = Info()
        info.size = C.sizeof(info)
        handle = self.api.MonitorFromPoint(self.W.POINT(*point), 2)
        if not handle or not self.api.GetMonitorInfoW(handle, C.byref(info)):
            raise OSError("Monitor indisponível para a sessão.")
        r = info.work
        return Monitor(r.left, r.top, r.right-r.left, r.bottom-r.top)

    def windows(self, pids):
        result = []
        def visit(window, _):
            pid = self.W.DWORD()
            self.api.GetWindowThreadProcessId(window, C.byref(pid))
            if pid.value in pids and self.api.IsWindowVisible(window) and not self.api.GetWindow(window, 4):
                result.append((window, pid.value))
            return True
        self.api.EnumWindows(self.callback(visit), 0)
        return result

    def place(self, window, monitor):
        self.api.ShowWindow(window, 9)  # restaurar antes de mudar de monitor
        self.api.MoveWindow(window, monitor.x, monitor.y, monitor.width, monitor.height, True)
        self.api.ShowWindow(window, 3)  # maximizar no monitor escolhido

    def close(self):
        pass


class Placement:
    """Ajusta cada janela uma vez; depois respeita movimentação/redimensionamento."""
    def __init__(self, anchor=None, backend=None):
        self.backend = backend or (_Windows() if paths.is_windows() else _X11())
        try:
            point = anchor if anchor is not None else self.backend.pointer()
            self.monitor = (self.backend.monitor(point) if hasattr(self.backend, "monitor") else
                            select_monitor(self.backend.monitors(), point))
        except Exception:
            self.backend.close()
            raise
        self.done = set()

    def poll(self, owned_pids):
        for window, pid in self.backend.windows(set(owned_pids)):
            identity = window, pid
            if identity not in self.done:
                self.backend.place(window, self.monitor)
                self.done.add(identity)

    def close(self):
        self.backend.close()
