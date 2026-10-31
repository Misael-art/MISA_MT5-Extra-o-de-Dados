"""Servidor Flask da sessão do App, restrito a loopback e somente leitura."""
import hmac
import logging
import secrets
import threading
import time
import urllib.error
import urllib.request

from flask import Flask, abort, render_template, request, url_for
from werkzeug.serving import WSGIRequestHandler, make_server

log = logging.getLogger(__name__)
HOST = "127.0.0.1"
DEFAULT_PORT = 5055


class _QuietHandler(WSGIRequestHandler):
    def log_request(self, code="-", size="-"):
        # URLs contêm a capacidade efêmera da sessão; não gravá-las no log.
        pass


def configured_port(config):
    try:
        port = config.getint("DASHBOARD", "port", fallback=DEFAULT_PORT)
    except ValueError as exc:
        raise ValueError("A porta em [DASHBOARD] precisa ser um número entre 1 e 65535.") from exc
    if not 1 <= port <= 65535:
        raise ValueError("A porta em [DASHBOARD] precisa estar entre 1 e 65535.")
    return port


def create_app(key, port):
    app = Flask(__name__)
    app.config.update(MAX_CONTENT_LENGTH=1024, TRUSTED_HOSTS=[HOST, "localhost"])

    @app.before_request
    def local_read():
        allowed = {f"{HOST}:{port()}", f"localhost:{port()}"}
        if request.host not in allowed:
            abort(403)
        origin = request.headers.get("Origin")
        if origin and origin not in {"http://" + host for host in allowed}:
            abort(403)
        if request.method not in {"GET", "HEAD"}:
            abort(405)
        given = request.args.get("k", "")
        if not hmac.compare_digest(given.encode(), key.encode()):
            abort(403)

    @app.after_request
    def protect(response):
        response.headers.update({"Cache-Control": "no-store", "Referrer-Policy": "no-referrer",
                                 "X-Content-Type-Options": "nosniff", "X-Frame-Options": "DENY",
                                 "Content-Security-Policy": "default-src 'self'; object-src 'none'; "
                                 "base-uri 'none'; frame-ancestors 'none'; form-action 'none'"})
        return response

    @app.route("/")
    def index():
        return render_template("index.html", css_url=url_for("static", filename="css/dashboard.css", k=key))

    @app.route("/health")
    def health():
        return {"service": "mt5x-dashboard", "ready": True}

    return app


class DashboardServer:
    """Uma instância pertencente ao chamador; start/stop idempotentes e serializados."""

    def __init__(self, config=None, *, port=None):
        if port is None:
            port = configured_port(config) if config is not None else DEFAULT_PORT
        if isinstance(port, bool) or not isinstance(port, int) or not 0 <= port <= 65535:
            raise ValueError("Porta inválida; use 1 a 65535 (0 apenas para QA explícita).")
        self._requested_port = port
        self._port = port
        self._key = secrets.token_urlsafe(32)
        self._lock = threading.RLock()
        self._httpd = self._thread = None
        self.app = create_app(self._key, lambda: self.port)

    @property
    def port(self):
        return self._port

    @property
    def url(self):
        return f"http://{HOST}:{self.port}/?k={self._key}"

    def start(self, timeout=3.0):
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return self
            try:
                self._httpd = make_server(HOST, self._requested_port, self.app, threaded=True,
                                          request_handler=_QuietHandler)
            except (SystemExit, OSError) as exc:
                raise RuntimeError(f"Não foi possível abrir a porta {self._requested_port} do Dashboard. "
                                   "Confira o processo que a utiliza ou configure [DASHBOARD] port.") from exc
            self._port = self._httpd.server_port
            self._thread = threading.Thread(target=self._httpd.serve_forever,
                                            kwargs={"poll_interval": 0.05}, daemon=True,
                                            name="mt5x-dashboard")
            try:
                self._thread.start()
                self._wait_ready(timeout)
            except BaseException:
                self.stop()
                raise
            return self

    def _wait_ready(self, timeout):
        deadline = time.monotonic() + timeout
        # Não herdar proxy do usuário para o handshake de loopback.
        client = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        while time.monotonic() < deadline:
            try:
                with client.open(self.url.replace("/?", "/health?"), timeout=min(0.2, timeout)) as response:
                    if response.status == 200:
                        return
            except (OSError, urllib.error.URLError):
                if not self._thread.is_alive():
                    break
                time.sleep(0.01)
        raise RuntimeError("O Dashboard não ficou pronto. Confira a porta e tente abrir novamente.")

    def stop(self):
        with self._lock:
            if self._httpd is not None:
                if self._thread is not None and self._thread.is_alive():
                    self._httpd.shutdown()
                    self._thread.join(timeout=3)
                self._httpd.server_close()
                self._httpd = self._thread = None

    def __enter__(self):
        return self.start()

    def __exit__(self, *_):
        self.stop()
