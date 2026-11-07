"""
Conexão do App com o Google Drive (T20.30-HOST1-DRIVE, etapa de vínculo).

Fluxo amigável: o usuário clica em "Conectar Google Drive"; o App procura sozinho um cliente OAuth de aplicativo
instalado (cofre do sistema, pasta de configuração e arquivos ``client_secret*.json`` baixados recentemente), abre o
navegador deste computador no consentimento do Google e recebe a resposta num endereço local (127.0.0.1, porta
livre, PKCE + state). Nada precisa ser copiado à mão: o cliente encontrado e o token de renovação ficam no cofre de
senhas do sistema (``keyring``: Secret Service no Linux, Gerenciador de Credenciais no Windows).

Escopo único ``drive.file``: o App só enxerga os arquivos e a pasta que ele mesmo cria ("MT5X Backup").
A validação (``verify``) é somente leitura e bloqueia o envio quando a conta muda, a pasta some, vai para a lixeira,
fica compartilhada ou não aceita gravação. O envio manual cifrado fica em ``drive_backup``; envio automático desativado.

Sem Tkinter; a UI (ui/resources.py) e a CLI (``mt5x resources drive``) chamam estas funções em segundo plano.
"""
from __future__ import annotations

import base64
import hashlib
import http.server
import json
import os
import secrets
import socket
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_URL = "https://oauth2.googleapis.com/token"
REVOKE_URL = "https://oauth2.googleapis.com/revoke"
DRIVE_API = "https://www.googleapis.com/drive/v3"
SCOPE = "https://www.googleapis.com/auth/drive.file"
FOLDER_NAME = "MT5X Backup"
FOLDER_MIME = "application/vnd.google-apps.folder"
CONSOLE_URL = "https://console.cloud.google.com/apis/credentials"
SERVICE = "mt5x-google-drive"
KEY_CLIENT, KEY_TOKEN, KEY_LINK = "client", "refresh_token", "link"
SEARCH_MAX_AGE_DAYS = 30
SEARCH_MAX_BYTES = 64 * 1024
CONNECT_TIMEOUT_S = 300
HTTP_TIMEOUT_S = 30
RETRIES = 3
KEYRING_TIMEOUT_S = 120


class DriveError(Exception):
    """Falha com mensagem para o usuário (``message``) e código estável (``code``) para a UI/CLI."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


# --- cofre ---------------------------------------------------------------------------------------------------------

class KeyringStore:
    """Guarda segredos no cofre do sistema. Sem cofre utilizável, falha explicitamente (nunca grava em texto puro)."""

    def __init__(self, service: str = SERVICE):
        self.service = service

    def _kr(self):
        try:
            import keyring
            from keyring.backends import fail
        except ImportError as exc:
            raise DriveError("keyring_missing", "Falta o componente de cofre de senhas (pacote keyring). "
                                                "Instale com: pip install -r requirements-optional.txt") from exc
        backend = keyring.get_keyring()
        if isinstance(backend, fail.Keyring) or getattr(backend, "priority", 1) <= 0:
            raise DriveError("keyring_unavailable", "Nenhum cofre de senhas do sistema está disponível "
                                                    "(no Linux, abra a sessão do KDE/GNOME com o chaveiro ativo).")
        return keyring

    def locked(self) -> bool:
        """True se o cofre do Linux está bloqueado (consultar não abre a janela de desbloqueio)."""
        if not sys.platform.startswith("linux"):
            return False
        try:
            import secretstorage
            conn = secretstorage.dbus_init()
            try:
                return secretstorage.get_default_collection(conn).is_locked()
            finally:
                conn.close()
        except Exception:
            return False

    def _call(self, fn, *args):
        """Chamada ao cofre com limite: um cofre bloqueado espera o usuário desbloquear, mas não para sempre."""
        box = {}

        def run():
            try:
                box["value"] = fn(*args)
            except Exception as exc:                 # devolvido à thread chamadora
                box["error"] = exc

        worker = threading.Thread(target=run, name="mt5x-keyring", daemon=True)
        worker.start()
        worker.join(KEYRING_TIMEOUT_S)
        if worker.is_alive():
            raise DriveError("keyring_locked", "O cofre de senhas do sistema não respondeu. Desbloqueie a carteira "
                                               "(KWallet/Chaveiro) na janela que abriu e tente de novo.")
        if "error" in box:
            raise DriveError("keyring_error", "O cofre não concluiu a operação. Confira o chaveiro do sistema "
                             "e tente novamente.") from None
        return box.get("value")

    def get(self, key: str) -> Optional[str]:
        kr = self._kr()
        return self._call(kr.get_password, self.service, key)

    def set(self, key: str, value: str) -> None:
        kr = self._kr()
        self._call(kr.set_password, self.service, key, value)

    def delete(self, key: str) -> None:
        kr = self._kr()
        try:
            self._call(kr.delete_password, self.service, key)
        except kr.errors.PasswordDeleteError:
            pass


# --- cliente OAuth -------------------------------------------------------------------------------------------------

def parse_client(data) -> dict:
    """Valida um JSON de cliente OAuth "App para computador" e devolve só os campos usados. Erro amigável se não for."""
    if isinstance(data, (str, bytes)):
        try:
            data = json.loads(data)
        except ValueError as exc:
            raise DriveError("client_invalid", "O arquivo não é um JSON de credencial do Google.") from exc
    if not isinstance(data, dict):
        raise DriveError("client_invalid", "O arquivo não é um JSON de credencial do Google.")
    if "web" in data and "installed" not in data:
        raise DriveError("client_wrong_type", "Esta credencial é do tipo \"Aplicativo da Web\". Crie uma do tipo "
                                              "\"App para computador\" no Console do Google.")
    body = data.get("installed")
    if not isinstance(body, dict):
        raise DriveError("client_invalid", "O arquivo não é uma credencial OAuth do tipo \"App para computador\".")
    client_id, secret = body.get("client_id"), body.get("client_secret")
    if not isinstance(client_id, str) or not client_id.endswith(".apps.googleusercontent.com") or not secret:
        raise DriveError("client_invalid", "A credencial não traz client_id/client_secret válidos.")
    return {"client_id": client_id, "client_secret": str(secret), "project_id": str(body.get("project_id") or "")}


def _config_dir() -> Path:
    if sys.platform == "win32":
        return Path(os.environ.get("APPDATA") or Path.home() / "AppData" / "Roaming") / "mt5x"
    return Path(os.environ.get("XDG_CONFIG_HOME") or Path.home() / ".config") / "mt5x"


def _xdg_download_dir() -> Optional[Path]:
    conf = Path(os.environ.get("XDG_CONFIG_HOME") or Path.home() / ".config") / "user-dirs.dirs"
    try:
        for line in conf.read_text(encoding="utf-8").splitlines():
            if line.startswith("XDG_DOWNLOAD_DIR="):
                return Path(line.split("=", 1)[1].strip().strip('"').replace("$HOME", str(Path.home())))
    except OSError:
        pass
    return None


def search_dirs() -> list:
    home = Path.home()
    dirs = [_config_dir(), _xdg_download_dir(), home / "Downloads", home / "Transferências", home / "Desktop",
            home / "Área de Trabalho", home / "Documents", home / "Documentos", home]
    seen, out = set(), []
    for d in dirs:
        if d and d not in seen and d.is_dir():
            seen.add(d)
            out.append(d)
    return out


def find_client_files(dirs=None, now: Optional[float] = None) -> list:
    """
    Procura credenciais "App para computador" válidas, sem descer em subpastas e sem abrir arquivos grandes.
    Devolve [{path, client_id, project_id, mtime}] da mais nova para a mais velha. A pasta de configuração do App
    vale sem limite de idade; as demais só arquivos dos últimos SEARCH_MAX_AGE_DAYS dias.
    """
    now = time.time() if now is None else now
    config = _config_dir()
    found = []
    for folder in (dirs if dirs is not None else search_dirs()):
        try:
            entries = list(Path(folder).iterdir())
        except OSError:
            continue
        for path in entries:
            name = path.name.lower()
            if not name.endswith(".json") or not ("client_secret" in name or "oauth" in name or "credentials" in name):
                continue
            try:
                stat = path.stat()
                if not path.is_file() or stat.st_size > SEARCH_MAX_BYTES:
                    continue
                if Path(folder) != config and now - stat.st_mtime > SEARCH_MAX_AGE_DAYS * 86400:
                    continue
                client = parse_client(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, DriveError):
                continue
            found.append({"path": str(path), "client_id": client["client_id"], "project_id": client["project_id"],
                          "mtime": stat.st_mtime})
    found.sort(key=lambda c: c["mtime"], reverse=True)
    return found


# --- HTTP ----------------------------------------------------------------------------------------------------------

def _http(method: str, url: str, data=None, headers=None, json_body=None, opener=None) -> dict:
    """Requisição com nova tentativa para rede e 5xx/429. Erros viram DriveError com mensagem amigável."""
    body, hdrs = None, dict(headers or {})
    if json_body is not None:
        body = json.dumps(json_body).encode()
        hdrs["Content-Type"] = "application/json"
    elif data is not None:
        body = urllib.parse.urlencode(data).encode()
        hdrs["Content-Type"] = "application/x-www-form-urlencoded"
    send = opener or (lambda req: urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_S))
    for attempt in range(RETRIES):
        req = urllib.request.Request(url, data=body, headers=hdrs, method=method)
        try:
            with send(req) as resp:
                raw = resp.read()
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as exc:
            try:
                payload = json.loads(exc.read() or b"{}")
            except ValueError:
                payload = {}
            if exc.code in (429, 500, 502, 503, 504) and attempt < RETRIES - 1:
                time.sleep(2 ** attempt)
                continue
            raise _http_error(exc.code, payload) from exc
        except (urllib.error.URLError, socket.timeout, ConnectionError) as exc:
            if attempt < RETRIES - 1:
                time.sleep(2 ** attempt)
                continue
    raise DriveError("network", "Sem conexão com o Google. Confira a internet e tente de novo.")


def _http_error(status: int, payload: dict) -> DriveError:
    err = payload.get("error")
    code = err if isinstance(err, str) else (err or {}).get("status") or str(status)
    if code == "invalid_grant":
        return DriveError("revoked", "O acesso ao Google Drive foi revogado ou expirou. Clique em Conectar de novo.")
    if code in ("invalid_client", "unauthorized_client"):
        return DriveError("client_rejected", "O Google recusou esta credencial (cliente apagado ou de outro tipo). "
                                             "Escolha outra credencial ou crie uma nova no Console.")
    if status == 403:
        detail = (err or {}).get("message", "") if isinstance(err, dict) else ""
        if "has not been used" in detail or "disabled" in detail:
            return DriveError("api_disabled", "A Google Drive API está desativada no projeto da credencial. "
                                              "Ative-a no Console e tente de novo.")
        return DriveError("forbidden", "O Google negou o acesso à pasta. Conecte de novo para recriá-la.")
    if status == 404:
        return DriveError("not_found", "A pasta do App no Drive não foi encontrada.")
    if status == 410:
        return DriveError("upload_expired", "A sessão de envio expirou. Retome a cópia para abrir outra sessão.")
    return DriveError("http", f"O Google respondeu HTTP {status}. Confira a autorização e tente de novo.")


# --- consentimento no navegador ------------------------------------------------------------------------------------

_DONE_PAGE = ("<!doctype html><meta charset='utf-8'><title>MT5X</title><body style='font-family:sans-serif;"
              "margin:3em'><h2>{title}</h2><p>{text}</p><p>Pode fechar esta aba e voltar ao MT5X.</p></body>")


def _pkce():
    verifier = secrets.token_urlsafe(64)
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    return verifier, challenge


def authorize(client: dict, open_browser: Callable[[str], bool] = webbrowser.open,
              timeout: float = CONNECT_TIMEOUT_S, cancel: Optional[threading.Event] = None) -> dict:
    """
    Abre o consentimento do Google e espera a volta em 127.0.0.1. Devolve a resposta do token (com refresh_token).
    ``cancel`` permite à UI desistir; ``timeout`` encerra sozinho se o usuário abandonar a aba.
    """
    verifier, challenge = _pkce()
    state = secrets.token_urlsafe(24)
    result: dict = {}
    got = threading.Event()

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            if query.get("state", [None])[0] != state:
                self.send_response(400)
                self.end_headers()
                return
            if "error" in query:
                result["error"] = query["error"][0]
                title, text = "Conexão cancelada", "O Google não autorizou o acesso."
            else:
                result["code"] = query.get("code", [None])[0]
                title, text = "Google Drive conectado", "O MT5X recebeu a autorização."
            page = _DONE_PAGE.format(title=title, text=text).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(page)))
            self.end_headers()
            self.wfile.write(page)
            got.set()

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    server.timeout = 0.5
    redirect = f"http://127.0.0.1:{server.server_port}/"
    url = AUTH_URL + "?" + urllib.parse.urlencode({
        "client_id": client["client_id"], "redirect_uri": redirect, "response_type": "code", "scope": SCOPE,
        "state": state, "code_challenge": challenge, "code_challenge_method": "S256",
        "access_type": "offline", "prompt": "consent select_account"})
    try:
        if not open_browser(url):
            raise DriveError("browser", "Não foi possível abrir o navegador. Configure o navegador padrão e conecte de novo.")
        deadline = time.monotonic() + timeout
        while not got.is_set():
            if cancel is not None and cancel.is_set():
                raise DriveError("cancelled", "Conexão cancelada.")
            if time.monotonic() > deadline:
                raise DriveError("timeout", "O consentimento não foi concluído a tempo. Clique em Conectar de novo.")
            server.handle_request()
    finally:
        server.server_close()
    if result.get("error"):
        raise DriveError("denied", "Você não autorizou o acesso no Google.")
    if not result.get("code"):
        raise DriveError("denied", "O Google não devolveu a autorização.")
    return _http("POST", TOKEN_URL, data={
        "code": result["code"], "client_id": client["client_id"], "client_secret": client["client_secret"],
        "redirect_uri": redirect, "grant_type": "authorization_code", "code_verifier": verifier})


# --- serviço -------------------------------------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class DriveLink:
    """Vínculo do App com uma conta e uma pasta do Drive. ``store`` e ``opener`` são injetáveis nos testes."""

    def __init__(self, store=None, opener=None):
        self.store = store or KeyringStore()
        self.opener = opener

    # leitura de estado
    def client(self) -> Optional[dict]:
        raw = self.store.get(KEY_CLIENT)
        return json.loads(raw) if raw else None

    def link(self) -> Optional[dict]:
        raw = self.store.get(KEY_LINK)
        return json.loads(raw) if raw else None

    def status(self) -> dict:
        """Estado sem rede: o que a UI mostra ao abrir a aba. Cofre bloqueado não abre janela aqui."""
        if getattr(self.store, "locked", lambda: False)():
            return {"state": "locked", "message": "O cofre de senhas do sistema está bloqueado. Clique em Conectar "
                                                  "e desbloqueie a carteira quando ela pedir."}
        try:
            client, link, token = self.client(), self.link(), self.store.get(KEY_TOKEN)
        except DriveError as exc:
            return {"state": "unavailable", "code": exc.code, "message": exc.message}
        if not client:
            candidates = find_client_files()
            return {"state": "no_client", "candidates": candidates,
                    "message": ("Credencial do Google encontrada neste computador; clique em Conectar."
                                if candidates else "Nenhuma credencial do Google encontrada neste computador.")}
        if not token or not link:
            return {"state": "disconnected", "client_id": client["client_id"],
                    "message": "Pronto para conectar ao Google Drive."}
        return {"state": "connected", "client_id": client["client_id"], **link,
                "message": f"Conectado a {link.get('account')} · pasta \"{link.get('folder_name')}\"."}

    # cliente
    def use_client_file(self, path) -> dict:
        client = parse_client(Path(path).read_text(encoding="utf-8"))
        self.store.set(KEY_CLIENT, json.dumps(client))
        return client

    def ensure_client(self) -> dict:
        client = self.client()
        if client:
            return client
        candidates = find_client_files()
        if not candidates:
            raise DriveError("no_client", "Nenhuma credencial do Google encontrada neste computador. Crie uma do "
                                          "tipo \"App para computador\" no Console do Google e baixe o JSON; o App "
                                          "a encontra sozinho na pasta Downloads.")
        return self.use_client_file(candidates[0]["path"])

    # token
    def access_token(self) -> str:
        client, refresh = self.client(), self.store.get(KEY_TOKEN)
        if not client or not refresh:
            raise DriveError("disconnected", "O Google Drive não está conectado.")
        resp = _http("POST", TOKEN_URL, data={"client_id": client["client_id"], "client_secret": client["client_secret"],
                                              "refresh_token": refresh, "grant_type": "refresh_token"},
                     opener=self.opener)
        return resp["access_token"]

    def _api(self, method, path, token, json_body=None, params=None):
        url = DRIVE_API + path + ("?" + urllib.parse.urlencode(params) if params else "")
        return _http(method, url, headers={"Authorization": "Bearer " + token}, json_body=json_body,
                     opener=self.opener)

    # conexão
    def connect(self, open_browser=webbrowser.open, timeout=CONNECT_TIMEOUT_S, cancel=None, authorize_fn=None) -> dict:
        """Clique em Conectar: acha o cliente, pede consentimento, cria/reusa a pasta e confere. Devolve o vínculo."""
        client = self.ensure_client()
        tokens = (authorize_fn or authorize)(client, open_browser=open_browser, timeout=timeout, cancel=cancel)
        refresh, access = tokens.get("refresh_token"), tokens.get("access_token")
        if not refresh or not access:
            raise DriveError("no_refresh", "O Google não devolveu um token de renovação. Conecte de novo.")
        if SCOPE not in tokens.get("scope", SCOPE).split():
            raise DriveError("scope", "A permissão de arquivos do Drive não foi concedida. Marque-a no consentimento.")
        about = self._api("GET", "/about", access, params={"fields": "user(emailAddress,displayName)"})
        account = about.get("user", {}).get("emailAddress")
        previous = self.link()
        folder = None
        if previous and previous.get("account") == account and previous.get("folder_id"):
            try:
                folder = self._folder(previous["folder_id"], access)
                if folder.get("trashed"):
                    folder = None
            except DriveError:
                folder = None
        if folder is None:
            folder = self._api("POST", "/files", access, json_body={"name": FOLDER_NAME, "mimeType": FOLDER_MIME},
                               params={"fields": "id,name"})
        self.store.set(KEY_TOKEN, refresh)
        link = {"account": account, "display_name": about.get("user", {}).get("displayName"),
                "folder_id": folder["id"], "folder_name": folder.get("name", FOLDER_NAME), "connected_at": _now()}
        self.store.set(KEY_LINK, json.dumps(link))
        return self.verify()

    def _folder(self, folder_id, token):
        return self._api("GET", "/files/" + folder_id, token,
                         params={"fields": "id,name,mimeType,trashed,shared,capabilities(canAddChildren)"})

    def verify(self, *, token=None) -> dict:
        """Validação somente leitura do destino. ``ok`` falso bloqueia envio; ``problems`` explica o que corrigir."""
        link = self.link()
        if not link:
            raise DriveError("disconnected", "O Google Drive não está conectado.")
        token = token or self.access_token()
        about = self._api("GET", "/about", token, params={"fields": "user(emailAddress),storageQuota(limit,usage)"})
        problems = []
        account = about.get("user", {}).get("emailAddress")
        if account != link["account"]:
            problems.append(f"A conta mudou de {link['account']} para {account}. Desconecte e conecte de novo.")
        try:
            folder = self._folder(link["folder_id"], token)
        except DriveError as exc:
            if exc.code not in ("not_found", "forbidden"):
                raise
            folder = None
            problems.append("A pasta do App não existe mais no Drive. Clique em Conectar para recriá-la.")
        if folder:
            if folder.get("mimeType") != FOLDER_MIME:
                problems.append("O destino não é uma pasta.")
            if folder.get("trashed"):
                problems.append("A pasta do App está na lixeira do Drive. Restaure-a ou conecte de novo.")
            if folder.get("shared") is not False:
                problems.append("A privacidade da pasta não foi comprovada. Remova o compartilhamento e verifique de novo.")
            if not folder.get("capabilities", {}).get("canAddChildren"):
                problems.append("O App não tem permissão de gravar na pasta.")
        quota = about.get("storageQuota", {})
        free = None
        if quota.get("limit"):
            free = int(quota["limit"]) - int(quota.get("usage", 0))
        link.update({"verified_at": _now(), "ok": not problems, "problems": problems, "free_bytes": free})
        self.store.set(KEY_LINK, json.dumps(link))
        return {"state": "connected", **link}

    def disconnect(self, revoke: bool = True) -> dict:
        """Revoga no Google (melhor esforço, avisando se falhar) e apaga token e vínculo do cofre. Mantém o cliente."""
        refresh = self.store.get(KEY_TOKEN)
        warning = None
        if refresh and revoke:
            try:
                _http("POST", REVOKE_URL, data={"token": refresh}, opener=self.opener)
            except DriveError as exc:
                if exc.code != "revoked":
                    warning = "Não foi possível revogar no Google agora (" + exc.message + "). Revogue em " \
                              "myaccount.google.com/permissions se quiser."
        self.store.delete(KEY_TOKEN)
        self.store.delete(KEY_LINK)
        return {"state": "disconnected", "warning": warning}

    def forget_client(self) -> None:
        self.disconnect()
        self.store.delete(KEY_CLIENT)
