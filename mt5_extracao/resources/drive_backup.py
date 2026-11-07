"""
Cópia cifrada das bases no Google Drive (T20.30-HOST1-DRIVE, etapas 2 e 3: salvar, conferir e ensaiar restauração).

Fluxo de ``send``:
  1. ``DriveLink.verify`` (somente leitura) precisa terminar sem problemas; qualquer problema bloqueia o envio.
  2. Snapshot SQLite consistente pelo mecanismo existente (``sqlite3.backup`` + ``quick_check``), numa pasta de
     preparo ao lado das cópias locais, nunca na base ativa.
  3. Cifra AES-256-GCM em blocos (``MT5XENC1``): cada bloco autentica o cabeçalho, o número do bloco e a marca de
     último bloco, de modo que troca de ordem, corte ou adulteração falham na leitura.
  4. Envio retomável da Drive API (blocos de 8 MiB; após falha de rede pergunta ao Google até onde chegou).
  5. Confere o MD5 informado pelo Drive contra o arquivo cifrado local e grava o manifesto em ``appProperties``
     (escopo ativa/pesquisa/qa, SHA-256 do conteúdo original, tamanhos, origem).

A chave fica no cofre do sistema e tem um código de recuperação que o usuário guarda fora do Drive e do OAuth;
sem confirmar que guardou o código, nada é enviado. ``drill`` baixa a cópia para uma pasta temporária, confere MD5,
decifra, compara o SHA-256 e roda ``PRAGMA integrity_check``; a base ativa nunca é tocada. Nada é apagado no Drive.
"""
from __future__ import annotations

import base64
import hashlib
import json
import secrets
import shutil
import socket
import sqlite3
import struct
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

from mt5_extracao.resources import cloud_drive as cd

MAGIC = b"MT5XENC1"
BLOCK = 4 * 1024 * 1024
UPLOAD_CHUNK = 8 * 1024 * 1024          # múltiplo de 256 KiB, como a API exige
UPLOAD_URL = "https://www.googleapis.com/upload/drive/v3/files"
KEY_ENC, KEY_ENC_CONFIRMED = "backup_key", "backup_key_confirmed"
SCOPES = ("ativa", "pesquisa", "qa")
KIND = "mt5x-backup"


# --- chave ---------------------------------------------------------------------------------------------------------

def recovery_code(key: bytes) -> str:
    """Código legível (grupos de 4) com verificação: os 2 últimos bytes são o início do SHA-256 da chave."""
    raw = key + hashlib.sha256(key).digest()[:2]
    text = base64.b32encode(raw).decode().rstrip("=")
    return "-".join(text[i:i + 4] for i in range(0, len(text), 4))


def key_from_code(code: str) -> bytes:
    text = "".join(ch for ch in code.upper() if ch.isalnum())
    try:
        raw = base64.b32decode(text + "=" * (-len(text) % 8))
    except ValueError as exc:
        raise cd.DriveError("code_invalid", "Código de recuperação inválido.") from exc
    key, check = raw[:32], raw[32:]
    if len(key) != 32 or hashlib.sha256(key).digest()[:2] != check:
        raise cd.DriveError("code_invalid", "Código de recuperação inválido (confira as letras e os números).")
    return key


class Keys:
    def __init__(self, store):
        self.store = store

    def get(self) -> Optional[bytes]:
        raw = self.store.get(KEY_ENC)
        return base64.b64decode(raw) if raw else None

    def ensure(self) -> dict:
        """Cria a chave na primeira vez. Devolve {code, confirmed}; o código só deve ser mostrado ao usuário."""
        key = self.get()
        if key is None:
            key = secrets.token_bytes(32)
            self.store.set(KEY_ENC, base64.b64encode(key).decode())
            self.store.delete(KEY_ENC_CONFIRMED)
        return {"code": recovery_code(key), "confirmed": self.store.get(KEY_ENC_CONFIRMED) == "1"}

    def confirm(self, typed_code: str) -> None:
        """O usuário redigita o código: prova que o guardou antes de liberar o envio."""
        key = self.get()
        if key is None or key_from_code(typed_code) != key:
            raise cd.DriveError("code_mismatch", "O código digitado não confere com a chave desta máquina.")
        self.store.set(KEY_ENC_CONFIRMED, "1")

    def restore(self, code: str) -> None:
        """Num computador novo: recria a chave a partir do código guardado."""
        self.store.set(KEY_ENC, base64.b64encode(key_from_code(code)).decode())
        self.store.set(KEY_ENC_CONFIRMED, "1")


# --- cifra ---------------------------------------------------------------------------------------------------------

def encrypt_file(src: Path, dst: Path, key: bytes) -> dict:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    aes, prefix = AESGCM(key), secrets.token_bytes(8)
    header = MAGIC + prefix
    sha, size = hashlib.sha256(), 0
    with open(src, "rb") as fin, open(dst, "wb") as fout:
        fout.write(header)
        index, block = 0, fin.read(BLOCK)
        while True:
            following = fin.read(BLOCK) if block else b""
            last = not following
            sha.update(block)
            size += len(block)
            aad = header + struct.pack(">IB", index, last)
            sealed = aes.encrypt(prefix + struct.pack(">I", index), block, aad)
            fout.write(struct.pack(">I", len(sealed)) + sealed)
            if last:
                break
            index, block = index + 1, following
    return {"plain_sha256": sha.hexdigest(), "plain_bytes": size}


def decrypt_file(src: Path, dst: Path, key: bytes) -> dict:
    from cryptography.exceptions import InvalidTag
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    aes, sha, size = AESGCM(key), hashlib.sha256(), 0
    with open(src, "rb") as fin, open(dst, "wb") as fout:
        header = fin.read(len(MAGIC) + 8)
        if header[:len(MAGIC)] != MAGIC:
            raise cd.DriveError("format", "O arquivo baixado não é uma cópia cifrada do MT5X.")
        prefix, index, finished = header[len(MAGIC):], 0, False
        while True:
            head = fin.read(4)
            if not head:
                break
            if finished:
                raise cd.DriveError("corrupt", "Há dados depois do último bloco da cópia.")
            sealed = fin.read(struct.unpack(">I", head)[0])
            plain = None
            for last in (False, True):
                try:
                    plain = aes.decrypt(prefix + struct.pack(">I", index), sealed,
                                        header + struct.pack(">IB", index, last))
                    finished = last
                    break
                except InvalidTag:
                    continue
            if plain is None:
                raise cd.DriveError("corrupt", "A cópia está adulterada ou a chave é outra (bloco "
                                               f"{index} não confere).")
            sha.update(plain)
            size += len(plain)
            fout.write(plain)
            index += 1
        if not finished:
            raise cd.DriveError("corrupt", "A cópia está incompleta (falta o último bloco).")
    return {"plain_sha256": sha.hexdigest(), "plain_bytes": size}


def _md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# --- HTTP de envio retomável ---------------------------------------------------------------------------------------

def _raw(method, url, body=None, headers=None, opener=None):
    """(status, headers, corpo). 308 é resposta normal do envio retomável; outros HTTPError viram DriveError."""
    send = opener or (lambda req: urllib.request.urlopen(req, timeout=cd.HTTP_TIMEOUT_S * 4))
    req = urllib.request.Request(url, data=body, headers=headers or {}, method=method)
    try:
        with send(req) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as exc:
        if exc.code == 308:
            return 308, dict(exc.headers or {}), b""
        raw = exc.read() or b"{}"
        try:
            payload = json.loads(raw)
        except ValueError:
            payload = {}
        if exc.code in (429, 500, 502, 503, 504):
            raise ConnectionError(f"HTTP {exc.code}") from exc
        raise cd._http_error(exc.code, payload) from exc


def _committed(headers) -> int:
    rng = {k.lower(): v for k, v in headers.items()}.get("range")
    return int(rng.rsplit("-", 1)[1]) + 1 if rng else 0


def upload(path: Path, token: str, metadata: dict, opener=None, progress: Optional[Callable] = None,
           retries: int = 5) -> dict:
    total = path.stat().st_size
    status, headers, _ = _raw("POST", UPLOAD_URL + "?uploadType=resumable&fields=id,name,size,md5Checksum",
                              json.dumps(metadata).encode(),
                              {"Authorization": "Bearer " + token, "Content-Type": "application/json; charset=UTF-8",
                               "X-Upload-Content-Type": "application/octet-stream",
                               "X-Upload-Content-Length": str(total)}, opener)
    session = {k.lower(): v for k, v in headers.items()}.get("location")
    if not session:
        raise cd.DriveError("upload", "O Google não abriu a sessão de envio.")
    offset, failures = 0, 0
    with open(path, "rb") as f:
        while True:
            try:
                if failures:
                    st, hd, body = _raw("PUT", session, b"", {"Content-Range": f"bytes */{total}"}, opener)
                    if st in (200, 201):
                        return json.loads(body)
                    offset = _committed(hd)
                f.seek(offset)
                chunk = f.read(UPLOAD_CHUNK)
                end = offset + len(chunk) - 1
                rng = f"bytes {offset}-{end}/{total}" if chunk else f"bytes */{total}"
                st, hd, body = _raw("PUT", session, chunk, {"Content-Range": rng}, opener)
                failures = 0
                if st in (200, 201):
                    if progress:
                        progress(total, total)
                    return json.loads(body)
                offset = _committed(hd)
                if progress:
                    progress(offset, total)
            except (urllib.error.URLError, socket.timeout, ConnectionError) as exc:
                failures += 1
                if failures > retries:
                    raise cd.DriveError("network", f"Envio interrompido ({exc}). Tente de novo; o Drive não "
                                                   "guarda cópia parcial como válida.") from exc
                time.sleep(min(30, 2 ** failures))


# --- serviço -------------------------------------------------------------------------------------------------------

def _staging(base: Path) -> Path:
    folder = base / ".envio-drive"
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def snapshot(source: Path, target: Path) -> None:
    src = sqlite3.connect(f"file:{source}?mode=ro", uri=True, timeout=30)
    dst = sqlite3.connect(str(target))
    try:
        src.backup(dst, pages=4096)
        check = dst.execute("PRAGMA quick_check").fetchone()[0]
    finally:
        dst.close()
        src.close()
    if check != "ok":
        raise cd.DriveError("snapshot", f"A cópia local não passou na conferência ({check}); nada foi enviado.")


class DriveBackup:
    def __init__(self, link: Optional[cd.DriveLink] = None, opener=None):
        self.link = link or cd.DriveLink(opener=opener)
        self.opener = opener if opener is not None else self.link.opener
        self.keys = Keys(self.link.store)

    def _ready(self) -> tuple:
        state = self.keys.ensure()
        if not state["confirmed"]:
            raise cd.DriveError("key_unconfirmed", "Guarde o código de recuperação e confirme-o antes do primeiro "
                                                   "envio (sem ele, ninguém decifra as cópias).")
        verified = self.link.verify()
        if not verified.get("ok"):
            raise cd.DriveError("destination", "Envio bloqueado: " + "; ".join(verified.get("problems") or []))
        return verified, self.link.access_token()

    def send(self, source, scope: str = "ativa", staging_dir=None, progress=None) -> dict:
        source = Path(source).resolve()
        if scope not in SCOPES:
            raise cd.DriveError("scope_invalid", f"Escopo desconhecido: {scope} (use {', '.join(SCOPES)}).")
        if not source.is_file():
            raise cd.DriveError("source", f"Base não encontrada: {source}")
        verified, token = self._ready()
        stage = _staging(Path(staging_dir) if staging_dir else source.parent / "copias")
        need = source.stat().st_size * 2.2
        free = shutil.disk_usage(stage).free
        if free < need:
            raise cd.DriveError("disk", f"Disco livre insuficiente para preparar o envio ({free // 2**20} MiB; "
                                        f"precisa ~{int(need) // 2**20} MiB).")
        if verified.get("free_bytes") is not None and verified["free_bytes"] < source.stat().st_size * 1.1:
            raise cd.DriveError("quota", "Espaço insuficiente no Google Drive para esta cópia.")
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        plain, sealed = stage / f"{source.stem}.{stamp}.db", stage / f"{source.stem}.{stamp}.db.mt5xenc"
        try:
            snapshot(source, plain)
            info = encrypt_file(plain, sealed, self.keys.get())
            md5 = _md5(sealed)
            props = {"kind": KIND, "scope": scope, "source": source.name[:60], "format": "MT5XENC1",
                     "plain_sha256": info["plain_sha256"], "plain_bytes": str(info["plain_bytes"]),
                     "created_at": stamp}
            meta = {"name": sealed.name, "parents": [verified["folder_id"]], "appProperties": props,
                    "mimeType": "application/octet-stream"}
            remote = upload(sealed, token, meta, opener=self.opener, progress=progress)
            if remote.get("md5Checksum") != md5 or int(remote.get("size", -1)) != sealed.stat().st_size:
                raise cd.DriveError("mismatch", "O Drive recebeu um arquivo diferente do enviado (MD5/tamanho). "
                                                f"Arquivo remoto {remote.get('id')} não deve ser usado.")
            return {"ok": True, "file_id": remote["id"], "name": remote["name"], "bytes": int(remote["size"]),
                    "md5": md5, "scope": scope, **info, "account": verified["account"]}
        finally:
            plain.unlink(missing_ok=True)
            sealed.unlink(missing_ok=True)

    def list(self) -> list:
        link = self.link.link()
        if not link:
            raise cd.DriveError("disconnected", "O Google Drive não está conectado.")
        token = self.link.access_token()
        q = (f"'{link['folder_id']}' in parents and trashed=false and "
             f"appProperties has {{ key='kind' and value='{KIND}' }}")
        out, page = [], None
        while True:
            params = {"q": q, "fields": "nextPageToken,files(id,name,size,md5Checksum,createdTime,appProperties)",
                      "orderBy": "createdTime desc", "pageSize": 100}
            if page:
                params["pageToken"] = page
            resp = self.link._api("GET", "/files", token, params=params)
            out.extend(resp.get("files", []))
            page = resp.get("nextPageToken")
            if not page:
                return out

    def drill(self, file_id: Optional[str] = None, work_dir=None) -> dict:
        """Ensaio de restauração isolado: baixa, confere, decifra e testa a integridade numa pasta temporária."""
        key = self.keys.get()
        if key is None:
            raise cd.DriveError("no_key", "Não há chave neste computador. Use o código de recuperação para restaurá-la.")
        files = self.list()
        target = next((f for f in files if f["id"] == file_id), None) if file_id else (files[0] if files else None)
        if target is None:
            raise cd.DriveError("not_found", "Nenhuma cópia do MT5X encontrada na pasta do Drive.")
        token = self.link.access_token()
        props = target.get("appProperties", {})
        with tempfile.TemporaryDirectory(prefix="mt5x-ensaio-", dir=work_dir) as tmp:
            sealed, plain = Path(tmp) / "copia.mt5xenc", Path(tmp) / "restaurada.db"
            req = urllib.request.Request(cd.DRIVE_API + f"/files/{target['id']}?alt=media",
                                         headers={"Authorization": "Bearer " + token})
            send = self.opener or (lambda r: urllib.request.urlopen(r, timeout=cd.HTTP_TIMEOUT_S * 4))
            try:
                with send(req) as resp, open(sealed, "wb") as out:
                    shutil.copyfileobj(resp, out, 1 << 20)
            except urllib.error.URLError as exc:
                raise cd.DriveError("network", f"Download interrompido ({exc}).") from exc
            md5 = _md5(sealed)
            if md5 != target.get("md5Checksum"):
                raise cd.DriveError("mismatch", "O arquivo baixado não confere com o MD5 do Drive.")
            info = decrypt_file(sealed, plain, key)
            if info["plain_sha256"] != props.get("plain_sha256"):
                raise cd.DriveError("mismatch", "O conteúdo decifrado não confere com o manifesto da cópia.")
            con = sqlite3.connect(f"file:{plain}?mode=ro", uri=True)
            try:
                integrity = con.execute("PRAGMA integrity_check").fetchone()[0]
                tables = con.execute("SELECT count(*) FROM sqlite_master WHERE type='table'").fetchone()[0]
            finally:
                con.close()
        return {"ok": integrity == "ok", "file_id": target["id"], "name": target["name"], "scope": props.get("scope"),
                "created_at": props.get("created_at"), "md5": md5, "integrity": integrity, "tables": tables, **info}
