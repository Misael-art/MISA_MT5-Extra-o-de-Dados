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
     (escopo ativa/pesquisa/qa, SHA-256 do conteúdo original, contagens, tamanhos, identidade da origem).

A chave fica no cofre do sistema e tem um código de recuperação que o usuário guarda fora do Drive e do OAuth;
sem confirmar que guardou o código, nada é enviado. ``drill`` baixa a cópia para uma pasta temporária, confere MD5,
decifra, compara SHA-256/contagens e roda ``PRAGMA integrity_check``; a base ativa nunca é tocada. Nada é apagado no Drive.
ID remoto pré-gerado, recibo e sessão no cofre permitem retomar a mesma cópia após reiniciar o processo.
Em falha, só o preparo cifrado persiste. Acima de 70 MB o download é manual, inclusive o total de partes.
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
MANUAL_DOWNLOAD_BYTES = 70_000_000


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
        if len(header) != len(MAGIC) + 8 or header[:len(MAGIC)] != MAGIC:
            raise cd.DriveError("format", "O arquivo baixado não é uma cópia cifrada do MT5X.")
        prefix, index, finished = header[len(MAGIC):], 0, False
        while True:
            head = fin.read(4)
            if not head:
                break
            if finished:
                raise cd.DriveError("corrupt", "Há dados depois do último bloco da cópia.")
            if len(head) != 4:
                raise cd.DriveError("corrupt", "Cabeçalho de bloco incompleto.")
            length = struct.unpack(">I", head)[0]
            if not 16 <= length <= 4 * 1024 * 1024 + 16:
                raise cd.DriveError("corrupt", "Tamanho de bloco inválido.")
            sealed = fin.read(length)
            if len(sealed) != length:
                raise cd.DriveError("corrupt", "Bloco incompleto.")
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


def _table_counts(path):
    con = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True)
    try:
        names = [row[0] for row in con.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
        return {name: con.execute('SELECT count(*) FROM "' + name.replace('"', '""') + '"').fetchone()[0]
                for name in names}
    finally:
        con.close()


def _counts_hash(counts):
    return hashlib.sha256(json.dumps(counts, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def receipt_message(receipt):
    checksum = "conferido (MD5)" if receipt["remote_verification_state"] == "verified_md5" else "pendente"
    restored = "restauração conferida" if receipt["restore_state"] == "verified" else "restauração ainda não ensaiada"
    return f"Cópia enviada: {receipt['name']}; checksum remoto {checksum}; {restored}."


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
           retries: int = 5, session=None, checkpoint=None) -> dict:
    total = path.stat().st_size
    resumed = bool(session)
    if not session:
        status, headers, _ = _raw("POST", UPLOAD_URL + "?uploadType=resumable&fields=id,name,size,md5Checksum",
                              json.dumps(metadata).encode(),
                              {"Authorization": "Bearer " + token, "Content-Type": "application/json; charset=UTF-8",
                               "X-Upload-Content-Type": "application/octet-stream",
                               "X-Upload-Content-Length": str(total)}, opener)
        session = {k.lower(): v for k, v in headers.items()}.get("location")
    if not session:
        raise cd.DriveError("upload", "O Google não abriu a sessão de envio.")
    endpoint = urllib.parse.urlparse(session)
    if endpoint.scheme != "https" or endpoint.hostname != "www.googleapis.com" or not endpoint.path.startswith("/upload/"):
        raise cd.DriveError("upload", "O Google devolveu um destino de envio não reconhecido.")
    if checkpoint:
        checkpoint(session)
    offset, failures, stalls = 0, 0, 0
    with open(path, "rb") as f:
        while True:
            try:
                if failures or resumed:
                    resumed = False
                    st, hd, body = _raw("PUT", session, b"", {"Content-Range": f"bytes */{total}"}, opener)
                    if st in (200, 201):
                        return json.loads(body)
                    offset = _committed(hd)
                    if not 0 <= offset <= total:
                        raise cd.DriveError("upload", "O Drive devolveu uma posição de envio inválida.")
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
                next_offset = _committed(hd)
                if not offset <= next_offset <= offset + len(chunk):
                    raise cd.DriveError("upload", "O Drive devolveu uma posição de envio inválida.")
                stalls = stalls + 1 if next_offset == offset else 0
                if stalls > retries:
                    raise cd.DriveError("network", "O envio não avançou. A cópia preparada foi preservada para retomar.")
                offset = next_offset
                if progress:
                    progress(offset, total)
            except (urllib.error.URLError, socket.timeout, ConnectionError) as exc:
                failures += 1
                if failures > retries:
                    raise cd.DriveError("network", "Envio interrompido. Tente de novo; o Drive não "
                                                   "guarda cópia parcial como válida.") from exc
                time.sleep(min(30, 2 ** failures))


# --- serviço -------------------------------------------------------------------------------------------------------

def _staging(base: Path) -> Path:
    folder = base / ".envio-drive"
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def snapshot(source: Path, target: Path) -> None:
    from mt5_extracao.resources.files import consistent_snapshot
    consistent_snapshot(source, target)


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
        token = self.link.access_token()
        verified = self.link.verify(token=token)
        if not verified.get("ok"):
            raise cd.DriveError("destination", "Envio bloqueado: " + "; ".join(verified.get("problems") or []))
        return verified, token

    def send(self, source, scope: str = "ativa", staging_dir=None, progress=None) -> dict:
        from mt5_extracao.resources.actions import exclusive
        source = Path(source).resolve()
        if scope not in SCOPES:
            raise cd.DriveError("scope_invalid", "Escopo desconhecido; use ativa, pesquisa ou qa.")
        if not source.is_file():
            raise cd.DriveError("source", "Base não encontrada.")
        stage = _staging(Path(staging_dir) if staging_dir else source.parent / "copias")
        with exclusive(stage.parent, "envio Drive"):
            return self._send_locked(source, scope, stage, progress)

    def _send_locked(self, source, scope, stage, progress):
        verified, token = self._ready()
        size = source.stat().st_size
        if shutil.disk_usage(stage).free < size * 2.2:
            raise cd.DriveError("disk", "Disco livre insuficiente para preparar o envio.")
        # O plaintext é temporário; só o ciphertext necessário à retomada persiste em falha.
        with tempfile.TemporaryDirectory(prefix="snapshot-", dir=stage) as tmp:
            plain = Path(tmp) / "snapshot.db"
            snapshot(source, plain)
            with plain.open("rb") as handle:
                plain_hash = hashlib.file_digest(handle, "sha256").hexdigest()
            counts_hash = _counts_hash(_table_counts(plain))
            key = self.keys.get()
            source_id = hashlib.sha256(str(source).encode()).hexdigest()
            copy_id = hashlib.sha256(json.dumps(["sqlite-backup/v1", source_id, scope, plain_hash,
                                               hashlib.sha256(key).hexdigest()]).encode()).hexdigest()
            journal_key = "backup_upload/" + copy_id
            raw = self.link.store.get(journal_key)
            journal = json.loads(raw) if raw else None
            sealed = stage / (copy_id + ".mt5xenc")

            def save():
                self.link.store.set(journal_key, json.dumps(journal))

            if journal:
                if journal["folder_id"] != verified["folder_id"] or journal["account"] != verified["account"]:
                    raise cd.DriveError("destination", "O destino da cópia mudou. Retome na conta e pasta originais.")
                try:
                    remote = self._remote(journal["file_id"], token)
                except cd.DriveError as exc:
                    if exc.code != "not_found":
                        raise
                else:
                    return self._finish_upload(remote, journal, journal_key, sealed)
            else:
                if verified.get("free_bytes") is not None and verified["free_bytes"] < plain.stat().st_size * 1.1:
                    raise cd.DriveError("quota", "Espaço insuficiente no Google Drive para esta cópia.")
                ids = self.link._api("GET", "/files/generateIds", token, params={"count": 1, "space": "drive"})
                stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                journal = {"copy_id": copy_id, "file_id": ids["ids"][0], "folder_id": verified["folder_id"],
                           "account": verified["account"], "name": source.stem + "." + stamp + ".db.mt5xenc",
                           "scope": scope, "plain_sha256": plain_hash, "plain_bytes": plain.stat().st_size,
                           "table_counts_sha256": counts_hash,
                           "created_at": stamp, "session": None, "upload_state": "prepared"}
                save()
            if not sealed.exists():
                if journal.get("session") or journal.get("md5"):
                    raise cd.DriveError("staging_missing", "A cópia cifrada da tentativa está ausente. Preserve o recibo "
                                        "e recupere o preparo original antes de retomar.")
                candidate = Path(tmp) / "sealed"
                encrypt_file(plain, candidate, key)
                candidate.replace(sealed)
                journal.update(md5=_md5(sealed), bytes=sealed.stat().st_size)
                save()
            elif _md5(sealed) != journal.get("md5"):
                raise cd.DriveError("corrupt", "O preparo cifrado não confere com o recibo; nenhum envio foi feito.")
            if verified.get("free_bytes") is not None and verified["free_bytes"] < journal["bytes"]:
                raise cd.DriveError("quota", "Espaço insuficiente no Google Drive para esta cópia.")
            props = {"kind": KIND, "copy_id": copy_id, "scope": scope, "source_id": source_id, "format": "MT5XENC1",
                     "plain_sha256": plain_hash, "plain_bytes": str(journal["plain_bytes"]),
                     "table_counts_sha256": counts_hash,
                     "created_at": journal["created_at"]}
            metadata = {"id": journal["file_id"], "name": journal["name"], "parents": [verified["folder_id"]],
                        "appProperties": props, "mimeType": "application/octet-stream"}

            def checkpoint(session):
                journal.update(session=session, upload_state="uploading")
                save()  # URL de sessão é um segredo: fica somente no cofre.

            try:
                remote = upload(sealed, token, metadata, opener=self.opener, progress=progress,
                                session=journal.get("session"), checkpoint=checkpoint)
            except cd.DriveError as exc:
                if exc.code in ("not_found", "upload_expired"):
                    journal["session"] = None
                    save()
                raise
            return self._finish_upload(remote, journal, journal_key, sealed)

    def _remote(self, file_id, token):
        return self.link._api("GET", "/files/" + file_id, token,
                              params={"fields": "id,name,size,md5Checksum,appProperties,parents,trashed"})

    def _finish_upload(self, remote, journal, journal_key, sealed):
        checksum = remote.get("md5Checksum")
        if (remote.get("id") != journal["file_id"] or
                (checksum and checksum != journal["md5"]) or int(remote.get("size", -1)) != journal["bytes"]):
            raise cd.DriveError("mismatch", "O Drive recebeu um arquivo diferente do preparo. Preserve o recibo; "
                                "a cópia remota não deve ser usada.")
        # A resposta final de upload pode omitir propriedades; a releitura deve vinculá-las.
        if "appProperties" in remote and (remote["appProperties"].get("copy_id") != journal["copy_id"] or
                                          remote.get("parents") != [journal["folder_id"]] or
                                          remote.get("trashed") is not False):
            raise cd.DriveError("mismatch", "A identidade ou a pasta da cópia remota não confere.")
        result = {"ok": bool(checksum), "snapshot_state": "verified", "upload_state": "sent",
                  "remote_verification_state": "verified_md5" if checksum else "pending", "restore_state": "not_tested",
                  **{k: journal[k] for k in ("copy_id", "file_id", "name", "bytes", "md5", "scope", "plain_sha256",
                                            "plain_bytes", "account")}}
        journal.update(upload_state="sent", remote_verification_state=result["remote_verification_state"], session=None)
        self.link.store.set(journal_key, json.dumps(journal))
        if checksum:
            sealed.unlink(missing_ok=True)
        restored = self.link.store.get("backup_restore/" + journal["copy_id"])
        if restored:
            receipt = json.loads(restored)
            if receipt.get("file_id") == journal["file_id"] and receipt.get("md5") == checksum:
                result["restore_state"] = receipt["restore_state"]
        return result

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

    def download_plan(self, file_id=None, destination=None):
        files = self.list()
        target = next((f for f in files if f["id"] == file_id), None) if file_id else (files[0] if files else None)
        if target is None:
            raise cd.DriveError("not_found", "Cópia não encontrada na conta selecionada.")
        return {"state": "manual_download_plan", "file_id": target["id"], "name": target["name"],
                "bytes": int(target.get("size", -1)), "md5": target.get("md5Checksum"),
                "url": "https://drive.google.com/file/d/" + urllib.parse.quote(target["id"], safe="") + "/view",
                "destination": str(destination or target["name"]), "automatic_download": False,
                "validate_argv": ["resources", "drive", "drill", "--file-id", target["id"],
                                  "--downloaded-file", str(destination or target["name"])]}

    def drill(self, file_id: Optional[str] = None, work_dir=None, downloaded_file=None) -> dict:
        """Ensaio de restauração isolado: baixa, confere, decifra e testa a integridade numa pasta temporária."""
        key = self.keys.get()
        if key is None:
            raise cd.DriveError("no_key", "Não há chave neste computador. Use o código de recuperação para restaurá-la.")
        files = self.list()
        target = next((f for f in files if f["id"] == file_id), None) if file_id else (files[0] if files else None)
        if target is None:
            raise cd.DriveError("not_found", "Nenhuma cópia do MT5X encontrada na pasta do Drive.")
        size = int(target.get("size", -1))
        if size < 0:
            raise cd.DriveError("download_budget", "O tamanho da cópia não foi informado pelo Drive; confira antes de baixar.")
        if size > MANUAL_DOWNLOAD_BYTES and downloaded_file is None:
            raise cd.DriveError("manual_download", "Download acima de 70 MB: baixe a cópia cifrada pelo navegador "
                                "e retome o ensaio com --downloaded-file ARQ (sem dividir para contornar o limite).")
        if shutil.disk_usage(work_dir or tempfile.gettempdir()).free < size * (1.1 if downloaded_file else 2.2):
            raise cd.DriveError("disk", "Disco livre insuficiente para o ensaio de restauração.")
        token = self.link.access_token()
        props = target.get("appProperties", {})
        with tempfile.TemporaryDirectory(prefix="mt5x-ensaio-", dir=work_dir) as tmp:
            sealed, plain = Path(tmp) / "copia.mt5xenc", Path(tmp) / "restaurada.db"
            req = urllib.request.Request(cd.DRIVE_API + f"/files/{target['id']}?alt=media",
                                         headers={"Authorization": "Bearer " + token})
            send = self.opener or (lambda r: urllib.request.urlopen(r, timeout=cd.HTTP_TIMEOUT_S * 4))
            try:
                if downloaded_file is not None:
                    sealed = Path(downloaded_file).resolve()
                    if not sealed.is_file() or sealed.stat().st_size != size:
                        raise cd.DriveError("mismatch", "O arquivo baixado manualmente tem tamanho diferente do manifesto.")
                else:
                    with send(req) as resp, open(sealed, "wb") as out:
                        count = 0
                        while True:
                            chunk = resp.read(min(1 << 20, size - count + 1))
                            if not chunk:
                                break
                            count += len(chunk)
                            if count > size:
                                raise cd.DriveError("mismatch", "O download excedeu o tamanho informado pelo Drive.")
                            out.write(chunk)
                    if count != size:
                        raise cd.DriveError("mismatch", "Download incompleto; tente novamente.")
            except urllib.error.URLError as exc:
                raise cd.DriveError("network", "Download interrompido. Confira a conexão e tente novamente.") from exc
            md5 = _md5(sealed)
            if md5 != target.get("md5Checksum"):
                raise cd.DriveError("mismatch", "O arquivo baixado não confere com o MD5 do Drive.")
            info = decrypt_file(sealed, plain, key)
            if (info["plain_sha256"] != props.get("plain_sha256") or
                    info["plain_bytes"] != int(props.get("plain_bytes", -1))):
                raise cd.DriveError("mismatch", "O conteúdo decifrado não confere com o manifesto da cópia.")
            counts = _table_counts(plain)
            if props.get("table_counts_sha256") and _counts_hash(counts) != props["table_counts_sha256"]:
                raise cd.DriveError("mismatch", "As contagens não conferem com o manifesto da cópia.")
            con = sqlite3.connect(f"file:{plain}?mode=ro", uri=True)
            try:
                integrity = con.execute("PRAGMA integrity_check").fetchone()[0]
                tables = con.execute("SELECT count(*) FROM sqlite_master WHERE type='table'").fetchone()[0]
            finally:
                con.close()
        result = {"ok": integrity == "ok", "restore_state": "verified" if integrity == "ok" else "failed",
                "remote_verification_state": "verified_md5", "file_id": target["id"], "name": target["name"], "scope": props.get("scope"),
                "created_at": props.get("created_at"), "md5": md5, "integrity": integrity, "tables": tables,
                "table_counts": counts, "counts_state": "verified" if props.get("table_counts_sha256") else "legacy_no_manifest",
                **info}
        if props.get("copy_id"):
            self.link.store.set("backup_restore/" + props["copy_id"], json.dumps(result))
        return result
