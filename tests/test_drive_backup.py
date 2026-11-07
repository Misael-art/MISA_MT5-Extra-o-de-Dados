"""Cópia cifrada no Drive: cifra por blocos, chave com código de recuperação, envio retomável e ensaio isolado."""
import hashlib
import io
import json
import sqlite3
import urllib.error
import urllib.parse

import pytest

from mt5_extracao.resources import cloud_drive as cd
from mt5_extracao.resources import drive_backup as db_mod
from tests.test_cloud_drive import CLIENT, FakeGoogle, FakeResp, MemoryStore, fake_authorize


class Resp(FakeResp):
    def __init__(self, body=b"", status=200, headers=None):
        super().__init__(body)
        self.status, self.headers = status, headers or {}


class UploadingGoogle(FakeGoogle):
    """Acrescenta envio retomável (com uma queda de rede no meio), listagem e download à FakeGoogle."""

    def __init__(self, drop_after_chunks=1):
        super().__init__()
        self.sessions, self.blobs, self.drop = {}, {}, drop_after_chunks
        self.quota = 10**12

    def __call__(self, req):
        url = urllib.parse.urlparse(req.full_url)
        q = urllib.parse.parse_qs(url.query)
        if url.path.startswith("/upload/") and req.get_method() == "POST":
            sid = f"s{len(self.sessions)}"
            self.sessions[sid] = {"meta": json.loads(req.data), "data": b""}
            return Resp(headers={"Location": f"https://www.googleapis.com/upload/session/{sid}"})
        if url.path.startswith("/upload/session/"):
            s = self.sessions[url.path.rsplit("/", 1)[1]]
            rng = req.get_header("Content-range")
            total = int(rng.rsplit("/", 1)[1])
            if not rng.startswith("bytes */"):
                if self.drop == 0:
                    self.drop = -1
                    raise urllib.error.URLError("queda simulada")
                self.drop -= 1
                start = int(rng.split()[1].split("-")[0])
                assert start == len(s["data"])
                s["data"] += req.data
            if len(s["data"]) < total:
                hdr = {"Range": f"bytes=0-{len(s['data']) - 1}"} if s["data"] else {}
                raise urllib.error.HTTPError(req.full_url, 308, "resume", hdr, io.BytesIO(b""))
            fid = f"b{len(self.blobs)}"
            meta = s["meta"]
            self.blobs[fid] = {"id": fid, "name": meta["name"], "size": str(len(s["data"])),
                               "md5Checksum": hashlib.md5(s["data"]).hexdigest(), "createdTime": "t",
                               "appProperties": meta["appProperties"], "data": s["data"]}
            return Resp(json.dumps({k: v for k, v in self.blobs[fid].items() if k != "data"}).encode())
        if url.path.endswith("/files") and req.get_method() == "GET":
            files = [{k: v for k, v in b.items() if k != "data"} for b in reversed(list(self.blobs.values()))]
            return Resp(json.dumps({"files": files}).encode())
        if "/files/" in url.path and q.get("alt") == ["media"]:
            return Resp(self.blobs[url.path.rsplit("/", 1)[1]]["data"])
        return super().__call__(req)


@pytest.fixture
def base(tmp_path):
    path = tmp_path / "mt5_data.db"
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE bars (t INTEGER, v REAL)")
    con.executemany("INSERT INTO bars VALUES (?, ?)", [(i, i / 3) for i in range(60_000)])
    con.commit()
    con.close()
    return path


@pytest.fixture
def ready(tmp_path, monkeypatch):
    monkeypatch.setattr(cd, "search_dirs", lambda: [])
    monkeypatch.setattr(db_mod.time, "sleep", lambda s: None)
    monkeypatch.setattr(db_mod, "UPLOAD_CHUNK", 256 * 1024)
    monkeypatch.setattr(db_mod, "BLOCK", 64 * 1024)
    google, store = UploadingGoogle(), MemoryStore()
    client = tmp_path / "client_secret.json"
    client.write_text(json.dumps(CLIENT))
    link = cd.DriveLink(store=store, opener=google)
    link.use_client_file(client)
    link.connect(authorize_fn=fake_authorize)
    return db_mod.DriveBackup(link), google, store


def test_encrypt_roundtrip_and_tamper_detection(tmp_path, monkeypatch):
    monkeypatch.setattr(db_mod, "BLOCK", 1000)
    src, enc, out = tmp_path / "a", tmp_path / "a.enc", tmp_path / "a.out"
    src.write_bytes(bytes(range(256)) * 20)
    key = bytes(32)
    info = db_mod.encrypt_file(src, enc, key)
    assert db_mod.decrypt_file(enc, out, key) == info and out.read_bytes() == src.read_bytes()
    data = bytearray(enc.read_bytes())
    data[100] ^= 1
    enc.write_bytes(bytes(data))
    with pytest.raises(cd.DriveError):
        db_mod.decrypt_file(enc, out, key)
    db_mod.encrypt_file(src, enc, key)
    enc.write_bytes(enc.read_bytes()[:-500])                       # corte no fim
    with pytest.raises(cd.DriveError):
        db_mod.decrypt_file(enc, out, key)
    db_mod.encrypt_file(src, enc, key)
    with pytest.raises(cd.DriveError):                             # chave errada
        db_mod.decrypt_file(enc, out, b"\x01" * 32)


def test_empty_file_roundtrip(tmp_path):
    src, enc, out = tmp_path / "e", tmp_path / "e.enc", tmp_path / "e.out"
    src.write_bytes(b"")
    db_mod.encrypt_file(src, enc, bytes(32))
    assert db_mod.decrypt_file(enc, out, bytes(32))["plain_bytes"] == 0


def test_recovery_code_roundtrip_and_typo_detection():
    key = bytes(range(32))
    code = db_mod.recovery_code(key)
    assert db_mod.key_from_code(code.lower().replace("-", " ")) == key
    broken = ("B" if code[0] != "B" else "C") + code[1:]
    with pytest.raises(cd.DriveError):
        db_mod.key_from_code(broken)


def test_send_requires_confirmed_code_then_uploads_resumes_and_drill_passes(ready, base):
    backup, google, store = ready
    with pytest.raises(cd.DriveError) as exc:
        backup.send(base)
    assert exc.value.code == "key_unconfirmed" and not google.blobs
    with pytest.raises(cd.DriveError):
        backup.keys.confirm("AAAA-AAAA")
    backup.keys.confirm(backup.keys.ensure()["code"])
    sent = backup.send(base, scope="ativa")
    assert sent["ok"] and sent["plain_bytes"] > 0
    blob = google.blobs[sent["file_id"]]
    assert blob["appProperties"]["scope"] == "ativa" and b"CREATE TABLE" not in blob["data"]   # cifrado
    assert not list((base.parent / "copias" / ".envio-drive").iterdir())                      # preparo limpo
    drill = backup.drill()
    assert drill["ok"] and drill["integrity"] == "ok" and drill["tables"] == 1


def test_send_blocked_when_destination_has_problems(ready, base):
    backup, google, _ = ready
    backup.keys.confirm(backup.keys.ensure()["code"])
    next(iter(google.folders.values()))["shared"] = True
    with pytest.raises(cd.DriveError) as exc:
        backup.send(base)
    assert exc.value.code == "destination" and not google.blobs


def test_drill_detects_corrupted_remote_copy(ready, base):
    backup, google, _ = ready
    backup.keys.confirm(backup.keys.ensure()["code"])
    sent = backup.send(base)
    blob = google.blobs[sent["file_id"]]
    data = bytearray(blob["data"])
    data[200] ^= 1
    blob["data"] = bytes(data)
    blob["md5Checksum"] = hashlib.md5(blob["data"]).hexdigest()       # Drive coerente, conteúdo adulterado
    with pytest.raises(cd.DriveError) as exc:
        backup.drill()
    assert exc.value.code == "corrupt"


def test_restore_key_on_new_machine_decrypts_existing_copy(ready, base):
    backup, google, store = ready
    code = backup.keys.ensure()["code"]
    backup.keys.confirm(code)
    backup.send(base)
    store.delete(db_mod.KEY_ENC)
    with pytest.raises(cd.DriveError):
        backup.drill()
    backup.keys.restore(code)
    assert backup.drill()["ok"]
