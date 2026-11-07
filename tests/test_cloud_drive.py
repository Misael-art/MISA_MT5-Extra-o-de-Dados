"""Vínculo com o Google Drive: busca do cliente, consentimento local, validação do destino e erros amigáveis."""
import io
import json
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

import pytest

from mt5_extracao.resources import cloud_drive as cd

CLIENT = {"installed": {"client_id": "123-abc.apps.googleusercontent.com", "client_secret": "s3cr3t",
                        "project_id": "mt5x-backup", "redirect_uris": ["http://localhost"]}}


class MemoryStore:
    def __init__(self):
        self.data = {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value):
        self.data[key] = value

    def delete(self, key):
        self.data.pop(key, None)


class FakeResp(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeGoogle:
    """Responde token, about, files e revoke como a API do Google, registrando as chamadas."""

    def __init__(self, account="dono@example.com"):
        self.account = account
        self.folders = {}
        self.calls = []
        self.refresh_error = None

    def __call__(self, req):
        url = urllib.parse.urlparse(req.full_url)
        body = req.data.decode() if req.data else ""
        self.calls.append((req.get_method(), url.path))
        if url.path == "/token":
            if self.refresh_error:
                raise urllib.error.HTTPError(req.full_url, 400, "bad", {}, io.BytesIO(
                    json.dumps({"error": self.refresh_error}).encode()))
            return FakeResp(json.dumps({"access_token": "at"}).encode())
        if url.path == "/revoke":
            return FakeResp(b"")
        if url.path.endswith("/about"):
            return FakeResp(json.dumps({"user": {"emailAddress": self.account, "displayName": "Dono"},
                                        "storageQuota": {"limit": "1000", "usage": "400"}}).encode())
        if url.path.endswith("/files") and req.get_method() == "POST":
            fid = f"f{len(self.folders) + 1}"
            self.folders[fid] = {"id": fid, "name": json.loads(body)["name"], "mimeType": cd.FOLDER_MIME,
                                 "trashed": False, "shared": False, "capabilities": {"canAddChildren": True}}
            return FakeResp(json.dumps({"id": fid, "name": self.folders[fid]["name"]}).encode())
        if "/files/" in url.path:
            fid = url.path.rsplit("/", 1)[1]
            if fid not in self.folders:
                raise urllib.error.HTTPError(req.full_url, 404, "nf", {}, io.BytesIO(b'{"error":{"code":404}}'))
            return FakeResp(json.dumps(self.folders[fid]).encode())
        raise AssertionError(url.path)


def fake_authorize(client, open_browser, timeout, cancel):
    return {"access_token": "at", "refresh_token": "rt", "scope": cd.SCOPE}


@pytest.fixture
def downloads(tmp_path, monkeypatch):
    monkeypatch.setattr(cd, "search_dirs", lambda: [tmp_path])
    monkeypatch.setattr(cd, "_config_dir", lambda: tmp_path / "config")
    return tmp_path


def test_parse_client_accepts_desktop_and_explains_wrong_types():
    assert cd.parse_client(json.dumps(CLIENT))["client_id"].endswith(".apps.googleusercontent.com")
    with pytest.raises(cd.DriveError) as web:
        cd.parse_client({"web": CLIENT["installed"]})
    assert web.value.code == "client_wrong_type"
    with pytest.raises(cd.DriveError):
        cd.parse_client("{not json")
    with pytest.raises(cd.DriveError):
        cd.parse_client({"installed": {"client_id": "x", "client_secret": "y"}})


def test_search_finds_recent_valid_client_and_ignores_others(downloads):
    (downloads / "client_secret_123.json").write_text(json.dumps(CLIENT))
    (downloads / "client_secret_web.json").write_text(json.dumps({"web": CLIENT["installed"]}))
    (downloads / "notes.json").write_text(json.dumps(CLIENT))                      # nome não sugere credencial
    old = downloads / "oauth_old.json"
    old.write_text(json.dumps(CLIENT))
    stale = time.time() - (cd.SEARCH_MAX_AGE_DAYS + 1) * 86400
    os.utime(old, (stale, stale))
    found = cd.find_client_files([downloads])
    assert [f["path"] for f in found] == [str(downloads / "client_secret_123.json")]


def test_connect_finds_client_creates_private_folder_and_verifies(downloads):
    (downloads / "client_secret_123.json").write_text(json.dumps(CLIENT))
    store, google = MemoryStore(), FakeGoogle()
    link = cd.DriveLink(store=store, opener=google)
    assert link.status()["state"] == "no_client"
    result = link.connect(authorize_fn=fake_authorize)
    assert result["ok"] and result["account"] == "dono@example.com" and result["free_bytes"] == 600
    assert result["folder_name"] == cd.FOLDER_NAME and store.data[cd.KEY_TOKEN] == "rt"
    assert "s3cr3t" not in store.data[cd.KEY_LINK]                                  # segredo só no registro do cliente
    assert link.status()["state"] == "connected"
    link.connect(authorize_fn=fake_authorize)                                        # reconectar reusa a pasta
    assert len(google.folders) == 1


def test_verify_blocks_on_account_change_shared_or_trashed_folder(downloads):
    store, google = MemoryStore(), FakeGoogle()
    link = cd.DriveLink(store=store, opener=google)
    link.use_client_file(_write(downloads, CLIENT))
    link.connect(authorize_fn=fake_authorize)
    folder = next(iter(google.folders.values()))
    folder["shared"] = True
    google.account = "outra@example.com"
    result = link.verify()
    assert not result["ok"] and len(result["problems"]) == 2
    google.folders.clear()
    assert any("não existe" in p for p in link.verify()["problems"])


def test_revoked_token_gives_actionable_message_and_disconnect_clears_store(downloads):
    store, google = MemoryStore(), FakeGoogle()
    link = cd.DriveLink(store=store, opener=google)
    link.use_client_file(_write(downloads, CLIENT))
    link.connect(authorize_fn=fake_authorize)
    google.refresh_error = "invalid_grant"
    with pytest.raises(cd.DriveError) as exc:
        link.verify()
    assert exc.value.code == "revoked" and "Conectar" in exc.value.message
    assert link.disconnect()["state"] == "disconnected"
    assert cd.KEY_TOKEN not in store.data and cd.KEY_LINK not in store.data and cd.KEY_CLIENT in store.data


def test_network_failure_retries_then_reports(monkeypatch):
    monkeypatch.setattr(cd.time, "sleep", lambda s: None)
    calls = []

    def down(req):
        calls.append(1)
        raise urllib.error.URLError("offline")

    with pytest.raises(cd.DriveError) as exc:
        cd._http("GET", "https://example.invalid/x", opener=down)
    assert exc.value.code == "network" and len(calls) == cd.RETRIES


def test_authorize_uses_loopback_pkce_and_checks_state(monkeypatch):
    sent = {}
    monkeypatch.setattr(cd, "_http", lambda method, url, data=None, **k: sent.update(data) or
                        {"access_token": "at", "refresh_token": "rt"})

    def browser(url):
        q = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        assert q["scope"] == [cd.SCOPE] and q["code_challenge_method"] == ["S256"]
        redirect = q["redirect_uri"][0]
        assert redirect.startswith("http://127.0.0.1:")

        def answer():
            try:                                                    # resposta forjada (state errado) é recusada
                urllib.request.urlopen(redirect + "?code=evil&state=wrong", timeout=5)
            except urllib.error.HTTPError as exc:
                assert exc.code == 400
            urllib.request.urlopen(redirect + "?" + urllib.parse.urlencode(
                {"code": "good", "state": q["state"][0]}), timeout=5).read()

        threading.Thread(target=answer, daemon=True).start()
        return True

    tokens = cd.authorize(cd.parse_client(CLIENT), open_browser=browser, timeout=10)
    assert tokens["refresh_token"] == "rt" and sent["code"] == "good" and sent["code_verifier"]


def test_authorize_can_be_cancelled():
    cancel = threading.Event()
    cancel.set()
    with pytest.raises(cd.DriveError) as exc:
        cd.authorize(cd.parse_client(CLIENT), open_browser=lambda url: True, timeout=10, cancel=cancel)
    assert exc.value.code == "cancelled"


def _write(folder, data):
    path = folder / "client_secret_x.json"
    path.write_text(json.dumps(data))
    return path
