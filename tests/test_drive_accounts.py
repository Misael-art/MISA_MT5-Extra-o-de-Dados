"""Duas identidades simuladas; nenhum consentimento, cofre ou Drive real."""
import io
import hashlib
import json
import urllib.error
import urllib.parse
import urllib.request

import pytest

from mt5_extracao.resources import cloud_drive as cd, drive_backup
from tests.test_cloud_drive import CLIENT, MemoryStore, FakeResp
from tests.test_drive_backup import UploadingGoogle, base


class TwoAccounts:
    def __init__(self):
        self.google = {name: UploadingGoogle(drop_after_chunks=-1) for name in ("a", "b")}
        self.revoked = set()
        self.swap = {}
        self.refresh_accounts = {}
        for name, google in self.google.items():
            google.account = name + "@example.invalid"
            google.permission_id = "permission-" + name

    def authorize(self, name):
        def consent(client, **kwargs):
            suffix = "" if client["client_id"] == CLIENT["installed"]["client_id"] else "-" + hashlib.sha256(client["client_id"].encode()).hexdigest()[:8]
            refresh = "rt-" + name + suffix
            self.refresh_accounts[refresh] = (name, client["client_id"], client["client_secret"])
            return {"access_token": "at-" + name, "refresh_token": refresh, "scope": cd.SCOPE}
        return consent

    def __call__(self, req):
        parsed = urllib.parse.urlparse(req.full_url)
        if parsed.path == "/token":
            fields = urllib.parse.parse_qs(req.data.decode())
            refresh = fields["refresh_token"][0]
            if refresh in self.revoked:
                raise urllib.error.HTTPError(req.full_url, 400, "revoked", {},
                                            io.BytesIO(b'{"error":"invalid_grant"}'))
            name, client_id, secret = self.refresh_accounts[refresh]
            assert fields["client_id"] == [client_id] and fields["client_secret"] == [secret]
            return FakeResp(json.dumps({"access_token": "at-" + self.swap.get(name, name)}).encode())
        if parsed.path == "/revoke":
            self.revoked.add(urllib.parse.parse_qs(req.data.decode())["token"][0])
            return FakeResp(b"")
        if parsed.path.startswith("/upload/session/"):
            name, session = parsed.path.rsplit("/", 1)[1].split("-", 1)
            url = req.full_url.rsplit("/", 1)[0] + "/" + session
            forwarded = urllib.request.Request(url, data=req.data, headers=dict(req.headers), method=req.get_method())
            return self.google[name](forwarded)
        name = req.get_header("Authorization").rsplit("-", 1)[1]
        response = self.google[name](req)
        if parsed.path.startswith("/upload/") and req.get_method() == "POST":
            old = response.headers["Location"]
            response.headers["Location"] = old.rsplit("/", 1)[0] + "/" + name + "-" + old.rsplit("/", 1)[1]
        return response


@pytest.fixture
def accounts(tmp_path, monkeypatch):
    monkeypatch.setattr(cd, "_config_dir", lambda: tmp_path / "config")
    monkeypatch.setattr(cd, "search_dirs", lambda: [])
    monkeypatch.setattr(drive_backup.time, "sleep", lambda _: None)
    store, api = MemoryStore(), TwoAccounts()
    store.set(cd.KEY_CLIENT, json.dumps(cd.parse_client(CLIENT)))
    manager = cd.DriveAccounts(store, api)
    a = manager.connect(authorize_fn=api.authorize("a"))["account_id"]
    b = manager.connect(authorize_fn=api.authorize("b"))["account_id"]
    return manager, api, store, a, b


def test_two_accounts_are_explicit_and_vault_namespaces_are_separate(accounts):
    manager, api, store, a, b = accounts
    assert a != b and len(manager.accounts()) == 2
    assert manager.status()["state"] == "choose_account"
    with pytest.raises(cd.DriveError) as caught:
        manager.link()
    assert caught.value.code == "account_required"
    assert manager.link(a).store.get(cd.KEY_TOKEN) == "rt-a"
    assert manager.link(b).store.get(cd.KEY_TOKEN) == "rt-b"
    public = json.dumps(manager.accounts())
    assert "rt-a" not in public and "rt-b" not in public and "s3cr3t" not in public


def test_email_is_only_a_label_and_stable_identity_survives(accounts):
    manager, api, _, a, b = accounts
    api.google["a"].account = "renamed@example.invalid"
    assert manager.link(a).verify()["ok"]
    assert manager.link(a).status()["account"] == "renamed@example.invalid"
    reconnected = manager.connect(authorize_fn=api.authorize("a"))
    assert reconnected["account_id"] == a and len(api.google["a"].folders) == 1


def test_changing_default_client_does_not_replace_existing_credentials(accounts):
    manager, api, store, a, b = accounts
    original = manager.link(a).client()
    changed = dict(original, client_id="client2.apps.googleusercontent.com", client_secret="synthetic-client2")
    store.set(cd.KEY_CLIENT, json.dumps(changed))
    third = manager.connect(authorize_fn=api.authorize("a"))["account_id"]
    assert third not in (a, b) and len(manager.accounts()) == 3
    assert manager.link(a).client() == original
    assert manager.link(a).verify()["ok"] and manager.link(b).verify()["ok"]


def test_copy_identity_survives_account_label_change(accounts, base):
    manager, api, _, a, b = accounts
    backup = drive_backup.DriveBackup(manager.link(a))
    backup.keys.confirm(backup.keys.ensure()["code"])
    first = backup.send(base, scope="qa")
    api.google["a"].account = "renamed@example.invalid"
    again = backup.send(base, scope="qa")
    assert again["file_id"] == first["file_id"] and again["account"] == "renamed@example.invalid"


def test_swapped_and_revoked_tokens_never_fall_back_to_other_account(accounts, base):
    manager, api, store, a, b = accounts
    backup = drive_backup.DriveBackup(manager.link(a))
    backup.keys.confirm(backup.keys.ensure()["code"])
    api.swap["a"] = "b"
    with pytest.raises(cd.DriveError) as caught:
        backup.send(base, scope="qa")
    assert caught.value.code == "destination" and not api.google["b"].blobs
    api.swap.clear()
    api.revoked.add("rt-a")
    with pytest.raises(cd.DriveError) as caught:
        backup.send(base, scope="qa")
    assert caught.value.code == "revoked"
    assert manager.link(b).verify()["ok"] and manager.link(b).store.get(cd.KEY_TOKEN) == "rt-b"


def test_account_disconnect_and_wrong_reconnect_preserve_other_tokens(accounts):
    manager, api, store, a, b = accounts
    with pytest.raises(cd.DriveError) as caught:
        manager.connect(account_id=a, authorize_fn=api.authorize("b"))
    assert caught.value.code == "account_mismatch"
    manager.link(a).disconnect()
    assert manager.link(a).status()["state"] == "disconnected"
    assert manager.link(b).store.get(cd.KEY_TOKEN) == "rt-b" and manager.link(b).verify()["ok"]


def test_user_selected_copies_go_to_distinct_accounts(accounts, base):
    manager, api, _, a, b = accounts
    for account_id in [a, b]:
        backup = drive_backup.DriveBackup(manager.link(account_id))
        backup.keys.confirm(backup.keys.ensure()["code"])
        result = backup.send(base, scope="qa")
        assert result["account"] in ("a@example.invalid", "b@example.invalid")
        assert backup.drill(result["file_id"])["ok"]
    assert len(api.google["a"].blobs) == len(api.google["b"].blobs) == 1
    assert manager.link(a).store.get(drive_backup.KEY_ENC) != manager.link(b).store.get(drive_backup.KEY_ENC)


def test_quota_or_private_folder_failure_preserves_other_destination(accounts, base):
    manager, api, _, a, b = accounts
    backup = drive_backup.DriveBackup(manager.link(a))
    backup.keys.confirm(backup.keys.ensure()["code"])
    api.google["a"].quota = 400
    with pytest.raises(cd.DriveError) as caught:
        backup.send(base, scope="qa")
    assert caught.value.code == "quota"
    original = manager.link(a).link()["folder_id"]
    with pytest.raises(cd.DriveError):
        manager.select_folder(a, "missing")
    assert manager.link(a).link()["folder_id"] == original
    assert manager.link(b).verify()["ok"]


def test_failed_account_registration_keeps_existing_binding(accounts, monkeypatch):
    manager, api, store, a, b = accounts
    before = dict(store.data)
    original = store.set
    failed = []
    def fail_once(key, value):
        if key == cd.KEY_ACCOUNTS and not failed:
            failed.append(True)
            raise cd.DriveError("keyring_error", "synthetic vault failure")
        original(key, value)
    monkeypatch.setattr(store, "set", fail_once)
    with pytest.raises(cd.DriveError):
        manager.connect(authorize_fn=api.authorize("a"))
    assert store.data == before


def test_failed_registration_and_failed_rollback_can_reconnect_without_losing_other_accounts(accounts, monkeypatch):
    manager, api, store, a, b = accounts
    original = store.set
    failed = []
    def fault(key, value):
        if key == cd.KEY_ACCOUNTS and not failed:
            failed.append("registry")
            raise cd.DriveError("keyring_error", "private-sentinel")
        if key == "account/" + a + "/binding" and failed == ["registry"]:
            failed.append("rollback")
            raise cd.DriveError("keyring_error", "private-sentinel")
        original(key, value)
    monkeypatch.setattr(store, "set", fault)
    with pytest.raises(cd.DriveError) as caught:
        manager.connect(account_id=a, authorize_fn=api.authorize("a"))
    assert caught.value.code == "storage_partial"
    assert "private-sentinel" not in caught.value.message
    assert manager.link(b).verify()["ok"]
    monkeypatch.setattr(store, "set", original)
    assert manager.connect(account_id=a, authorize_fn=api.authorize("a"))["account_id"] == a
    assert manager.link(a).verify()["ok"] and manager.link(b).verify()["ok"]


def test_legacy_backup_and_recovery_key_survive_matching_account_enrollment(tmp_path, monkeypatch, base):
    monkeypatch.setattr(cd, "_config_dir", lambda: tmp_path / "config")
    monkeypatch.setattr(cd, "search_dirs", lambda: [])
    monkeypatch.setattr(drive_backup.time, "sleep", lambda _: None)
    store, api = MemoryStore(), TwoAccounts()
    store.set(cd.KEY_CLIENT, json.dumps(cd.parse_client(CLIENT)))
    legacy = cd.DriveLink(store, api)
    assert legacy.connect(authorize_fn=api.authorize("a"))["ok"]
    original_folder = legacy.link()["folder_id"]
    old_backup = drive_backup.DriveBackup(legacy)
    old_backup.keys.confirm(old_backup.keys.ensure()["code"])
    sent = old_backup.send(base, scope="qa")
    manager = cd.DriveAccounts(store, api)
    assert manager.status()["state"] == "legacy_connection"
    account_id = manager.connect(authorize_fn=api.authorize("a"))["account_id"]
    account_backup = drive_backup.DriveBackup(manager.link(account_id))
    assert account_backup.keys.get() == old_backup.keys.get()
    assert account_backup.keys.ensure()["confirmed"]
    assert manager.link(account_id).link()["folder_id"] == original_folder
    assert account_backup.drill(sent["file_id"])["ok"]
    assert store.get(drive_backup.KEY_ENC) is not None  # legado preservado para recuperação independente


def test_legacy_key_and_folder_never_migrate_to_different_identity(tmp_path, monkeypatch):
    monkeypatch.setattr(cd, "_config_dir", lambda: tmp_path / "config")
    monkeypatch.setattr(cd, "search_dirs", lambda: [])
    store, api = MemoryStore(), TwoAccounts()
    store.set(cd.KEY_CLIENT, json.dumps(cd.parse_client(CLIENT)))
    legacy = cd.DriveLink(store, api)
    legacy.connect(authorize_fn=api.authorize("a"))
    legacy_key = drive_backup.Keys(store).ensure()["code"]
    old_folder = legacy.link()["folder_id"]
    manager = cd.DriveAccounts(store, api)
    other = manager.connect(authorize_fn=api.authorize("b"))["account_id"]
    assert manager.link(other).link()["folder_id"] != old_folder or api.google["b"] is not api.google["a"]
    assert drive_backup.Keys(manager.link(other).store).get() is None
    assert drive_backup.Keys(store).ensure()["code"] == legacy_key


def test_cli_lists_accounts_and_selects_one_without_opening_database(accounts, monkeypatch, tmp_path, capsys):
    from mt5_extracao import cli
    manager, api, _, a, b = accounts
    monkeypatch.setattr(cd, "DriveAccounts", lambda: manager)
    target = tmp_path / "unopened.db"
    assert cli.main(["--json", "--db", str(target), "resources", "drive", "accounts"]) == cli.EXIT_OK
    listed = json.loads(capsys.readouterr().out)
    assert {row["account_id"] for row in listed["data"]["accounts"]} == {a, b}
    assert cli.main(["--json", "--db", str(target), "resources", "drive", "verify", "--account", b]) == cli.EXIT_OK
    selected = json.loads(capsys.readouterr().out)
    assert selected["data"]["account_id"] == b and selected["data"]["account"] == "b@example.invalid"
    assert not target.exists()


@pytest.mark.parametrize("reason,code", [("storageQuotaExceeded", "quota"), ("userRateLimitExceeded", "rate_limit")])
def test_known_api_capacity_errors_are_actionable_without_echoing_payload(reason, code):
    error = cd._http_error(403, {"error": {"errors": [{"reason": reason}], "message": "private-sentinel"}})
    assert error.code == code and "private-sentinel" not in error.message


def test_explicit_inaccessible_folder_is_not_replaced_by_new_folder(accounts):
    manager, api, store, a, b = accounts
    count = len(api.google["a"].folders)
    with pytest.raises(cd.DriveError):
        manager.connect(account_id=a, folder_id="missing", authorize_fn=api.authorize("a"))
    assert len(api.google["a"].folders) == count
    assert manager.link(a).verify()["ok"]


def test_backup_without_account_does_not_use_legacy_token(accounts, monkeypatch):
    manager, api, store, a, b = accounts
    store.set(cd.KEY_TOKEN, "synthetic-legacy-token")
    monkeypatch.setattr(cd, "DriveAccounts", lambda **kwargs: manager)
    with pytest.raises(cd.DriveError) as caught:
        drive_backup.DriveBackup()
    assert caught.value.code == "account_required"
    assert store.get(cd.KEY_TOKEN) == "synthetic-legacy-token"


def test_private_key_dialog_keeps_original_account_if_selection_changes(monkeypatch):
    from types import SimpleNamespace
    from mt5_extracao.ui import drive_panel
    received = []
    panel = SimpleNamespace(frame=None, selected_account="account-a")
    panel._run = lambda work, done, **kwargs: received.append(kwargs["account_id"])
    def typed(*args, **kwargs):
        panel.selected_account = "account-b"
        return "synthetic-code"
    monkeypatch.setattr(drive_panel.simpledialog, "askstring", typed)
    drive_panel.DrivePanel.restore_key(panel)
    assert received == ["account-a"]
