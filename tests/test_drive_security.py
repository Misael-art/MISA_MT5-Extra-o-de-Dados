"""Segredos não passam por argv, JSON ou mensagens de falhas do transporte."""
import io
import json
import os
import sys
import urllib.error
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from mt5_extracao import cli
from mt5_extracao.resources import cloud_drive as cd, commands
from tests.test_drive_backup import base, ready  # noqa: F401 - fixtures sintéticas


@pytest.mark.parametrize("argument", [["--code", "private-sentinel"], ["--code=private-sentinel"]])
def test_old_code_argument_is_rejected_without_echo(argument, capsys):
    assert cli.main(["resources", "drive", "restore-key", *argument]) == cli.EXIT_USAGE
    output = capsys.readouterr()
    assert "private-sentinel" not in output.out + output.err
    assert "terminal privado" in output.err


@pytest.mark.parametrize("action", ["key", "confirm-key", "restore-key"])
def test_noninteractive_key_commands_fail_before_reading_vault(action, monkeypatch):
    monkeypatch.setattr(sys, "stdin", io.StringIO())
    keys = SimpleNamespace(ensure=lambda: pytest.fail("cofre não deve ser acessado"))
    with pytest.raises(cd.DriveError, match="terminal") as caught:
        commands._drive_private_key(action, keys)
    assert caught.value.code == "private_terminal"


@pytest.mark.parametrize("action,method", [("confirm-key", "confirm"), ("restore-key", "restore")])
def test_private_input_is_hidden_and_result_contains_no_secret(action, method, monkeypatch):
    import getpass
    tty = SimpleNamespace(isatty=lambda: True)
    monkeypatch.setattr(sys, "stdin", tty)
    monkeypatch.setattr(sys, "stderr", tty)
    typed = []
    monkeypatch.setattr(getpass, "getpass", lambda prompt, stream: "private-sentinel")
    result = commands._drive_private_key(action, SimpleNamespace(**{method: typed.append}))
    assert typed == ["private-sentinel"] and result["confirmed"]
    assert "private-sentinel" not in json.dumps(result)


def test_http_messages_do_not_copy_remote_payload_or_network_errors(monkeypatch):
    assert "private-sentinel" not in str(cd._http_error(400, {"error_description": "private-sentinel"}))
    monkeypatch.setattr(cd.time, "sleep", lambda _: None)

    def down(req):
        raise urllib.error.URLError("private-sentinel")

    with pytest.raises(cd.DriveError) as caught:
        cd._http("GET", "https://example.invalid", opener=down)
    assert "private-sentinel" not in caught.value.message


def test_vault_errors_do_not_echo_secret_values():
    def backend():
        raise RuntimeError("private-sentinel")
    with pytest.raises(cd.DriveError) as caught:
        cd.KeyringStore()._call(backend)
    assert caught.value.code == "keyring_error"
    assert "private-sentinel" not in caught.value.message


def test_upload_checksum_and_restore_are_separate_states(ready, base):
    from mt5_extracao.resources.drive_backup import receipt_message
    backup, _, _ = ready
    backup.keys.confirm(backup.keys.ensure()["code"])
    sent = backup.send(base, scope="qa")
    assert sent["snapshot_state"] == "verified"
    assert sent["upload_state"] == "sent"
    assert sent["remote_verification_state"] == "verified_md5"
    assert sent["restore_state"] == "not_tested"
    assert "restauração ainda não ensaiada" in receipt_message(sent)
    assert backup.drill(sent["file_id"])["restore_state"] == "verified"
    repeated = backup.send(base, scope="qa")
    assert repeated["restore_state"] == "verified"
    assert "restauração conferida" in receipt_message(repeated)


def test_missing_remote_checksum_remains_pending(ready, base, monkeypatch):
    from mt5_extracao.resources import drive_backup
    backup, _, _ = ready
    backup.keys.confirm(backup.keys.ensure()["code"])
    upload = drive_backup.upload

    def no_checksum(*args, **kwargs):
        result = upload(*args, **kwargs)
        result.pop("md5Checksum")
        return result

    monkeypatch.setattr(drive_backup, "upload", no_checksum)
    sent = backup.send(base, scope="qa")
    assert not sent["ok"] and sent["upload_state"] == "sent"
    assert sent["remote_verification_state"] == "pending" and sent["restore_state"] == "not_tested"


def test_official_launcher_requires_private_terminal_without_opening_database(tmp_path):
    root = Path(__file__).resolve().parents[1]
    target = tmp_path / "must-not-exist.db"
    if os.name == "nt":
        # run.bat executa a CLI diretamente sem iniciar MT5; não aceita --offline.
        launcher = [os.environ.get("COMSPEC", "cmd.exe"), "/d", "/c", str(root / "run.bat"), "--cli"]
    else:
        launcher = [str(root / "run.sh"), "--offline", "--cli"]
    result = subprocess.run([*launcher, "--json", "--db", str(target),
                             "resources", "drive", "restore-key"], capture_output=True, text=True, timeout=30)
    assert result.returncode == cli.EXIT_FAIL, result.stderr
    data = json.loads(result.stdout)
    assert data["data"]["code"] == "private_terminal"
    assert not target.exists()


def test_snapshot_preserves_existing_target_and_handles_uri_characters(tmp_path):
    from mt5_extracao.resources.files import consistent_snapshot
    import sqlite3
    source, target = tmp_path / "a#b%c space.db", tmp_path / "copy.db"
    with sqlite3.connect(source) as con:
        con.execute("CREATE TABLE example (id INTEGER PRIMARY KEY)")
        con.execute("INSERT INTO example (id) VALUES (7)")
    consistent_snapshot(source, target)
    with sqlite3.connect(target) as con:
        assert con.execute("SELECT id FROM example").fetchall() == [(7,)]
    original = target.read_bytes()
    with pytest.raises(FileExistsError):
        consistent_snapshot(source, target)
    assert target.read_bytes() == original


def test_retry_of_same_copy_after_service_restart_does_not_duplicate(ready, base):
    from mt5_extracao.resources.drive_backup import DriveBackup
    backup, google, _ = ready
    backup.keys.confirm(backup.keys.ensure()["code"])
    first = backup.send(base, scope="qa")
    second = DriveBackup(backup.link).send(base, scope="qa")
    assert first["file_id"] == second["file_id"]
    assert len(google.blobs) == 1


def test_interrupted_upload_resumes_after_restart_and_preserves_last_good(ready, base):
    from mt5_extracao.resources.drive_backup import DriveBackup
    import hashlib
    import sqlite3
    backup, google, store = ready
    backup.keys.confirm(backup.keys.ensure()["code"])
    first = backup.send(base, scope="qa")
    old_blob = dict(google.blobs[first["file_id"]])
    with sqlite3.connect(base) as con:
        con.execute("INSERT INTO bars VALUES (999999, 42)")
    before = hashlib.sha256(base.read_bytes()).hexdigest()
    blocked = [True]

    def flaky(req):
        if "/upload/session/" in req.full_url and blocked[0]:
            raise urllib.error.URLError("private-session-sentinel")
        return google(req)

    backup.opener = flaky
    with pytest.raises(cd.DriveError) as caught:
        backup.send(base, scope="qa")
    assert caught.value.code == "network"
    assert "private-session-sentinel" not in caught.value.message
    assert google.blobs[first["file_id"]] == old_blob
    assert any(key.startswith("backup_upload/") and json.loads(value).get("session") for key, value in store.data.items())
    assert list((base.parent / "copias" / ".envio-drive").glob("*.mt5xenc"))
    sessions = len(google.sessions)
    blocked[0] = False
    second = DriveBackup(backup.link, opener=flaky).send(base, scope="qa")
    assert second["ok"] and len(google.sessions) == sessions and len(google.blobs) == 2
    assert google.blobs[first["file_id"]] == old_blob
    assert hashlib.sha256(base.read_bytes()).hexdigest() == before


def test_upload_rejects_non_google_session_before_sending_data(tmp_path):
    from mt5_extracao.resources.drive_backup import upload
    source = tmp_path / "sealed"
    source.write_bytes(b"synthetic")
    with pytest.raises(cd.DriveError) as caught:
        upload(source, "fake-token", {}, session="https://example.invalid/upload/session",
               opener=lambda req: pytest.fail("nenhuma requisição permitida"))
    assert caught.value.code == "upload"


def test_large_download_is_manual_and_prepared_file_can_be_checked(ready, base, tmp_path, monkeypatch):
    from mt5_extracao.resources import drive_backup
    backup, google, _ = ready
    backup.keys.confirm(backup.keys.ensure()["code"])
    sent = backup.send(base, scope="qa")
    monkeypatch.setattr(drive_backup, "MANUAL_DOWNLOAD_BYTES", 1)
    plan = backup.download_plan(sent["file_id"])
    assert plan["bytes"] == sent["bytes"] and plan["md5"] == sent["md5"]
    assert plan["automatic_download"] is False
    with pytest.raises(cd.DriveError) as caught:
        backup.drill(sent["file_id"])
    assert caught.value.code == "manual_download"
    downloaded = tmp_path / "downloaded.mt5xenc"
    downloaded.write_bytes(google.blobs[sent["file_id"]]["data"])
    assert backup.drill(sent["file_id"], downloaded_file=downloaded)["ok"]


@pytest.mark.parametrize("tail", [b"\0", b"\xff" * 4])
def test_malformed_ciphertext_lengths_are_bounded(tmp_path, tail):
    from mt5_extracao.resources import drive_backup
    source, output = tmp_path / "bad", tmp_path / "plain"
    source.write_bytes(drive_backup.MAGIC + bytes(8) + tail)
    with pytest.raises(cd.DriveError) as caught:
        drive_backup.decrypt_file(source, output, bytes(32))
    assert caught.value.code == "corrupt"


def test_missing_vault_key_can_be_deleted_without_leaking_backend_errors(monkeypatch):
    class MissingPassword(Exception):
        pass
    def missing(*args):
        raise MissingPassword("private-sentinel")
    fake = SimpleNamespace(delete_password=missing, errors=SimpleNamespace(PasswordDeleteError=MissingPassword))
    store = cd.KeyringStore()
    monkeypatch.setattr(store, "_kr", lambda: fake)
    store.delete("unset")
