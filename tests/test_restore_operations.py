"""Intenção, consulta e recuperação de restauração nos artefatos de Recursos."""
import configparser
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.resources import files


@pytest.fixture
def restoration(tmp_path):
    active, chosen = tmp_path / "active.db", tmp_path / "selected.db"
    for path, value in ((active, 99), (chosen, 7)):
        with sqlite3.connect(path) as conn:
            conn.execute("CREATE TABLE proof(id INTEGER)")
            conn.execute("INSERT INTO proof VALUES(?)", (value,))
    config = configparser.ConfigParser(interpolation=None)
    config["RESOURCES"] = {"backup_dir": str(tmp_path / "copies"), "archive_dir": str(tmp_path / "archive"),
                           "backup_keep": "1"}
    db = DatabaseManager(db_path=str(active))
    try:
        yield db, config, chosen
    finally:
        db.close()


def value(path):
    with sqlite3.connect(path) as conn:
        return conn.execute("SELECT id FROM proof").fetchone()[0]


def fail_after_copy(monkeypatch):
    save = files._save_restore
    failed = False

    def save_with_failure(path, record):
        nonlocal failed
        if record["state"] == "succeeded" and not failed:
            failed = True
            raise OSError("resultado interrompido após a cópia")
        return save(path, record)

    monkeypatch.setattr(files, "_save_restore", save_with_failure)


def test_completed_id_is_query_only_even_after_database_changes(restoration, monkeypatch):
    db, config, chosen = restoration
    monkeypatch.delattr(files.hashlib, "file_digest", raising=False)
    first = files.restore_backup(db, config, chosen)
    assert first["ok"] and first["state"] == "succeeded"
    with sqlite3.connect(db.db_path) as conn:
        conn.execute("UPDATE proof SET id=42")
    repeated = files.restore_backup(db, config, chosen, operation_id=first["operation_id"])
    assert repeated["operation_id"] == first["operation_id"]
    assert repeated["state"] == "succeeded" and value(db.db_path) == 42
    assert len(files.list_backups(db, config)) == 1
    if os.name != "nt":
        assert Path(first["safety_copy"]).stat().st_mode & 0o777 == 0o600
        assert Path(first["receipt_path"]).stat().st_mode & 0o777 == 0o600


def test_uncertain_outcome_blocks_repeat_and_recovers_explicit_safety_copy(restoration, monkeypatch):
    db, config, chosen = restoration
    fail_after_copy(monkeypatch)
    first = files.restore_backup(db, config, chosen)
    assert not first["ok"] and first["state"] == "uncertain"
    assert value(db.db_path) == 7 and value(first["safety_copy"]) == 99
    assert files.restore_status(db, config, first["operation_id"])["state"] == "uncertain"
    blocked = files.restore_backup(db, config, chosen)
    assert not blocked["ok"] and blocked["operation_id"] == first["operation_id"]
    assert len(files.list_backups(db, config)) == 1
    recovered = files.restore_backup(db, config, first["safety_copy"], recovery_of=first["operation_id"])
    assert recovered["ok"] and recovered["operation_id"] != first["operation_id"]
    assert value(db.db_path) == 99
    old = files.restore_status(db, config, first["operation_id"])
    assert old["state"] == "recovered" and old["recovery_operation_id"] == recovered["operation_id"]


def test_changed_safety_copy_never_authorizes_recovery(restoration, monkeypatch):
    db, config, chosen = restoration
    fail_after_copy(monkeypatch)
    first = files.restore_backup(db, config, chosen)
    with sqlite3.connect(first["safety_copy"]) as conn:
        conn.execute("UPDATE proof SET id=123")
    result = files.restore_backup(db, config, first["safety_copy"], recovery_of=first["operation_id"])
    assert not result["ok"] and "SHA-256" in result["error"]
    assert value(db.db_path) == 7 and len(files.list_backups(db, config)) == 1


def test_changing_backup_directory_preserves_pending_operation(restoration, monkeypatch, tmp_path):
    db, config, chosen = restoration
    fail_after_copy(monkeypatch)
    first = files.restore_backup(db, config, chosen)
    config.set("RESOURCES", "backup_dir", str(tmp_path / "other-copies"))
    blocked = files.restore_backup(db, config, chosen)
    assert not blocked["ok"] and blocked["operation_id"] == first["operation_id"]
    assert files.restore_status(db, config, first["operation_id"])["state"] == "uncertain"
    recovered = files.restore_backup(db, config, first["safety_copy"], recovery_of=first["operation_id"])
    assert recovered["ok"] and value(db.db_path) == 99


def test_alias_of_active_database_preserves_pending_operation(restoration, monkeypatch, tmp_path):
    db, config, chosen = restoration
    fail_after_copy(monkeypatch)
    first = files.restore_backup(db, config, chosen)
    alias = tmp_path / "active-alias.db"
    os.link(db.db_path, alias)
    other = DatabaseManager(db_path=str(alias))
    try:
        blocked = files.restore_backup(other, config, chosen)
        assert not blocked["ok"] and blocked["operation_id"] == first["operation_id"]
    finally:
        other.close()


@pytest.mark.parametrize("kind", ["missing", "partial", "unknown_schema", "inconsistent_result", "nonfinite"])
def test_unknown_manifest_blocks_effects(restoration, kind):
    db, config, chosen = restoration
    first = files.restore_backup(db, config, chosen)
    manifest = Path(first["receipt_path"])
    record = json.loads(manifest.read_text())
    if kind == "missing":
        manifest.unlink()
    elif kind == "partial":
        manifest.write_text('{"schema":')
    else:
        if kind == "unknown_schema":
            record["schema"] = "mt5x-restore/v999"
        elif kind == "nonfinite":
            record["owner"]["started"] = float("nan")
        else:
            record["result"]["state"] = "applying"
        manifest.write_text(json.dumps(record))
    assert files.restore_status(db, config, first["operation_id"])["state"] == "unknown"
    result = files.restore_backup(db, config, chosen)
    assert not result["ok"] and value(db.db_path) == 7
    assert len(files.list_backups(db, config)) == 1
    assert files.backup(db, config)["ok"]
    assert Path(first["safety_copy"]).is_file()


def test_missing_id_never_creates_an_operation(restoration):
    db, config, chosen = restoration
    result = files.restore_backup(db, config, chosen, operation_id="0" * 32)
    assert not result["ok"] and result["state"] == "unknown"
    assert value(db.db_path) == 99 and files.list_backups(db, config) == []


def test_intent_must_be_durable_before_backup_or_effect(restoration, monkeypatch):
    db, config, chosen = restoration

    def unavailable(*args):
        raise OSError("disco indisponível no manifesto")

    monkeypatch.setattr(files, "_save_restore", unavailable)
    result = files.restore_backup(db, config, chosen)
    assert not result["ok"] and result["state"] == "unknown"
    assert value(db.db_path) == 99 and files.list_backups(db, config) == []


def test_interrupted_preparation_is_reconciled_before_a_new_explicit_request(restoration, monkeypatch):
    db, config, chosen = restoration
    save = files._save_restore

    def interrupted(path, record):
        if record["state"] == "applying":
            raise KeyboardInterrupt
        save(path, record)

    monkeypatch.setattr(files, "_save_restore", interrupted)
    with pytest.raises(KeyboardInterrupt):
        files.restore_backup(db, config, chosen)
    monkeypatch.setattr(files, "_save_restore", save)
    previous = files.restore_status(db, config)["operations"][0]
    assert previous["state"] == "failed" and value(db.db_path) == 99
    fresh = files.restore_backup(db, config, chosen)
    assert fresh["ok"] and fresh["operation_id"] != previous["operation_id"]


def test_database_identity_is_revalidated_before_effect(restoration, monkeypatch):
    db, config, chosen = restoration
    real_backup = files._backup_locked

    def changed_after_backup(*args, **kwargs):
        result = real_backup(*args, **kwargs)
        monkeypatch.setattr(files, "session_owner_key", lambda db, **kwargs: "f" * 64)
        return result

    monkeypatch.setattr(files, "_backup_locked", changed_after_backup)
    result = files.restore_backup(db, config, chosen)
    assert not result["ok"] and result["state"] == "failed"
    assert value(db.db_path) == 99 and value(result["safety_copy"]) == 99


def test_cli_consults_uncertain_outcome_and_recovers_explicitly(restoration, monkeypatch, tmp_path, capsys):
    from mt5_extracao import cli
    db, config, chosen = restoration
    config["DATABASE"] = {"type": "sqlite", "path": str(db.db_path)}
    config_path = tmp_path / "cli.ini"
    with config_path.open("w") as stream:
        config.write(stream)
    prefix = ["--config", str(config_path), "--json", "resources"]
    fail_after_copy(monkeypatch)
    assert cli.main([*prefix, "restore-backup", str(chosen), "--yes"]) == cli.EXIT_FAIL
    first = json.loads(capsys.readouterr().out)["data"]
    assert first["state"] == "uncertain" and value(db.db_path) == 7
    assert cli.main([*prefix, "restore-status", "--operation-id", first["operation_id"]]) == cli.EXIT_FAIL
    assert json.loads(capsys.readouterr().out)["data"]["state"] == "uncertain"
    assert cli.main([*prefix, "restore-backup", first["safety_copy"], "--yes",
                     "--recover-operation", first["operation_id"]]) == cli.EXIT_OK
    recovered = json.loads(capsys.readouterr().out)["data"]
    assert recovered["state"] == "succeeded" and value(db.db_path) == 99


def test_backup_retention_preserves_uncertain_copy_and_receipt_after_completion(restoration, monkeypatch):
    db, config, chosen = restoration
    fail_after_copy(monkeypatch)
    first = files.restore_backup(db, config, chosen)
    assert files.backup(db, config)["ok"]
    assert Path(first["safety_copy"]).is_file()
    recovered = files.restore_backup(db, config, first["safety_copy"], recovery_of=first["operation_id"])
    assert recovered["ok"]
    assert files.backup(db, config)["ok"]
    assert not Path(first["safety_copy"]).exists()
    assert Path(first["receipt_path"]).is_file()
    assert files.restore_status(db, config, first["operation_id"])["state"] == "recovered"


def test_process_exit_after_effect_is_queryable_and_recoverable(restoration, tmp_path):
    db, config, chosen = restoration
    config_path = tmp_path / "qa.ini"
    with config_path.open("w") as stream:
        config.write(stream)
    script = """
import configparser,os,sys
from pathlib import Path
from mt5_extracao.bootstrap import paths
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.resources import files
paths.state_dir=lambda:Path(sys.argv[4])
config=configparser.ConfigParser(interpolation=None)
config.read(sys.argv[2])
db=DatabaseManager(db_path=sys.argv[1])
save=files._save_restore
def exit_after_effect(path,record):
    if record['state']=='succeeded':
        os._exit(17)
    return save(path,record)
files._save_restore=exit_after_effect
files.restore_backup(db,config,sys.argv[3])
"""
    env = {**os.environ, "XDG_STATE_HOME": str(tmp_path / "child-state")}
    from mt5_extracao.bootstrap import paths
    result = subprocess.run([sys.executable, "-c", script, str(db.db_path), str(config_path), str(chosen),
                             str(paths.state_dir())],
                            env=env, capture_output=True, text=True, timeout=30)
    assert result.returncode == 17, result.stderr
    assert value(db.db_path) == 7
    pending = files.restore_status(db, config)["operations"][0]
    assert pending["state"] == "uncertain"
    recovered = files.restore_backup(db, config, pending["safety_copy"], recovery_of=pending["operation_id"])
    assert recovered["ok"] and value(db.db_path) == 99
