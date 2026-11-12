"""Retenção limitada às cópias produzidas e conferidas para a base atual."""
import os
import json
from pathlib import Path
import shutil

import pytest

from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.resources import files
from tests.test_restore_operations import restoration


@pytest.mark.parametrize("name", ["unknown.db", "active__20000101-000000.db"])
def test_backup_preserves_unknown_and_legacy_files(restoration, name):
    db, config, chosen = restoration
    folder = files.backup_dir(db, files.policy_mod.load(config))
    folder.mkdir()
    unknown = folder / name
    shutil.copyfile(chosen, unknown)
    os.utime(unknown, (1, 1))
    before = unknown.read_bytes()
    result = files.backup(db, config)
    assert result["ok"]
    assert unknown.is_file(), "a retenção apagou uma cópia sem prova de posse"
    assert unknown.read_bytes() == before


def test_backup_preserves_copies_owned_by_another_database(restoration, tmp_path):
    db, config, chosen = restoration
    other = DatabaseManager(db_path=str(chosen))
    try:
        previous = files.backup(other, config)
        assert previous["ok"]
        os.utime(previous["path"], (1, 1))
        result = files.backup(db, config)
        assert result["ok"]
        assert os.path.isfile(previous["path"]), "a retenção apagou a cópia de outra base"
    finally:
        other.close()


@pytest.mark.parametrize("change", ["content", "missing", "corrupt", "unknown-schema", "replacement"])
def test_changed_or_unproven_copy_is_preserved(restoration, change):
    db, config, chosen = restoration
    previous = files.backup(db, config)
    assert previous["ok"]
    path = Path(previous["path"])
    receipt = files._backup_receipt(path)
    if change == "content":
        content = path.read_bytes()
        path.write_bytes(content[:-1] + bytes([content[-1] ^ 1]))
    elif change == "missing":
        receipt.unlink()
    elif change == "corrupt":
        receipt.write_text("{")
    elif change == "unknown-schema":
        data = json.loads(receipt.read_text())
        data["schema"] = "unknown/v999"
        receipt.write_text(json.dumps(data))
    else:
        replacement = path.with_suffix(".replacement")
        shutil.copyfile(path, replacement)
        replacement.replace(path)
    os.utime(path, (1, 1))
    before = path.read_bytes()
    result = files.backup(db, config)
    assert result["ok"] and path.is_file()
    assert path.read_bytes() == before
    assert str(path) in result["preserved"]


def test_unknown_newer_file_does_not_consume_managed_retention_slot(restoration):
    db, config, chosen = restoration
    config.set("RESOURCES", "backup_keep", "2")
    previous = files.backup(db, config)
    path = Path(previous["path"])
    os.utime(path, (1, 1))
    unknown = path.parent / "unknown.db"
    shutil.copyfile(chosen, unknown)
    os.utime(unknown, (4000000000, 4000000000))
    result = files.backup(db, config)
    assert result["ok"] and path.is_file() and unknown.is_file()
    assert not result["pruned"]


def test_unknown_file_never_counts_as_last_managed_backup(restoration):
    db, config, chosen = restoration
    folder = files.backup_dir(db, files.policy_mod.load(config))
    folder.mkdir()
    shutil.copyfile(chosen, folder / "unknown.db")
    assert files.last_backup(db, config) is None
    assert files.backup_due(db, config)
    result = files.backup(db, config)
    assert result["ok"] and files.last_backup(db, config) is not None
    assert not files.backup_due(db, config)


def test_backup_receipt_is_private_and_result_survives_expiry(restoration):
    db, config, _ = restoration
    previous = files.backup(db, config)
    path = Path(previous["path"])
    receipt = files._backup_receipt(path)
    if os.name != "nt":
        assert receipt.stat().st_mode & 0o777 == 0o600
    assert files._backup_record(db, path) is not None
    os.utime(path, (1, 1))
    result = files.backup(db, config)
    assert result["ok"] and result["pruned"] == [str(path)]
    assert not path.exists() and receipt.is_file()
    query = files.backup_status(db, config, previous["backup_id"])
    assert query["ok"] and not query["backups"][0]["copy_available"]


def test_copy_changed_during_hash_is_preserved(restoration, monkeypatch):
    db, config, _ = restoration
    previous = files.backup(db, config)
    path = Path(previous["path"])
    os.utime(path, (1, 1))
    real = files._file_sha256

    def changed_after_hash(candidate):
        digest = real(candidate)
        if candidate == path:
            replacement = path.with_suffix(".replacement")
            shutil.copyfile(path, replacement)
            replacement.replace(path)
        return digest

    monkeypatch.setattr(files, "_file_sha256", changed_after_hash)
    result = files.backup(db, config)
    assert result["ok"] and path.is_file()
    assert str(path) in result["preserved"]


def test_gui_defers_backup_and_reports_preserved_files(monkeypatch):
    from types import SimpleNamespace
    from tests.test_restore_operations_ui import panel_with_worker
    panel, work, database, config = panel_with_worker(monkeypatch)
    messages, calls = [], []
    panel.ui = SimpleNamespace(log=messages.append)

    def backup(*args):
        calls.append(args)
        return {"ok": True, "path": "own.db", "bytes": 1, "preserved": ["unknown.db"]}

    monkeypatch.setattr(files, "backup", backup)
    panel.backup()
    assert not calls and len(work) == 1
    task, done = work[0]
    done(task())
    assert calls == [(database, config, "ui")]
    assert any("preservado" in message for message in messages)


def test_retention_intent_must_be_durable_before_any_delete(restoration, monkeypatch):
    db, config, _ = restoration
    previous = files.backup(db, config)
    path = Path(previous["path"])
    os.utime(path, (1, 1))

    def unavailable(*args):
        raise OSError("recibo indisponível antes do expurgo")

    monkeypatch.setattr(files, "_save_backup", unavailable)
    result = files.backup(db, config)
    assert result["ok"] and result["retention_state"] == "uncertain"
    assert path.is_file() and not result["pruned"]


@pytest.mark.parametrize("after_delete", [False, True])
def test_interrupted_retention_is_queryable_without_repetition(restoration, monkeypatch, after_delete):
    db, config, _ = restoration
    previous = files.backup(db, config)
    path = Path(previous["path"])
    os.utime(path, (1, 1))
    real_save = files._save_backup
    with monkeypatch.context() as patch:
        if after_delete:
            def interrupted_save(database, target, record):
                if record["retention"] and record["state"] == "complete":
                    raise OSError("resultado interrompido após exclusão")
                return real_save(database, target, record)
            patch.setattr(files, "_save_backup", interrupted_save)
        else:
            def interrupted_prune(*args):
                raise OSError("interrupção antes da exclusão")
            patch.setattr(files, "_prune_backup", interrupted_prune)
        result = files.backup(db, config)
    assert result["ok"] and result["retention_state"] == "uncertain"
    assert path.exists() == (not after_delete)
    # Outro ciclo preserva os arquivos enquanto o resultado continua pendente.
    pending = files.backup(db, config)
    assert pending["ok"] and pending["retention_state"] == "uncertain" and not pending["pruned"]
    monkeypatch.setattr(files, "_prune_backup", lambda *args: pytest.fail("consulta não solicita exclusões"))
    query = files.backup_status(db, config, result["backup_id"])
    assert query["ok"] and query["backups"][0]["state"] == "complete"
    assert query["backups"][0]["retention"][0]["state"] == ("absent" if after_delete else "not_applied")
    assert path.exists() == (not after_delete)


def test_pending_retention_with_changed_target_remains_uncertain(restoration, monkeypatch):
    db, config, _ = restoration
    previous = files.backup(db, config)
    path = Path(previous["path"])
    os.utime(path, (1, 1))
    with monkeypatch.context() as patch:
        def interrupted(*args):
            raise OSError("interrupção antes da exclusão")
        patch.setattr(files, "_prune_backup", interrupted)
        result = files.backup(db, config)
    path.write_bytes(b"own changed file")
    query = files.backup_status(db, config, result["backup_id"])
    assert not query["ok"] and query["backups"][0]["state"] == "pruning"
    assert path.read_bytes() == b"own changed file"


def test_gui_retention_query_is_deferred_to_worker(monkeypatch):
    from tests.test_restore_operations_ui import panel_with_worker
    panel, work, database, config = panel_with_worker(monkeypatch)
    calls = []
    monkeypatch.setattr(files, "backup_status", lambda *args: calls.append(args) or {"ok": True, "backups": []})
    panel.check_backup()
    assert not calls and len(work) == 1
    task, done = work[0]
    done(task())
    assert calls == [(database, config)]


def test_unknown_backup_id_is_query_only(restoration):
    db, config, _ = restoration
    previous = files.backup(db, config)
    folder = Path(previous["path"]).parent
    before = {path.name: path.read_bytes() for path in folder.iterdir()}
    result = files.backup_status(db, config, "0" * 32)
    assert not result["ok"] and result["state"] == "unknown"
    assert {path.name: path.read_bytes() for path in folder.iterdir()} == before


@pytest.mark.parametrize("change", ["unknown-state", "partial-intent", "outside-folder"])
def test_unrecognized_retention_never_authorizes_deletion(restoration, change):
    db, config, _ = restoration
    previous = files.backup(db, config)
    path = Path(previous["path"])
    receipt = files._backup_receipt(path)
    record = json.loads(receipt.read_text())
    if change == "unknown-state":
        record["state"] = "unknown"
    elif change == "partial-intent":
        record.update(state="pruning", retention=[{}])
    else:
        record.update(state="pruning", retention=[dict(path=str(path.parent.parent / "outside.db"),
                      backup_id="1" * 32, sha256="1" * 64, state="prepared")])
    receipt.write_text(json.dumps(record))
    os.utime(path, (1, 1))
    result = files.backup(db, config)
    assert result["ok"] and path.is_file() and str(path) in result["preserved"]
    query = files.backup_status(db, config, previous["backup_id"])
    assert not query["ok"] and query["state"] == "unknown"
