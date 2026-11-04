"""Uma leitura incompleta de pastas não inventa totais nem candidatos de limpeza."""
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from mt5_extracao.resources import files, storage
from tests.test_resources import db, _config


@pytest.mark.parametrize("partially_read", [False, True])
def test_directory_timeout_preserves_database_measurement_and_unknown_total(db, tmp_path, monkeypatch, partially_read):
    folder = tmp_path / "logs"
    folder.mkdir()
    (folder / "first.log").write_bytes(b"a" * 32)
    (folder / "second.log").write_bytes(b"b" * 64)
    monkeypatch.setattr(storage, "EXTRA_DIRS", {"Logs": lambda db, archive: folder})
    measured = []
    getsize = os.path.getsize

    def observe(path):
        size = getsize(path)
        if Path(path).parent == folder:
            measured.append(size)
        return size

    monkeypatch.setattr(storage.os.path, "getsize", observe)

    def expired():
        if not partially_read or measured:
            raise TimeoutError("prazo da leitura opcional")

    report = storage.measure(db, config=_config(tmp_path), directory_check=expired)
    assert report["db_bytes"] > 0 and report["tables"]
    assert report["dirs"]["Logs"]["bytes"] is None
    assert report["dirs_complete"] is False
    assert bool(measured) is partially_read
    assert any("prazo" in warning for warning in report["warnings"])
    assert sorted(p.stat().st_size for p in folder.iterdir()) == [32, 64]


def test_old_files_timeout_does_not_deliver_a_partial_list_or_delete(db, tmp_path, monkeypatch):
    folder = tmp_path / "old-logs"
    folder.mkdir()
    for name in ("first.log", "second.log"):
        path = folder / name
        path.write_bytes(b"old")
        os.utime(path, (1, 1))
    monkeypatch.setattr(files, "_folders", lambda db, policy: {"Logs": (folder, 1)})
    seen = []
    protected = files._artifact_protected

    def observe(path, *args):
        seen.append(path)
        return protected(path, *args)

    monkeypatch.setattr(files, "_artifact_protected", observe)

    def check():
        if seen:
            raise TimeoutError("prazo da lista")

    with pytest.raises(TimeoutError, match="prazo"):
        files.old_files(db, _config(tmp_path), check=check)
    assert len(seen) == 1 and sorted(p.name for p in folder.iterdir()) == ["first.log", "second.log"]
    # A chamada legada continua completa e somente de leitura.
    assert len(files.old_files(db, _config(tmp_path))) == 2


def test_scan_deadline_uses_monotonic_clock_without_sleep(monkeypatch):
    ticks = iter([10.0, 10.5, 11.0])
    monkeypatch.setattr(storage, "time", SimpleNamespace(monotonic=lambda: next(ticks)))
    check = storage.scan_deadline(1)
    check()
    with pytest.raises(TimeoutError):
        check()
