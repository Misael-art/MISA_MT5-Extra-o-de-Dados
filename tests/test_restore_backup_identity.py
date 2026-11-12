"""Restauração usa exatamente a cópia escolhida, inclusive nomes especiais."""
import configparser
import sqlite3

import pytest
from sqlalchemy import text

from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.resources import files


@pytest.mark.parametrize("name", ["a#b%c space.db", "ação %.db", "normal.db"])
def test_restore_preserves_selected_content_and_safety_copy(tmp_path, name):
    db = DatabaseManager(db_path=str(tmp_path / "active.db"))
    chosen = tmp_path / name
    cfg = configparser.ConfigParser(interpolation=None)
    cfg["RESOURCES"] = {"backup_dir": str(tmp_path / "copies"), "archive_dir": str(tmp_path / "archive")}
    try:
        with db.engine.begin() as conn:
            conn.execute(text("CREATE TABLE proof(id INTEGER)"))
            conn.execute(text("INSERT INTO proof VALUES(99)"))
        with sqlite3.connect(chosen) as conn:
            conn.execute("CREATE TABLE proof(id INTEGER)")
            conn.execute("INSERT INTO proof VALUES(7)")
        result = files.restore_backup(db, cfg, chosen)
        assert result["ok"], result
        with db.engine.connect() as conn:
            assert conn.execute(text("SELECT id FROM proof")).scalar_one() == 7
        with sqlite3.connect(result["safety_copy"]) as conn:
            assert conn.execute("SELECT id FROM proof").fetchone() == (99,)
        with sqlite3.connect(chosen) as conn:
            assert conn.execute("SELECT id FROM proof").fetchone() == (7,)
        assert not (tmp_path / "a").exists()
    finally:
        db.close()


@pytest.mark.parametrize("kind", ["missing", "empty", "corrupt", "active", "active_alias"])
def test_invalid_source_never_replaces_active_database(tmp_path, kind):
    db = DatabaseManager(db_path=str(tmp_path / "active.db"))
    chosen = tmp_path / "missing#copy.db"
    cfg = configparser.ConfigParser(interpolation=None)
    cfg["RESOURCES"] = {"backup_dir": str(tmp_path / "copies"), "archive_dir": str(tmp_path / "archive")}
    try:
        with db.engine.begin() as conn:
            conn.execute(text("CREATE TABLE proof(id INTEGER)"))
            conn.execute(text("INSERT INTO proof VALUES(99)"))
        if kind == "empty":
            chosen.touch()
        elif kind == "corrupt":
            chosen.write_bytes(b"incomplete sqlite backup")
        elif kind == "active":
            chosen = tmp_path / "active.db"
        elif kind == "active_alias":
            chosen.hardlink_to(tmp_path / "active.db")
        result = files.restore_backup(db, cfg, chosen)
        assert not result["ok"]
        assert result["safety_copy"] is None
        with db.engine.connect() as conn:
            assert conn.execute(text("SELECT id FROM proof")).scalar_one() == 99
        if kind == "missing":
            assert not chosen.exists() and not (tmp_path / "missing").exists()
    finally:
        db.close()
