"""Restauração usa exatamente a cópia escolhida, inclusive nomes especiais."""
import configparser
import os
import sqlite3

import pandas as pd
import pytest
from sqlalchemy import text

from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.resources import files


@pytest.mark.parametrize("active_basis,chosen_basis,chosen_zone,compatible", [
    ("broker", "utc", None, False),
    ("utc", "utc", "Europe/Athens", False),
    ("broker", "broker", None, True),
])
@pytest.mark.parametrize("interrupted", [False, True])
def test_restored_clock_contract_is_revalidated_before_writing(
        tmp_path, monkeypatch, active_basis, chosen_basis, chosen_zone, compatible, interrupted):
    db = DatabaseManager(db_path=str(tmp_path / "active.db"), time_basis=active_basis)
    chosen = tmp_path / "selected.db"
    source = DatabaseManager(db_path=str(chosen), time_basis=chosen_basis, broker_timezone=chosen_zone)
    config = configparser.ConfigParser(interpolation=None)
    config["RESOURCES"] = {"backup_dir": str(tmp_path / "copies"), "archive_dir": str(tmp_path / "archive")}
    frame = pd.DataFrame({"time": pd.to_datetime(["2024-01-02 09:00"]), "open": [1.0],
                          "high": [1.0], "low": [1.0], "close": [1.0], "tick_volume": [1],
                          "spread": [1], "real_volume": [1]})
    try:
        assert db.save_ohlcv_data("QA", "1 minuto", frame)
        assert source.save_ohlcv_data("QA", "1 minuto", frame)
        source.close()
        if interrupted:
            from tests.test_restore_operations import fail_after_copy
            fail_after_copy(monkeypatch)
        result = files.restore_backup(db, config, chosen)
        assert result["ok"] is not interrupted, result
        assert result["state"] == ("uncertain" if interrupted else "succeeded")
        frame["time"] += pd.Timedelta(minutes=1)
        assert db.save_ohlcv_data("QA", "1 minuto", frame) is compatible
        assert db.get_metadata("time_basis") == chosen_basis
        assert db.get_metadata("broker_timezone") == chosen_zone
        table = db.get_table_name_for_symbol("QA", "1 minuto")
        with db.engine.connect() as conn:
            assert conn.execute(text(f'SELECT COUNT(*) FROM "{table}"')).scalar_one() == 1 + int(compatible)
    finally:
        source.close()
        db.close()


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
            os.link(tmp_path / "active.db", chosen)
        result = files.restore_backup(db, cfg, chosen)
        assert not result["ok"]
        assert result["safety_copy"] is None
        with db.engine.connect() as conn:
            assert conn.execute(text("SELECT id FROM proof")).scalar_one() == 99
        if kind == "missing":
            assert not chosen.exists() and not (tmp_path / "missing").exists()
    finally:
        db.close()
