import hashlib
import json
import sqlite3

import pytest

from mt5_extracao import cli
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.resources import asset_package


@pytest.fixture
def origin(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "appdata"))
    source = tmp_path / "source.sqlite"
    with sqlite3.connect(source) as db:
        db.executescript("""
            CREATE TABLE _dataset_origin (slot TEXT PRIMARY KEY, origin_id TEXT NOT NULL);
            INSERT INTO _dataset_origin VALUES ('default', 'testing-namespace'), ('provider', 'synthetic');
            CREATE TABLE _metadata (key TEXT PRIMARY KEY, value TEXT);
            INSERT INTO _metadata VALUES ('time_basis', 'utc'), ('private_other', 'excluded');
            CREATE TABLE _symbol_tables (symbol TEXT, timeframe TEXT, table_name TEXT UNIQUE);
            INSERT INTO _symbol_tables VALUES ('TEST', '1_hora', 'legacy_bars'), ('OTHER', '1_hora', 'other_bars'),
                ('TEST', 'ticks', 'registered_ticks'), ('TEST', 'book', 'registered_book');
            CREATE TABLE legacy_bars (time TEXT PRIMARY KEY, close REAL, extra BLOB, note TEXT DEFAULT 'legacy');
            CREATE INDEX extra_idx ON legacy_bars(extra);
            INSERT INTO legacy_bars VALUES ('2026-01-01', 1.25, X'00FF', 'café');
            CREATE TABLE other_bars (time TEXT PRIMARY KEY, close REAL);
            INSERT INTO other_bars VALUES ('2026-01-01', 99);
            CREATE TABLE registered_ticks (time_msc INTEGER, seq INTEGER, bid REAL, ask REAL, PRIMARY KEY(time_msc,seq));
            INSERT INTO registered_ticks VALUES (10, 0, 1, 2), (10, 1, 1, 2);
            CREATE TABLE registered_book (time_utc TEXT, price REAL);
            INSERT INTO registered_book VALUES ('2026-01-01T00:00:00Z', 3.25);
            CREATE TABLE _ohlcv_versions (table_name TEXT, time TEXT, content_sha256 TEXT, payload_json TEXT);
            INSERT INTO _ohlcv_versions VALUES ('legacy_bars', '2026-01-01', 'old', '{}'), ('other_bars', 't', 'excluded', '{}');
            CREATE TABLE _ohlcv_receipts (table_name TEXT, received_at_utc TEXT, origin TEXT);
            INSERT INTO _ohlcv_receipts VALUES ('legacy_bars', '2026-01-01T00:00:00Z', 'synthetic');
            CREATE VIEW unrelated_view AS SELECT close FROM other_bars;
        """)
    return source


def test_launcher_handler_exports_registered_families_and_raw_legacy_values(origin, tmp_path, capsys):
    config = tmp_path / "qa.ini"
    config.write_text("[DATABASE]\ntype = sqlite\n", encoding="utf-8")
    target = tmp_path / "package"
    before = origin.read_bytes()
    assert cli.main(["--config", str(config), "--db", str(origin), "--json", "resources",
                     "package-export", "--symbols", "TEST", "--out", str(target)]) == 0
    manifest = json.loads((target / asset_package.MANIFEST).read_text(encoding="utf-8"))
    assert json.loads(capsys.readouterr().out)["data"] == manifest
    assert origin.read_bytes() == before
    assert manifest["excluded_views"] == ["unrelated_view"]
    assert manifest["excluded_tables"] == ["other_bars"]
    assert manifest["clock_metadata"] == {"time_basis": "utc"}
    assert manifest["database_sha256"] == hashlib.sha256((target / asset_package.DATABASE).read_bytes()).hexdigest()
    with sqlite3.connect(target / asset_package.DATABASE) as db:
        assert db.execute("SELECT close, extra, note FROM legacy_bars").fetchall() == [(1.25, b"\x00\xff", "café")]
        assert db.execute("SELECT content_sha256 FROM _ohlcv_versions").fetchall() == [("old",)]
        assert db.execute("SELECT COUNT(*) FROM registered_ticks").fetchone() == (2,)
        assert db.execute("SELECT COUNT(*) FROM registered_book").fetchone() == (1,)
        assert db.execute("SELECT name FROM sqlite_schema WHERE name='extra_idx'").fetchone() == ("extra_idx",)


def test_cancel_leaves_no_target_and_retry_cannot_replace_a_package(origin, tmp_path):
    target = tmp_path / "package"
    reader = DatabaseManager(db_path=str(origin), read_only=True)
    def cancel():
        raise TimeoutError("cancelled")
    try:
        with pytest.raises(TimeoutError):
            asset_package.export(reader, ["TEST"], target, check=cancel)
        assert not target.exists() and not list(tmp_path.glob(".mt5x-package-*"))
        asset_package.export(reader, ["TEST"], target, check=lambda: None)
        before = (target / asset_package.MANIFEST).read_bytes()
        with pytest.raises(FileExistsError):
            asset_package.export(reader, ["TEST"], target, check=lambda: None)
        assert (target / asset_package.MANIFEST).read_bytes() == before
    finally:
        reader.close()


def test_export_refuses_a_foreign_key_to_another_assets_table(origin, tmp_path):
    with sqlite3.connect(origin) as source:
        source.execute("ALTER TABLE legacy_bars ADD COLUMN other TEXT REFERENCES other_bars(time)")
    reader = DatabaseManager(db_path=str(origin), read_only=True)
    try:
        with pytest.raises(ValueError, match="fora do pacote"):
            asset_package.export(reader, ["TEST"], tmp_path / "invalid", check=lambda: None)
        assert not (tmp_path / "invalid").exists()
    finally:
        reader.close()
