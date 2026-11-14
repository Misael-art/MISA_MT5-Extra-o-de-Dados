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


def test_reunion_dry_run_import_retry_conflict_and_source_integrity(origin, tmp_path):
    collection = tmp_path / "collection"
    collection.mkdir()
    check = lambda: None
    reader = DatabaseManager(db_path=str(origin), read_only=True)
    try:
        first = tmp_path / "first"
        asset_package.export(reader, ["TEST"], first, check=check)
        assert asset_package.reunion_plan(first, collection, check=check)["state"] == "ready"
        assert not list(collection.iterdir())
        result = asset_package.reunite(first, collection, check=check)
        assert result["state"] == "imported"
        assert asset_package.reunite(first, collection, check=check)["state"] == "already_present"
        with sqlite3.connect(origin) as db:
            db.execute("UPDATE legacy_bars SET close = 42")
        second = tmp_path / "second"
        asset_package.export(reader, ["TEST"], second, check=check)
        assert asset_package.reunion_plan(second, collection, check=check)["state"] == "conflict"
        with pytest.raises(ValueError, match="conteúdo diferente"):
            asset_package.reunite(second, collection, check=check)
        with (first / asset_package.DATABASE).open("ab") as stream:
            stream.write(b"tampered")
        with pytest.raises(ValueError, match="não corresponde"):
            asset_package.reunite(first, collection, check=check)
        assert asset_package.inspect_package(result["target"], check=check)["package_id"] == result["package_id"]
    finally:
        reader.close()


def test_scope_preserves_revision_definitions_snapshot_blocks_and_study_links(origin, tmp_path):
    with sqlite3.connect(origin) as db:
        db.executescript("""
            CREATE TABLE _feature_definitions (feature_id TEXT, algorithm_version TEXT, PRIMARY KEY(feature_id,algorithm_version));
            INSERT INTO _feature_definitions VALUES ('needed','1'),('needed','2'),('other','1');
            CREATE TABLE _feature_values (symbol TEXT, feature_id TEXT, algorithm_version TEXT, revision INTEGER,
                FOREIGN KEY(feature_id,algorithm_version) REFERENCES _feature_definitions(feature_id,algorithm_version));
            INSERT INTO _feature_values VALUES ('TEST','needed','1',1),('TEST','needed','2',2),('OTHER','other','1',1);
            CREATE TABLE _dataset_manifests(manifest_id TEXT PRIMARY KEY,payload_json TEXT);
            INSERT INTO _dataset_manifests VALUES ('selected','{"symbol":"TEST","frames":{"bars":["used","shared"]}}'),
                ('excluded','{"symbol":"OTHER","frames":{"bars":["shared","alone"]}}');
            CREATE TABLE _dataset_blocks(block_id TEXT PRIMARY KEY,payload_json TEXT);
            INSERT INTO _dataset_blocks VALUES ('used','selected'),('shared','common'),('alone','outside');
            CREATE TABLE _chart_study_notes(note_id TEXT PRIMARY KEY,symbol TEXT,note_text TEXT);
            INSERT INTO _chart_study_notes VALUES ('n1','TEST','keep'),('n2','OTHER','outside');
            CREATE TABLE _chart_study_captures(note_id TEXT PRIMARY KEY,snapshot_json TEXT);
            INSERT INTO _chart_study_captures VALUES ('n1','{"frozen":true}'),('n2','{}');
            CREATE TABLE _chart_study_requests(request_id TEXT PRIMARY KEY,note_id TEXT,response_json TEXT);
            INSERT INTO _chart_study_requests VALUES ('r1','n1','{}'),('r2','n2','{}');
            CREATE TABLE _preset_revisions(preset_id TEXT,revision INT,robot_key TEXT,symbol TEXT,timeframe TEXT,
                PRIMARY KEY(preset_id,revision));
            INSERT INTO _preset_revisions VALUES ('p1',1,'robot','TEST','M5'),('p1',2,'robot','TEST','M5');
            CREATE TABLE _preset_selections(preset_id TEXT,revision INT,robot_key TEXT,symbol TEXT,timeframe TEXT);
            INSERT INTO _preset_selections VALUES ('p1',2,'robot','TEST','M5');
            CREATE TABLE _preset_imports(package_hash TEXT PRIMARY KEY,preset_id TEXT,revision INT);
            INSERT INTO _preset_imports VALUES ('h1','p1',1),('h2','p2',1);
        """)
    reader = DatabaseManager(db_path=str(origin), read_only=True)
    target = tmp_path / "study-package"
    try:
        manifest = asset_package.export(reader, ["TEST"], target, check=lambda: None)
        assert asset_package.inspect_package(target, check=lambda: None) == manifest
        with sqlite3.connect(target / asset_package.DATABASE) as db:
            assert db.execute("SELECT algorithm_version FROM _feature_definitions ORDER BY 1").fetchall() == [("1",), ("2",)]
            assert db.execute("SELECT block_id FROM _dataset_blocks ORDER BY 1").fetchall() == [("shared",), ("used",)]
            assert db.execute("SELECT note_id FROM _chart_study_captures").fetchall() == [("n1",)]
            assert db.execute("SELECT request_id FROM _chart_study_requests").fetchall() == [("r1",)]
            assert db.execute("SELECT revision FROM _preset_selections").fetchall() == [(2,)]
            assert db.execute("SELECT package_hash FROM _preset_imports").fetchall() == [("h1",)]
        with sqlite3.connect(origin) as db:
            db.execute("DELETE FROM _dataset_blocks WHERE block_id='used'")
        with pytest.raises(ValueError, match="vínculos incompletos"):
            asset_package.export(reader, ["TEST"], tmp_path / "missing-block", check=lambda: None)
    finally:
        reader.close()
