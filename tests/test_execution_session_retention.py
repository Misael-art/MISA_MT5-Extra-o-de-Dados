"""T20.25: quarentena reversível apenas de perfis próprios encerrados."""
import configparser
import json
from pathlib import Path

import pytest

from mt5_extracao import cli
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.resources import files, actions
from mt5_extracao.robots import sessions, mt5_tools, store
from mt5_extracao.bootstrap import paths


@pytest.fixture
def context(tmp_path, monkeypatch):
    monkeypatch.setattr(paths, "is_windows", lambda: False)
    config = configparser.ConfigParser()
    prefix = tmp_path / "wine"
    config.read_dict({"MT5": {"path": str(prefix / "drive_c" / "original" / "terminal64.exe"), "wine_prefix": str(prefix)},
                      "DATABASE": {"type": "sqlite", "path": str(tmp_path / "data.sqlite")},
                      "RESOURCES": {"archive_dir": str(tmp_path / "archive")}})
    db = DatabaseManager(db_path=str(tmp_path / "data.sqlite"))
    session_id = "a" * 32
    directory = sessions.session_root(config) / session_id
    directory.mkdir(parents=True)
    payload = {"artifact_owner": "mt5x-session", "artifact_version": 1, "session": session_id,
               "owner_store": files.session_owner_key(db),
               "state": "completed", "resources_released": True, "finished_at": "2026-10-18T10:00:00",
               "wine_prefix": str(prefix)}
    (directory / "relatorio.json").write_text(json.dumps(payload))
    (directory / "terminal64.exe").write_bytes(b"own-terminal")
    (directory / "report.htm").write_bytes(b"preserve-all-results")
    yield db, config, directory, payload
    db.close()


def test_quarantine_and_restore_preserve_all_bytes_and_audit(context):
    db, config, directory, payload = context
    before = {p.name: p.read_bytes() for p in directory.iterdir()}
    assert files.list_sessions(db, config)[0]["eligible"]
    moved = files.move_session(db, config, directory.name)
    target = Path(moved["path"])
    assert moved["ok"] and target.parent.name == ".quarantine" and not directory.exists()
    assert {p.name: p.read_bytes() for p in target.iterdir()} == before
    assert files.list_sessions(db, config)[0]["quarantined"]
    assert files.move_session(db, config, directory.name, restore=True)["ok"]
    assert {p.name: p.read_bytes() for p in directory.iterdir()} == before
    assert {row["action"] for row in actions.load_actions(db)} == {"quarentena de perfil", "restaurar perfil"}


@pytest.mark.parametrize("field", ["report", "artifact_directory", "artifact_files"])
def test_referenced_profile_cannot_be_quarantined(context, field):
    db, config, directory, payload = context
    value = str(directory) if field == "artifact_directory" else str(directory / "report.htm")
    metrics = {field: [value] if field == "artifact_files" else value}
    store.save_run(db, {"run_id": "owned-result", "robot_key": "mql5:own", "source": "strategy tester", "symbol": "EURUSD",
                        "timeframe": "H1", "data_start": "", "data_end": "", "bars": 0,
                        "created_at": "2026-10-18", "metrics": metrics, "equity": []})
    result = files.move_session(db, config, directory.name)
    assert not result["ok"] and "referenciados" in result["error"] and directory.exists()


def test_demo_evidence_in_existing_catalog_protects_session(context):
    db, config, directory, payload = context
    store.save_record(db, "mql5:own", "mql5", "Own", "", "novo", {"demo_sessions": [{"session": directory.name}]})
    assert not files.move_session(db, config, directory.name)["ok"]
    assert directory.exists()


@pytest.mark.parametrize("change", [{"state": "running"}, {"resources_released": False},
                                   {"artifact_owner": "foreign"}, {"finished_at": ""}])
def test_unknown_or_active_profile_is_preserved(context, change):
    db, config, directory, payload = context
    (directory / "relatorio.json").write_text(json.dumps(dict(payload, **change)))
    assert not files.move_session(db, config, directory.name)["ok"] and directory.exists()


def test_symlink_and_foreign_destination_are_never_overwritten(context, tmp_path):
    db, config, directory, payload = context
    foreign = tmp_path / "foreign"; foreign.write_bytes(b"foreign")
    link = directory / "link"; link.symlink_to(foreign)
    assert not files.move_session(db, config, directory.name)["ok"] and foreign.read_bytes() == b"foreign"
    link.unlink()
    assert files.move_session(db, config, directory.name)["ok"]
    directory.mkdir(); sentinel = directory / "foreign"; sentinel.write_bytes(b"new-owner")
    result = files.move_session(db, config, directory.name, restore=True)
    assert not result["ok"] and "já existe" in result["error"] and sentinel.read_bytes() == b"new-owner"


def test_live_process_prevents_quarantine_even_if_manifest_claims_finished(context, monkeypatch):
    db, config, directory, payload = context
    monkeypatch.setattr(mt5_tools, "_native_context_processes", lambda context: [object()])
    assert not files.move_session(db, config, directory.name)["ok"] and directory.exists()


def test_cli_lists_moves_and_restores_only_with_explicit_flag(context, tmp_path, capsys):
    db, config, directory, payload = context
    cfg = tmp_path / "config.ini"
    with cfg.open("w") as out: config.write(out)
    args = ["--config", str(cfg), "resources", "sessions"]
    assert cli.main(args) == cli.EXIT_OK
    assert directory.name in capsys.readouterr().out
    assert cli.main(args + ["--quarantine", directory.name]) == cli.EXIT_USAGE
    assert directory.exists()
    assert cli.main(args + ["--quarantine", directory.name, "--yes"]) == cli.EXIT_OK
    assert "não libera espaço" in capsys.readouterr().out
    assert cli.main(args + ["--restore", directory.name, "--yes"]) == cli.EXIT_OK and directory.exists()


def test_profile_referenced_by_another_database_is_preserved(context, tmp_path):
    db, config, directory, payload = context
    store.save_run(db, {"run_id": "source-result", "robot_key": "mql5:own", "source": "strategy tester",
        "symbol": "EURUSD", "timeframe": "H1", "data_start": "", "data_end": "", "bars": 0,
        "created_at": "2026-10-18", "metrics": {"artifact_directory": str(directory)}, "equity": []})
    other = DatabaseManager(db_path=str(tmp_path / "other.sqlite"))
    try:
        result = files.move_session(other, config, directory.name)
        assert not result["ok"] and "outro banco" in result["error"] and directory.exists()
        assert not files.list_sessions(other, config)[0]["eligible"]
    finally:
        other.close()


def test_legacy_profile_without_store_binding_is_preserved(context):
    db, config, directory, payload = context
    payload.pop("owner_store")
    (directory / "relatorio.json").write_text(json.dumps(payload))
    assert not files.move_session(db, config, directory.name)["ok"] and directory.exists()
    assert not files.list_sessions(db, config)[0]["eligible"]


def test_postgresql_owner_binding_does_not_depend_on_password():
    from sqlalchemy.engine import make_url
    from types import SimpleNamespace
    first = make_url("postgresql+psycopg://analyst:dummy-first@localhost/research")
    def wrapped(url):
        return SimpleNamespace(engine=SimpleNamespace(url=url))
    key = files.session_owner_key(wrapped(first))
    assert key and len(key) == 64
    assert files.session_owner_key(wrapped(first.set(password="dummy-second"))) == key
    assert files.session_owner_key(wrapped(first.set(database="another"))) != key
    assert files.session_owner_key(wrapped(first.set(username="another"))) != key
    # O host pode ser um diretório de socket; casefold misturaria endereços POSIX.
    assert files.session_owner_key(wrapped(first.set(host="/pg/A"))) != files.session_owner_key(wrapped(first.set(host="/pg/a")))
