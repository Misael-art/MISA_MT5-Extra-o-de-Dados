"""T20.25: retenção não apaga artefatos de execução referenciados/ativos."""
import configparser
import json
import os
from datetime import datetime,timedelta

import pytest
from sqlalchemy import text

from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.resources import files,storage
from mt5_extracao.robots import store


@pytest.fixture
def artifacts(tmp_path,monkeypatch):
    root=tmp_path/"project";exports=root/"exports";logs=root/"logs"
    exports.mkdir(parents=True);logs.mkdir()
    monkeypatch.setattr(storage,"_project_root",lambda:root)
    config=configparser.ConfigParser()
    config["RESOURCES"]={"exports_keep_days":"1","logs_keep_days":"1","archive_dir":str(tmp_path/"archive")}
    db=DatabaseManager(db_path=str(tmp_path/"artifacts.sqlite"))
    def old(path):
        path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(b"preservar")
        before=(datetime.now()-timedelta(days=20)).timestamp();os.utime(path,(before,before))
        return path
    yield db,config,exports,logs,old
    db.close()


def record(db,metrics):
    store.save_run(db,dict(run_id="r",robot_key="mql5:qa",source="strategy tester",symbol="QA",timeframe="H1",
                          data_start="",data_end="",bars=0,metrics=metrics,equity=[],created_at="2025-01-01"))


def test_report_log_and_explicit_artifact_files_are_protected(artifacts):
    db,config,exports,logs,old=artifacts
    report=old(exports/"native.htm");native_log=old(logs/"native.log");parameters=old(exports/"native.set")
    orphan=old(exports/"unreferenced.txt")
    record(db,dict(report=str(report),tester_log=str(native_log),artifact_files=[str(parameters)]))
    found=files.old_files(db,config)
    assert {item["path"] for item in found}=={str(orphan)}
    result=files.clean_files(db,config)
    assert result["removed"]==1 and not orphan.exists()
    assert report.read_bytes()==native_log.read_bytes()==parameters.read_bytes()==b"preservar"


def test_explicit_directory_and_old_session_reference_protect_siblings(artifacts):
    db,config,exports,logs,old=artifacts
    directory=exports/"ctx";binary=old(directory/"terminal64.exe")
    marker=old(directory/"relatorio.json");report=old(directory/"tester.htm")
    copied=old(directory/"MQL5"/"Profiles"/"Tester"/"p.set")
    record(db,dict(report=str(report)))
    assert files.old_files(db,config)==[]
    files.clean_files(db,config)
    assert binary.exists() and marker.exists() and copied.exists()


def test_native_tree_without_proven_finished_state_is_not_generically_deleted(artifacts):
    db,config,exports,logs,old=artifacts
    unknown=old(exports/"robot_sessions"/"old-unknown"/"terminal64.exe")
    assert files.old_files(db,config)==[]
    files.clean_files(db,config)
    assert unknown.exists()


def test_current_library_directory_is_protected(artifacts):
    db,config,exports,logs,old=artifacts
    code=old(exports/"library"/"expert.mq5")
    config["ROBOTS"]={"library_dirs":str(code.parent)}
    assert files.old_files(db,config)==[]
    assert code.exists()


def test_unreadable_execution_metadata_blocks_cleanup(artifacts):
    db,config,exports,logs,old=artifacts
    candidate=old(exports/"unknown.txt")
    record(db,{})
    with db.engine.begin() as conn:
        conn.execute(text("UPDATE _robot_runs SET metrics_json='broken'"))
    with pytest.raises(ValueError,match="Metadados"):
        files.clean_files(db,config)
    assert candidate.exists()


def test_registered_robot_path_is_protected_without_library_config(artifacts):
    db, config, exports, logs, old = artifacts
    code = old(exports / "registered.mq5")
    store.save_record(db, "mql5:registered", "mql5", "Registered", str(code), "novo", {})
    assert files.old_files(db, config) == []
    files.clean_files(db, config)
    assert code.exists()


def test_operational_mt5_installation_inside_exports_is_protected(artifacts):
    db, config, exports, logs, old = artifacts
    executable = old(exports / "original-mt5" / "terminal64.exe")
    history = old(executable.parent / "Bases" / "server" / "history.dat")
    config["MT5"] = {"path": str(executable)}
    assert files.old_files(db, config) == []
    files.clean_files(db, config)
    assert executable.exists() and history.exists()


def test_native_book_referenced_outside_private_profile_is_protected(artifacts):
    db, config, exports, logs, old = artifacts
    book = old(exports / "native-book.csv")
    record(db, {"trades_csv": str(book)})
    assert files.old_files(db, config) == []
    assert files.clean_files(db, config)["removed"] == 0
    assert book.read_bytes() == b"preservar"


def test_recent_files_do_not_resolve_protection_but_old_files_still_do(artifacts, monkeypatch):
    db, config, exports, logs, old = artifacts
    recent = logs / "recent.txt"
    recent.write_bytes(b"recent")
    protected = old(logs / "referenced.txt")
    orphan = old(logs / "orphan.txt")
    record(db, {"tester_log": str(protected)})
    original = files._artifact_protected
    checked = []
    def counted(path, *args):
        checked.append(path)
        return original(path, *args)
    monkeypatch.setattr(files, "_artifact_protected", counted)
    assert {item["path"] for item in files.old_files(db, config)} == {str(orphan)}
    assert set(checked) == {protected, orphan}
    assert files.clean_files(db, config)["removed"] == 1
    assert recent.read_bytes() == b"recent" and protected.read_bytes() == b"preservar"
    assert not orphan.exists()
