"""T20.25: artefatos por execução, sem MT5 real."""
from pathlib import Path
import os
import subprocess
import sys
from types import SimpleNamespace

import pytest

from mt5_extracao.bootstrap.paths import state_dir as original_state_dir
from mt5_extracao.bootstrap import paths
from mt5_extracao.robots import mt5_tools

from mt5_extracao.robots import backtest, budget, store
from mt5_extracao.timeframes import Timeframe
from tests.test_robots_tester import home, fake_terminal, report_html


def test_repeated_tests_keep_both_reports_and_parameter_files(home):
    calls=[]
    run=fake_terminal(report_html(),calls)
    first=backtest.run_mql5(home.db,home.config,home.robot,"EURUSD","H1",preset="Demo_Conservador",run=run,is_running=lambda:False)
    old=Path(first["metrics"]["report"]).read_bytes()
    second=backtest.run_mql5(home.db,home.config,home.robot,"EURUSD","H1",preset="Demo_Conservador",run=run,is_running=lambda:False)
    assert first["metrics"]["report"] != second["metrics"]["report"]
    assert Path(first["metrics"]["report"]).read_bytes() == old
    assert Path(second["metrics"]["report"]).is_file()
    assert calls[0][1]["ExpertParameters"] != calls[1][1]["ExpertParameters"]
    assert first["metrics"]["artifact_id"] != second["metrics"]["artifact_id"]


def test_unpublished_attempt_and_resumed_run_have_distinct_artifacts(home):
    calls=[]
    first=budget.Budget(home.db,home.config)
    real=fake_terminal(report_html(),calls)
    def canceled(*args):
        result=real(*args)
        if "/config:" in args[0]:
            first.cancel()
        return result
    results=backtest.run_batch(home.db,home.config,[home.robot],["EURUSD"],[Timeframe.H1],budget=first,
                                run=canceled,is_running=lambda:False)
    assert results[0]["status"] == "interrupted" and not store.load_runs(home.db)
    original=next((home.prefix/"drive_c"/"mt5x_sessions").glob("*/"+calls[0][1]["Report"]+".htm"))
    previous=original.read_bytes()
    resumed=budget.Budget(home.db,home.config,batch_id=first.batch_id)
    results=backtest.run_batch(home.db,home.config,[home.robot],["EURUSD"],[Timeframe.H1],budget=resumed,
                                batch_id=first.batch_id,run=real,is_running=lambda:False)
    assert results[0]["run"] and original.read_bytes()==previous
    assert calls[0][1]["Report"] != calls[1][1]["Report"]
    assert len(store.load_runs(home.db)) == 1


def test_global_lease_is_reentrant_and_never_removes_its_file():
    with mt5_tools.tester_lease():
        lock=paths.state_dir()/"native-tester.lock"
        assert lock.is_file()
        inode=lock.stat().st_ino
        with mt5_tools.tester_lease():
            assert lock.stat().st_ino==inode
    assert lock.is_file() and lock.stat().st_ino==inode


def test_process_exit_releases_lease_and_waiter_can_cancel(tmp_path):
    directory=paths.state_dir()
    script="""import sys
from pathlib import Path
from mt5_extracao.bootstrap import paths
from mt5_extracao.robots.mt5_tools import tester_lease
paths.state_dir=lambda:Path(sys.argv[1])
with tester_lease():
 print('ready',flush=True)
 sys.stdin.read(1)
"""
    child=subprocess.Popen([sys.executable,"-c",script,str(directory)],stdin=subprocess.PIPE,
                           stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
    try:
        assert child.stdout.readline().strip()=="ready"
        calls=[]
        def check():
            calls.append(1)
            if len(calls)>1:
                raise RuntimeError("espera cancelada")
        with pytest.raises(RuntimeError,match="espera cancelada"):
            with mt5_tools.tester_lease(check):
                pytest.fail("duas posses nativas simultâneas")
        assert len(calls)>1
    finally:
        child.terminate();child.wait(timeout=5)
        for stream in (child.stdin,child.stdout,child.stderr):
            stream.close()
    with mt5_tools.tester_lease():
        assert (directory/"native-tester.lock").is_file()


def test_windows_branch_locks_the_same_byte_and_releases_only_its_lease(monkeypatch):
    calls=[]
    def locking(fd,mode,count):
        calls.append((mode,count,os.lseek(fd,0,os.SEEK_CUR)))
    monkeypatch.setattr(paths,"is_windows",lambda:True)
    monkeypatch.setattr(mt5_tools,"os",SimpleNamespace(**{**vars(os),"name":"nt"}))
    monkeypatch.setitem(sys.modules,"msvcrt",SimpleNamespace(LK_NBLCK=1,LK_UNLCK=0,locking=locking))
    with mt5_tools.tester_lease():
        pass
    assert calls==[(1,1,0),(0,1,0)]


@pytest.mark.parametrize("windows",[False,True])
def test_state_path_is_shared_across_projects_and_independent_of_temp_and_wine(tmp_path,monkeypatch,windows):
    monkeypatch.setattr(paths,"is_windows",lambda:windows)
    env=dict(XDG_STATE_HOME=str(tmp_path/"state"),LOCALAPPDATA=str(tmp_path/"state"),
             TMPDIR=str(tmp_path/"temporary"),MT5X_WINEPREFIX=str(tmp_path/"wine"))
    assert original_state_dir(env)==tmp_path/"state"/"mt5-extracao"


def test_binary_preparation_copies_private_profile_without_claiming_instrumentation(home,tmp_path):
    from mt5_extracao.robots import catalog,sessions
    original=tmp_path/"Opaque.ex5";original.write_bytes(b"installed-binary")
    robot=catalog.robot_from_file(original)
    installed=mt5_tools.install(robot,home.mt5)[0].target
    config_dir=home.mt5/"Config";config_dir.mkdir()
    credentials=config_dir/"accounts.dat";credentials.write_bytes(b"opaque-cache")
    session=sessions.Session(home.db,home.config,robot,"EURUSD","H1",root=home.prefix/"drive_c"/"private-runs")
    with mt5_tools.tester_lease():
        compiled=session._prepare_expert(allow_binary=True)
        mt5_tools.release_native_context(session)
    assert compiled.read_bytes()==installed.read_bytes()==original.read_bytes()
    assert session.instrumented is False
    assert session.child_config._mt5x_session is session
    assert mt5_tools.install_dir(session.child_config).resolve()==session.directory.resolve()
    copied=session.directory/"Config"/"accounts.dat"
    assert copied.read_bytes()==credentials.read_bytes()
    compiled.write_bytes(b"private-change");copied.write_bytes(b"private-cache")
    assert installed.read_bytes()==original.read_bytes()==b"installed-binary"
    assert credentials.read_bytes()==b"opaque-cache"


def test_legacy_binary_is_not_silently_used_for_source_only_controls(home,tmp_path):
    from mt5_extracao.robots import catalog,sessions
    original=tmp_path/"Opaque.ex5";original.write_bytes(b"binary")
    robot=catalog.robot_from_file(original)
    session=sessions.Session(home.db,home.config,robot,"EURUSD","H1",root=home.prefix/"drive_c"/"private-runs")
    with pytest.raises(ValueError,match="fonte"):
        session._prepare_expert()
    assert not session.directory.exists()


def test_orphan_native_process_blocks_new_lease_without_erasing_ownership(tmp_path, monkeypatch):
    import json
    marker = paths.state_dir() / "native-active.json"
    marker.parent.mkdir(parents=True, exist_ok=True)
    original = json.dumps({"version": 1, "session": "orphan", "directory": str(tmp_path / "private"), "prefix": ""})
    marker.write_text(original)
    monkeypatch.setattr(mt5_tools, "_native_context_processes", lambda context: [object()])
    with pytest.raises(RuntimeError, match="anterior ainda"):
        with mt5_tools.tester_lease():
            pytest.fail("Não pode abrir outro Tester enquanto o filho anterior vive")
    assert marker.read_text() == original
    monkeypatch.setattr(mt5_tools, "_native_context_processes", lambda context: [])
    with mt5_tools.tester_lease():
        assert not marker.exists()


def test_native_context_release_never_removes_other_sessions_marker(tmp_path, monkeypatch):
    import json
    from types import SimpleNamespace
    marker = paths.state_dir() / "native-active.json"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(json.dumps({"session": "someone-else"}))
    monkeypatch.setattr(mt5_tools, "_native_context_processes", lambda context: pytest.fail("Não inspecionar posse alheia"))
    mt5_tools.release_native_context(SimpleNamespace(id="ours"))
    assert json.loads(marker.read_text())["session"] == "someone-else"


def test_unreadable_native_owner_fails_closed_but_releases_os_lock(monkeypatch):
    marker = paths.state_dir() / "native-active.json"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text("incomplete")
    with pytest.raises(RuntimeError, match="ilegível"):
        with mt5_tools.tester_lease():
            pytest.fail("Posse desconhecida")
    marker.unlink()
    with mt5_tools.tester_lease():
        assert mt5_tools._TESTER_LEASE.depth == 1


@pytest.mark.parametrize("symbol", ["../outside", "..\\outside", "/absolute", "C:\\outside", "EURUSD\n[Experts]"])
def test_private_preparation_rejects_escaping_symbol_before_creating_directory(home, tmp_path, symbol):
    from mt5_extracao.robots import sessions
    destination = tmp_path / "uncreated"
    with pytest.raises(ValueError, match="Símbolo inválido"):
        sessions.prepare(home.config, home.robot, destination, symbol)
    assert not destination.exists()


@pytest.mark.parametrize("windows", [False, True])
def test_inaccessible_native_candidate_blocks_release_but_foreign_process_does_not(home, monkeypatch, windows):
    import psutil
    from types import SimpleNamespace
    from mt5_extracao.robots import sessions
    monkeypatch.setattr(paths, "is_windows", lambda: windows)
    expected = str(home.mt5 / "terminal64.exe") if windows else paths.linux_to_wine_path(home.mt5 / "terminal64.exe", home.prefix)
    def denied():
        raise psutil.AccessDenied(123)
    candidate = SimpleNamespace(exe=denied, cmdline=lambda: [expected], environ=denied, name=lambda: "terminal64.exe")
    monkeypatch.setattr(psutil, "process_iter", lambda: iter([candidate]))
    with pytest.raises(RuntimeError, match="processo nativo"):
        sessions.owned_processes(home.config, strict=True)
    foreign = SimpleNamespace(exe=denied, cmdline=denied, name=lambda: "unrelated-program")
    monkeypatch.setattr(psutil, "process_iter", lambda: iter([foreign]))
    assert sessions.owned_processes(home.config, strict=True) == []


def test_private_wine_launcher_is_owned_until_it_exits(home, monkeypatch):
    import psutil
    from mt5_extracao.robots import sessions
    from types import SimpleNamespace
    bat = paths.linux_to_wine_path(home.mt5 / "launch.bat", home.prefix)
    process = SimpleNamespace(cmdline=lambda: ["wine", "cmd", "/c", bat],
                              environ=lambda: {"WINEPREFIX": str(home.prefix)}, name=lambda: "wine")
    monkeypatch.setattr(psutil, "process_iter", lambda: iter([process]))
    assert sessions.owned_processes(home.config, strict=True, launchers=True) == [process]
    assert sessions.owned_processes(home.config, strict=True) == []


def test_cancelled_session_stops_its_native_process_before_releasing_lease(home, monkeypatch):
    from mt5_extracao.robots import sessions
    session = sessions.Session(home.db, home.config, home.robot, "EURUSD", "H1")
    stopped = []
    def prepare(**kwargs):
        session.child_config = home.config
        raise sessions.Cancelled("cancelled with a child alive")
    monkeypatch.setattr(session, "_prepare_expert", prepare)
    monkeypatch.setattr(session, "_stop_owned", lambda: stopped.append("stopped"))
    assert session.run().state == "cancelled"
    assert stopped == ["stopped"]


def test_stop_waits_for_owned_launcher_instead_of_only_sending_terminate(home, monkeypatch):
    from mt5_extracao.robots import sessions
    from types import SimpleNamespace
    session = sessions.Session(home.db, home.config, home.robot, "EURUSD", "H1")
    session.child_config = home.config
    events = []
    session.process = SimpleNamespace(poll=lambda: None, terminate=lambda: events.append("terminate"),
        wait=lambda timeout: events.append("wait") if 0 < timeout <= 15 else pytest.fail("Espera ilimitada"))
    monkeypatch.setattr(sessions, "owned_processes", lambda *args, **kwargs: [])
    session._stop_owned()
    assert events == ["terminate", "wait"]


def test_native_preparation_reserves_copy_space_before_writing(home):
    from mt5_extracao.robots import sessions, budget
    original = home.mt5 / "terminal64.exe"
    original.write_bytes(b"x" * (3 * 1024**2))
    def check(*, path=None, reserve_bytes=0):
        if reserve_bytes > 1024**2:
            raise budget.Interrupted("Disco insuficiente para copiar o arquivo do perfil")
    session = sessions.Session(home.db, home.config, home.robot, "EURUSD", "H1",
                               native_options={"control": SimpleNamespace(check=check)})
    with mt5_tools.tester_lease():
        try:
            with pytest.raises(budget.Interrupted, match="Disco insuficiente"):
                session._prepare_expert(allow_binary=True, compiled_override=home.source.with_suffix(".ex5"))
            assert not (session.directory / "terminal64.exe").exists()
        finally:
            mt5_tools.release_native_context(session)
    assert original.stat().st_size == 3 * 1024**2


def test_interrupted_preparation_can_be_quarantined_without_publishing_a_run(home, monkeypatch):
    import json
    from mt5_extracao.robots import sessions, budget, store
    from mt5_extracao.resources import files
    original = home.mt5 / "terminal64.exe"
    original.write_bytes(b"x" * (3 * 1024**2))
    def check(*, path=None, reserve_bytes=0):
        if reserve_bytes > 1024**2:
            raise budget.Interrupted("Disco insuficiente para copiar o arquivo do perfil")
    monkeypatch.setattr(mt5_tools, "compile_source", lambda *a, **k: pytest.fail("Orçamento recusado não pode compilar"))
    session = sessions.Session(home.db, home.config, home.robot, "EURUSD", "H1",
                               native_options={"control": SimpleNamespace(check=check)})
    session.run()
    assert isinstance(session.exception, budget.Interrupted)
    report = json.loads(session.report.read_text())
    assert report["resources_released"] is True and report["state"] == "failed"
    assert store.load_runs(home.db) == [] and not (session.directory / "terminal64.exe").exists()
    assert files.move_session(home.db, home.config, session.id)["ok"]
    assert files.move_session(home.db, home.config, session.id, restore=True)["ok"]


def test_history_copy_checks_each_file_before_writing(home):
    from mt5_extracao.robots import sessions, budget
    original = home.mt5 / "Bases" / "Server" / "history" / "EURUSD" / "history.bin"
    original.parent.mkdir(parents=True)
    original.write_bytes(b"h" * (3 * 1024**2))
    def check(*, path=None, reserve_bytes=0):
        if reserve_bytes > 1024**2:
            raise budget.Interrupted("Disco insuficiente para copiar o histórico")
    session = sessions.Session(home.db, home.config, home.robot, "EURUSD", "H1",
                               native_options={"control": SimpleNamespace(check=check)})
    with mt5_tools.tester_lease():
        try:
            with pytest.raises(budget.Interrupted, match="Disco insuficiente"):
                session._prepare_expert(allow_binary=True, compiled_override=home.source.with_suffix(".ex5"))
            assert not (session.directory / original.relative_to(home.mt5)).exists()
        finally:
            mt5_tools.release_native_context(session)
    assert original.stat().st_size == 3 * 1024**2


def test_copy_reservation_uses_existing_destination_volume_with_real_budget(home):
    from mt5_extracao.robots import sessions, budget
    original = home.mt5 / "terminal64.exe"
    original.write_bytes(b"x" * (3 * 1024**2))
    measured = []
    def sample(path):
        assert path.exists()
        measured.append(path)
        return dict(cpu=0, ram=0, rss=1, cpu_seconds=0, read_bytes=0, write_bytes=0, free_bytes=2 * 1024**2)
    control = budget.Budget(home.db, home.config, limits=budget.Limits(min_free_mb=1), sample=sample)
    session = sessions.Session(home.db, home.config, home.robot, "EURUSD", "H1", native_options={"control": control})
    with mt5_tools.tester_lease():
        try:
            with pytest.raises(budget.Interrupted, match="Disco sem espaço"):
                session._prepare_expert(allow_binary=True, compiled_override=home.source.with_suffix(".ex5"))
            assert not (session.directory / "terminal64.exe").exists() and measured
        finally:
            mt5_tools.release_native_context(session)


@pytest.mark.parametrize('status,running', [('zombie',True),('running',False)])
def test_empty_linux_command_line_is_ignored_only_after_observed_exit(home,monkeypatch,status,running):
    import psutil
    from mt5_extracao.robots import sessions
    process=SimpleNamespace(cmdline=lambda:[],name=lambda:'wine',status=lambda:status,is_running=lambda:running,
                            terminate=lambda:pytest.fail('Não encerrar processo sem identidade'))
    monkeypatch.setattr(paths,'is_windows',lambda:False)
    monkeypatch.setattr(psutil,'process_iter',lambda:iter([process]))
    assert sessions.owned_processes(home.config,strict=True,launchers=True)==[]


def test_live_empty_linux_command_line_keeps_strict_ownership_refusal(home,monkeypatch):
    import psutil
    from mt5_extracao.robots import sessions
    process=SimpleNamespace(cmdline=lambda:[],name=lambda:'wine',status=lambda:'running',is_running=lambda:True)
    monkeypatch.setattr(paths,'is_windows',lambda:False)
    monkeypatch.setattr(psutil,'process_iter',lambda:iter([process]))
    with pytest.raises(RuntimeError,match='processo nativo'):sessions.owned_processes(home.config,strict=True,launchers=True)


def test_windows_exact_executable_does_not_depend_on_nonempty_arguments(home,monkeypatch):
    import psutil
    from mt5_extracao.robots import sessions
    process=SimpleNamespace(exe=lambda:str(home.mt5/'terminal64.exe'),cmdline=lambda:[],name=lambda:'terminal64.exe')
    monkeypatch.setattr(paths,'is_windows',lambda:True)
    monkeypatch.setattr(psutil,'process_iter',lambda:iter([process]))
    assert sessions.owned_processes(home.config,strict=True)==[process]
