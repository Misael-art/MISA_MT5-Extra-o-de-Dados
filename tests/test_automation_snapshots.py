"""Captura opcional do SO: apenas janela própria, contrato Windows/Linux simulado."""
from pathlib import Path
from types import SimpleNamespace
import pytest
from PIL import Image
from mt5_extracao.automation import snapshots


@pytest.fixture
def context(tmp_path,monkeypatch):
    job=SimpleNamespace(state="running",visible=True,child_config={},mode="replay",directory=tmp_path)
    backend=SimpleNamespace(windows=lambda pids:[(1234,11)],close=lambda:None)
    monkeypatch.setattr(snapshots.sessions,"owned_processes",lambda *a,**kw:[SimpleNamespace(pid=11)])
    monkeypatch.setattr(snapshots.session_window,"_X11",lambda:backend)
    monkeypatch.setattr(snapshots.session_window,"_Windows",lambda:backend)
    return job,backend


@pytest.mark.parametrize("windows",[False,True])
def test_owned_capture_uses_exact_window_and_reports_backend(context,monkeypatch,windows):
    job,backend=context;seen=[]
    monkeypatch.setattr(snapshots.paths,"is_windows",lambda:windows)
    def write(target):Image.new("RGB",(400,300),"gray").save(target)
    monkeypatch.setattr(snapshots,"_windows",lambda window,target:seen.append(window) or write(target))
    monkeypatch.setattr(snapshots.shutil,"which",lambda name:"import")
    def run(argv,**kw):
        assert "shell" not in kw and argv[argv.index("-window")+1]=="1234"
        seen.append(1234);write(argv[-1]);return SimpleNamespace(returncode=0)
    monkeypatch.setattr(snapshots.subprocess,"run",run)
    result=snapshots.capture(job,{})
    assert seen==[1234] and result["backend"]=="owned_window" and result["width"]==400
    assert Path(result["path"]).parent==job.directory/"captures"


@pytest.mark.parametrize("windows_list",[[],[(0,11)],[(111,999)],[(111,11),(112,11)]])
def test_missing_ambiguous_or_foreign_windows_never_get_captured(context,monkeypatch,windows_list):
    job,backend=context;backend.windows=lambda pids:windows_list
    monkeypatch.setattr(snapshots.paths,"is_windows",lambda:False)
    monkeypatch.setattr(snapshots.subprocess,"run",lambda *a,**k:pytest.fail("captura não autorizada"))
    with pytest.raises(ValueError):snapshots.capture(job,{})
    assert not (job.directory/"captures").exists()


def test_changed_window_discards_pixels_and_never_returns_them(context,monkeypatch):
    job,backend=context;calls=[]
    backend.windows=lambda pids:[] if calls else [(1234,11)]
    monkeypatch.setattr(snapshots.paths,"is_windows",lambda:True)
    def capture(window,target):
        calls.append(window);Image.new("RGB",(10,10)).save(target)
    monkeypatch.setattr(snapshots,"_windows",capture)
    with pytest.raises(RuntimeError,match="mudou"):snapshots.capture(job,{})
    assert not list((job.directory/"captures").glob("*.png"))


@pytest.mark.parametrize("params",[dict(width=400),dict(chart="12345"),dict(chart="1"),dict(command="buy")])
def test_owned_window_options_do_not_silently_ignore_fields(context,params):
    with pytest.raises(ValueError):snapshots.capture(context[0],params)
