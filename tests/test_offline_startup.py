"""T17.22: análise local explícita, sem alcançar SDK/terminal nem alterar configuração."""
import configparser
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from mt5_extracao import mt5_connector
from mt5_extracao.bootstrap import paths
from mt5_extracao.database_manager import DatabaseManager
from tests.market_data import random_walk

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("windows", [False, True])
def test_offline_connector_keeps_sdk_unresolved_and_another_backend_untouched(tmp_path, monkeypatch, windows):
    calls = []

    def forbidden(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("modo offline alcançou SDK/terminal")

    monkeypatch.setattr(paths, "is_windows", lambda: windows)
    config = tmp_path / "config.ini"
    config.write_text("[MT5]\npath = terminal-configurado\n[BRIDGE]\nenabled = true\n", encoding="utf-8")
    original = config.read_bytes()
    monkeypatch.setattr(mt5_connector, "get_mt5", forbidden)
    for backend in (None, SimpleNamespace(initialize=forbidden, shutdown=forbidden, last_error=forbidden)):
        monkeypatch.setattr(mt5_connector, "mt5", backend)
        connector = mt5_connector.MT5Connector(str(config), offline=True)
        assert mt5_connector.mt5 is backend
        assert not connector.initialize(force_restart=True)
        assert not connector.is_initialized
        assert connector.get_symbols() is None and connector.get_total_symbols() == 0
        assert connector.get_available_timeframes()
        assert connector.last_error() is None
        connector.shutdown()
        assert connector.get_connection_status()["mode"] == "Análise local (offline)"
    assert config.read_bytes() == original
    assert calls == []


def test_default_connector_still_resolves_backend(tmp_path, monkeypatch):
    calls = []
    backend = SimpleNamespace()
    monkeypatch.setattr(mt5_connector, "mt5", None)
    monkeypatch.setattr(mt5_connector, "get_mt5", lambda path: calls.append(path) or backend)
    config = tmp_path / "config.ini"
    config.write_text("[MT5]\npath = terminal-configurado\n", encoding="utf-8")
    connector = mt5_connector.MT5Connector(str(config))
    assert calls == [str(config)] and not connector.offline


@pytest.mark.parametrize("arguments,offline", [([], False), (["--offline"], True)])
def test_main_parses_mode_and_only_closes_its_own_connector(tmp_path, arguments, offline):
    script = r'''
import app,json,sys
from types import SimpleNamespace
from mt5_extracao import mt5_backend
events=[]
def forbidden(*args,**kwargs):raise AssertionError('global SDK resolved during cleanup')
mt5_backend.get_mt5=forbidden
app.tk.Tk=lambda:SimpleNamespace(mainloop=lambda:events.append('loop'))
def create(root,offline=False):
    events.append(offline)
    return SimpleNamespace(mt5_connector=SimpleNamespace(shutdown=lambda:events.append('owned shutdown')))
app.MT5Extracao=create
arguments,expected=json.loads(sys.argv[1])
assert app.main(arguments)==0
assert events==[expected,'loop','owned shutdown'],events
'''
    result = subprocess.run([sys.executable, "-c", script, json.dumps([arguments, offline])], cwd=tmp_path,
                            env={**os.environ, "PYTHONPATH": str(ROOT)}, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr


def test_main_help_does_not_create_window_or_resolve_sdk(tmp_path):
    script = r'''
import app
from mt5_extracao import mt5_backend
def forbidden(*args,**kwargs):raise AssertionError('help created window or resolved SDK')
app.tk.Tk=mt5_backend.get_mt5=forbidden
try:app.main(['--help'])
except SystemExit as error:assert error.code==0
else:raise AssertionError('help did not exit')
'''
    result = subprocess.run([sys.executable, "-c", script], cwd=tmp_path,
                            env={**os.environ, "PYTHONPATH": str(ROOT)}, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "--offline" in result.stdout


def test_main_entrypoint_analyzes_local_data_and_exits_without_sdk(tmp_path):
    tk = pytest.importorskip("tkinter")
    try:
        probe = tk.Tk()
    except tk.TclError as error:
        pytest.skip(f"sem display gráfico: {error}")
    probe.destroy()
    import gc
    gc.collect()
    (tmp_path / "config").mkdir()
    config = tmp_path / "config/config.ini"
    settings = configparser.ConfigParser()
    settings.read_dict({"MT5": {"path": "terminal-configurado"}, "BRIDGE": {"enabled": "true"},
                        "DATABASE": {"type": "sqlite", "path": str(tmp_path / "local.db")}})
    with config.open("w", encoding="utf-8") as stream:
        settings.write(stream)
    original = config.read_bytes()
    db = DatabaseManager(db_path=str(tmp_path / "local.db"))
    db.save_ohlcv_data("WIN$N", "1 minuto", random_walk(600, freq="min", start="2024-01-02 09:00"))
    db.close()
    script = r'''
import json,time,traceback,tkinter as tk
from types import SimpleNamespace
from tkinter import messagebox
from mt5_extracao import mt5_backend,mt5_connector
calls=[]
def forbidden(*args,**kwargs):
    calls.append('SDK/terminal/dialogue')
    raise AssertionError('SDK/terminal/dialogue reached in offline mode')
mt5_backend.get_mt5=forbidden
mt5_connector.get_mt5=forbidden
mt5_connector.mt5=SimpleNamespace(initialize=forbidden,shutdown=forbidden,last_error=forbidden)
messagebox.showerror=messagebox.showwarning=messagebox.askyesno=forbidden
import app
original=app.MT5Extracao
errors=[];proof={}
def create(root,offline=False):
    instance=original(root,offline=offline)
    ui=instance.ui_manager
    assert ui is not None and offline
    deadline=time.monotonic()+25
    def inspect_local():
        try:
            if 'WIN$N' not in ui.analysis_panel.available:
                assert time.monotonic()<deadline,'inventory timeout'
                root.after(50,inspect_local);return
            assert 'offline' in ui.mt5_status_value.cget('text')
            assert ui.check_button.instate(['disabled']) and ui.mt5_view_button.instate(['disabled'])
            ui.check_mt5_status();ui.connect_mt5();ui.open_mt5_view();ui._offer_demo_account()
            assert ui.open_analysis('WIN$N','1 minuto')
            root.after(50,inspect_analysis)
        except BaseException:
            errors.append(traceback.format_exc());root.destroy()
    def inspect_analysis():
        try:
            panel=ui.analysis_panel
            if panel.data is None:
                assert time.monotonic()<deadline,'analysis timeout'
                root.after(50,inspect_analysis);return
            assert len(panel.data)==600 and set(panel._canvases)=={'chart','season','returns'}
            assert not instance.mt5_connector.is_initialized and instance.offline
            proof.update(rows=len(panel.data),mode=ui.mt5_status_value.cget('text'),sdk_calls=len(calls))
            root.destroy()
        except BaseException:
            errors.append(traceback.format_exc());root.destroy()
    root.after(50,inspect_local)
    return instance
app.MT5Extracao=create
assert app.main(['--offline'])==0
assert not errors,errors
assert calls==[],calls
assert proof.get('rows')==600
print('OFFLINE_PROOF '+json.dumps(proof))
'''
    result = subprocess.run([sys.executable, "-c", script], cwd=tmp_path,
                            env={**os.environ, "PYTHONPATH": str(ROOT)}, capture_output=True, text=True, timeout=45)
    assert result.returncode == 0, result.stdout + result.stderr
    proof = json.loads(next(line.removeprefix("OFFLINE_PROOF ") for line in result.stdout.splitlines()
                            if line.startswith("OFFLINE_PROOF ")))
    assert proof["rows"] == 600 and proof["sdk_calls"] == 0
    assert config.read_bytes() == original
