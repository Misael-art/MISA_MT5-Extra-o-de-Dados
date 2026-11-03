"""A abertura da GUI não instala pacotes nem ignora dependências ausentes."""
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]


def test_missing_dependency_stops_before_runtime_imports_and_shows_installer(tmp_path):
    script = r'''
import runpy,sys,tkinter
from tkinter import messagebox
from mt5_extracao.bootstrap import doctor
events=[]
class Window:
 def withdraw(self):events.append('withdraw')
 def destroy(self):events.append('destroy')
tkinter.Tk=lambda:Window()
messagebox.showerror=lambda title,message:events.append((title,message))
original=doctor.check_module
doctor.check_module=lambda module,package:doctor.Check(package,doctor.FAIL,'ausente') if module=='pandas' else original(module,package)
sys.argv=['app.py','--offline']
try:runpy.run_path(sys.argv[0],run_name='__main__')
except SystemExit as error:assert error.code==1
else:raise AssertionError('GUI prosseguiu sem pandas')
assert events[0]=='withdraw' and events[-1]=='destroy'
assert events[1][0]=='Dependências indisponíveis'
assert 'pandas' in events[1][1]
assert ('install.bat' if sys.platform=='win32' else './install.sh') in events[1][1]
'''
    result = subprocess.run([sys.executable, "-c", script], cwd=ROOT,
                            env={**os.environ, "PYTHONPATH": str(ROOT)},
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "pandas" in result.stderr


def test_constructor_consumes_success_flag_and_lists_missing_dependencies(monkeypatch):
    import app

    events = []
    root = SimpleNamespace(after=lambda delay, callback: events.append((delay, callback)),
                           destroy=lambda: events.append("destroyed"))
    monkeypatch.setattr(app.MT5Extracao, "setup_root_window", lambda self: None)
    monkeypatch.setattr(app, "ErrorHandler", lambda: SimpleNamespace(install_global_handler=lambda: None))
    monkeypatch.setattr(app, "verificar_dependencias_criticas",
                        lambda offline, config_path: (False, ["pandas"]))
    monkeypatch.setattr(app.messagebox, "showerror", lambda title, message: events.append((title, message)))
    instance = app.MT5Extracao(root, offline=True)
    assert instance.ui_manager is None and instance.db_manager is None
    assert events[0][0] == "Erro de Dependências"
    assert "pandas" in events[0][1]
    assert ("install.bat" if os.name == "nt" else "./install.sh") in events[0][1]
    assert events[1][0] == 100


def test_dependency_check_returns_missing_module_without_installing(monkeypatch):
    import app

    original = app.bootstrap_doctor.check_module
    monkeypatch.setattr(app.bootstrap_doctor, "check_module",
                        lambda module, package: app.bootstrap_doctor.Check(package, app.bootstrap_doctor.FAIL, "ausente")
                        if module == "psutil" else original(module, package))
    success, missing = app.verificar_dependencias_criticas(offline=True)
    assert not success and missing == ["psutil"]


def test_explicit_offline_config_opens_only_qa_database(tmp_path, monkeypatch):
    import app

    qa_path = tmp_path / "qa.sqlite"
    qa_config = tmp_path / "qa.ini"
    qa_config.write_text(f"[APP]\nbroker_utc_offset=0\n[DATABASE]\ntype=sqlite\npath={qa_path}\n",
                         encoding="utf-8")
    errors = []
    monkeypatch.setattr(app.messagebox, "showerror", lambda *args: errors.append(args))
    instance = object.__new__(app.MT5Extracao)
    instance.config_path = str(qa_config)
    assert instance.load_config_and_db()
    try:
        assert qa_path.exists() and not errors
        assert instance.db_manager.db_path == str(qa_path)
        assert instance.config_path == str(qa_config)
    finally:
        instance.db_manager.close()


def test_alternate_config_requires_offline_before_creating_window(tmp_path):
    import app
    import pytest

    with pytest.raises(SystemExit, match="2"):
        app.main(["--config", str(tmp_path / "qa.ini")])
