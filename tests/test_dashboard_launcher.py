"""Fixture do launcher: Dashboard não inicia ponte; demais percursos são preservados."""
import json
import os
from pathlib import Path
import shutil
import subprocess

import pytest


@pytest.mark.skipif(os.name == "nt" or shutil.which("bash") is None, reason="launcher POSIX")
@pytest.mark.parametrize("arguments,bridge", [
    (["dashboard", "--no-browser"], False),
    (["--json", "--config", "config com espaço.ini", "--db", "dashboard", "dashboard"], False),
    (["--verbose", "dashboard"], False),
    (["tables"], False),
    (["extract", "--symbols", "X"], True),
])
def test_launcher_preserves_bridge_boundary(tmp_path, arguments, bridge):
    source = Path(__file__).resolve().parents[1]
    shutil.copyfile(source / "run.sh", tmp_path / "run.sh")
    python = tmp_path / ".venv/bin/python"
    python.parent.mkdir(parents=True)
    # Interpretador falso: responde a classe pelo módulo real e ecoa a chamada da CLI.
    python.write_text("#!/usr/bin/env python3\nimport json,sys\n"
                      f"if sys.argv[1:3] == ['-m', 'mt5_extracao.launch_policy']:\n"
                      f"    sys.path.insert(0, {str(source)!r})\n"
                      "    from mt5_extracao import launch_policy\n"
                      "    sys.exit(launch_policy.main(sys.argv[3:]))\n"
                      "print(json.dumps(sys.argv[1:]))\n")
    python.chmod(0o700)
    (tmp_path / "config").mkdir()
    (tmp_path / "config/config.ini").write_text("[DASHBOARD]\n")
    script = tmp_path / "scripts/mt5-bridge.sh"
    script.parent.mkdir()
    script.write_text("#!/bin/sh\ntouch bridge-called\n")
    script.chmod(0o700)
    result = subprocess.run(["bash", str(tmp_path / "run.sh"), "--cli", *arguments],
                            capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == ["-m", "mt5_extracao.cli", *arguments]
    assert (tmp_path / "bridge-called").exists() == bridge


@pytest.mark.skipif(os.name == "nt" or shutil.which("bash") is None, reason="launcher POSIX")
def test_launcher_keeps_bridge_failure_for_desktop_ui_and_log(tmp_path):
    source = Path(__file__).resolve().parents[1]
    shutil.copyfile(source / "run.sh", tmp_path / "run.sh")
    python = tmp_path / ".venv/bin/python"
    python.parent.mkdir(parents=True)
    python.write_text("#!/usr/bin/env python3\nimport os,json\nprint(json.dumps(os.environ.get('MT5X_BRIDGE_START_ERROR')))\n")
    python.chmod(0o700)
    (tmp_path / "config").mkdir()
    (tmp_path / "config/config.ini").write_text("[BRIDGE]\n")
    script = tmp_path / "scripts/mt5-bridge.sh"
    script.parent.mkdir()
    script.write_text("#!/bin/sh\necho 'PID sem posse; porta preservada' >&2\nexit 1\n")
    script.chmod(0o700)
    result = subprocess.run(["bash", str(tmp_path / "run.sh")], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0
    assert json.loads(result.stdout) == 'PID sem posse; porta preservada'
    assert 'PID sem posse' in (tmp_path / 'logs/mt5_bridge.log').read_text()
    result = subprocess.run(["bash", str(tmp_path / "run.sh"), "--offline"], capture_output=True, text=True, timeout=10)
    assert json.loads(result.stdout) == ''
