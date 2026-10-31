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
    (["tables"], True),
])
def test_launcher_preserves_bridge_boundary(tmp_path, arguments, bridge):
    source = Path(__file__).resolve().parents[1]
    shutil.copyfile(source / "run.sh", tmp_path / "run.sh")
    python = tmp_path / ".venv/bin/python"
    python.parent.mkdir(parents=True)
    python.write_text("#!/usr/bin/env python3\nimport json,sys\nprint(json.dumps(sys.argv[1:]))\n")
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
