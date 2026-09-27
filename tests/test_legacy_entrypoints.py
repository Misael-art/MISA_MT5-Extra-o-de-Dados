"""T4.1: pontos de entrada antigos viraram wrappers finos para os comandos novos."""
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("script", ["verificador.py", "check_mt5_config.py", "mt5_troubleshooter.py"])
def test_doctor_wrappers(script):
    r = subprocess.run([sys.executable, script, "--json", "--no-probe"], cwd=str(REPO), capture_output=True, text=True)
    assert "descontinuado" in r.stderr
    assert '"status"' in r.stdout  # saída do doctor --json


def test_removed_files():
    for name in ("fix_try.py", "mt5_diagnostico.json", "mt5_extracao/ui_manager.py.bak",
                 "test_mt5_connection.py", "mt5_workaround.py"):
        assert not (REPO / name).exists(), name
    assert (REPO / "scripts" / "manual" / "test_mt5_connection.py").exists()
