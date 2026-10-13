"""T3.3: pyproject.toml e requirements.txt declaram as mesmas dependências."""
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]


def _normalize(req):
    return req.replace(" ", "").replace("'", '"').lower()


def test_pyproject_dependencies_match_requirements():
    tomllib = pytest.importorskip("tomllib")  # Python 3.11+
    project = tomllib.loads((REPO / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    reqs = [l.split("#")[0].strip() for l in (REPO / "requirements.txt").read_text(encoding="utf-8").splitlines()]
    reqs = [r for r in reqs if r]
    assert sorted(map(_normalize, project["dependencies"])) == sorted(map(_normalize, reqs))
    assert project["scripts"]["mt5x"] == "mt5_extracao.cli:main"


def test_setup_py_removed():
    assert not (REPO / "setup.py").exists()
