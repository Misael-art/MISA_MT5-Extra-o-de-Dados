"""Versão mínima do Python é a mesma no empacotamento, doctor, instaladores e CI."""
import re
from pathlib import Path

from mt5_extracao.bootstrap import doctor

ROOT = Path(__file__).resolve().parents[1]


def read(path):
    return (ROOT / path).read_text(encoding="utf-8")


def test_minimum_and_maximum_python_agree_everywhere():
    minimum = doctor.MIN_PYTHON
    assert re.search(r'requires-python = ">=(\d+)\.(\d+)"', read("pyproject.toml")).groups() == tuple(map(str, minimum))
    assert re.search(r"^MIN_PY_MINOR=(\d+)", read("scripts/install/linux.sh"), re.M).group(1) == str(minimum[1])
    windows = read("scripts/install/windows.ps1")
    assert re.search(r"^\$PythonMinor = @\(3, (\d+)\)", windows, re.M).group(1) == str(minimum[1])
    assert re.search(r"^\$PythonMaxMinor = (\d+)", windows, re.M).group(1) == str(doctor.MAX_TESTED_PYTHON[1])
    candidates = re.findall(r"python3\.(\d+)", re.search(r"for c in ([^;]+);", read("scripts/install/linux.sh")).group(1))
    candidates += re.findall(r'"3\.(\d+)"', re.search(r"foreach \(\$v in @\(([^)]*)\)", windows).group(1))
    assert all(minimum[1] <= int(m) <= doctor.MAX_TESTED_PYTHON[1] for m in candidates)


def test_ci_runs_the_declared_minimum():
    versions = set(re.findall(r'python-version: "(\d+\.\d+)"', read(".github/workflows/ci.yml")))
    assert "%d.%d" % doctor.MIN_PYTHON in versions
