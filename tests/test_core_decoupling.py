"""O núcleo não pode depender de MetaTrader5 nem de tkinter no import (T2.2 / T2.3)."""
import re
import subprocess
import sys
from pathlib import Path

from fakes import TIMEFRAMES

REPO = Path(__file__).resolve().parents[1]
CORE_MODULES = ["mt5_extracao.mt5_connector", "mt5_extracao.error_handler", "mt5_extracao.historical_extractor",
                "mt5_extracao.database_manager", "mt5_extracao.indicator_calculator", "mt5_extracao.security",
                "mt5_extracao.mt5_backend", "mt5_extracao.timeframes"]


def test_core_imports_without_metatrader5_and_tkinter(tmp_path):
    code = ("import sys; sys.modules['MetaTrader5'] = None; sys.modules['tkinter'] = None\n"
            + "".join(f"import {m}\n" for m in CORE_MODULES))
    result = subprocess.run([sys.executable, "-c", code], cwd=str(REPO), capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_only_backend_imports_metatrader5():
    offenders = []
    for path in list((REPO / "mt5_extracao").glob("*.py")) + [REPO / "app.py"]:
        if path.name == "mt5_backend.py":
            continue
        if re.search(r"^\s*(import MetaTrader5|from MetaTrader5)", path.read_text(encoding="utf-8"), re.M):
            offenders.append(path.name)
    assert offenders == []


def test_timeframe_constants_match_official_values():
    from mt5_extracao import timeframes
    for name, value in TIMEFRAMES.items():
        assert getattr(timeframes, name) == value, name
    assert set(timeframes.MINUTES) == set(TIMEFRAMES.values())


def test_connector_without_ui_answers_no(connector):
    assert connector._ask_user("t", "m") is False
    connector.ask_user = lambda t, m: True
    assert connector._ask_user("t", "m") is True


def test_connector_last_error(connector, fake_mt5):
    assert connector.last_error() == (1, "Success")
