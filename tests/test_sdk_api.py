"""T10.7: a superfície pública do SDK. Mudar a API exige mudar este teste (e avisar em docs/sdk/api.md)."""
import inspect
import re
from pathlib import Path

from mt5_extracao import sdk

REPO = Path(__file__).resolve().parents[1]

# nome -> parâmetros, na ordem (classes e constantes: None)
SURFACE = {
    "API_VERSION": None, "CONTRACT_VERSION": None,
    "Robot": None, "Problem": None, "CompileResult": None, "BatteryItem": None, "BatteryReport": None,
    "CheckResult": None, "ParityReport": None, "SignalSpec": None, "DeclaredStrategy": None, "Strategy": None, "StrategyInfo": None, "BacktestConfig": None,
    "load_catalog": ["config", "db"],
    "get_robot": ["robots", "key"],
    "robot_from_file": ["path"],
    "import_library": ["db", "folder"],
    "validate_contract": ["robot"],
    "new_robot": ["name", "folder", "robots", "config", "magic"],
    "install_robot": ["config", "robot", "mt5", "overwrite_presets", "upgrade_safety"],
    "compile_robot": ["config", "robot", "mt5", "run", "overwrite_presets", "upgrade_safety"],
    "run_python": ["db", "config", "robot", "symbol", "timeframe", "start", "end", "params", "strategy", "source",
                   "trade_from", "real_spread"],                # T10.12: novo no fim, com padrão
    "run_mql5": ["db", "config", "robot", "symbol", "timeframe", "start", "end", "preset", "params", "run",
                 "is_running", "source", "model"],              # parâmetros novos no fim, com padrão
    "run_battery": ["db", "config", "robot", "robots", "preset", "deep", "runner", "is_running", "symbol"],
    "check": ["db", "config", "robot", "robots", "symbol", "timeframe", "start", "end", "preset", "run", "is_running"],
    "write_junit": ["result", "path"],
    "compare_parity": ["python_trades", "mql5_trades", "tolerance_minutes", "warmup_until"],     # T10.6, T10.10
    "run_parity": ["db", "config", "robot", "strategy_robot", "symbol", "timeframe", "start", "end", "preset",
                   "runner", "is_running"],
    "generate_robot": ["spec", "name", "folder", "robots", "config", "magic"],                   # T10.10
    "build_matrix": ["db", "config", "robots"],                                                  # T11.6
    "matrix_positives": ["matrix", "robot_key"],
    "get_strategy": ["key", "params"],
    "run_backtest": ["df", "strategy", "spec", "config", "start_index", "signals"],
    "validate_strategy": ["df", "key", "spec", "config", "criteria", "params"],
}


def test_public_names_are_exactly_the_documented_ones():
    assert sorted(sdk.__all__) == sorted(SURFACE) and len(set(sdk.__all__)) == len(sdk.__all__)
    assert all(hasattr(sdk, name) for name in sdk.__all__)
    assert sdk.API_VERSION == 1 and sdk.CONTRACT_VERSION == 1


def test_signatures_did_not_change():
    for name, expected in SURFACE.items():
        if expected is not None:
            assert list(inspect.signature(getattr(sdk, name)).parameters) == expected, name


def test_every_public_name_is_in_the_reference():
    reference = (REPO / "docs" / "sdk" / "api.md").read_text(encoding="utf-8")
    missing = [name for name in sdk.__all__ if not re.search(r"`(sdk\.)?%s[`(]" % re.escape(name), reference)]
    assert missing == []


def test_sdk_never_sends_orders_and_exposes_no_order_function():
    forbidden = re.compile(r"order|ordem", re.I)
    assert [name for name in sdk.__all__ if forbidden.search(name)] == []
