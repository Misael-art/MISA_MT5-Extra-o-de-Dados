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
    "query_series": ["db", "selection"],
    "query_catalog": [],
    "get_robot": ["robots", "key"],
    "robot_from_file": ["path"],
    "import_library": ["db", "folder"],
    "validate_contract": ["robot"],
    "new_robot": ["name", "folder", "robots", "config", "magic"],
    "install_robot": ["config", "robot", "mt5", "overwrite_presets", "upgrade_safety"],
    "compile_robot": ["config", "robot", "mt5", "run", "overwrite_presets", "upgrade_safety"],
    "run_python": ["db", "config", "robot", "symbol", "timeframe", "start", "end", "params", "strategy", "source",
                   "trade_from", "real_spread", "cashflows", "data_manifest_id", "control"],  # T20.24: orçamento opcional
    "run_mql5": ["db", "config", "robot", "symbol", "timeframe", "start", "end", "preset", "params", "run",
                 "is_running", "source", "model", "visual", "read_csv", "trades_csv_override", "compiled_override",
                 "export_trades", "tester_port", "control"],               # T17.28: opcionais no fim; defaults legados preservados
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
    "run_backtest": ["df", "strategy", "spec", "config", "start_index", "signals", "cashflows", "check", "observation_timeframe"],
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


def test_cashflow_options_are_optional_and_old_positional_calls_still_bind():
    for function, legacy_count in ((sdk.run_python, 12), (sdk.run_backtest, 6)):
        signature = inspect.signature(function)
        assert signature.parameters["cashflows"].default is None
        bound = signature.bind(*([None] * legacy_count))
        bound.apply_defaults()
        assert bound.arguments["cashflows"] is None
    signature = inspect.signature(sdk.run_python)
    assert signature.parameters["data_manifest_id"].default is None
    bound = signature.bind(*([None] * 13))  # chamada anterior incluindo cashflows
    bound.apply_defaults()
    assert bound.arguments["data_manifest_id"] is None


def test_mql5_session_options_are_optional_and_preserve_legacy_defaults():
    parameters = inspect.signature(sdk.run_mql5).parameters
    expected = {"visual": None, "read_csv": True, "trades_csv_override": None,
                "compiled_override": None, "export_trades": True, "tester_port": None}
    assert {key: parameters[key].default for key in expected} == expected
    assert parameters["model"].default is None
    assert parameters["source"].default == "strategy tester"


def test_every_public_name_is_in_the_reference():
    reference = (REPO / "docs" / "sdk" / "api.md").read_text(encoding="utf-8")
    missing = [name for name in sdk.__all__ if not re.search(r"`(sdk\.)?%s[`(]" % re.escape(name), reference)]
    assert missing == []


def test_sdk_never_sends_orders_and_exposes_no_order_function():
    forbidden = re.compile(r"order|ordem", re.I)
    assert [name for name in sdk.__all__ if forbidden.search(name)] == []


def test_budget_callbacks_append_optional_arguments_without_breaking_old_calls():
    for function, count, option in ((sdk.run_python, 14, "control"), (sdk.run_mql5, 19, "control"),
                                    (sdk.run_backtest, 7, "check")):
        signature = inspect.signature(function)
        assert signature.parameters[option].default is None
        bound = signature.bind(*([None] * count))
        bound.apply_defaults()
        assert bound.arguments[option] is None


def test_trajectory_capture_is_optional_and_preserves_previous_sdk_calls():
    signature = inspect.signature(sdk.run_backtest)
    assert signature.parameters["observation_timeframe"].default is None
    bound = signature.bind(*([None] * 8))  # chamada anterior incluindo check
    bound.apply_defaults()
    assert bound.arguments["observation_timeframe"] is None
    explicit = signature.bind(*([None] * 8), observation_timeframe="M15")
    assert explicit.arguments["observation_timeframe"] == "M15"


def test_sdk_backtest_forwards_legacy_arguments_without_composition_extension(monkeypatch):
    captured = []
    sentinel = object()
    monkeypatch.setattr(sdk, "_run_backtest", lambda *args: captured.append(args) or sentinel)
    arguments = tuple(object() for _ in range(9))
    assert sdk.run_backtest(*arguments) is sentinel
    assert captured == [arguments]
    assert "exit_rules_override" not in inspect.signature(sdk.run_backtest).parameters


def test_sdk_python_run_keeps_legacy_surface_and_forwards_all_arguments(monkeypatch):
    captured = []
    sentinel = object()
    monkeypatch.setattr(sdk, "_run_python", lambda *args: captured.append(args) or sentinel)
    arguments = tuple(object() for _ in range(15))
    assert sdk.run_python(*arguments) is sentinel
    assert captured == [arguments]
    assert "composition" not in inspect.signature(sdk.run_python).parameters
    assert sdk.run_python(*arguments[:5]) is sentinel
    assert captured[-1] == (*arguments[:5], None, None, None, None, "backtest", None, True, None, None, None)
