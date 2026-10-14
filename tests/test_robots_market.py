"""T10.11: pré-checagem e pacote local do MQL5 Market, sem envio externo."""
import pytest

from mt5_extracao.cli import build_parser
from mt5_extracao.robots import battery, catalog, market, mt5_tools


def make_robot(tmp_path, version="1.000", extra=""):
    source = tmp_path / "MarketRobot.mq5"
    source.write_text(
        f'#property version "{version}"\n'
        '#property description "A simple example for MetaTrader."\n'
        'input double InpVolume = 0.10; // Volume\n'
        'void OnTick() {\n'
        '  double lot_min = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);\n'
        '  double lot_step = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);\n'
        '  double margin = 0;\n'
        '  OrderCalcMargin(ORDER_TYPE_BUY, _Symbol, lot_min + lot_step, SymbolInfoDouble(_Symbol, SYMBOL_ASK), margin);\n'
        f'  {extra}\n'
        '}\n', encoding="utf-8")
    return catalog.robot_from_file(source), source


def compile_result(tmp_path):
    compiled = tmp_path / "MarketRobot.ex5"
    compiled.write_bytes(b"ex5")
    return mt5_tools.CompileResult(True, errors=0, warnings=0, compiled=str(compiled))


def run(robot, path, symbol, timeframe):
    return {"robot_key": robot.key, "source": "strategy tester", "symbol": symbol, "timeframe": timeframe,
            "metrics": {"source_fingerprint": catalog.fingerprint(robot), "tester_log": str(path),
                        "tester_log_captured": True}}


def test_check_accepts_two_error_free_logs_for_two_symbols_and_timeframes(tmp_path):
    robot, _ = make_robot(tmp_path)
    logs = [tmp_path / "run-1.log", tmp_path / "run-2.log"]
    for path in logs:
        path.write_text("Tester: completed, 0 errors, 0 warnings\n", encoding="utf-8")
    items = market.check(robot, compile_result(tmp_path),
                         [run(robot, logs[0], "EURUSD", "H1"), run(robot, logs[1], "GBPUSD", "M15")])
    assert market.ready(items)
    assert next(item for item in items if item.id == "tester_logs").status == battery.OK
    assert {item.id for item in items} >= {"compile", "version", "description", "dll", "tester_logs"}


def test_check_rejects_old_version_dll_fixed_environment_and_tester_error(tmp_path):
    robot, source = make_robot(tmp_path, version="0.10",
                               extra='CopyRates("EURUSD", PERIOD_H1, 0, 2, rates); '
                                     'if(AccountInfoInteger(ACCOUNT_LOGIN) == 123456) Print("C:\\\\temp\\\\robot");')
    source.write_text(source.read_text(encoding="utf-8").replace(
        '#property description "A simple example for MetaTrader."',
        '#property description "A simple example for MetaTrader."\n#import "helper.dll"'), encoding="utf-8")
    robot = catalog.robot_from_file(source)
    bad_log = tmp_path / "run.log"
    bad_log.write_text("Tester: error opening history\n", encoding="utf-8")
    items = market.check(robot, compile_result(tmp_path), [run(robot, bad_log, "EURUSD", "H1")])
    failed = {item.id for item in items if item.status == battery.FAIL}
    assert {"version", "dll", "fixed_environment", "tester_logs"} <= failed
    assert not market.ready(items)


def test_check_requires_fresh_isolated_logs_and_both_dimensions(tmp_path):
    robot, _ = make_robot(tmp_path)
    old = {"robot_key": robot.key, "source": "strategy tester", "symbol": "EURUSD", "timeframe": "H1",
           "metrics": {"source_fingerprint": catalog.fingerprint(robot)}}
    items = market.check(robot, compile_result(tmp_path), [old])
    assert next(item for item in items if item.id == "tester_logs").status == battery.FAIL
    one = tmp_path / "one.log"
    one.write_text("Tester: completed, 0 errors\n", encoding="utf-8")
    items = market.check(robot, compile_result(tmp_path), [run(robot, one, "EURUSD", "H1")])
    check = next(item for item in items if item.id == "tester_logs")
    assert check.status == battery.FAIL and "1 ativo" in check.detail


def test_run_tester_log_capture_keeps_only_bytes_added_during_run(tmp_path):
    data = tmp_path / "terminal-data"
    log_dir = data / "Tester" / "Agent-127.0.0.1-3000" / "logs"
    log_dir.mkdir(parents=True)
    daily_log = log_dir / "20261014.log"
    daily_log.write_bytes(b"previous run\n")
    before = mt5_tools._tester_log_snapshot(data)
    with daily_log.open("ab") as handle:
        handle.write(b"current run: 0 errors\n")
    captured = mt5_tools._capture_tester_log(data, before, "a1b2c3")
    assert captured == mt5_tools.tester_run_log(data, "a1b2c3")
    contents = captured.read_bytes()
    assert b"current run" in contents and b"previous run" not in contents
    with pytest.raises(ValueError, match="Identificador"):
        mt5_tools.tester_run_log(data, "../outside")


def test_pack_contains_only_requested_artifacts(tmp_path):
    robot, source = make_robot(tmp_path)
    result = compile_result(tmp_path)
    package = tmp_path / "package"
    files = market.pack(robot, result, package)
    assert {path.relative_to(package).as_posix() for path in files} == {
        "MarketRobot.ex5", "description.txt", "LEIA-ME.txt"}
    assert sorted(path.name for path in package.iterdir()) == ["LEIA-ME.txt", "MarketRobot.ex5", "description.txt"]
    assert not any(path.suffix == ".mq5" for path in package.rglob("*"))
    with_source = tmp_path / "package-with-source"
    source_files = market.pack(robot, result, with_source, with_source=True)
    assert any(path == with_source / "source" / source.name for path in source_files)
    assert (with_source / "source" / source.name).read_bytes() == source.read_bytes()


def test_pack_does_not_overwrite_nonempty_destination(tmp_path):
    robot, _ = make_robot(tmp_path)
    result = compile_result(tmp_path)
    package = tmp_path / "package"
    package.mkdir()
    (package / "user.txt").write_text("keep", encoding="utf-8")
    with pytest.raises(FileExistsError, match="não está vazia"):
        market.pack(robot, result, package)


def test_market_cli_accepts_pack_and_explicit_source_flag():
    args = build_parser().parse_args(["robots", "market", "DemoGuard", "--pack", "bundle", "--with-source"])
    assert args.action == "market" and args.path == "DemoGuard"
    assert args.pack == "bundle" and args.with_source is True

