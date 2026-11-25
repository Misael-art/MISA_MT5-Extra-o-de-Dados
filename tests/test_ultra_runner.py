from pathlib import Path
from types import SimpleNamespace

import pytest

from mt5_extracao.robots import ultra_runner as runner


@pytest.mark.parametrize("rotated", [b"new\n", b"new\r\n"], ids=["lf", "crlf"])
def test_journal_captures_only_appended_or_rotated_bytes(tmp_path, rotated):
    logs = tmp_path / "MQL5/logs"
    logs.mkdir(parents=True)
    live = logs / "20261124.log"
    live.write_bytes("previous\n".encode("utf-16"))
    before = runner._offsets(tmp_path, "MQL5")
    with live.open("ab") as stream:
        stream.write("MT5X_ULTRA_DEMO|1|1\n".encode("utf-16-le"))
    paths = runner._collect(tmp_path, "MQL5", before, tmp_path / "first")
    assert paths[0].read_text(encoding="utf-16") == "MT5X_ULTRA_DEMO|1|1\n"
    assert runner._marked(paths, "MT5X_ULTRA_DEMO|")[1] == ["1|1"]
    live.write_bytes(rotated)
    paths = runner._collect(tmp_path, "MQL5", before, tmp_path / "second")
    assert paths[0].read_bytes() == rotated


def test_bindings_refuse_conflicting_journals_and_existing_sources(tmp_path):
    source, other = tmp_path / "one", tmp_path / "two"
    source.write_text("probe|a")
    other.write_text("probe|b")
    with pytest.raises(ValueError, match="divergem"):
        runner._marked([source, other], "probe|")
    with pytest.raises(ValueError, match="preservado"):
        runner._install(source, other, tmp_path)
    assert other.read_text() == "probe|b"


def test_run_history_and_gate_tester_on_observed_demo(tmp_path, monkeypatch):
    data = tmp_path / "portable"
    data.mkdir()
    journal = "MT5X_ULTRA_HISTORY_B1|BEGIN"
    mock = monkeypatch.setattr
    def export(db, package, folder, **options):
        folder.mkdir()
        for name in ("UltraImport.mq5", "UltraTerminalProbe.mq5", "UltraPreparedTester.mq5", "UltraTesterContract.mq5", "UltraPlatformOrders.mq5"):
            (folder / name).write_text("// MQL5")
        (folder / "UX_test.csv").write_text("1791190800000,100,101\n")
        return dict(package_hash="ABC", symbol="UX_test", sha256={}, ticks=1, tester_contract={
            "symbol": "UT_test", "primer_bars": 128, "tick_cash_cents_per_step": 100, "margin_cents_per_step": 24700})
    mock(runner.ultra_tester, "export", export)
    mock(runner.mt5, "data_dir", lambda cfg: data)
    mock(runner.mt5, "install_dir", lambda cfg: data)
    mock(runner.mt5, "find_metaeditor", lambda cfg: data / "editor")
    mock(runner.mt5.paths, "state_dir", lambda: tmp_path / "lock")
    mock(runner.mt5, "_native_context_processes", lambda context: [])
    mock(runner.mt5, "terminal_running", lambda cfg: False)
    def compile(src, *args, **kwargs):
        binary = src.with_suffix(".ex5")
        binary.write_bytes(b"compiled")
        return SimpleNamespace(ok=True, warnings=0, compiled=str(binary), log_path="")
    mock(runner.mt5, "compile_source", compile)
    demo = "0|0"
    def invoke(cfg, data, name, chart, tag, out, timeout):
        out.mkdir()
        path = out / "run.log"
        if name.endswith("UltraDemoStatus"):
            path.write_text("MT5X_ULTRA_DEMO|" + demo)
        elif name.endswith("UltraTesterContract"):
            path.write_text("MT5X_ULTRA_PREP|ABC|UT_test|128|1|100|24700")
        elif name.endswith("UltraImport"):
            path.write_text("MT5X_ULTRA_IMPORT|ABC|1|0")
        else:
            path.write_text(journal)
        return [path]
    mock(runner, "_script", invoke)
    def compare(db, package, blob, *, origin):
        if origin == "terminal_history":
            assert blob.decode() == journal
        else:
            assert blob.decode() == "MT5X_ULTRA_B1|BEGIN"
        return {"status": "modeled_book_matches"}
    mock(runner.ultra_tester, "compare", compare)
    package = {"policy": {"cash": 100000, "currency": "USD"}}
    result = runner.run(None, None, package, tmp_path / "audit", origin="terminal_history")
    assert result["status"] == "modeled_book_matches" and result["ticks"] == 1
    assert (Path(result["directory"]) / result["journal"]).is_file()
    assert runner.run(None, None, package, tmp_path / "audit")["status"] == "demo_login_required"
    ready = runner.run(None, None, package, tmp_path / "audit", prepare_only=True)
    assert ready["status"] == "tester_contract_verified"
    assert not (Path(ready["directory"]) / "UltraDemoStatus").exists()
    demo = "1|0"
    assert runner.run(None, None, package, tmp_path / "audit")["status"] == "demo_account_required"

    def test_platform(cfg, folder, ini, report, *, timeout):
        assert "Currency=USD" in ini and "Model=4" in ini and "UseCloud=0" in ini and "Symbol=UT_test" in ini
        logs = folder / "Tester/Agent-127.0.0.1/logs"
        logs.mkdir(parents=True)
        (logs / "agent.log").write_text("MT5X_ULTRA_B1|BEGIN")
        report_file = folder / (report + ".htm")
        report_file.write_text("fixture")
        return report_file

    demo = "1|1"
    mock(runner.mt5, "run_tester", test_platform)
    result = runner.run(None, None, package, tmp_path / "audit")
    assert result["status"] == "modeled_book_matches"
    assert result["origin"] == "strategy_tester" and "tester-logs" in result["journal"]
    assert list(Path(result["directory"]).glob("*.htm"))
    from mt5_extracao.robots import safety_lib
    import json
    from hashlib import sha256
    lib = tmp_path / "lib"
    includes = lib / "Include/QA"
    includes.mkdir(parents=True)
    header = "HDK_ExecutionResult.mqh"
    (includes / header).write_bytes(b"fixture")
    (lib / "safety.json").write_text(json.dumps({"version": "fixture", "files": [header], "include_dir": "QA",
                                                "sha256": {header: sha256(b"fixture").hexdigest()}}))
    mock(safety_lib, "lib_dir", lambda cfg: lib)
    def relayed(cfg, folder, ini, report, *, timeout):
        logs = folder / "Tester/logs"
        logs.mkdir(exist_ok=True)
        (logs / "controller.log").write_text("MT5X_ULTRA_PLATFORM_BOOK1|BEGIN")
        file = folder / (report + ".htm")
        file.write_text("fixture")
        return file
    mock(runner.mt5, "run_tester", relayed)
    result = runner.run(None, None, package, tmp_path / "audit", origin="strategy_tester_orders")
    assert result["status"] == "journal_missing" and result["report_sha256"]
    (includes / header).write_bytes(b"unexpected")
    result = runner.run(None, None, package, tmp_path / "audit", origin="strategy_tester_orders")
    assert result["status"] == "safety_library_incompatible"
    mock(runner.mt5, "terminal_running", lambda cfg: True)
    assert runner.run(None, None, package, tmp_path / "audit")["status"] == "terminal_busy"


def test_cli_reads_frozen_package_and_preserves_pending_status(tmp_path, monkeypatch, capsys):
    import json
    from mt5_extracao import cli
    package = tmp_path / "package.json"
    package.write_text('{"receipt":"fixture"}', encoding="utf-8")
    calls = []
    def run(db, config, data, output, *, origin, chart):
        calls.append((data["receipt"], origin, chart))
        return {"status": "demo_login_required"}
    monkeypatch.setattr(runner, "run", run)
    config = tmp_path / "config.ini"
    config.write_text(f"[DATABASE]\ntype=sqlite\npath={tmp_path / 'qa.sqlite'}\n", encoding="utf-8")
    code = cli.main(["--config", str(config), "--json", "robots", "ultra-test",
                     str(package), "--report", str(tmp_path / "proof")])
    assert calls == [("fixture", "strategy_tester", "EURUSD")]
    assert code == cli.EXIT_FAIL and json.loads(capsys.readouterr().out)["data"]["status"] == "demo_login_required"


def test_restores_persisted_binary_and_prefix_in_a_new_attempt(tmp_path):
    from mt5_extracao.robots import ultra_restart
    import json

    data = tmp_path / "portable"
    files = data / "MQL5/Files"
    files.mkdir(parents=True)
    binding = "a"*64
    identity = "1"*32
    package = "b"*64
    exported = {"checkpoint_binding": binding, "restart_id": identity}
    native_file = files / ("MT5XUltra_" + binding + ".bin")
    record = binding + "|199|1234|0|0|1234|1|1"
    initial = tmp_path / "first"
    initial.mkdir()
    receipt = {"package_hash": package, "origin": "terminal_restart"}
    def save(status, **fields):
        receipt.update(status=status, **fields)
        return dict(receipt)
    def write_prefix(phase):
        assert phase == "save"
        native_file.write_bytes(binding.encode() + b"snapshot")
        path = initial / "raw.log"
        path.write_text("MT5X_ULTRA_RESTART_CTL|SAVED|" + record)
        return [path]
    _, pending = ultra_restart.complete(folder=initial, data=data, exported=exported, parent=None, invoke=write_prefix,
                                       marked=runner._marked, install=runner._install, save=save, running=lambda: True)
    assert pending["status"] == "terminal_busy_after_checkpoint"
    persisted = initial / "receipt.json"
    persisted.write_text(json.dumps(pending), encoding="utf-8")
    # A nova sessão pode restaurar num perfil portátil sem o arquivo da sessão anterior.
    native_file.unlink()
    parent = ultra_restart.read_parent(persisted, package)
    next_attempt = tmp_path / "second"
    next_attempt.mkdir()
    def restore(phase):
        assert phase == "restore" and native_file.read_bytes() == (initial / "partial.bin").read_bytes()
        path = next_attempt / "raw.log"
        path.write_text("MT5X_ULTRA_RESTART_CTL|LOADED|" + record)
        return [path]
    journal, pending = ultra_restart.complete(folder=next_attempt, data=data, exported=exported, parent=parent,
                                              invoke=restore, marked=runner._marked, install=runner._install,
                                              save=save, running=lambda: False)
    assert not pending and ["SAVED", "LOADED"] == [line.split("|")[1] for line in journal.read_text().splitlines()]
    (initial / "partial.bin").write_bytes(b"altered")
    with pytest.raises(ValueError, match="diverge"):
        ultra_restart.read_parent(persisted, package)


def test_pinned_s09_headers_require_matching_configured_library(tmp_path, monkeypatch):
    from hashlib import sha256
    from mt5_extracao.robots import ultra_hadouken
    root = tmp_path / "Include/Framework/HADOUKEN"
    data = {"Strategies/cross.mqh": b"cross\n", "Contracts/market.mqh": b"market\n"}
    pins = {name: sha256(blob).hexdigest() for name, blob in data.items()}
    monkeypatch.setattr(ultra_hadouken, "HEADERS", pins)
    monkeypatch.setattr(ultra_hadouken, "library_dirs", lambda config: [tmp_path])
    for name, blob in data.items():
        path = root / name
        path.parent.mkdir(parents=True)
        path.write_bytes(blob.replace(b"\n", b"\r\n"))
    assert ultra_hadouken.headers(None) == {name: root / name for name in pins}
    (root / "Contracts/market.mqh").write_bytes(b"changed")
    with pytest.raises(ValueError, match="incompatível"):
        ultra_hadouken.headers(None)
