from pathlib import Path
from types import SimpleNamespace

import pytest

from mt5_extracao.robots import ultra_runner as runner


def test_journal_captures_only_appended_or_rotated_bytes(tmp_path):
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
    live.write_text("new\n", encoding="utf-8")
    paths = runner._collect(tmp_path, "MQL5", before, tmp_path / "second")
    assert paths[0].read_bytes() == b"new\n"


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
    def export(db, package, folder):
        folder.mkdir()
        for name in ("UltraImport.mq5", "UltraTerminalProbe.mq5", "UltraConcurrent.mq5"):
            (folder / name).write_text("// MQL5")
        (folder / "UX_test.csv").write_text("1791190800000,100,101\n")
        return dict(package_hash="ABC", symbol="UX_test", sha256={}, ticks=1)
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
    result = runner.run(None, None, {}, tmp_path / "audit", origin="terminal_history")
    assert result["status"] == "modeled_book_matches" and result["ticks"] == 1
    assert (Path(result["directory"]) / result["journal"]).is_file()
    assert runner.run(None, None, {}, tmp_path / "audit")["status"] == "demo_login_required"
    demo = "1|0"
    assert runner.run(None, None, {}, tmp_path / "audit")["status"] == "demo_account_required"

    def test_platform(cfg, folder, ini, report, *, timeout):
        assert "Model=4" in ini and "UseCloud=0" in ini and "Symbol=UX_test" in ini
        logs = folder / "Tester/Agent-127.0.0.1/logs"
        logs.mkdir(parents=True)
        (logs / "agent.log").write_text("MT5X_ULTRA_B1|BEGIN")
        report_file = folder / (report + ".htm")
        report_file.write_text("fixture")
        return report_file

    demo = "1|1"
    mock(runner.mt5, "run_tester", test_platform)
    result = runner.run(None, None, {"policy": {"cash": 100000}}, tmp_path / "audit")
    assert result["status"] == "modeled_book_matches"
    assert result["origin"] == "strategy_tester" and "tester-logs" in result["journal"]
    assert list(Path(result["directory"]).glob("*.htm"))
    mock(runner.mt5, "terminal_running", lambda cfg: True)
    assert runner.run(None, None, {}, tmp_path / "audit")["status"] == "terminal_busy"


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
