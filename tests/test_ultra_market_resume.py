from contextlib import nullcontext
from hashlib import sha256
import json

import pytest

from mt5_extracao.robots import ultra_market_resume as resume


def test_journal_spans_midnight_and_excludes_another_attempt(tmp_path):
    agent = tmp_path / "agent"
    agent.mkdir()
    portfolio = "MT5X_ULTRA_PORTFOLIO|"
    market = "MT5X_ULTRA_MARKET|"
    header = portfolio + "PRESET|same|1"
    first = [header, market + "BEGIN|market|old|EURUSD", market + "END|old|8|1|0", header,
             portfolio + "OWNER|11", market + "BEGIN|market|wanted|EURUSD"]
    second = [portfolio + "FINAL|wanted", market + "END|wanted|8|1|0", header, market + "BEGIN|market|later|EURUSD"]
    (agent / "1.log").write_text("\n".join(first) + "\n", encoding="utf-16")
    (agent / "2.log").write_text("\n".join(second) + "\n", encoding="utf-8")
    receipt = dict(binding="wanted", manifest_hash="market", preset_sha256="same")
    target = tmp_path / "journal"
    assert resume._copy_journal(agent, target, receipt)
    assert target.read_text().splitlines() == first[3:] + second[:2]
    assert not resume._copy_journal(agent, tmp_path / "incomplete", dict(receipt, binding="later"))


def test_completed_native_run_reconciles_once_without_relaunch(tmp_path, monkeypatch):
    attempt, binding, policy, manifest = "a" * 32, "b" * 64, "c" * 64, "d" * 64
    original = tmp_path / attempt
    original.mkdir()
    common = tmp_path / "common"
    common.mkdir()
    data = tmp_path / "terminal"
    agent = data / "Tester/Agent-127.0.0.1-8000/logs"
    agent.mkdir(parents=True)
    def put(root, name, data):
        path = root / name
        path.write_bytes(data)
        return sha256(data).hexdigest()
    main = "UltraMarketPortfolio.mq5"
    program = put(original, main, b"observed source")
    ex5 = put(original, "UltraMarketPortfolio.ex5", b"observed expert")
    tape = f"MT5XUltraMarket_{binding}.bin"
    tape_sha = put(common, tape, b"observed quotes")
    (data / f"UltraMarket_{attempt[:16]}.htm").write_text("native report")
    (agent / "20261126.log").write_text(
        f"MT5X_ULTRA_PORTFOLIO|PRESET|{policy}|1\nMT5X_ULTRA_MARKET|BEGIN|{manifest}|{binding}|EURUSD\n"
        f"MT5X_ULTRA_PORTFOLIO|FINAL|{binding}\nMT5X_ULTRA_MARKET|END|{binding}|1|1|0\n")
    preset = {"preset_sha256": policy}
    (original / "preset-frozen.json").write_text(json.dumps(preset))
    receipt = dict(attempt=attempt, mode="portfolio", binding=binding, manifest_hash=manifest, tape=tape,
                   status="tester_timeout", tester_port=8000, preset=preset, preset_sha256=policy, source_sha256=program,
                   source_files={main: program}, binary_sha256=ex5)
    source = original / "receipt.json"
    source.write_text(json.dumps(receipt))
    source_bytes = source.read_bytes()
    monkeypatch.setattr(resume.ultra_market_preset, "read", lambda _: preset)
    monkeypatch.setattr(resume.mt5, "terminal_running", lambda _: True)
    monkeypatch.setattr(resume.mt5, "tester_lease", nullcontext)
    monkeypatch.setattr(resume.mt5, "data_dir", lambda _: data)
    monkeypatch.setattr(resume.mt5, "common_files_dir", lambda _: common)
    monkeypatch.setattr(resume.ultra_market, "verify", lambda _: {"manifest_hash": manifest})
    calls = []
    monkeypatch.setattr(resume.ultra_market, "compare_probe", lambda *a, **kw: {"tape_sha256": tape_sha})
    def compare(bundle, ticks, *args, **kwargs):
        calls.append(bundle["binding"])
        return {"status": "single_owner_matches", "tick_comparison": ticks}
    monkeypatch.setattr(resume.ultra_market_portfolio_audit, "compare", compare)
    monkeypatch.setattr(resume, "tester_report", lambda *a: "native")
    monkeypatch.setattr(resume, "write_market_report", lambda _, path: path.write_text("audited"))
    assert resume.recover(None, tmp_path, source)["status"] == "awaiting_native_completion"
    assert not (original / "reconciled").exists()
    monkeypatch.setattr(resume.mt5, "terminal_running", lambda _: False)
    out = resume.recover(None, tmp_path, source)
    monkeypatch.setattr(resume.mt5, "terminal_running", lambda _: True)
    assert resume.recover(None, tmp_path, source) == out and calls == [binding]
    assert out["status"] == "single_owner_matches" and source.read_bytes() == source_bytes
    (original / "reconciled/report.htm").write_text("changed")
    with pytest.raises(ValueError, match="divergiram"):
        resume.recover(None, tmp_path, source)


def test_research_recovers_older_partial_journal_only_for_matching_arm(tmp_path, monkeypatch):
    from mt5_extracao.laboratory.ultra_market_paired import _recover_attempt
    folder = tmp_path / "signal" / ("a" * 32)
    folder.mkdir(parents=True)
    source = folder / "receipt.json"
    attempt = dict(status="failed", receipt=str(source))
    receipt = dict(status="failed", last_completed="journal_saved", reason="O journal deve vincular uma única execução.",
                   preset_sha256="p", manifest_hash="m")
    source.write_text(json.dumps(receipt))
    monkeypatch.setattr(resume, "recover", lambda _, snapshot, path: {"receipt": path})
    assert _recover_attempt(None, tmp_path, folder.parent, attempt, "p", "m") == {"receipt": source}
    with pytest.raises(ValueError, match="plano"):
        _recover_attempt(None, tmp_path, folder.parent, attempt, "other", "m")
    source.write_text(json.dumps(dict(receipt, reason="erro de margem")))
    assert _recover_attempt(None, tmp_path, folder.parent, attempt, "p", "m") is None
