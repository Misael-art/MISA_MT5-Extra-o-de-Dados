from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from mt5_extracao.laboratory import ultra_market as market
from mt5_extracao.laboratory import ultra_market_cycle


START = "2026-11-23T08:00:00"
END = "2026-11-23T08:02:00"
AXIS = int(datetime.fromisoformat(START).replace(tzinfo=timezone.utc).timestamp())
DUPLICATE = (AXIS * 1000 + 50, 1.0996299999999999, 1.09966, 0.0, 0, 6, 0.0)
BOUNDARY = ((AXIS + 60) * 1000, 1.09967, 1.09969, 0.0, 0, 4, 0.0)


class Broker:
    ACCOUNT_TRADE_MODE_DEMO = 0
    COPY_TICKS_ALL = -1
    TICK_FLAG_LAST, TICK_FLAG_VOLUME, TICK_FLAG_BUY, TICK_FLAG_SELL = 8, 16, 32, 64

    def __init__(self, blocks=None, *, trade_mode=0):
        self.blocks = blocks if blocks is not None else [[DUPLICATE, DUPLICATE, BOUNDARY], [BOUNDARY]]
        self.trade_mode = trade_mode
        self.calls = []

    def account_info(self):
        return SimpleNamespace(login=99901234, server="DemoServer", trade_mode=self.trade_mode, currency="USD",
                               name="Private Name", margin_mode=2, leverage=100)

    def terminal_info(self):
        return SimpleNamespace(connected=True, build=6324)

    def symbol_info(self, symbol):
        return SimpleNamespace(name=symbol, custom=False, trade_tick_size=0.00001)

    def symbol_select(self, symbol, selected):
        return selected

    def symbol_info_tick(self, symbol):
        return SimpleNamespace(time_msc=BOUNDARY[0])

    def copy_ticks_range(self, name, a, b, mode):
        self.calls.append((name, a, b, mode))
        day = self.blocks[len(self.calls) - 1]
        return None if day is None else [dict(zip(market.TICK_FIELDS, row)) for row in day]


@pytest.fixture
def snapshot(tmp_path):
    source = tmp_path / "market"
    backend = Broker()
    manifest = market.capture(backend, "EURUSD.demo", START, END, source, expected_server="DemoServer", chunk_minutes=1)
    return source, manifest, backend


def test_tick_tape_preserves_duplicate_rows_and_broker_boundaries(snapshot):
    source, manifest, broker = snapshot
    assert market.verify(source) == manifest
    assert broker.calls == [("EURUSD.demo", AXIS, AXIS + 60, -1), ("EURUSD.demo", AXIS + 60, AXIS + 120, -1)]
    assert [part["rows"] for part in manifest["blocks"]] == [2, 1]
    assert list(market._csv_rows(source, manifest)) == [DUPLICATE, DUPLICATE, BOUNDARY]
    assert manifest["coverage"]["duplicate"] == manifest["coverage"]["same_ms"] == 1
    assert manifest["coverage"]["off_grid"] == 0
    assert manifest["coverage"]["spread_ticks"] == dict(min=2, max=3, sum=8, count=3)
    assert manifest["coverage"]["positive_reported_volume"] == 0
    assert manifest["time"]["utc_offset"] == "unproven"
    assert manifest["contract"]["historical"]["commission"] is None
    assert "99901234" not in str(manifest) and "Private Name" not in str(manifest)
    assert market._price_atoms(1.099629, Decimal("0.00001")) is None
    (source / manifest["blocks"][0]["file"]).write_bytes(b"replaced")
    with pytest.raises(ValueError, match="fita diverge"):
        market.verify(source)


def test_capture_rejects_real_account_and_preserves_failed_snapshot_state(tmp_path):
    target = tmp_path / "market"
    real = Broker(trade_mode=2)
    with pytest.raises(ValueError, match="DEMO"):
        market.capture(real, "EURUSD", START, END, target, expected_server="DemoServer", chunk_minutes=1)
    assert not real.calls
    backend = Broker([[DUPLICATE], None])
    with pytest.raises(RuntimeError, match="bloco"):
        market.capture(backend, "EURUSD", START, END, target, expected_server="DemoServer", chunk_minutes=1)
    assert list(tmp_path.iterdir()) == []


def test_m1_readback_does_not_treat_quote_count_as_real_volume(snapshot, monkeypatch):
    source, _, broker = snapshot
    monkeypatch.setattr(broker, "TIMEFRAME_M1", 1, raising=False)
    def rates(_symbol, _tf, start, _end):
        quote, count = (DUPLICATE[1], 2) if start == AXIS else (BOUNDARY[1], 1)
        return [dict(time=start, open=quote, high=quote, low=quote, close=quote,
                     tick_volume=count, real_volume=0)]
    monkeypatch.setattr(broker, "copy_rates_range", rates, raising=False)
    result = market.verify_m1(broker, source)
    assert result["status"] == "m1_ohlc_matches"
    assert result["compared_bars"] == 2
    assert result["tick_volume_mismatches"] == result["real_volume_bars"] == 0


def test_probe_binds_agent_and_finds_first_missing_same_millisecond_tick(snapshot, tmp_path):
    source, manifest, _ = snapshot
    expert = tmp_path / "Probe.mq5"
    bundle = market.export_probe(source, expert, attempt="1" * 32)
    binding = bundle["binding"]
    assert manifest["manifest_hash"] in expert.read_text()
    assert bundle["orders"] is False
    tape = tmp_path / "tape.bin"
    earned = f'{market.PROBE_MARKER}BEGIN|{manifest["manifest_hash"]}|{binding}|EURUSD.demo|6324|2\n'
    def compare(rows, ended=1):
        tape.write_bytes(b"".join(market.PROBE_RECORD.pack(*row) for row in rows))
        journal = earned + f"{market.PROBE_MARKER}END|{binding}|{len(rows)}|{ended}|0"
        return market.compare_probe(source, tape, binding=binding, agent_journal=journal)
    assert compare([DUPLICATE, DUPLICATE, BOUNDARY])["status"] == "tick_stream_matches"
    assert compare([DUPLICATE, DUPLICATE, BOUNDARY], ended=0)["status"] == "incomplete"
    result = compare([DUPLICATE, BOUNDARY])
    assert result["status"] == "diverged"
    assert result["matching_prefix"] == 1
    assert result["first_divergence"]["field"] == "time_msc"
    with pytest.raises(ValueError, match="uma única execução"):
        market.compare_probe(source, tape, binding=binding, agent_journal=earned)


def test_quotes_remain_exact_when_the_tester_removes_one_broker_flag(snapshot, tmp_path):
    source, manifest, _ = snapshot
    tape = tmp_path / "tape.bin"
    rows = [(*row[:5], row[5] ^ 1024, row[6]) for row in market._csv_rows(source, manifest)]
    tape.write_bytes(b"".join(map(lambda row: market.PROBE_RECORD.pack(*row), rows)))
    binding = "2" * 64
    journal = (f'{market.PROBE_MARKER}BEGIN|{manifest["manifest_hash"]}|{binding}|EURUSD.demo|6324|2\n'
               f"{market.PROBE_MARKER}END|{binding}|3|1|0")
    result = market.compare_probe(source, tape, binding=binding, agent_journal=journal)
    assert result["status"] == "diverged"
    assert result["quotes_exact"]
    assert result["first_divergence"]["field"] == "flags"
    assert result["changed_flag_bits"] == {"0x400": 3}


def test_s09_market_reference_uses_next_tick_only_and_requires_warmup(tmp_path):
    msc = AXIS * 1000
    levels = [110000 + i for i in range(30)] + [109950, 109948, 109900, 109899, 109900]
    ticks = [(msc + i*60000 + 17, level/100000, (level+2)/100000, 0.0, 0, 6, 0.0)
             for i, level in enumerate(levels)]
    source = tmp_path / "market"
    manifest = market.capture(Broker([ticks]), "EURUSD", START, "2026-11-23T08:35:00", source,
                              expected_server="DemoServer", chunk_minutes=60)
    bars, entry, exit_ = ultra_market_cycle.expected(source, manifest)
    assert all(b[-1] == 0 for b in bars[:30])
    assert entry == {"msc": ticks[31][0], "side": -1, "price": 109948, "stop": 109968, "target": 109918, "steps": 1}
    assert exit_ == {"msc": ticks[32][0], "price": 109902, "reason": "TP", "steps": 1, "profit": 46}
    with pytest.raises(ValueError, match="contrato técnico"):
        ultra_market_cycle.export(source, tmp_path / "Cycle.mq5", attempt="1"*32)


@pytest.mark.parametrize("from_agent", [True, False])
def test_tester_runner_requires_agent_evidence_and_preserves_report(snapshot, tmp_path, monkeypatch, from_agent):
    from mt5_extracao.robots import ultra_market_runner as runner

    source, manifest, _ = snapshot
    data = tmp_path / "terminal"
    data.mkdir()
    common = tmp_path / "common"
    common.mkdir()
    for name in ("data_dir", "install_dir", "find_metaeditor"):
        monkeypatch.setattr(runner.mt5, name, lambda config: data)
    monkeypatch.setattr(runner.mt5, "wine_prefix", lambda config: None)
    monkeypatch.setattr(runner.mt5, "terminal_running", lambda config: False)
    monkeypatch.setattr(runner.mt5, "common_files_dir", lambda config: common)
    monkeypatch.setattr(runner.ultra_runner, "_demo_startup", lambda: "")

    def compile_source(expert, *_args, **_kwargs):
        compiled = expert.with_suffix(".ex5")
        compiled.write_bytes(b"compiled")
        return SimpleNamespace(ok=True, warnings=0, log_path=None, compiled=compiled)

    def run_tester(config, folder, ini, key, **_kwargs):
        assert "Model=4" in ini and "ExecutionMode=0" in ini and "Leverage=1:100" in ini
        saved = next((tmp_path / "runs").glob("*/receipt.json"))
        binding = json.loads(saved.read_text())["binding"]
        tape = common / f"MT5XUltraMarket_{binding}.bin"
        tape.write_bytes(b"".join(market.PROBE_RECORD.pack(*row) for row in (DUPLICATE, DUPLICATE, BOUNDARY)))
        report = folder / f"{key}.htm"
        report.write_text("<table><tr><td>Real ticks</td><td>100%</td></tr></table>")
        logs = folder / "Tester"
        if from_agent:
            logs /= "Agent-127.0.0.1-3000"
        logs /= "logs"
        logs.mkdir(parents=True)
        (logs / "tester.log").write_text(
            f'{market.PROBE_MARKER}BEGIN|{manifest["manifest_hash"]}|{binding}|EURUSD.demo|6324|2\n'
            f"{market.PROBE_MARKER}END|{binding}|3|1|0\n")
        return report

    @contextmanager
    def lease():
        yield

    monkeypatch.setattr(runner.mt5, "tester_lease", lease)
    monkeypatch.setattr(runner.mt5, "compile_source", compile_source)
    monkeypatch.setattr(runner.mt5, "run_tester", run_tester)
    result = runner.run(None, source, tmp_path / "runs")
    assert result["status"] == ("tick_stream_matches" if from_agent else "journal_missing")
    assert Path(result["receipt_path"]).parent.joinpath("report.htm").is_file()
    assert result["tester_data_quality"]["real_tick_fraction_reported"] == 1.0


def test_portfolio_compares_pattern_on_closed_bars_and_identifies_first_difference(monkeypatch, tmp_path):
    from mt5_extracao.laboratory.ultra_market_portfolio_audit import _pattern_comparison

    def field(name):
        return {"op": "field", "name": name}

    def compare(operator, left, right):
        return {"op": "compare", "comparison": operator, "left": field(left), "right": field(right)}

    definition = dict(schema_version=1, pattern_id="eurusd.test", strategy_version="1", original_hypothesis="Teste OHLC",
        origin="user", parent_attempt_ids=[], universe_definition={"mode": "explicit", "symbols": ["EURUSD"]},
        data_requirements=["ohlcv"], timeframe="M1", timezone_calendar_versions={"clock": "mt5_raw_clock"},
        features_and_versions=[], context_expression=None, condition_expression=compare("gt", "close", "prior_close"),
        decision_clock="closed_bar_available", price_reference="prior_close", side="long",
        simulated_entry="next_executable_open", outcome_definition={"horizon_bars": 1}, repetition_policy="all",
        invalidation_rules=[{"id": "top", "action": "cancel_before_entry", "when": compare("gte", "close", "high")}],
        execution_policy="research_only", cost_model_id=None, complexity={"terms": 1}, implementation_signature="test")
    pattern = dict(definition=definition, apply_invalidation=True)
    t = AXIS * 1000
    # A primeira barra não conhece prior_close. A segunda fecha na máxima e invalida.
    quotes = [(t+1, 1.09960), (t+60_001, 1.09965), (t+60_002, 1.09966), (t+120_007, 1.09965)]
    monkeypatch.setattr(market, "_csv_rows", lambda *_: iter(quotes))
    manifest = {"symbol": "EURUSD", "manifest_hash": "a" * 64}
    observed = f"MT5X_ULTRA_MARKET_PATTERN|{t+120_007}|{t+120_000}|1|1|109960"
    matched = _pattern_comparison(tmp_path, manifest, pattern, observed, Decimal("0.00001"))
    assert matched["expected"] == matched["observed"] == matched["invalidated"] == 1
    assert matched["first_divergence"] is None
    tampered = _pattern_comparison(tmp_path, manifest, pattern, observed.replace("|1|109960", "|0|109960"), Decimal("0.00001"))
    first = tampered["first_divergence"]
    assert first == {"ordinal": 0, "expected": (t+120_007, t+120_000, 1, 1, 109960),
                     "observed": (t+120_007, t+120_000, 1, 0, 109960)}
