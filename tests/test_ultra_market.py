from datetime import datetime, timezone
from decimal import Decimal
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
                               name="Private Name", margin_mode=2)

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
