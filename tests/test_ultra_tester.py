from copy import deepcopy
from hashlib import sha256

import pytest

from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.experiments.schema import digest
from mt5_extracao.laboratory import ultra, ultra_tester
from mt5_extracao.presets.compiler import compile_ultra_research
from scripts.demo_ultra import prepare


@pytest.fixture
def campaign(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "qa.sqlite"), broker_utc_offset=0)
    try:
        receipt = ultra.freeze(db, prepare(db))["receipt"]
        ultra.replay(db, receipt)
        yield db, compile_ultra_research(db, receipt)
    finally:
        db.close()


def test_export_frozen_expert_in_priority_order_and_refuse_overwrite(campaign, tmp_path):
    db, package = campaign
    folder = tmp_path / "tester"
    result = ultra_tester.export(db, package, folder)
    assert result["ticks"] == 372
    assert len((folder / (result["symbol"] + ".csv")).read_text().splitlines()) == 372
    expert = (folder / "UltraConcurrent.mq5").read_text()
    assert "#define DON_SLOT 0" in expert
    assert "book.module[1].max_losses=2;" in expert
    assert "MQL_TESTER" in expert and "SymbolInfoTick(_Symbol,quote)" in expert
    assert "OrderSend" not in expert and "OrderSend" not in (folder / "UltraTerminalProbe.mq5").read_text()
    assert "CopyTicksRange(" in (folder / "UltraTerminalProbe.mq5").read_text()
    assert package["package_hash"] in expert
    with pytest.raises(FileExistsError):
        ultra_tester.export(db, package, folder)
    bad = deepcopy(package)
    bad["policy"]["modules"][0]["steps"] += 1
    with pytest.raises(ValueError, match="diverge"):
        ultra_tester.export(db, bad, tmp_path / "bad")
    assert not (tmp_path / "bad").exists()


def fixture_rows(db, package):
    modules, frames = ultra_tester._context(db, package)
    header = ("BEGIN", package["package_hash"], package["source_manifest"], digest(ultra_tester._templates()), 6324)
    records = [header]
    for _, _, kind, _, values in ultra_tester._expected_rows(frames, modules, package["policy"]):
        records.append((kind, *values))
    return records + [("END", len(frames), 0)]


def journal(rows, origin="strategy_tester"):
    return "\n".join("agent\t" + ultra_tester.MARKERS[origin] + "|".join(map(str, row)) for row in rows).encode("utf-16")


def test_first_divergence_in_partial_fill_and_cash_and_source_boundaries(campaign):
    db, package = campaign
    rows = fixture_rows(db, package)
    # 904 registros emitidos por UltraTerminalProbe no MT5 6324, sem cabeçalho e rodapé do journal.
    body = "".join("|".join(map(str, row)) + "\n" for row in rows[1:-1])
    assert sha256(body.encode()).hexdigest() == "5079da7d54157163c4eb19962d71bc873eaffbc3937b1d00dc702d16760a0d74"
    def compare(records, origin="strategy_tester"):
        return ultra_tester.compare(db, package, journal(records, origin), origin=origin)
    passed = compare(rows)
    assert (passed["frames_compared"], passed["events_compared"], passed["end_equity"]) == (372, 80, 93440)
    assert passed["native_order_parity"] == "pending"
    position = next(i for i, row in enumerate(rows) if row[0] == "EVENT" and row[5:7] == ("fill", "partial"))
    changed = list(rows)
    changed[position] = (*rows[position][:7], 2, *rows[position][8:])
    first = compare(changed)["first_divergence"]
    assert first["ordinal"] == 198 and first["field"] == "EVENT.steps" and first["observed"] == "2"
    changed = list(rows)
    changed[position+1] = ("MARK", -1, *rows[position+1][2:])
    assert compare(changed)["first_divergence"]["field"] == "MARK.cash"
    removed = rows[:position] + rows[position+2:]
    assert compare(removed)["first_divergence"]["ordinal"] == 198
    assert compare(rows, "terminal_history")["origin"] == "terminal_history"
    with pytest.raises(ValueError, match="origem"):
        ultra_tester.compare(db, package, journal(rows, "terminal_history"), origin="strategy_tester")
    for broken in [rows[:-1], rows + rows, [("BEGIN", "other", *rows[0][2:])] + rows[1:]]:
        with pytest.raises(ValueError, match="Journal"):
            compare(broken)


def test_checkpoint_requires_same_cursor_and_first_partial(campaign, tmp_path):
    db, package = campaign
    exported = ultra_tester.export(db, package, tmp_path / "restart", restart_id="a"*32)
    binding = exported["checkpoint_binding"]
    assert binding in (tmp_path / "restart/UltraRestartProbe.mq5").read_text()
    rows = fixture_rows(db, package)
    rev = digest([digest(ultra_tester._templates()), ultra_tester._restart_template()])
    rows[0] = (*rows[0][:3], rev, 6324)
    key = next(row for row in rows if row[0] == "EVENT" and row[5:7] == ("fill", "partial"))
    at = next(i for i, row in enumerate(rows) if row[:3] == ("FRAME", *key[1:3])) + 2
    cursor = "|".join(map(str, [binding, 199, *key[1:5], key[7], key[11]]))
    cut = ultra_tester.RESTART_CONTROL
    prefix = journal(rows[:at], "terminal_restart").decode("utf-16")
    suffix = journal(rows[at:], "terminal_restart").decode("utf-16")
    def compare(controls):
        log = "\n".join([prefix, *controls, suffix]).encode("utf-8")
        return ultra_tester.compare(db, package, log, origin="terminal_restart")
    controls = [cut + action + "|" + cursor for action in ("SAVED", "LOADED")]
    result = compare(controls)
    assert result["status"] == "modeled_book_matches"
    assert result["checkpoint"]["frames_before_restart"] == 199 and result["checkpoint"]["pending"] == 1
    with pytest.raises(ValueError, match="coincidir"):
        compare(controls[:1])
    with pytest.raises(ValueError, match="primeiro fill parcial"):
        compare([control[:-1]+"2" for control in controls])


def test_tester_primer_precedes_window_and_preserves_frozen_quotes(campaign, tmp_path):
    db, package = campaign
    normal = ultra_tester.export(db, package, tmp_path / "ordinary")
    boosted = ultra_tester.export(db, package, tmp_path / "prepared", prepare_tester=True)
    contract = boosted["tester_contract"]
    csv = normal["symbol"] + ".csv"
    assert boosted["sha256"][csv] == normal["sha256"][csv] and boosted["ticks"] == 372
    assert contract["symbol"] != normal["symbol"]
    assert contract["primer_first_msc"] + 128*60000 < contract["sample_first_msc"]
    assert contract["primer_first_msc"] < contract["sample_first_msc"] // 86400000 * 86400000
    assert (contract["tick_cash_cents_per_step"], contract["margin_cents_per_step"]) == (100, 24700)
    expert = (tmp_path / "prepared/UltraPreparedTester.mq5").read_text()
    assert contract["symbol"] in expert and "quote.time_msc<FIRST_MSC" in expert and "MQL_TESTER" in expert
    assert boosted["sha256"]["UltraConcurrent.mq5"] == normal["sha256"]["UltraConcurrent.mq5"]


def test_linear_grid_preserves_control_and_frozen_total_budget(campaign):
    from mt5_extracao.laboratory import ultra_entries
    db, package = campaign
    result = ultra_entries.compare(db, package, 1)
    assert result["status"] == "compared", result
    assert result["frames_compared"] == 372 and result["direct"]["metrics"]["end_equity"] == 93440
    assert result["protocol"]["risk_limit"] == package["policy"]["risk_limit"]
    assert result["linear"]["metrics"]["end_equity"] - 93440 == result["net_difference"]
    fills = [e for e in result["linear"]["decisions"] if e["event"] == "fill"]
    orders = {e["cycle_id"]: e for e in result["linear"]["decisions"] if e["event"] == "order"}
    armed = {e["cycle_id"]: e for e in result["linear"]["decisions"] if e["event"] == "armed"}
    assert fills and all(e["risk"] <= package["policy"]["risk_limit"] for e in orders.values())
    for fill in fills:
        order = orders[fill["cycle_id"]]
        side = armed[fill["cycle_id"]]["side"]
        limit = order["ask"] if side == 1 else order["bid"]
        assert side * (limit-fill["price"]) >= fill["last_level"] * fill["spacing_ticks"]
    assert result["direct"]["structure"] and result["native_status"] == "capability_absent"
    with pytest.raises(ValueError, match="stop"):
        ultra_entries.compare(db, package, 3)


@pytest.mark.parametrize("empty", [False, True], ids=["no_signal", "no_candidate"])
def test_cash_control_has_separate_tick_and_platform_acceptances(campaign, tmp_path, empty):
    from mt5_extracao.laboratory import ultra_platform as platform
    db, old = campaign
    definition = {k: deepcopy(v) for k,v in old["policy"].items() if k not in ("tick_cash","engine_hash","revision")}
    module = definition["modules"][0]
    module["params"]["entry_period"] = 100
    definition["modules"] = [] if empty else [module]
    receipt = ultra.freeze(db, definition)["receipt"]
    ultra.replay(db, receipt)
    package = compile_ultra_research(db, receipt)
    cap = ultra_tester.package_capabilities(db, package)
    assert cap["supported_origins"] == [ultra_tester.PLATFORM]
    exported = ultra_tester.export(db, package, tmp_path / "one", prepare_tester=True)
    assert "UltraConcurrent.mq5" not in exported["sha256"]
    modules, frames = ultra_tester._context(db, package, platform=True)
    assert all(e["event"] == "session" for f in frames for e in f["events"])
    from decimal import Decimal
    rev = ultra_tester._platform_revision(definition["modules"], Decimal(str(package["specification"]["tick_size"])))
    head = ("BEGIN", package["package_hash"], package["source_manifest"], rev, 6324)
    records = [head, *((k,*v) for _,_,k,_,v in ultra_tester._expected_rows(frames, modules, package["policy"])),
               ("END", len(frames), 0)]
    physical = [("BEGIN", *head[1:4], cap["tester_contract"]["symbol"], 6324, "ACCOUNT_MARGIN_MODE_RETAIL_HEDGING",
                 "USD",100000)]
    physical.extend(("ACCOUNT", *f["mark"]["key"],100000,100000,0,0,0) for f in frames)
    physical.append(("END", len(frames), 1, 0, 0, 100000, 100000))
    def compare(book_rows, platform_rows):
        text = journal(book_rows, ultra_tester.PLATFORM).decode("utf-16")
        blob = (text + "\n" + "\n".join(platform.MARKER+"|".join(map(str,r)) for r in platform_rows)).encode()
        return ultra_tester.compare(db,package,blob,origin=ultra_tester.PLATFORM)
    result = compare(records,physical)
    assert (result["status"],result["native_order_parity"],result["cycles"]) == ("platform_orders_match","journal_matches",[])
    assert result["no_order_reason"] == ("no_candidate" if empty else "no_confirmed_signal")
    if empty:
        assert package["selection_status"] == ultra.inspect_receipt(db, receipt)["status"] == "no_candidate"
        assert cap["owner_signals"] == {} and all(f["mark"]["cash"] == 100000 for f in frames)
        from mt5_extracao.laboratory import ultra_entries
        entries = ultra_entries.compare(db, package, 1)
        assert entries["net_difference"] == 0 and entries["frames_compared"] == len(frames)
    incomplete = compare(records,physical[:-1])
    assert incomplete["modeled_status"] == "modeled_book_matches" and incomplete["status"] == "platform_journal_incomplete"
    corrupted = records.copy()
    pos = next(i for i, row in enumerate(corrupted) if row[0] == "FRAME")
    corrupted[pos] = (*corrupted[pos][:3], -1, *corrupted[pos][4:])
    result = compare(corrupted,physical)
    assert result["modeled_status"] == "diverged" and result["native_order_parity"] == "journal_matches"
