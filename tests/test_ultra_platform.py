"""Transcrição sintética mínima com tickets independentes dos valores do livro Python."""
import json

import pytest

from mt5_extracao.laboratory import ultra_platform as platform
from mt5_extracao.laboratory.ultra_cycle_report import write_report


PACKAGE = {"package_hash": "p", "source_manifest": "m", "specification": {"tick_size": "0.25", "volume_step": 0.1},
           "policy": {"currency": "USD", "cash": 1000, "tick_cash": 5, "fee": 1, "margin": 10, "risk_limit": 400,
                      "modules": [{"owner": "A", "strategy": "<regra>"}]}}
JOURNAL = """BEGIN|p|m|r|UT|6324|ACCOUNT_MARGIN_MODE_RETAIL_HEDGING|USD|1000
REQUEST|1000|0|0|1000|TRADE_ACTION_PENDING|0|0|ORDER_TYPE_BUY_LIMIT|1|101|90|103|0|1|10008|11|0|7|1
ORDER|1000|0|11|2000|ORDER_TYPE_BUY_LIMIT|1|101|90|103
ACCOUNT|1000|0|1000|1000|10|1|0
TX|TRADE_TRANSACTION_REQUEST|0|0|0|2000|7|10008|0|0
TX|TRADE_TRANSACTION_DEAL_ADD|11|21|11|0|0|0|1|101
POSITION|1100|0|11|2000|11|POSITION_TYPE_BUY|1|25.25|90|103
ACCOUNT|1100|0|999|994|10|0|1
TX|TRADE_TRANSACTION_DEAL_ADD|12|22|11|0|0|0|1|103
ACCOUNT|1200|0|1008|1008|0|0|0
HISTORY|11|2000|11|ORDER_STATE_FILLED|ORDER_TYPE_BUY_LIMIT|1100|1|0|90|103
HISTORY|12|0|11|ORDER_STATE_FILLED|ORDER_TYPE_SELL|1200|1|0|0|0
DEAL|21|11|11|2000|1100|DEAL_ENTRY_IN|DEAL_TYPE_BUY|DEAL_REASON_EXPERT|1|101|-1|0|0|0
DEAL|22|12|11|0|1200|DEAL_ENTRY_OUT|DEAL_TYPE_SELL|DEAL_REASON_TP|1|103|-1|0|0|10
END|3|1|2|2|1008|1008"""


def compare(source=JOURNAL):
    common = {"owner": "A", "cycle_id": "a", "cycle_version": "v"}
    def e(kind, **fields):
        return dict(common, event=kind, **fields)
    frames = [
        {"mark": {"key": [1000, 0], "bid": 100, "ask": 101, "cash": 1000, "equity": 1000, "margin": 10},
         "events": [e("armed", available_msc=1000, side=1), e("order", steps=1, stop=90, target=103)]},
        {"mark": {"key": [1100, 0], "bid": 100, "ask": 101, "cash": 999, "equity": 994, "margin": 10},
         "events": [e("fill", steps=1, price=101, fee=1, stop=90, target=103)]},
        {"mark": {"key": [1200, 0], "bid": 103, "ask": 104, "cash": 1008, "equity": 1008, "margin": 0},
         "events": [e("exit", steps=1, price=103, fee=1, reason="target")]}]
    return platform.reconcile(PACKAGE, frames, [platform.MARKER + s for s in source.splitlines()], "UT", "r", 6324)


def test_cycle_from_intent_to_balance_and_accessible_report(tmp_path):
    result = compare()
    cycle, = result["cycles"]
    assert result["status"] == "platform_orders_match" and [r["ticket"] for r in cycle["deals"]] == [21, 22]
    assert [r["ticket"] for r in cycle["history"]] == [11, 12]
    assert len(cycle["snapshots"]) == len(cycle["transactions"]) == 3
    json.dumps(result, allow_nan=False)
    path = tmp_path / "cycles.html"
    write_report(dict(result, origin="strategy_tester_orders"), path)
    html = path.read_text(encoding="utf-8")
    assert "<summary>A · a</summary>" in html and "&lt;regra&gt;" in html
    with pytest.raises(FileExistsError):
        write_report(dict(result, origin="strategy_tester_orders"), path)


def test_slices_aggregate_two_physical_tickets_and_keep_tick_precision():
    common = {"owner": "A", "cycle_id": "a", "cycle_version": "v"}
    def event(kind, **fields):
        return dict(common, event=kind, **fields)
    fill = event("fill", steps=1, price=101, fee=1, stop=90, target=103)
    frames = [
        {"mark": {"key": [1000, 0], "bid": 100, "ask": 101, "cash": 999, "equity": 994, "margin": 20},
         "events": [event("armed", available_msc=1000, side=1), event("order", steps=2, stop=90, target=103), fill]},
        {"mark": {"key": [1100, 0], "bid": 100, "ask": 101, "cash": 998, "equity": 988, "margin": 20},
         "events": [fill]},
        {"mark": {"key": [1200, 0], "bid": 103, "ask": 104, "cash": 1016, "equity": 1016, "margin": 0},
         "events": [event("exit", steps=2, price=103, fee=2, reason="target")]},
    ]
    rows = """BEGIN|p|m|r|UT|6324|ACCOUNT_MARGIN_MODE_RETAIL_HEDGING|USD|1000
REQUEST|1000|0|0|1000|TRADE_ACTION_DEAL|0|0|ORDER_TYPE_BUY|1|101|90|103|0|1|10009|11|21|7|1
TX|TRADE_TRANSACTION_REQUEST|11|21|0|2000|7|10009|1|101
TX|TRADE_TRANSACTION_DEAL_ADD|11|21|11|0|0|0|1|101
POSITION|1000|0|11|2000|11|POSITION_TYPE_BUY|1|25.25|90|103
ACCOUNT|1000|0|999|994|10|0|1
REQUEST|1100|0|0|1000|TRADE_ACTION_DEAL|0|0|ORDER_TYPE_BUY|1|101|90|103|0|1|10009|12|22|8|1
TX|TRADE_TRANSACTION_REQUEST|12|22|0|2000|8|10009|1|101
TX|TRADE_TRANSACTION_DEAL_ADD|12|22|12|0|0|0|1|101
POSITION|1100|0|11|2000|11|POSITION_TYPE_BUY|1|25.25|90|103
POSITION|1100|0|12|2000|12|POSITION_TYPE_BUY|1|25.25|90|103
ACCOUNT|1100|0|998|988|20|0|2
TX|TRADE_TRANSACTION_DEAL_ADD|13|23|11|0|0|0|1|103
TX|TRADE_TRANSACTION_DEAL_ADD|14|24|12|0|0|0|1|103
ACCOUNT|1200|0|1016|1016|0|0|0
HISTORY|11|2000|11|ORDER_STATE_FILLED|ORDER_TYPE_BUY|1000|1|0|90|103
HISTORY|12|2000|12|ORDER_STATE_FILLED|ORDER_TYPE_BUY|1100|1|0|90|103
HISTORY|13|0|11|ORDER_STATE_FILLED|ORDER_TYPE_SELL|1200|1|0|0|0
HISTORY|14|0|12|ORDER_STATE_FILLED|ORDER_TYPE_SELL|1200|1|0|0|0
DEAL|21|11|11|2000|1000|DEAL_ENTRY_IN|DEAL_TYPE_BUY|DEAL_REASON_EXPERT|1|101|-1|0|0|0
DEAL|22|12|12|2000|1100|DEAL_ENTRY_IN|DEAL_TYPE_BUY|DEAL_REASON_EXPERT|1|101|-1|0|0|0
DEAL|23|13|11|0|1200|DEAL_ENTRY_OUT|DEAL_TYPE_SELL|DEAL_REASON_TP|1|103|-1|0|0|10
DEAL|24|14|12|0|1200|DEAL_ENTRY_OUT|DEAL_TYPE_SELL|DEAL_REASON_TP|1|103|-1|0|0|10
END|3|1|4|4|1016|1016"""
    def sliced(source):
        return platform.reconcile(PACKAGE, frames, [s if s.startswith("position closed") else platform.MARKER + s
                                                     for s in source.splitlines()],
                                  "UT", "r", 6324, sliced=True)
    assert sliced(rows)["status"] == "platform_orders_match"
    first = sliced(rows.replace("DEAL|21|11|11|2000|1000", "DEAL|21|11|11|2000|1100"))["first_divergence"]
    assert (first["ordinal"], first["field"]) == (1, "deal.execution")
    frames[-1] = {"mark": {"key": [1200, 0], "bid": 102, "ask": 103, "cash": 998, "equity": 1008, "margin": 20}, "events": []}
    closed = rows.replace("TX|TRADE_TRANSACTION_DEAL_ADD|13|23|11|0|0|0|1|103\n", "").replace(
        "TX|TRADE_TRANSACTION_DEAL_ADD|14|24|12|0|0|0|1|103\n", "").replace(
        "ACCOUNT|1200|0|1016|1016|0|0|0", "POSITION|1200|0|11|2000|11|POSITION_TYPE_BUY|1|25.25|90|103\n"
        "POSITION|1200|0|12|2000|12|POSITION_TYPE_BUY|1|25.25|90|103\nACCOUNT|1200|0|998|1008|20|0|2").replace(
        "DEAL_REASON_TP|1|103|-1|0|0|10", "DEAL_REASON_CLIENT|1|102|-1|0|0|5").replace(
        "END|3|1|4|4|1016|1016", "position closed due end of test at 25.50 [#11 buy 1 UT]\n"
        "position closed due end of test at 25.50 [#12 buy 1 UT]\nEND|3|1|4|4|1006|1006")
    result = sliced(closed)
    assert result["status"] == "platform_orders_match"
    assert result["end_settlement"] == {"marked_equity": 1008, "exit_commission": 2,
                                         "expected_settled_cash": 1006, "observed_tickets": [23, 24]}
    unproven = sliced(closed.replace("position closed due end of test at 25.50 [#11 buy 1 UT]\n", ""))
    assert unproven["first_divergence"]["field"] == "deal.execution"


@pytest.mark.parametrize("old,new,field,ordinal", [
    ("|25.25|90|103", "|25.25|89|103", "position.protected", 2),
    ("|25.25|90|103", "|25.50|90|103", "position.price_times_steps", 2),
    ("|1|101|-1|0|0|0", "|1|101|-2|0|0|0", "deal.execution", 2),
    ("|1|101|-1|0|0|0", "|1|101|0|-1|0|0", "deal.execution", 2),
    ("ORDER_TYPE_SELL|1200|1|0", "ORDER_TYPE_SELL|1200|2|0", "history.filled_volume", 3),
    ("|2000|7|10008|0|0", "|2000|8|10008|0|0", "transaction.request", 1),
    ("|0|1|10008|11|0|7", "|0|0|10006|11|0|7", "request.accepted", 1),
    ("DEAL_ADD|12|22|11", "DEAL_ADD|12|23|11", "deal.ticket_links", 3),
    ("END|3", "TX|TRADE_TRANSACTION_DEAL_ADD|99|99|99|0|0|0|1|99\nEND|3", "history.unbound", 3),
    ("REQUEST|1000", "VETO|1000|0|0|1000|netting_independent_protection\nREQUEST|1000", "platform.veto", 1),
])
def test_first_difference_with_equal_closing_equity(old, new, field, ordinal):
    assert old in JOURNAL
    result = compare(JOURNAL.replace(old, new))
    assert (result["first_divergence"]["field"], result["first_divergence"]["ordinal"]) == (field, ordinal)
    assert result["platform_final"]["equity"] == 1008


def test_first_callback_with_zero_request_id_uses_matching_order_ticket_and_magic():
    source = JOURNAL.replace("|10008|11|0|7|1", "|10008|11|0|0|1")
    source = source.replace("REQUEST|0|0|0|2000|7|10008", "REQUEST|11|0|0|2000|0|10008")
    result = compare(source)
    assert result["status"] == "platform_orders_match" and result["unbound_history"] == []
    for wrong in ("REQUEST|12|0|0|2000|0|10008", "REQUEST|11|0|0|2001|0|10008"):
        mismatched = compare(source.replace("REQUEST|11|0|0|2000|0|10008", wrong))
        assert mismatched["first_divergence"]["field"] == "transaction.request"
        assert mismatched["unbound_history"]


def test_incomplete_and_duplicate_traces():
    with pytest.raises(ValueError, match="completa"):
        compare(JOURNAL.rpartition("\nEND|")[0])
    with pytest.raises(ValueError, match="duplicado"):
        compare(JOURNAL.replace("HISTORY|12", "HISTORY|11"))


def test_platform_report_matches_deals_and_detects_altered_ticket_and_cost():
    def row(fields):
        return "<tr>"+"".join("<td>"+v+"</td>" for v in ["2026.01.01 00:00", *fields])+"</tr>"
    base = row(["1", "", "balance", "", "", "", "", "", "", "10", "10", ""])
    html = base + row(["21","UT","buy","in","0.1","25.25","11","-0.01","0","0","9.99",""]) + row(
        ["22","UT","sell","out","0.1","25.75","12","-0.01","0","0.10","10.08",""])
    result = compare()
    assert platform.compare_report(PACKAGE, result, html) == {"status":"report_matches","deals":2,"cash":1008}
    assert platform.compare_report(PACKAGE, result, html.replace(">22<",">23<"))["field"] == "deal_ticket"
    assert platform.compare_report(PACKAGE, result, html.replace(">-0.01<",">0<"))["field"] == "commission_and_fee"
    assert platform.compare_report(PACKAGE, result, base)["field"] == "missing_deals"
