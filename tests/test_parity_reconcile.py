"""Reconciliação um a um: sinal, proteção, execução e financeiro separados; vínculo causal por identificadores."""
from datetime import datetime, timezone

import pytest

from mt5_extracao.robots import parity

ENTRY = "2026-10-01 08:30:00"
BAR = str(int(datetime(2026, 10, 1, 8, 25, tzinfo=timezone.utc).timestamp()))   # barra M5 do sinal (decisão 08:30)


def py(**changes):
    base = dict(entry_time=ENTRY, exit_time="2026-10-01 10:00:00", direction=1, exit_reason="stop", volume=1.0,
                stop_price=1.1, target_price=1.3, entry_price=1.2, exit_price=1.1, pnl=-100.0)
    base.update(changes)
    return base


def nat(**changes):
    base = dict(entry_time="2026.10.01 08:30:05", time="2026.10.01 10:00:12", direction="buy", exit_reason="sl",
                volume="1.00", sl="1.10000", tp="1.30000", entry_price="1.2", exit_price="1.1", result="-100",
                commission="0", swap="0", fee="0", position_id="2")
    base.update(changes)
    return base


def log(*lines):
    return "\n".join(lines) + "\n"


DECISION = "2026.10.01 08:30:00   SAREF_DECISION decision=2026.10.01 08:30 side=1 close=1.2 atr=0.01 stop=1.1 run=120 eligible=1 orders=1"
SIGNAL = f"2026.10.01 08:30:00   HDK_STRATEGY_SIGNAL event_at={BAR} direction=1 opened=1 execution_state=4 order=2 deal=2 reason="
ZERO = dict(commission=0.0, swap=0.0, fee=0.0)


def reconcile(python, native, text=log(DECISION, SIGNAL), costs=ZERO, **kw):
    return parity.reconcile_trades(python, native, events=parity.parse_strategy_events(text), python_costs=costs, **kw)


def test_fully_equal_pair_with_causal_chain_is_identical():
    report = reconcile([py()], [nat()])
    row = report["rows"][0]
    assert row["identical"] and row["link"] == "confirmed" and row["flags"] == []
    assert report["summary"]["links"]["confirmed"] == 1 and report["summary"]["identical"] == 1


@pytest.mark.parametrize("field,native_change", [
    ("entry_price", dict(entry_price="1.20002")), ("exit_price", dict(exit_price="1.10003")),
    ("result", dict(result="-100.5")), ("commission", dict(commission="-3.5")), ("swap", dict(swap="-0.2")),
    ("fee", dict(fee="-0.1")), ("volume", dict(volume="0.99")), ("sl", dict(sl="1.10001")), ("tp", dict(tp="1.29990"))])
def test_every_compared_field_breaks_identity(field, native_change):
    row = reconcile([py()], [nat(**native_change)])["rows"][0]
    assert row["comparisons"][field]["state"] == "different" and not row["identical"]
    assert f"{field}:different" in row["flags"]


@pytest.mark.parametrize("value", ["", "nan", "inf", "abc"])
def test_missing_or_non_finite_is_never_zero_or_equal(value):
    row = reconcile([py()], [nat(commission=value)])["rows"][0]
    assert row["comparisons"]["commission"]["state"] == "missing_native" and not row["identical"]


def test_python_costs_unknown_keep_financial_group_open():
    row = reconcile([py()], [nat()], costs=None)["rows"][0]
    assert row["comparisons"]["fee"]["state"] == "missing_python" and not row["groups"]["financial"]
    assert not row["identical"]


def test_tolerance_is_explicit_and_not_widened():
    report = reconcile([py()], [nat(entry_price="1.200004")])
    assert report["rows"][0]["comparisons"]["entry_price"]["state"] == "equal"
    assert report["summary"]["tolerances"] == parity.TOLERANCES


def test_opposite_or_ineligible_decision_is_not_a_link():
    opposite = DECISION.replace("side=1", "side=-1")
    assert reconcile([py()], [nat()], text=log(opposite, SIGNAL))["rows"][0]["link"] == "incompatible"
    ineligible = DECISION.replace("eligible=1", "eligible=0")
    assert reconcile([py()], [nat()], text=log(ineligible, SIGNAL))["rows"][0]["link"] == "incompatible"


def test_missing_decision_and_time_only_candidate():
    assert reconcile([py()], [nat()], text=log(SIGNAL))["rows"][0]["link"] == "missing"
    row = reconcile([py()], [nat(position_id="9")])["rows"][0]                  # ordem do host ≠ posição
    assert row["link"] == "candidate_temporal" and not row["identical"]


def test_repeated_decision_in_same_source_is_ambiguous():
    repeated = DECISION.replace("close=1.2", "close=1.21")
    assert reconcile([py()], [nat()], text=log(DECISION, repeated, SIGNAL))["rows"][0]["link"] == "ambiguous"


def test_copies_from_two_log_sources_are_one_event():
    text = log("===== Tester/Agent/logs/a.log =====", DECISION, SIGNAL,
               "===== Tester/logs/b.log =====", DECISION, SIGNAL)
    events = parity.parse_strategy_events(text)
    assert len(events["decision"]) == 1 and len(events["signal"]) == 1
    assert reconcile([py()], [nat()], text=text)["rows"][0]["link"] == "confirmed"


def test_same_minute_repetition_is_reported_not_overwritten():
    report = reconcile([py()], [nat(position_id="2"), nat(entry_time="2026.10.01 08:30:40", position_id="8")])
    assert report["summary"]["collisions"] and report["summary"]["paired"] == 1
    assert report["only_native"][0]["position_id"] == "8"


def test_empty_csv_reason_is_filled_only_from_host_log_of_that_position():
    rows = [nat(exit_reason="", position_id="2")]
    closes = parity.parse_close_lines("x Posição #2 fechada (fim do dia)\ny Posição #99 fechada (fim do dia)\n")
    row = reconcile([py(exit_reason="fim do dia")], rows, host_closes=closes)["rows"][0]
    assert row["native_reason"] == "fim do dia" and row["native_reason_source"] == "log_host"
    row = reconcile([py(exit_reason="fim do dia")], [nat(exit_reason="", position_id="7")], host_closes=closes)["rows"][0]
    assert row["native_reason"] == "(vazio)" and "exit_reason:different" in row["flags"]


def test_expert_close_reason_is_refined_only_by_host_log():
    closes = parity.parse_close_lines("Posição #2 fechada (fim do dia)\n")
    row = reconcile([py(exit_reason="fim do dia")], [nat(exit_reason="expert")], host_closes=closes)["rows"][0]
    assert row["native_reason"] == "fim do dia" and row["native_reason_source"] == "log_host"
    row = reconcile([py(exit_reason="fim do dia")], [nat(exit_reason="expert")])["rows"][0]
    assert row["native_reason"] == "expert" and "exit_reason:different" in row["flags"]     # sem prova, não vira fim do dia


def _sources(*blocks):
    return log(*[line for name, lines in blocks for line in (f"===== {name} =====", *lines)])


@pytest.mark.parametrize("order", [("A", "B"), ("B", "A")])
def test_dedup_is_independent_of_source_order(order):
    blocks = {"A": ("A", (DECISION, SIGNAL)), "B": ("B", (DECISION, DECISION, SIGNAL))}
    text = _sources(*(blocks[k] for k in order))
    events = parity.parse_strategy_events(text)
    assert len(events["decision"]) == 2                        # repetição dentro de B preservada
    assert events["decision"][0]["_sources"] == {"A": 1, "B": 2}
    assert reconcile([py()], [nat()], text=text)["rows"][0]["link"] == "ambiguous"


@pytest.mark.parametrize("order", [("A", "B"), ("B", "A")])
def test_pure_copies_across_sources_confirm_in_any_order(order):
    blocks = {"A": ("A", (DECISION, SIGNAL)), "B": ("B", (DECISION, SIGNAL))}
    text = _sources(*(blocks[k] for k in order))
    assert reconcile([py()], [nat()], text=text)["rows"][0]["link"] == "confirmed"


def test_conflicting_events_across_sources_stay_ambiguous():
    other = DECISION.replace("stop=1.1", "stop=1.15")
    text = _sources(("A", (DECISION, SIGNAL)), ("B", (other, SIGNAL)))
    assert len(parity.parse_strategy_events(text)["decision"]) == 2
    assert reconcile([py()], [nat()], text=text)["rows"][0]["link"] == "ambiguous"


@pytest.mark.parametrize("counts", [(1,2,1),(2,3,2),(1,1,1)])
def test_all_source_permutations_keep_exact_counts_and_signal_repetition(counts):
    from itertools import permutations
    blocks=[(name,(DECISION,)*n+(SIGNAL,)*n) for name,n in zip("ABC",counts)]
    expected=None
    for perm in permutations(blocks):
        events=parity.parse_strategy_events(_sources(*perm))
        assert len(events["decision"])==len(events["signal"])==max(counts)
        assert events["signal"][0]["_sources"]==dict(zip("ABC",counts))
        if expected is not None:assert events==expected
        expected=events
        assert reconcile([py()],[nat()],text=_sources(*perm))["rows"][0]["link"] == ("confirmed" if max(counts)==1 else "ambiguous")


@pytest.mark.parametrize("order",[("A","B"),("B","A")])
def test_conflicting_signal_sources_never_confirm(order):
    blocks={"A":("A",(DECISION,SIGNAL)),"B":("B",(DECISION,SIGNAL+" conflict=1"))}
    assert reconcile([py()],[nat()],text=_sources(*(blocks[k] for k in order)))["rows"][0]["link"]=="ambiguous"
