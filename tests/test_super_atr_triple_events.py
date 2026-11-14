"""Protocolo Triplo com dados públicos sintéticos e decoder real do journal.

O modelo é simulado aqui para isolar adulterações do trace. A matemática do
header original é exercitada no framework, por oráculo e pelo adaptador C++.
"""
import calendar
from datetime import datetime
import hashlib

import pytest

from mt5_extracao.presets import execution
from mt5_extracao.presets import super_atr_triple_events as events
from tests import test_native_journal as journal_fixture


@pytest.fixture
def example(tmp_path, monkeypatch):
    seed = calendar.timegm(datetime(2026, 11, 1).timetuple()); at = seed+600; available = at+301
    params = dict(seed_start_broker="2026-11-01T00:00", period=2, mean=0, deviation1=1., deviation2=2., deviation3=3., initial_side=1, entries_enabled=True)
    raw = journal_fixture.fixture(); prefix = journal_fixture.ROOT+"_R_1."
    raw = {key:value for key, value in raw.items() if "_A_" not in key}; raw[journal_fixture.ROOT+"_SEQ"] = 1
    for key, value in dict(T=0, C=available, G=available-1, E=available+120).items(): raw[prefix+key] = value
    intent_key = "triple-episode-"+str(at)
    intent = journal_fixture.sha(f"HDK_INTENT1|{journal_fixture.CONTEXT}|{len(intent_key)}:{intent_key}")
    replacement = {}; journal_fixture.digest(replacement, "I", intent)
    symbol = journal_fixture.sha("EURUSD")
    payload = f"REQ1|{symbol}|12|5|"+"|".join(f"{float(raw[prefix+f]):.16e}" for f in "Q P L T K A C G N F Y J H b".split())
    journal_fixture.digest(replacement, "W", journal_fixture.sha(payload))
    raw.update({prefix+key:value for key, value in replacement.items()})
    binary = journal_fixture.document(raw); (tmp_path/"native_portfolio.state").write_bytes(binary)
    journal = journal_fixture.journal.read(binary, filename=journal_fixture.NAME, context_hash=journal_fixture.CONTEXT, symbol="EURUSD", magic=12)
    journal.update(reconciled=True, native_filename=journal_fixture.NAME)
    lines = [f"HDK_TRIPLE_START schema=1 at={seed+1} seed_at={seed}"]
    lines += [f"HDK_TRIPLE_BAR schema=1 at={seed+i*300} received_at={available} open=100 high=101 low=99 close=100" for i in range(3)]
    lines += [f"HDK_TRIPLE_DECISION schema=1 bar_at={at} confirmed_at={at+300} available_at={available} seed_at={seed} available=1 consensus=1 episode_at={at} gaps=0 first=90 second=88 third=86",
        f"HDK_TRIPLE_ENTRY schema=1 event_at={at} episode_at={at} direction=1 opened=1 state=4 order=77 deal=78 planned_volume=2 curve_stop=90 reason=filled",
        f"HDK_STRATEGY_SIGNAL event_at={at} confirmed_at={at+300} available_at={available} direction=1 opened=1 execution_state=4 volume=2 order=77 deal=78 reason=filled",
        f"HDK_TRIPLE_SIZING schema=1 event_at={at} plan_known=1 planned_volume=2 risk_money=20 risk_budget=50 stop=90 price_tick=1 margin_required=200 margin_free=1000 volume_min=0.1 volume_max=100 volume_step=0.1"]
    for ticket, moment, entry, side, price, profit, order in ((78, available, 0, 0, 100, 0, 77), (80, available+100, 1, 1, 99, -2, 79)):
        lines += [f"S09_DEAL position={journal_fixture.POSITION} deal={ticket} at_msc={moment*1000} entry={entry} type={side} volume=2 price={price} profit={profit} commission=0 swap=0 fee=0 digits=2 order={order} entry_sl=90 entry_tp=0 order_sl=90 order_tp=0 order_state=4 order_position={journal_fixture.POSITION}"]
    log = tmp_path/"Tester/Agent-QA/logs/test.log"; log.parent.mkdir(parents=True); log.write_text("\n".join(lines)+"\n")
    class Model:
        closed = False
        def __init__(self, *, workspace): assert workspace == tmp_path
        def calculate(self, bars, **kwargs):
            assert len(bars) == 3 and kwargs["deviations"] == (1., 2., 3.)
            return [dict(available=False, levels=[None]*3, consensus=None, episode_index=-1),
                    dict(available=True, levels=[90,88,86], consensus=1, episode_index=-1),
                    dict(available=True, levels=[90,88,86], consensus=1, episode_index=2)]
        def close(self): Model.closed = True
    artifact = dict(available=True, framework_commit="f"*40, qualification="TEST_MODEL_ONLY")
    monkeypatch.setattr(events.super_atr_triple, "source", lambda *a: (artifact, Model))
    rows = [dict(direction=1, entry_time=events._clock(available), exit_time=events._clock(available+100),
                 entry_price=100, exit_price=99, stop_price=90, target_price=0, volume=2, pnl=-2, exit_reason="sl")]
    def validate():
        signals = execution.native_signals(tmp_path, marker="HDK_STRATEGY_SIGNAL")
        return events.validate(tmp_path, params, signals, rows, timeframe="M5", framework_root=tmp_path,
            journal=journal, symbol="EURUSD", magic=12, partial_plan=dict(mode="strategy_default", params={}))
    return dict(validate=validate, log=log, journal=journal, directory=tmp_path, params=params,
                rows=rows, seed=seed, at=at, available=available, Model=Model)


def test_complete_trace_reconciles_original_observations_typed_fill_and_journal(example):
    reference, sizing, deals = example["validate"]()
    assert reference["source_conformity"] and not reference["external_parity"]
    assert not reference["allows_live"] and not reference["restart_operational_qualified"]
    assert len(deals) == 2 and sizing[0]["filled_volume"] == 2 and reference["bars"] == 3
    assert example["Model"].closed
    execution.validate_sizing_budgets(sizing, execution.native_signals(example["directory"], marker="HDK_STRATEGY_SIGNAL"),
        example["rows"], deposit=10000, risk_pct=.5)


@pytest.mark.parametrize("old,new", [
    ("HDK_TRIPLE_DECISION schema=1", "HDK_TRIPLE_DECISION schema=2"),
    ("consensus=1", "consensus=0"), ("first=90", "first=91"), ("gaps=0", "gaps=1"),
    ("stop=90 price_tick", "stop=91 price_tick"), ("price_tick=1", "price_tick=0"),
    ("risk_money=20", "risk_money=51"), ("margin_free=1000", "margin_free=100"),
    ("plan_known=1", "plan_known=0"), ("entry_tp=0", "entry_tp=140"),
    ("order_sl=90", "order_sl=89"), ("order_state=4", "order_state=3"),
    ("execution_state=4", "execution_state=5"),
])
def test_divergence_and_unknown_states_never_receive_acceptance(example, old, new):
    log = example["log"]; log.write_text(log.read_text().replace(old, new))
    with pytest.raises(ValueError): example["validate"]()


def test_restart_cannot_rearm_an_old_consensus_episode(example):
    log = example["log"]; text = log.read_text()
    text = text.replace(f"at={example['seed']+1} seed_at", f"at={example['available']} seed_at")
    log.write_text(text)
    with pytest.raises(ValueError, match="episódio anterior"): example["validate"]()


def test_duplicate_bar_with_changed_values_is_refused_across_restart(example):
    log = example["log"]
    log.write_text(log.read_text()+f"HDK_TRIPLE_BAR schema=1 at={example['seed']} received_at={example['available']+1} open=100 high=102 low=99 close=100\n")
    with pytest.raises(ValueError, match="barra já observada mudou"): example["validate"]()


def test_receipt_cannot_override_journal_bytes_or_missing_reconciliation(example):
    example["journal"]["entries"][0]["initial_stop"] = 91
    with pytest.raises(ValueError, match="bytes do diário"): example["validate"]()
    example["journal"]["reconciled"] = False
    with pytest.raises(ValueError, match="diário final"): example["validate"]()


def test_unavailable_is_distinct_from_valid_mixed_consensus(example):
    text = f"HDK_TRIPLE_START schema=1 at={example['seed']+1} seed_at={example['seed']}\n"
    text += f"HDK_TRIPLE_DECISION schema=1 bar_at={example['at']} available=0 reason=histórico ausente\n"
    _starts, bars, decisions = events._observations(text, example["params"], 300)
    assert not bars and decisions == [dict(bar_at=example["at"], available=False, reason="histórico ausente")]
    assert "consensus" not in decisions[0]
