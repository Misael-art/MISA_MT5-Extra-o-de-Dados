"""Eventos de dimensionamento: ausência/divergência nunca aprova um preenchimento."""
from pathlib import Path
import pytest
from mt5_extracao.presets.execution import native_sizing,first_difference

@pytest.fixture
def log(tmp_path):
    path=tmp_path/"Tester/Agent-local/logs/test.log";path.parent.mkdir(parents=True)
    def write(**values):
        fields=dict(event_at=100,lot_mode=0,requested_volume=2.35,planned_volume=2.35,risk_money=235,risk_budget=500,
                    margin_required=100,margin_free=1000,volume_min=.01,volume_max=100,volume_step=.01)
        fields.update(values)
        path.write_bytes(("S09_SIZING "+" ".join(f"{k}={v}" for k,v in fields.items())+"\n").encode("utf-16"))
        return tmp_path
    return write


def signal(volume=2.35,opened=True):return dict(event_at=100,opened=opened,filled_volume=volume if opened else None)
def read(log,**values):return native_sizing(log(**values),[signal()],{"InpLotMode":0,"InpFixedLot":2.35})


def test_requested_normalized_filled_risk_margin_and_native_spec_match(log):
    row,=read(log)
    assert row["requested_volume"]==row["planned_volume"]==row["filled_volume"]==2.35
    assert row["risk_money"]==235 and row["risk_budget"]==500
    assert row["margin_known"] and row["spec_known"] and row["volume_step"]==.01
    rows=native_sizing(log(lot_mode=1,requested_volume=0),[signal()],{"InpLotMode":1,"InpFixedLot":.01})
    assert rows[0]["requested_volume"] is None and rows[0]["mode"]=="risk_fraction"

@pytest.mark.parametrize("change",[{"event_at":101},{"lot_mode":1},{"requested_volume":2.36},
    {"planned_volume":2.34},{"risk_money":501},{"risk_budget":0},{"margin_required":801},
    {"margin_free":0},{"margin_required":1.7976931348623157e308},{"volume_min":0},
    {"volume_max":2},{"volume_step":.02},{"risk_money":"nan"}])
def test_divergent_or_unqualified_native_event_cannot_approve(log,change):
    with pytest.raises(ValueError):read(log,**change)


def test_rejected_entry_keeps_unknown_margin_and_missing_events_fail_closed(log,tmp_path):
    rows=native_sizing(log(planned_volume=0,risk_money=0,margin_required="unavailable",margin_free="unavailable"),
                      [signal(opened=False)],{"InpLotMode":0,"InpFixedLot":2.35})
    assert rows[0]["filled_volume"] is None and not rows[0]["margin_known"]
    assert rows[0]["margin_required"] is None
    with pytest.raises(ValueError,match="ausente"):
        native_sizing(tmp_path/"missing",[signal()],{"InpLotMode":0,"InpFixedLot":2.35})


def test_replay_reports_first_sizing_difference(log):
    rows=read(log);reference=dict(ledger=[],signals=[],sizing=rows)
    other=[{**rows[0],"risk_budget":499}]
    assert first_difference(reference,[],[],other)==dict(section="sizing",index=0,field="risk_budget",expected=500,observed=499)


def test_earlier_signal_precedes_downstream_ledger_and_fill_differences():
    ledger = [dict(entry_time="1970-01-01 00:01:40", exit_time="1970-01-01 00:02:00", pnl=5)]
    signals = [dict(event_at=80, available_at=95, direction=1)]
    fills = [dict(at_msc=120_500, commission=-1)]
    reference = dict(ledger=ledger, signals=signals, deals=fills)
    changed_ledger = [dict(ledger[0], pnl=3)]
    changed_fills = [dict(fills[0], commission=-3)]
    difference = first_difference(reference, changed_ledger, [dict(signals[0], direction=-1)], deals=changed_fills)
    assert difference == dict(section="signals", index=0, field="direction", expected=1, observed=-1)

    difference = first_difference(reference, changed_ledger, signals, deals=changed_fills)
    assert difference == dict(section="deals", index=0, field="commission", expected=-1, observed=-3)


def test_extra_later_fill_does_not_hide_an_earlier_change_in_the_same_trace():
    fills = [dict(at_msc=100_000, price=100)]
    reference = dict(ledger=[], signals=[], deals=fills)
    observed = [dict(at_msc=100_000, price=101), dict(at_msc=200_000, price=101)]
    assert first_difference(reference, [], [], deals=observed) == dict(
        section="deals", index=0, field="price", expected=100, observed=101)
    assert first_difference(reference, [], [], deals=[fills[0], observed[1]]) == dict(
        section="deals", field="count", expected=1, observed=2)
