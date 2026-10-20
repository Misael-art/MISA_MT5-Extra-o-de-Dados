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
