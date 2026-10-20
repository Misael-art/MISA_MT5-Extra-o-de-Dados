from copy import deepcopy
from pathlib import Path
import pytest
from mt5_extracao.presets.cooldown_events import read_events
from mt5_extracao.presets.execution import first_difference

FILTERS=[{"mode":"cooldown_v1","params":{"seconds":60}}]

def setup(tmp_path):
    path=tmp_path/"Tester/Agent-X/logs/test.log";path.parent.mkdir(parents=True)
    signals=[dict(event_at=99,direction=1,opened=True,deal_ticket=1),dict(event_at=179,direction=-1,opened=True,deal_ticket=4)]
    deals=[dict(position=10,deal=1,entry=0,at_msc=100000,volume=.1),dict(position=10,deal=2,entry=1,at_msc=110000,volume=.04),
           dict(position=10,deal=3,entry=1,at_msc=120250,volume=.06),dict(position=11,deal=4,entry=0,at_msc=180250,volume=.1)]
    lines=[]
    for origin,at,last,allowed in [(99,100000,0,1),(119,120250,120250,0),(178,180249,120250,0),(179,180250,120250,1)]:
        direction=-1 if origin==179 else 1
        lines.append(f"HDK_SESSION event=check event_at={origin} at={at//1000} direction={direction} enabled=0 session_start=0 weekday=0 minute=0 cycles=0 allowed=1 reason=disabled")
        lines.append(f"HDK_COOLDOWN event_at={origin} at_msc={at} direction={direction} last_close_msc={last} seconds=60 allowed={allowed} reason={'allowed' if allowed else 'cooldown'}")
    path.write_text("\n".join(lines));return path,signals,deals

def test_reference_uses_final_not_partial_close_and_is_causal(tmp_path):
    path,signals,deals=setup(tmp_path)
    rows=read_events(tmp_path,FILTERS,signals,deals)
    assert rows[-1]["allowed"] and not rows[-2]["allowed"]
    assert rows[1]["last_close_msc"]==120250
    future=deals+[dict(position=11,deal=5,entry=1,at_msc=200000,volume=.1)]
    assert rows==read_events(tmp_path,FILTERS,signals,future)
    reference={"ledger":[],"signals":[],"cooldown_events":rows}
    assert first_difference(reference,[],[],cooldown=rows) is None
    changed=deepcopy(rows);changed[1]["last_close_msc"]-=1
    assert first_difference(reference,[],[],cooldown=changed)["section"]=="cooldown_events"

@pytest.mark.parametrize("old,new",[("last_close_msc=120250","last_close_msc=110000"),("seconds=60","seconds=59"),
    ("at_msc=180249","at_msc=180250"),("reason=cooldown","reason=history_unavailable"),
    ("reason=cooldown","reason="),("HDK_COOLDOWN event_at=119","HDK_COOLDOWN event_at=118")])
def test_tamper_and_absence_are_not_accepted(tmp_path,old,new):
    path,signals,deals=setup(tmp_path);path.write_text(path.read_text().replace(old,new))
    with pytest.raises(ValueError):read_events(tmp_path,FILTERS,signals,deals)

def test_no_input_no_events_and_missing_required_event(tmp_path):
    path,signals,deals=setup(tmp_path)
    with pytest.raises(ValueError):read_events(tmp_path,[],signals,deals)
    lines=path.read_text().splitlines();path.write_text("\n".join(l for l in lines if not l.startswith("HDK_COOLDOWN event_at=119")))
    with pytest.raises(ValueError,match="Faltam"):read_events(tmp_path,FILTERS,signals,deals)
    path.write_text("\n".join(lines+lines[-2:]))
    with pytest.raises(ValueError):read_events(tmp_path,FILTERS,signals,deals)
