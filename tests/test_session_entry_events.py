from datetime import datetime,timezone
import pytest
from mt5_extracao.presets.session_events import read_events
from mt5_extracao.strategies.session_entry import SessionEntryPolicy


def fixture(tmp_path):
    at=int(datetime(2026,1,1,9,tzinfo=timezone.utc).timestamp());day=at-9*3600
    lines=[f"HDK_SESSION event=check event_at={at-300} at={at} direction=1 enabled=1 session_start={day} weekday=3 minute=540 cycles=0 allowed=1 reason=allowed",
           f"HDK_SESSION event=fill event_at={at-300} at={at} direction=1 enabled=1 session_start={day} weekday=3 minute=540 cycles=1 allowed=1 reason=allowed",
           f"HDK_SESSION event=check event_at={at} at={at+300} direction=-1 enabled=1 session_start={day} weekday=3 minute=545 cycles=1 allowed=0 reason=quota"]
    file=tmp_path/"Tester/Agent-local/logs/test.log";file.parent.mkdir(parents=True);file.write_text("\n".join(lines))
    filters=[dict(mode="session_v1",params=SessionEntryPolicy(max_cycles=1).to_dict())]
    signals=[dict(event_at=at-300,direction=1,opened=True,deal_ticket=7)]
    deals=[dict(position=42,deal=7,entry=0,at_msc=at*1000),dict(position=42,deal=8,entry=1,at_msc=(at+30)*1000),dict(position=42,deal=9,entry=1,at_msc=(at+60)*1000)]
    return file,filters,signals,deals


def test_independent_counter_ignores_reductions_and_reconciles_first_fill(tmp_path):
    file,filters,signals,deals=fixture(tmp_path)
    rows=read_events(tmp_path,filters,signals,deals)
    assert [row["cycles"] for row in rows]==[0,1,1]
    assert rows[-1]["reason"]=="quota"


@pytest.mark.parametrize("mutation",["count","missing_fill","duplicate","allow_after_quota","clock","truncated","missing_check","unknown"])
def test_tampered_or_missing_counter_never_qualifies(tmp_path,mutation):
    file,filters,signals,deals=fixture(tmp_path);text=file.read_text();lines=text.splitlines()
    if mutation=="count":text=text.replace("minute=545 cycles=1","minute=545 cycles=0")
    if mutation=="missing_fill":text="\n".join([lines[0],lines[2]])
    if mutation=="duplicate":text+="\n"+lines[0]
    if mutation=="allow_after_quota":text=text.replace("allowed=0 reason=quota","allowed=1 reason=allowed")
    if mutation=="clock":text=text.replace("weekday=3 minute=540","weekday=2 minute=540")
    if mutation=="truncated":text=text[:-4]
    if mutation=="missing_check":text="\n".join(lines[1:])
    if mutation=="unknown":text=text.replace("cycles=1 allowed=0 reason=quota","cycles=-1 allowed=0 reason=history_unavailable")
    file.write_text(text)
    with pytest.raises(ValueError):read_events(tmp_path,filters,signals,deals)
