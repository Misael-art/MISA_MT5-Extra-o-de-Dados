"""T19.23 inicial: inputs realmente lidos, sem terminal real e sem aceite por zero negócios."""
import configparser
import copy
import csv
import io
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text

from mt5_extracao.robots import catalog, mql, native_inputs, sessions, store

FIELDS = {"Period": "int", "Risk": "number", "Enabled": "bool", "PresetId": "token"}
EXPECTED = {"Period": 21, "Risk": .25, "Enabled": True, "PresetId": "manual_01"}
SID = "a" * 32


@pytest.fixture
def robot(tmp_path):
    source = tmp_path / "InputQA.mq5"
    source.write_text('input int Period=20;\ninput double Risk=0.5;\ninput bool Enabled=false;\n'
                      'input string PresetId="default";\nint OnInit(){return INIT_SUCCEEDED;}\nvoid OnTick(){}\n')
    return catalog.robot_from_file(source)


def rows(session=SID):
    return [["mt5x-native-inputs", "1", session, "EURUSD", "PERIOD_M1", "1", "0", "1791936000", "4"],
            ["input", "Period", "int", "21"], ["input", "Risk", "number", "0.25"],
            ["input", "Enabled", "bool", "1"], ["input", "PresetId", "token", "manual_01"], ["end", "4"]]


def receipt_file(path, data):
    stream = io.StringIO(); csv.writer(stream, delimiter=";", lineterminator="\r\n").writerows(data)
    path.write_bytes(stream.getvalue().encode("utf-16"))
    return path


def read(path, **overrides):
    kwargs = dict(session_id=SID, symbol="EURUSD", timeframe="M1", fields=FIELDS, expected=EXPECTED, complete=True)
    kwargs.update(overrides)
    return native_inputs.read(path, **kwargs)


def test_native_values_from_utf16_preserve_types_and_do_not_approve_operations(tmp_path):
    p = receipt_file(tmp_path/"receipt.csv", rows())
    proof = read(p)
    assert proof["values"] == EXPECTED and isinstance(proof["values"]["Period"], int)
    assert proof["preset_verified"] and proof["compared"] and proof["complete"]
    assert not proof["operational_acceptance"] and proof["demo"] and proof["tester"]
    assert len(proof["sha256"]) == 64
    assert not read(p, complete=False)["preset_verified"]
    assert not read(p, expected=None)["preset_verified"]


@pytest.mark.parametrize("index,value", [(0,"other"),(1,"2"),(2,"b"*32),(3,"USDJPY"),
                                        (4,"PERIOD_M5"),(5,"0"),(6,"2"),(7,"0"),(8,"3")])
def test_mismatched_context_or_version_fails_closed(tmp_path,index,value):
    data=rows();data[0][index]=value
    with pytest.raises(ValueError): read(receipt_file(tmp_path/"receipt.csv",data))


@pytest.mark.parametrize("change", ["duplicate","unknown","type","missing","footer","nan","bool","suffix"])
def test_malformed_or_missing_inputs_cannot_be_a_loaded_preset(tmp_path,change):
    data=rows()
    if change=="duplicate": data[2]=data[1].copy()
    elif change=="unknown": data[1][1]="Secret"
    elif change=="type": data[1][2]="number"
    elif change=="missing": data.pop(2)
    elif change=="footer": data.pop()
    elif change=="nan": data[2][3]="nan"
    elif change=="bool": data[3][3]="2"
    elif change=="suffix": data[1][3]="21||22"
    with pytest.raises(ValueError): read(receipt_file(tmp_path/"receipt.csv",data))


def test_expected_value_is_checked_against_actual_not_echoed_hash(tmp_path):
    data=rows();data[1][3]="20"
    with pytest.raises(ValueError,match="Period.*arquivo .set"):
        read(receipt_file(tmp_path/"receipt.csv",data))
    assert native_inputs.expected_values(dict(EXPECTED,Period="21||21||1||30||N"),FIELDS)==EXPECTED
    with pytest.raises(ValueError,match="lista pública"):
        native_inputs.expected_values({"Period":21},FIELDS)


def test_opt_in_wrapper_reads_variables_after_original_init_before_any_tick(robot):
    old=sessions.telemetry_source(robot,"status.txt")
    source=sessions.telemetry_source(robot,"status.txt",input_receipt=dict(
        filename=f"mt5x_session_{SID}_inputs.csv",session_id=SID,fields=FIELDS))
    assert "MT5X_RecordInputs" not in old
    assert '"Period","int",IntegerToString((long)Period)' in source
    assert '"Risk","number",DoubleToString(Risk,16)' in source
    assert '"PresetId","token",PresetId' in source
    assert "OrderSend(" not in source and "ACCOUNT_TRADE_MODE_DEMO" in source
    init=source[source.rindex("int OnInit() {"):]
    assert init.index("MT5X_OnInit()") < init.index("MT5X_RecordInputs()") < init.index("MT5X_Chart()")
    assert "!MT5X_RecordInputs()) return INIT_FAILED" in init
    with pytest.raises(ValueError,match="Tester DEMO"):
        sessions.telemetry_source(robot,"status.txt",account={"demo":True,"login":1,"server":"Demo"},
                                  input_receipt=dict(filename="x",session_id=SID,fields=FIELDS))


def test_unknown_inputs_or_types_and_live_receipt_rejected_before_launch(robot,tmp_path):
    for fields in ({"Secret":"token"},{"Risk":"int"},{"Risk":"code"},{"bad\"name":"token"}):
        with pytest.raises(ValueError):native_inputs.fields_for(robot,fields)
    with pytest.raises(ValueError,match="sessão atual"):
        native_inputs.source(robot,"../x",SID,FIELDS)
    with pytest.raises(ValueError,match="Tester DEMO"):
        sessions.Session(None,configparser.ConfigParser(),robot,"EURUSD","M1",mode="play",
                         input_receipt_fields=FIELDS,root=tmp_path)


@pytest.fixture
def db(tmp_path):
    value=SimpleNamespace(engine=create_engine("sqlite:///"+str(tmp_path/"test.sqlite")))
    yield value
    value.engine.dispose()


def save(db,session):
    record=dict(run_id="one",robot_key=session.robot.key,source="strategy tester",symbol="EURUSD",timeframe="M1",
                data_start="2026-10-14",data_end="2026-10-14",bars=20,params={"Period":"21"},
                metrics={"artifact_directory":str(session.directory),"trades":0,"net_profit":0},
                equity=[],trades=[],created_at="2026-10-14")
    store.save_run(db,record);session.record=record
    return record


def test_session_receipt_is_persisted_reopened_idempotently_without_rewriting_run(robot,tmp_path,db):
    session=sessions.Session(db,configparser.ConfigParser(),robot,"EURUSD","M1",root=tmp_path/"sessions",
                             input_receipt_fields=FIELDS,expected_native_inputs=EXPECTED)
    session.directory.mkdir(parents=True);common=tmp_path/"common";common.mkdir()
    receipt_file(common/session.input_receipt_file,rows(session.id))
    record=save(db,session)
    with db.engine.connect() as conn: before=dict(conn.execute(text("SELECT * FROM _robot_runs")).mappings().one())
    session._verify_input_receipt(common);session._verify_input_receipt(common)
    reopened,=store.load_runs(db)
    assert reopened["native_inputs"]["values"]==EXPECTED
    assert reopened["native_inputs"]["preset_verified"] and not reopened["native_inputs"]["operational_acceptance"]
    assert reopened["metrics"]["trades"]==0 and (session.directory/"native_inputs.csv").is_file()
    with db.engine.connect() as conn: assert dict(conn.execute(text("SELECT * FROM _robot_runs")).mappings().one())==before
    altered=dict(session.native_input_receipt,known_at=1)
    with pytest.raises(ValueError,match="outro recibo"):
        store.save_native_input_receipt(db,record["run_id"],altered,artifact_directory=session.directory,robot_key=robot.key)
    with pytest.raises(ValueError,match="diretório próprios"):
        store.save_native_input_receipt(db,record["run_id"],session.native_input_receipt,
                                        artifact_directory=tmp_path/"foreign",robot_key=robot.key)


def test_missing_or_mismatched_receipt_keeps_legacy_run_unapproved(robot,tmp_path,db):
    session=sessions.Session(db,configparser.ConfigParser(),robot,"EURUSD","M1",root=tmp_path/"sessions",
                             input_receipt_fields=FIELDS,expected_native_inputs=EXPECTED)
    session.directory.mkdir(parents=True);common=tmp_path/"common";common.mkdir();save(db,session)
    with pytest.raises(ValueError,match="não encontrado"):session._verify_input_receipt(common)
    data=rows(session.id);data[1][3]="20";receipt_file(common/session.input_receipt_file,data)
    with pytest.raises(ValueError,match="Preset não corresponde"):session._verify_input_receipt(common)
    assert "native_inputs" not in store.load_runs(db)[0]
    assert session.native_input_receipt is None
