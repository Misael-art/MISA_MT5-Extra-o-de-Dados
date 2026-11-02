"""T19.23 inicial: inputs realmente lidos, sem terminal real e sem aceite por zero negócios."""
import configparser
import copy
import csv
import hashlib
import io
import json
import math
from pathlib import Path
import re
import subprocess
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


def test_runtime_binding_has_independent_canonical_payload_and_requires_complete_inputs(robot):
    expected = ("HDK_EFFECTIVE_INPUTS1|4|7:Enabled4:bool1:16:Period3:int2:21"
                "8:PresetId5:token9:manual_014:Risk6:number20:4503599627370496p-54")
    assert native_inputs.binding_payload(robot, EXPECTED, FIELDS) == expected
    assert native_inputs.binding_payload(robot, dict(reversed(list(EXPECTED.items()))), dict(reversed(list(FIELDS.items())))) == expected
    with pytest.raises(ValueError, match="todos os inputs"):
        native_inputs.binding_payload(robot, {"Period":21}, {"Period":"int"})
    with pytest.raises(ValueError, match="64 bits"):
        native_inputs.binding_payload(robot, dict(EXPECTED, Period=2**63), FIELDS)
    for value in (math.inf, math.nan, float.fromhex("0x1.fffffffffffffp+1023")):
        with pytest.raises(ValueError):
            native_inputs.binding_payload(robot, dict(EXPECTED, Risk=value), FIELDS)


def test_actual_mql_binding_serialization_matches_binary64_on_simulated_platform(robot,tmp_path):
    """Compila o MQL gerado como C++; não representa MT5 ou aceite de broker."""
    # MQL long é int64 também no Windows; C++ long é int32 nesse host.
    # Traduzir só o tipo da plataforma, conservando o corpo MQL efetivo.
    source = re.sub(r"\blong\b", "mql_long", native_inputs.binding_source(robot, FIELDS))
    shim = r'''
#include <cmath>
#include <cstdint>
#include <iostream>
#include <limits>
#include <string>
using string=std::string;
using mql_long=std::int64_t;
static_assert(sizeof(mql_long)==8);
string IntegerToString(mql_long value){return std::to_string(value);}
int StringLen(const string &s){return s.size();}
bool MathIsValidNumber(double value){return std::isfinite(value);}
double MathAbs(double value){return std::fabs(value);}
const double EMPTY_VALUE=std::numeric_limits<double>::max();
bool MT5X_InputToken(string value){return value.find_first_not_of("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_.-")==string::npos;}
int Period=21;double Risk=.25;bool Enabled=true;string PresetId="manual_01";
'''
    numbers = [0., -0., .1, .25, -3.75, math.nextafter(1.,0.), math.nextafter(1.,2.),
               float.fromhex("0x0.0000000000001p-1022"), float.fromhex("0x1.0p-1022"), 1e-200, 1e200]
    checks = []
    for index, value in enumerate(numbers):
        expected = native_inputs.binding_payload(robot, dict(EXPECTED, Risk=value), FIELDS)
        checks.append(f"Risk={value.hex()};if(MT5X_RuntimeInputs()!={json.dumps(expected)})"
                      f'{{std::cerr<<"binary64 case {index}: "<<MT5X_RuntimeInputs();return 1;}}')
    checks.append('Risk=std::numeric_limits<double>::quiet_NaN();if(!MT5X_RuntimeInputs().empty())return 2;')
    file, binary = tmp_path/"binding.cpp", tmp_path/"binding"
    file.write_text(shim + source + "\nint main(){" + "\n".join(checks) + "}\n")
    built = subprocess.run(["g++","-std=c++17",str(file),"-o",str(binary)],capture_output=True,text=True,timeout=30)
    assert built.returncode == 0, built.stderr
    result = subprocess.run([str(binary)],capture_output=True,text=True,timeout=10)
    assert result.returncode == 0, result.stderr


def test_runtime_manifest_ack_requires_exact_artifacts_session_and_checksum(tmp_path):
    request = dict(preset_hash="b"*64,revision=4,strategy="s09_reference",actor="preset-app",source_hash="c"*64,symbol="EURUSD",timeframe="M5")
    manifest, data = native_inputs.runtime_manifest(request,session_id=SID,source_hash="c"*64,
        binary_hash="d"*64,wrapper_hash="e"*64,inputs_hash="f"*64)
    assert manifest["manifest_sha256"] == hashlib.sha256(data).hexdigest()
    lines = ["HDK_RUNTIME_BOUND_1",SID,manifest["manifest_sha256"],"1"*64,"f"*64,"c"*64,"d"*64,"b"*64,"4","e"*64,"EURUSD","5"]
    path = tmp_path/"runtime_bound.txt"
    path.write_bytes(native_inputs._runtime_document(lines))
    receipt = native_inputs.read_runtime_receipt(path, manifest)
    assert receipt["native_verified"] and not receipt["operational_acceptance"] and receipt["context_hash"] == "1"*64
    for index in range(12):
        changed = list(lines);changed[index] = "other"
        path.write_bytes(native_inputs._runtime_document(changed))
        with pytest.raises(ValueError):native_inputs.read_runtime_receipt(path,manifest)
    path.write_bytes(native_inputs._runtime_document(lines)[:-1])
    with pytest.raises(ValueError):native_inputs.read_runtime_receipt(path,manifest)
    with pytest.raises(ValueError):native_inputs.runtime_request(dict(request,binary_hash="0"*64))


def test_runtime_session_rejects_artifact_changes_and_persists_ack_with_input_receipt(robot,tmp_path,db,monkeypatch):
    source_hash=native_inputs.project_sources_hash(native_inputs.project_sources(robot))
    request=dict(preset_hash="b"*64,revision=1,strategy="s09_reference",actor="preset-app",
                 source_hash=source_hash,symbol="EURUSD",timeframe="M1")
    session=sessions.Session(db,configparser.ConfigParser(),robot,"EURUSD","M1",root=tmp_path/"sessions",
        input_receipt_fields=FIELDS,expected_native_inputs=EXPECTED,runtime_binding=request)
    session.directory.mkdir(parents=True)
    sessions.mt5_tools.install(robot,session.directory)
    wrapper=session.directory/"MQL5/Experts/mt5x_session.mq5"
    wrapper.write_text("// simulated compiler fixture")
    wrapper.with_suffix(".ex5").write_bytes(b"simulated binary; no native acceptance")
    common=tmp_path/"common";scoped=common/"mt5x_sessions"/session.id;scoped.mkdir(parents=True)
    monkeypatch.setattr(sessions.mt5_tools,"common_files_dir",lambda config:common)
    manifest,data=native_inputs.runtime_manifest(request,session_id=session.id,source_hash=source_hash,
        binary_hash=hashlib.sha256(wrapper.with_suffix(".ex5").read_bytes()).hexdigest(),
        wrapper_hash=hashlib.sha256(wrapper.read_bytes()).hexdigest(),
        inputs_hash=hashlib.sha256(native_inputs.binding_payload(robot,EXPECTED,FIELDS).encode()).hexdigest())
    manifest["project_files"]=native_inputs.project_sources(robot)
    bound=scoped/"runtime_binding.txt";bound.write_bytes(data)
    session.expert_wrapper=wrapper;session.runtime_binding_manifest=manifest
    session._verify_runtime_artifacts()
    for path in (Path(robot.path),wrapper,wrapper.with_suffix(".ex5"),bound):
        prior=path.read_bytes();path.write_bytes(prior+b"changed")
        with pytest.raises(ValueError):session._verify_runtime_artifacts()
        path.write_bytes(prior)
    receipt_file(common/session.input_receipt_file,rows(session.id));save(db,session)
    with pytest.raises(FileNotFoundError):session._verify_input_receipt(common)
    ack=["HDK_RUNTIME_BOUND_1",session.id,manifest["manifest_sha256"],"1"*64,manifest["inputs_hash"],
         source_hash,manifest["binary_hash"],request["preset_hash"],"1",manifest["wrapper_hash"],"EURUSD","1"]
    (scoped/"runtime_bound.txt").write_bytes(native_inputs._runtime_document(ack))
    session._verify_input_receipt(common)
    stored,=store.load_runs(db)
    assert stored["native_inputs"]["runtime_binding"]["native_verified"]
    assert stored["native_inputs"]["runtime_binding"]["context_hash"]=="1"*64
    assert not stored["native_inputs"]["operational_acceptance"]


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
        sessions.telemetry_source(robot,"status.txt",account={"demo":False,"login":1,"server":"Demo"},
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


def test_opt_in_demo_receipt_is_distinct_from_tester_and_rejects_real(robot,tmp_path):
    data=rows();data[0][5]="0"
    path=receipt_file(tmp_path/"live.csv",data)
    with pytest.raises(ValueError):read(path)
    assert read(path,tester=False)["tester"] is False
    data[0][6]="2";receipt_file(path,data)
    with pytest.raises(ValueError):read(path,tester=False)
    source=sessions.telemetry_source(robot,"status.txt",account={"demo":True,"login":1,"server":"Demo"},
        input_receipt=dict(filename=f"mt5x_session_{SID}_inputs.csv",session_id=SID,fields=FIELDS))
    assert "MQLInfoInteger(MQL_TESTER)!=false" in source
    assert "ACCOUNT_LOGIN)==1" in source and "MT5X_RecordInputs" in source


def test_datetime_is_broker_explicit_not_host_timezone():
    assert native_inputs.scalar("D'2026.09.01 00:00'","datetime")=="2026.09.01 00:00"
    with pytest.raises(ValueError):native_inputs.scalar("2026.09.01 00:00Z","datetime")
