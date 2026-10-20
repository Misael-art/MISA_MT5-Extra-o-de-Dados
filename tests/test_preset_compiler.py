"""T19.25 inicial: pacote verificado, sem instalar/iniciar terminal."""
from dataclasses import replace
import copy
import hashlib
import json

import pytest

from mt5_extracao.composition.native import default_composition
from mt5_extracao.presets.compiler import (PresetIdentity,PresetPackage,TestContext as NativeTestContext,
                                           compile_package,load_package)
from mt5_extracao.robots import mql
from tests.test_s09_native_profile import robot


def make(robot,**kwargs):
    return compile_package(default_composition(robot,symbol="EURUSD",timeframe="M5"),robot,
        PresetIdentity("manual_01",category="Manual",label="Cruzamento São João",default_origin="default_01"),
        NativeTestContext("2026-10-14","2026-10-15",deposit=100000),**kwargs)


def test_typed_native_bundle_roundtrip_utf16_and_existing_set_reader(robot,tmp_path):
    package=make(robot);path=tmp_path/"parameters.set";path.write_bytes(package.set_bytes)
    assert package.set_bytes.startswith(b"\xff\xfe")
    values=mql.parse_set(mql.read_text(path));manifest=package.manifest
    assert values==manifest["native"]["parameters"]
    assert values["InpRiskPct"]=="0.500" and values["InpPresetId"]=="manual_01"
    assert manifest["parameters_sha256"]==hashlib.sha256(package.set_bytes).hexdigest()
    loaded=load_package(package.to_json(),package.set_bytes,robot)
    assert loaded==package and loaded.package_hash==package.package_hash
    assert loaded.execution_hash==package.execution_hash
    assert loaded.manifest["identity"]["label"]=="Cruzamento São João"
    assert loaded.manifest["test_context"]["model"]=="0"
    assert not loaded.manifest["native"]["operational_acceptance"]


@pytest.mark.parametrize("field",["version","context","composition","config_hash","native","filename","sha","approval"])
def test_changed_manifest_or_unsupported_claim_does_not_load(robot,field):
    package=make(robot);m=package.manifest
    if field=="version":m["preset_package_schema_version"]=2
    elif field=="context":m["test_context"]["model"]="unknown"
    elif field=="composition":m["composition"]["strategy"]["params"]["fast"]=10
    elif field=="config_hash":m["config_hash"]="0"*64
    elif field=="native":m["native"]["fixed_constraints"]["daily_loss_pct"]=0
    elif field=="filename":m["parameters_file"]="../foreign.set"
    elif field=="sha":m["parameters_sha256"]="0"*64
    elif field=="approval":m["operational_acceptance"]=True
    with pytest.raises(ValueError):load_package(json.dumps(m),package.set_bytes,robot)


@pytest.mark.parametrize("change",["value","unknown","utf8","duplicate"])
def test_actual_set_must_match_declared_inputs_without_duplicate_or_unknown_fields(robot,change):
    package=make(robot);text=package.set_bytes.decode("utf-16")
    if change=="value":text=text.replace("InpRiskPct=0.500","InpRiskPct=1.000")
    elif change=="unknown":text+="Unknown=1\r\n"
    elif change=="duplicate":text+="InpStopPoints=100\r\n"
    changed=text.encode("utf-8" if change=="utf8" else "utf-16")
    with pytest.raises(ValueError,match="Pacote divergente"):
        load_package(package.to_json(),changed,robot)


def test_presentation_rename_keeps_config_inputs_and_execution_evidence_identity(robot):
    old=make(robot);m=old.manifest;identity=PresetIdentity(**dict(m["identity"],label="Nome novo"))
    new=compile_package(m["composition"],robot,identity,NativeTestContext(**m["test_context"]))
    assert old.set_bytes==new.set_bytes and old.execution_hash==new.execution_hash
    assert old.manifest["config_hash"]==new.manifest["config_hash"]
    assert old.package_hash!=new.package_hash
    changed=replace(identity,revision=2)
    revision=compile_package(m["composition"],robot,changed,NativeTestContext(**m["test_context"]))
    assert revision.execution_hash!=new.execution_hash


def test_context_or_operational_edit_changes_execution_identity_without_redefining_config_hash(robot):
    old=make(robot);m=old.manifest
    new=compile_package(m["composition"],robot,PresetIdentity(**m["identity"]),
                        NativeTestContext("2026-10-13","2026-10-15",deposit=100000))
    assert old.execution_hash!=new.execution_hash and old.manifest["config_hash"]==new.manifest["config_hash"]
    comp=copy.deepcopy(m["composition"]);comp["strategy"]["params"]["fast"]=10
    changed=compile_package(comp,robot,PresetIdentity(**m["identity"]))
    assert old.manifest["config_hash"]!=changed.manifest["config_hash"]
    assert old.set_bytes!=changed.set_bytes


@pytest.mark.parametrize("kwargs",[dict(id="../x"),dict(id=""),dict(category="Ultra"),dict(revision=True),
                                   dict(revision=0),dict(label=" "),dict(label="x"*81),dict(default_origin="bad/path")])
def test_invalid_identity_has_actionable_error(kwargs):
    params=dict(id="id",category="Manual",label="Meu preset");params.update(kwargs)
    with pytest.raises(ValueError):PresetIdentity(**params)


@pytest.mark.parametrize("kwargs",[dict(start="20261014"),dict(end="2026-10-14"),dict(end="2026-10-13"),
                                    dict(deposit=0),dict(deposit=float("nan")),dict(deposit=True),dict(model="bad")])
def test_invalid_test_context_does_not_create_executable_bundle(kwargs):
    params=dict(start="2026-10-14",end="2026-10-15",deposit=100000);params.update(kwargs)
    with pytest.raises(ValueError):NativeTestContext(**params)


def test_draft_without_window_is_explicit_and_compile_has_no_user_files_or_mt5_side_effects(robot,tmp_path):
    before=set(tmp_path.rglob("*"))
    package=compile_package(default_composition(robot,symbol="EURUSD",timeframe="M5"),robot,PresetIdentity("draft_id"))
    assert package.manifest["test_context"] is None and package.manifest["identity"]["category"]=="draft"
    assert set(tmp_path.rglob("*"))==before
    assert load_package(package.manifest,package.set_bytes,robot)==package
