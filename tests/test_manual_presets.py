"""T19.26 inicial: Manual salvo/reaberto, revisão, rótulo e pacote sem sobrescrever."""
from pathlib import Path
from types import SimpleNamespace
import json

import pytest
from sqlalchemy import create_engine,text

from mt5_extracao.presets import defaults,manual,store
from mt5_extracao.presets.compiler import TestContext as NativeTestContext,load_package
from tests.test_s09_native_profile import robot
from tests.test_default_presets import db


def source(db,robot):return defaults.ensure_default(db,robot,symbol="EURUSD",timeframe="M5")


def test_clone_named_manual_roundtrip_preserves_default_lineage_and_selected_snapshot(robot,db):
    default=source(db,robot);store.select_preset(db,robot.key,"EURUSD","M5",default.identity.id,1)
    own=manual.save_as_manual(db,robot,default.identity.id,"São João")
    assert own.identity.id!=default.identity.id and own.identity.default_origin==default.identity.id
    assert own.identity.category=="Manual" and own.identity.label=="São João" and own.identity.revision==1
    assert own.composition==default.composition and manual.get_manual(db,own.identity.id)==own
    assert store.get_preset(db,default.identity.id)==default
    assert store.selected_preset(db,robot.key,"EURUSD","M5")==default
    store.select_preset(db,robot.key,"EURUSD","M5",own.identity.id,1)
    defaults.ensure_default(db,robot,symbol="EURUSD",timeframe="M5")
    assert store.selected_preset(db,robot.key,"EURUSD","M5").identity.id==own.identity.id
    assert manual.list_manual(db,robot.key,"EURUSD","M5")==[own]
    assert not own.package(robot).manifest["native"]["operational_acceptance"]


@pytest.mark.parametrize("name",[""," ","CON","🚀","x"*81,None])
def test_invalid_manual_name_does_not_change_default_or_leave_rows(robot,db,name):
    default=source(db,robot)
    with pytest.raises(ValueError):manual.save_as_manual(db,robot,default.identity.id,name)
    assert manual.list_manual(db,robot.key,"EURUSD","M5")==[]
    assert store.get_preset(db,default.identity.id)==default


def test_case_accent_slug_collision_rejected_without_merging_ids_or_overwriting(robot,db):
    default=source(db,robot);first=manual.save_as_manual(db,robot,default.identity.id,"João")
    with pytest.raises(ValueError,match="Nome já usado"):
        manual.save_as_manual(db,robot,default.identity.id,"joao")
    assert manual.list_manual(db,robot.key,"EURUSD","M5")==[first]


def test_edit_creates_revision_while_identical_save_is_idempotent_and_stale_edit_rejected(robot,db):
    own=manual.save_as_manual(db,robot,source(db,robot).identity.id,"Teste")
    comp=own.composition.to_dict();comp["strategy"]["params"]["fast"]=21
    changed=manual.save_revision(db,robot,own.identity.id,comp,expected_revision=1)
    assert changed.identity.id==own.identity.id and changed.identity.revision==2
    assert changed.package(robot).execution_hash!=own.package(robot).execution_hash
    assert manual.save_revision(db,robot,own.identity.id,comp,expected_revision=1)==changed
    assert manual.get_manual(db,own.identity.id,1)==own
    stale=own.composition.to_dict();stale["strategy"]["params"]["fast"]=22
    with pytest.raises(ValueError,match="reabra a revisão"):
        manual.save_revision(db,robot,own.identity.id,stale,expected_revision=1)


def test_rename_changes_only_presentation_without_rewriting_revision_or_execution_identity(robot,db):
    own=manual.save_as_manual(db,robot,source(db,robot).identity.id,"Original")
    original=own.record;old=own.package(robot)
    renamed=manual.rename_manual(db,own.identity.id,"Nome novo")
    assert renamed.identity.revision==1 and renamed.identity.id==own.identity.id
    assert renamed.record==original and renamed.manifest==own.manifest
    assert renamed.package(robot).set_bytes==old.set_bytes
    assert renamed.package(robot).execution_hash==old.execution_hash
    assert renamed.package(robot).package_hash!=old.package_hash
    assert "__Nome-novo__" in renamed.name.filename


def test_export_load_import_is_verified_idempotent_and_preserves_context_identity(robot,db,tmp_path):
    own=manual.save_as_manual(db,robot,source(db,robot).identity.id,"Manual demo")
    context=NativeTestContext("2026-10-14","2026-10-15",deposit=100000)
    exported=manual.export_manual(db,robot,own.identity.id,tmp_path/"exports",test_context=context)
    assert "__Manual__Manual-demo__" in exported["set_path"].name
    assert exported["package"].manifest["parameters_file"]==exported["set_path"].name
    assert load_package(exported["manifest_path"].read_text(encoding="utf-8"),exported["set_path"].read_bytes(),robot)==exported["package"]
    before={p:p.read_bytes() for p in exported["set_path"].parent.iterdir()}
    assert manual.export_manual(db,robot,own.identity.id,tmp_path/"exports",test_context=context)==exported
    assert {p:p.read_bytes() for p in exported["set_path"].parent.iterdir()}==before
    # Mesmo banco conserva definição original sem janela; recibo de importação conserva contexto do pacote.
    assert manual.import_manual(db,robot,exported["manifest_path"])==own
    other=SimpleNamespace(engine=create_engine("sqlite:///"+str(tmp_path/"import.sqlite")))
    try:
        imported=manual.import_manual(other,robot,exported["manifest_path"])
        assert imported.identity==own.identity and imported.composition==own.composition
        assert imported.manifest["test_context"]==context.to_dict()
        assert manual.import_manual(other,robot,exported["manifest_path"])==imported
        assert len(manual.list_manual(other,robot.key,"EURUSD","M5"))==1
    finally:other.engine.dispose()
    with db.engine.connect() as conn:assert conn.execute(text("SELECT COUNT(*) FROM _preset_imports")).scalar()==1


def test_existing_divergent_file_is_never_overwritten_and_partial_publish_can_resume(robot,db,tmp_path,monkeypatch):
    own=manual.save_as_manual(db,robot,source(db,robot).identity.id,"Arquivo")
    exported=manual.export_manual(db,robot,own.identity.id,tmp_path/"exports")
    path=exported["set_path"];path.write_bytes(b"arquivo de terceiro")
    with pytest.raises(ValueError,match="Arquivo existente difere"):
        manual.export_manual(db,robot,own.identity.id,tmp_path/"exports")
    assert path.read_bytes()==b"arquivo de terceiro"
    real=manual.os.link
    def fail_manifest(src,dst):
        if str(dst).endswith(".manifest.json"):raise OSError("publicação interrompida")
        return real(src,dst)
    monkeypatch.setattr(manual.os,"link",fail_manifest)
    with pytest.raises(ValueError,match="publicar atomicamente"):
        manual.export_manual(db,robot,own.identity.id,tmp_path/"partial")
    assert not list((tmp_path/"partial").rglob("*.manifest.json"))
    monkeypatch.setattr(manual.os,"link",real)
    complete=manual.export_manual(db,robot,own.identity.id,tmp_path/"partial")
    assert complete["manifest_path"].is_file()


def test_malformed_import_and_unsupported_choice_do_not_install_execute_or_change_saved_preset(robot,db,tmp_path):
    own=manual.save_as_manual(db,robot,source(db,robot).identity.id,"Preservado")
    exported=manual.export_manual(db,robot,own.identity.id,tmp_path/"exports")
    m=json.loads(exported["manifest_path"].read_text(encoding="utf-8"));m["parameters_file"]="../outside.set"
    bad=tmp_path/"bad.json";bad.write_text(json.dumps(m))
    with pytest.raises(ValueError,match="sem caminhos"):manual.import_manual(db,robot,bad)
    comp=own.composition.to_dict();comp["sizing"]["params"]["risk_fraction"]=.01
    with pytest.raises(ValueError,match="até 0.5%"):
        manual.save_revision(db,robot,own.identity.id,comp,expected_revision=1)
    assert manual.get_manual(db,own.identity.id)==own


def test_default_export_uses_canonical_name_without_editing_reference(robot,db,tmp_path):
    default=source(db,robot)
    exported=manual.export_preset(db,robot,default.identity.id,tmp_path/"exports")
    assert "__Default__Normal__v001__" in exported["set_path"].name
    assert load_package(exported["manifest_path"].read_text(encoding="utf-8"),exported["set_path"].read_bytes(),robot)==exported["package"]
    assert store.get_preset(db,default.identity.id)==default
    with pytest.raises(ValueError,match="Manual/revisão"):
        manual.rename_manual(db,default.identity.id,"não editar Default")
