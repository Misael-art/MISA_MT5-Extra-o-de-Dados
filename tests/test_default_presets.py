"""T19.55: Default imutável/contextual, banco aditivo e seleção preservada."""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine,text

from mt5_extracao.composition import native
from mt5_extracao.presets import defaults,store
from mt5_extracao.robots import catalog
from tests.test_s09_native_profile import robot


@pytest.fixture
def db(tmp_path):
    value=SimpleNamespace(engine=create_engine("sqlite:///"+str(tmp_path/"presets.sqlite")))
    yield value
    value.engine.dispose()


def test_default_roundtrip_idempotence_selection_and_existing_data_preserved(robot,db):
    with db.engine.begin() as conn:
        conn.execute(text("CREATE TABLE user_candles (time TEXT,close REAL)"))
        conn.execute(text("INSERT INTO user_candles VALUES('2026-10-14',1.1)"))
    first=defaults.ensure_default(db,robot,symbol="EURUSD",timeframe="M5")
    again=defaults.ensure_default(db,robot,symbol="EURUSD",timeframe="M5")
    assert first==again and first.identity.category=="Default" and first.identity.revision==1
    assert first.identity.label=="Default · Padrão do código"
    assert first.manifest["native"]["expected_native_inputs"]["InpRiskPct"]==.5
    assert first.composition.to_dict()["strategy"]["params"]==dict(fast=20,slow=50,warmup_bars=250,stop_points=100,target_points=200)
    assert "__M5__Default__Normal__v001__" in first.name.filename
    assert first.package(robot).manifest==first.manifest
    store.select_preset(db,robot.key,"EURUSD","M5",first.identity.id,1)
    assert store.selected_preset(db,robot.key,"EURUSD","M5")==first
    assert store.get_preset(db,first.identity.id)==first
    assert store.list_presets(db,robot.key,"EURUSD","M5")==[first]
    with db.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM _preset_revisions")).scalar()==1
        assert conn.execute(text("SELECT close FROM user_candles")).scalar()==1.1


def test_snapshot_is_immutable_and_does_not_rewrite_source_defaults_or_users(robot,db):
    before=Path(robot.path).read_bytes()
    default=defaults.ensure_default(db,robot,symbol="EURUSD",timeframe="M5")
    value=default.manifest;value["composition"]["strategy"]["params"]["fast"]=10
    value["identity"]["label"]="edit"
    assert default.manifest["composition"]["strategy"]["params"]["fast"]==20
    assert default.manifest["identity"]["label"]=="Default · Padrão do código"
    with pytest.raises(FrozenInstanceError):default._manifest_json="changed"
    assert Path(robot.path).read_bytes()==before
    assert defaults.ensure_default(db,robot,symbol="EURUSD",timeframe="M5")==default


def test_contexts_are_distinct_and_cross_context_selection_is_rejected(robot,db):
    first=defaults.ensure_default(db,robot,symbol="EURUSD",timeframe="M5")
    other=defaults.ensure_default(db,robot,symbol="EURUSD",timeframe="M15")
    assert first.identity.id!=other.identity.id
    store.select_preset(db,robot.key,"EURUSD","M5",first.identity.id,1)
    with pytest.raises(ValueError,match="outro contexto"):
        store.select_preset(db,robot.key,"EURUSD","M5",other.identity.id,1)
    assert store.selected_preset(db,robot.key,"EURUSD","M5")==first
    assert store.selected_preset(db,robot.key,"EURUSD","M15") is None
    assert store.list_presets(db,robot.key,"EURUSD","M15")==[other]


def test_default_source_revision_preserves_old_record_and_selection(robot,db,monkeypatch):
    first=defaults.ensure_default(db,robot,symbol="EURUSD",timeframe="M5")
    store.select_preset(db,robot.key,"EURUSD","M5",first.identity.id,1)
    # Simula uma NOVA autoridade instalada já aprovada; sidecar externo não pode atualizar o pin.
    Path(robot.path).write_text("// revisão fake aprovada somente no teste\n")
    monkeypatch.setattr(native,"APPROVED_FINGERPRINT",catalog.fingerprint(robot))
    current=defaults.ensure_default(db,robot,symbol="EURUSD",timeframe="M5")
    assert current.identity.id!=first.identity.id
    assert store.get_preset(db,first.identity.id)==first
    assert store.selected_preset(db,robot.key,"EURUSD","M5")==first
    assert len(store.list_presets(db,robot.key,"EURUSD","M5"))==2
    with pytest.raises(ValueError,match="Pacote divergente"):
        first.package(robot)
    assert current.package(robot).manifest==current.manifest


def test_concurrent_repeated_default_creation_does_not_duplicate(robot,db):
    with ThreadPoolExecutor(max_workers=4) as pool:
        results=list(pool.map(lambda _:defaults.ensure_default(db,robot,symbol="EURUSD",timeframe="M5"),range(8)))
    assert len({r.identity.id for r in results})==1
    assert store.list_presets(db,robot.key,"EURUSD","M5")==[results[0]]


def test_reopen_other_connection_keeps_default_and_selection(robot,db):
    first=defaults.ensure_default(db,robot,symbol="EURUSD",timeframe="M5")
    store.select_preset(db,robot.key,"EURUSD","M5",first.identity.id,1)
    reopened=SimpleNamespace(engine=create_engine(db.engine.url))
    try:
        assert store.get_preset(reopened,first.identity.id)==first
        assert store.selected_preset(reopened,robot.key,"EURUSD","M5")==first
    finally:reopened.engine.dispose()


def test_absent_db_is_actionable_and_reading_existing_db_is_additive(robot,db):
    assert store.list_presets(db,robot.key,"EURUSD","M5")==[]
    assert store.get_preset(db,"missing") is None
    with pytest.raises(ValueError,match="banco disponível"):
        defaults.ensure_default(None,robot,symbol="EURUSD",timeframe="M5")
