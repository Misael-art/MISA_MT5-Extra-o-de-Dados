"""T19.55: Default imutável/contextual, banco aditivo e seleção preservada."""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError, replace
from importlib import import_module
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine,text

from mt5_extracao.composition import native
from mt5_extracao.presets import defaults, manual, store
from mt5_extracao.presets.compiler import read_package
from mt5_extracao.robots import catalog, mql
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


@pytest.mark.parametrize("name", ["ha_hilo", "madrid", "pullback", "stoch", "vwap"])
def test_shared_panel_revision_exports_new_presets_and_preserves_old_packages(name, db, tmp_path, monkeypatch):
    profile = import_module(f"mt5_extracao.composition.{name}_native")
    legacy_fingerprint = "PANEL_APPROVED_FINGERPRINT" if name == "pullback" else "APPROVED_FINGERPRINT"
    legacy_revision = getattr(profile, "PANEL_HADOUKEN_REVISION", profile.HADOUKEN_REVISION)
    version = getattr(profile, "PANEL_ROBOT_VERSION", profile.ROBOT_VERSION)
    old_source = tmp_path / "old" / "Expert.mq5"
    new_source = tmp_path / "new" / "Expert.mq5"
    for source in (old_source, new_source):
        source.parent.mkdir()
        source.write_text(f"// perfil simulado {source.parent.name}\n", encoding="utf-8")
    legacy = catalog.Robot(key=profile.ROBOT_KEY, kind=catalog.MQL5, name=name, version=version,
                           path=str(old_source), files=[str(old_source)], has_source=True,
                           missing_includes=["Canvas/Canvas.mqh"],
                           inputs=[mql.MqlInput(key, kind, "0") for key, kind in profile.INPUT_TYPES.items()])
    current = replace(legacy, path=str(new_source), files=[str(new_source)])
    monkeypatch.setattr(profile, legacy_fingerprint, catalog.fingerprint(legacy))
    monkeypatch.setattr(profile, "PANEL_SCOPE_FIX_FINGERPRINT", catalog.fingerprint(current))

    context = dict(symbol="EURUSD", timeframe="M5")
    previous = defaults.ensure_default(db, legacy, **context)
    default = defaults.ensure_default(db, current, **context)
    assert previous.identity.id != default.identity.id
    assert previous.package(legacy).manifest["native"]["hadouken_revision"] == legacy_revision
    with pytest.raises(ValueError, match="Pacote divergente"):
        previous.package(current)

    named = manual.save_as_manual(db, current, default.identity.id, "Perfil QA")
    exported = manual.export_preset(db, current, named.identity.id, tmp_path / "exported")
    package = read_package(exported["manifest_path"], current)
    description = package.manifest["native"]
    assert description["hadouken_revision"] == native.PANEL_SCOPE_FIX_HADOUKEN_REVISION
    assert description["robot"]["fingerprint"] == profile.PANEL_SCOPE_FIX_FINGERPRINT
    assert set(description["expected_native_inputs"]) == set(profile.INPUT_TYPES)
    assert description["modality"] == "native_tester_demo_only" and not description["operational_acceptance"]
    store.select_preset(db, current.key, "EURUSD", "M5", named.identity.id, 1)
    assert store.selected_preset(db, current.key, "EURUSD", "M5").identity.id == named.identity.id

    for invalid in (replace(current, version="0.01"), replace(current, inputs=current.inputs[:-1]),
                    replace(current, missing_includes=["Canvas/Other.mqh"])):
        with pytest.raises(ValueError):
            native.default_composition(invalid, **context)
    new_source.write_text("// bytes fora da revisão", encoding="utf-8")
    with pytest.raises(ValueError):
        read_package(exported["manifest_path"], current)


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
