import copy
import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text

from mt5_extracao.chart_study import snapshot, store
from mt5_extracao.chart_study.service import StudyService, atomic


@pytest.fixture
def db(tmp_path):
    engine = create_engine("sqlite:///" + str(tmp_path / "old.db"))
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE user_history (value TEXT)"))
        conn.execute(text("INSERT INTO user_history VALUES ('conservar')"))
    yield SimpleNamespace(engine=engine)
    engine.dispose()


@pytest.fixture
def capture():
    start = 1700000040
    rates = [dict(time=start + i*60, open=100+i/10, high=102+i/10, low=99+i/10,
                  close=101+i/10, tick_volume=100+i, spread=1, real_volume=0) for i in range(220)]
    return dict(contract=1, symbol="EURUSD", timeframe="M1", bar_epoch=rates[-1]["time"],
                anchor_price=122, rates=rates, magic="20251213", chart_id="123",
                native=[dict(id="rsi.14", name="RSI(14)", value=54, state="AVAILABLE")])


def request(kind="note_create", request_id="req_1", **extra):
    return dict(contract=1, kind=kind, request_id=request_id, note_id="note_1", text="Entrada\nObservação com ação", **extra)


def test_snapshot_uses_only_frozen_prefix_and_keeps_availability(capture):
    original = copy.deepcopy(capture)
    result = snapshot.build(capture, datetime(2026, 10, 22, tzinfo=timezone.utc))
    assert capture == original
    assert result["native"][0]["value"] == 54
    assert result["frozen_bar_count"] == 220
    assert {v["id"] for v in result["extractor"]} == {s["feature_id"] for s in snapshot.indicator_registry.definitions()}
    assert any(v["state"] == "AVAILABLE" for v in result["extractor"])
    assert result["captured_at_utc"].startswith("2026-10-22")
    assert result["computed_at_utc"]
    assert all(v["value"] is None for v in result["extractor"] if v["state"] != "AVAILABLE")
    bad = copy.deepcopy(capture)
    bad["rates"].append({**bad["rates"][-1], "time": bad["bar_epoch"]+60})
    with pytest.raises(ValueError, match="posteriores"):
        snapshot.build(bad, datetime.now(timezone.utc))


def test_notes_are_additive_idempotent_and_snapshot_never_changes_on_edit(db, capture):
    frozen = store.freeze(db, "note_1", capture, datetime.now(timezone.utc))
    first = store.apply(db, request(), capture, datetime.now(timezone.utc))
    saved = store.get(db, "note_1")
    assert saved["snapshot"] == frozen
    def forbidden(*args):
        pytest.fail("Uma repetição/edição não pode recalcular a fotografia")
    assert store.apply(db, request(), build_snapshot=forbidden) == first
    edited = request("note_update", "req_2", expected_revision=1)
    edited["text"] = "Texto corrigido\nMais uma linha"
    assert store.apply(db, edited, build_snapshot=forbidden)["revision"] == 2
    new = store.get(db, "note_1")
    assert new["note_text"] == edited["text"]
    for field in ("snapshot_json", "snapshot_sha256", "captured_at_utc", "capture_sha256"):
        assert new[field] == saved[field]
    with pytest.raises(ValueError, match="outra janela"):
        store.apply(db, request("note_update", "req_3", expected_revision=1))
    with pytest.raises(ValueError, match="outro conteúdo"):
        store.apply(db, {**request(), "text": "colisão"})
    with db.engine.connect() as conn:
        assert conn.execute(text("SELECT value FROM user_history")).scalar() == "conservar"
    assert len(store.list_notes(db, symbol="EURUSD", magic="20251213")) == 1
    assert store.list_notes(db, symbol="USDJPY") == []
    changed = copy.deepcopy(capture)
    changed["native"][0]["value"] = 55
    with pytest.raises(ValueError, match="original foi alterada"):
        store.freeze(db, "note_1", changed, datetime.now(timezone.utc))


def test_postbox_idle_atomic_reply_recovery_and_invalid_capture(db, tmp_path, capture):
    service = StudyService(db, None, common_dir=tmp_path)
    assert service.poll() == []
    assert not (tmp_path / "HDK_Study").exists()
    root = tmp_path / "HDK_Study"
    atomic(root / "captures/note_1.json", snapshot.json_text(capture))
    atomic(root / "requests/req_1.request.json", snapshot.json_text(request()))
    # .tmp não é consumido durante a escrita nativa.
    atomic(root / "requests/incompleto.tmp", "{")
    assert service.poll()[0]["state"] == "SAVED"
    assert "Observação com ação" in (root / "notes/note_1.tsv").read_text()
    assert (root / "processed/req_1.request.json").exists()
    assert service.poll() == []
    # Queda depois do commit: recuperar a mesma resposta, sem nova nota.
    (root / "processed/req_1.request.json").replace(root / "requests/req_1.request.json")
    assert service.poll()[0]["revision"] == 1
    atomic(root / "requests/req_4.request.json", snapshot.json_text({**request(request_id="req_4"), "note_id": "../../elsewhere"}))
    assert service.poll()[0]["state"] == "ERROR"
    assert (root / "rejected/req_4.request.json").exists()
    assert len(store.list_notes(db)) == 1
    # Os caches locais podem ser refeitos do banco, sem recapturar indicadores.
    (root / "notes/note_1.tsv").unlink()
    listing = dict(contract=1, kind="note_list", request_id="list_1", note_id="workspace_1",
                   symbol="EURUSD", timeframe="M1", magic="20251213")
    atomic(root / "requests/list_1.request.json", snapshot.json_text(listing))
    assert service.poll()[0]["count"] == 1
    assert (root / "notes/note_1.tsv").exists()
    assert "note_1" in (root / "workspaces/workspace_1.tsv").read_text()


@pytest.mark.parametrize("change", [lambda c: c.update(anchor_price=float("nan")),
    lambda c: c["native"][0].update(value=None), lambda c: c["rates"][0].update(high=0),
    lambda c: c["rates"].reverse()])
def test_invalid_capture_never_becomes_snapshot(capture, change):
    change(capture)
    with pytest.raises(ValueError):
        snapshot.build(capture, datetime.now(timezone.utc))


def test_preview_then_save_uses_identical_snapshot_and_text_is_not_invented(db, tmp_path, capture):
    root = tmp_path / "HDK_Study"
    service = StudyService(db, None, common_dir=tmp_path)
    atomic(root / "captures/note_1.json", snapshot.json_text(capture))
    preview = dict(contract=1, kind="capture_preview", request_id="preview_1", note_id="note_1")
    atomic(root / "requests/preview_1.request.json", snapshot.json_text(preview))
    assert service.poll()[0]["state"] == "CAPTURED"
    assert store.get(db, "note_1") is None
    original = (root / "captures/note_1.tsv").read_text()
    atomic(root / "requests/req_1.request.json", snapshot.json_text(request()))
    assert service.poll()[0]["state"] == "SAVED"
    saved = (root / "notes/note_1.tsv").read_text()
    assert original.splitlines()[1:] == saved.splitlines()[1:]
    assert original.splitlines()[0].split("\t")[-1] == saved.splitlines()[0].split("\t")[-1]


@pytest.mark.parametrize("timeframe", ["M1", "M5", "H4", "D1", "W1", "MN1", "M7"])
def test_capture_supports_native_and_custom_timeframes(capture, timeframe):
    capture["timeframe"] = timeframe
    snapshot.validate_capture(capture)


def test_transient_database_error_keeps_request_and_retry_saves_once(db, tmp_path, capture, monkeypatch):
    root = tmp_path / "HDK_Study"
    service = StudyService(db, None, common_dir=tmp_path)
    atomic(root / "captures/note_1.json", snapshot.json_text(capture))
    atomic(root / "requests/req_1.request.json", snapshot.json_text(request()))
    apply = store.apply
    def unavailable(*args):
        raise RuntimeError("Banco temporariamente ocupado")
    monkeypatch.setattr(store, "apply", unavailable)
    assert service.poll() == []
    assert (root / "requests/req_1.request.json").exists()
    monkeypatch.setattr(store, "apply", apply)
    assert service.poll()[0]["state"] == "SAVED"
    assert len(store.list_notes(db)) == 1


@pytest.mark.parametrize("windows", [False, True])
def test_common_postbox_discovery_in_wine_and_simulated_windows(db, tmp_path, capture, monkeypatch, windows):
    import configparser
    from mt5_extracao.robots import mt5_tools
    config = configparser.ConfigParser()
    config["MT5"] = {"wine_prefix": str(tmp_path / "wine")}
    roaming = tmp_path / "roaming" if windows else tmp_path / "wine/drive_c/users/test/AppData/Roaming"
    monkeypatch.setattr(mt5_tools.paths, "is_windows", lambda: windows)
    monkeypatch.setenv("APPDATA", str(roaming))
    common = roaming / "MetaQuotes/Terminal/Common/Files"
    root = common / "HDK_Study"
    atomic(root / "captures/note_1.json", snapshot.json_text(capture))
    atomic(root / "requests/req_1.request.json", snapshot.json_text(request()))
    assert StudyService(db, config).poll()[0]["state"] == "SAVED"
