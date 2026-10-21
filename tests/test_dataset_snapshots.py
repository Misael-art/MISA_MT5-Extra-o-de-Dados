"""T20.4: revisão reproduzível, leitura consistente e retenção de dados."""
import json
import threading
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import inspect, text

from mt5_extracao.analytics import datasets
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.strategies.specs import spec_from_symbol_info
from mt5_extracao.trade_analytics import calendar
from tests.test_backtester import bars


@pytest.fixture
def db(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "source.sqlite"))
    frame = bars([[100, 101, 99, 100]] * 6, start="2025-01-06 21:00", freq="h")
    frame["tick_volume"], frame["real_volume"] = 10, 0
    db.save_ohlcv_data("X", "1 hora", frame)
    db.save_symbol_spec("X", spec_from_symbol_info(SimpleNamespace(point=1, trade_tick_size=1,
                                                                  trade_tick_value=1)))
    calendar.save(db, "csv", [{"event_time": "2025-01-06 22:00", "symbol": "X", "impact": 2,
                              "name": "Evento de referência", "actual": 2, "forecast": 1}])
    yield db
    db.close()


def test_identity_is_stable_and_old_revision_survives_correction(db):
    first = datasets.capture(db, "X", "H1", source={"provider": "fixture", "server": "QA"})
    again = datasets.capture(db, "X", "1 hora", source={"provider": "fixture", "server": "QA"})
    assert first.manifest_id == again.manifest_id
    table = db.find_table_for_symbol("X", "1 hora")
    with db.engine.begin() as conn:
        blocks_before = conn.execute(text("SELECT COUNT(*) FROM _dataset_blocks")).scalar()
        conn.execute(text(f'UPDATE "{table}" SET close=102 WHERE time=(SELECT MIN(time) FROM "{table}")'))
    revised = datasets.capture(db, "X", "H1", source={"provider": "fixture", "server": "QA"})
    assert revised.manifest_id != first.manifest_id
    assert datasets.load(db, first.manifest_id).bars.close.iloc[0] == 100
    assert revised.bars.close.iloc[0] == 102
    with db.engine.connect() as conn:
        # Só o bloco diário corrigido cresce; eventos e outro dia são reutilizados.
        assert conn.execute(text("SELECT COUNT(*) FROM _dataset_blocks")).scalar() == blocks_before + 1
    altered = first.manifest
    altered["symbol"] = "OUTRO"
    assert datasets.load(db, first.manifest_id).manifest["symbol"] == "X"


def test_late_ingestion_and_spec_or_calendar_changes_have_new_identity(db):
    first = datasets.capture(db, "X", "H1")
    frame = bars([[100, 101, 99, 100]], start="2025-01-07 03:00", freq="h")
    frame["tick_volume"], frame["real_volume"] = 10, 0
    db.save_ohlcv_data("X", "1 hora", frame)
    second = datasets.capture(db, "X", "H1")
    assert len(second.bars) == 7 and len(first.bars) == 6
    assert second.manifest_id != first.manifest_id
    calendar.save(db, "csv", [{"event_time": "2025-01-06 22:00", "symbol": "X", "impact": 3,
                              "name": "Evento de referência", "actual": 3, "forecast": 1}])
    third = datasets.capture(db, "X", "H1")
    assert third.manifest_id != second.manifest_id
    assert first.events.impact.tolist() == [2]
    assert third.events.impact.tolist() == [3]


def test_snapshot_does_not_mix_bar_and_spec_revisions_during_concurrent_write(db, monkeypatch):
    with db.engine.connect() as conn:
        conn.exec_driver_sql("PRAGMA journal_mode=WAL")
        conn.commit()
    table = db.find_table_for_symbol("X", "1 hora")
    original = db.get_symbol_spec
    errors = []

    def writer():
        try:
            with db.engine.begin() as conn:
                conn.execute(text(f'UPDATE "{table}" SET close=200'))
                conn.execute(text("UPDATE _symbol_specs SET tick_value=2 WHERE symbol='X'"))
        except Exception as error:
            errors.append(error)

    def read_spec(symbol, *, connection=None):
        thread = threading.Thread(target=writer)
        thread.start()
        thread.join(timeout=5)
        assert not thread.is_alive() and not errors
        return original(symbol, connection=connection)

    monkeypatch.setattr(db, "get_symbol_spec", read_spec)
    snapshot = datasets.capture(db, "X", "H1")
    assert snapshot.bars.close.tolist() == [100] * 6
    assert snapshot.spec.tick_value == 1
    monkeypatch.setattr(db, "get_symbol_spec", original)
    newer = datasets.capture(db, "X", "H1")
    assert newer.bars.close.tolist() == [200] * 6 and newer.spec.tick_value == 2


def test_observed_session_revisions_are_preserved_without_free_metadata(db):
    from mt5_extracao.analytics import calendar as sessions
    frame = db.load_ohlcv("X", "1 hora")
    _, observed = sessions.sessionize_m1(frame, "X", symbol_info={"name": "X"})
    assert sessions.save_sessions(db, observed)
    original = datasets.capture(db, "X", "H1")
    assert not original.market_calendar.empty
    assert "source_metadata_json" not in original.market_calendar
    changed = dict(observed[0], input_hash="late_revision", close_utc="2025-01-07 10:00:00")
    assert sessions.save_sessions(db, [changed]) == 1
    revised = datasets.capture(db, "X", "H1")
    assert revised.manifest_id != original.manifest_id
    assert len(revised.market_calendar) == len(original.market_calendar) + 1
    pd.testing.assert_frame_equal(datasets.load(db, original.manifest_id).market_calendar,
                                  original.market_calendar)


def test_export_import_is_verified_and_does_not_replace_user_bars(db, tmp_path):
    snapshot = datasets.capture(db, "X", "H1")
    path = tmp_path / "snapshot.json"
    from mt5_extracao.data_exporter import DataExporter
    DataExporter(db).export_snapshot(snapshot.manifest_id, path)
    target = DatabaseManager(db_path=str(tmp_path / "target.sqlite"))
    try:
        existing = bars([[999, 1000, 998, 999]], start="2025-01-06", freq="h")
        existing["tick_volume"], existing["real_volume"] = 1, 0
        target.save_ohlcv_data("X", "1 hora", existing)
        imported = datasets.import_snapshot(target, path)
        assert imported.manifest_id == snapshot.manifest_id
        pd.testing.assert_frame_equal(imported.bars, snapshot.bars)
        assert target.load_ohlcv("X", "1 hora").close.tolist() == [999]
        assert datasets.import_snapshot(target, path).manifest_id == snapshot.manifest_id
        payload = json.loads(path.read_text())
        payload["manifest"]["symbol"] = "FORJADO"
        path.write_text(json.dumps(payload))
        with pytest.raises(ValueError, match="integridade"):
            datasets.import_snapshot(target, path)
        assert datasets.load(target, snapshot.manifest_id).manifest["symbol"] == "X"
    finally:
        target.close()


def test_missing_or_corrupt_reference_does_not_fall_back_to_current_data(db):
    snapshot = datasets.capture(db, "X", "H1")
    datasets.bind(db, "run-fixo", snapshot.manifest_id)
    with db.engine.begin() as conn:
        conn.execute(text("UPDATE _dataset_blocks SET payload_json='{}'"))
    with pytest.raises(ValueError, match="integridade"):
        datasets.for_run(db, "run-fixo")
    with pytest.raises(ValueError, match="indisponível"):
        datasets.load(db, "0" * 64)


def test_personal_configuration_cannot_become_manifest_provenance(db):
    with pytest.raises(ValueError, match="Proveniência"):
        datasets.capture(db, "X", "H1", source={"server": "QA", "password": "não-publicar"})
    assert not inspect(db.engine).has_table("_dataset_manifests")


def test_import_rejects_blocks_outside_the_manifest_before_writing(db, tmp_path):
    snapshot = datasets.capture(db, "X", "H1")
    path = tmp_path / "extra.json"
    datasets.export_snapshot(db, snapshot.manifest_id, path)
    payload = json.loads(path.read_text())
    payload["blocks"]["0" * 64] = "{}"
    path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="integridade"):
        datasets.import_snapshot(db, path)
    with db.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM _dataset_blocks WHERE block_id=:id"), {"id": "0" * 64}).scalar() == 0


def test_codec_preserves_float_bits_large_integer_and_missing_value(db):
    import numpy as np
    table = db.find_table_for_symbol("X", "1 hora")
    price = float(np.nextafter(1.0, 2.0))
    with db.engine.begin() as conn:
        conn.execute(text(f'UPDATE "{table}" SET close=:p, tick_volume=:v WHERE time=(SELECT MIN(time) FROM "{table}")'),
                     {"p": price, "v": 2**53 + 1})
        conn.execute(text(f'UPDATE "{table}" SET real_volume=NULL WHERE time=(SELECT MAX(time) FROM "{table}")'))
    expected = db.load_ohlcv("X", "1 hora")
    captured = datasets.capture(db, "X", "H1")
    pd.testing.assert_frame_equal(captured.bars, expected, check_exact=True)
    assert captured.bars.close.iloc[0] == price
    assert captured.bars.tick_volume.iloc[0] == 2**53 + 1
    assert pd.isna(captured.bars.real_volume.iloc[-1])
    assert captured.bars.real_volume.iloc[0] == 0


def test_f15_refuses_archiving_snapshot_and_backup_keeps_referenced_revision(db):
    from mt5_extracao.resources import actions, files
    captured = datasets.capture(db, "X", "H1")
    datasets.bind(db, "protected", captured.manifest_id)
    result = actions.archive_tables(db, None, ["_dataset_blocks"])[0]
    assert not result["ok"] and "interna" in result["error"]
    copied = files.backup(db, None, prune=False)
    assert copied["ok"], copied["error"]
    backup = DatabaseManager(db_path=copied["path"])
    try:
        assert datasets.for_run(backup, "protected").manifest_id == captured.manifest_id
        pd.testing.assert_frame_equal(datasets.load(backup, captured.manifest_id).bars, captured.bars)
    finally:
        backup.close()


def test_python_run_replay_cache_and_resimulation_use_the_bound_revision(db):
    from configparser import ConfigParser
    from mt5_extracao.robots import backtest, catalog
    from mt5_extracao.trade_analytics import cache, ledger
    from mt5_extracao.trade_analytics.execution import runner
    from mt5_extracao.trade_analytics.execution.engine import ExitPolicy
    from tests.test_backtester import Manual
    cfg = ConfigParser()
    cfg.read_dict({"STRATEGY": {"initial_equity": "1000", "risk_per_trade": "0.1"}})
    robot = SimpleNamespace(kind=catalog.PYTHON, key="python:manual")

    def run(**options):
        strategy = Manual(long=[0], stop={0: 80})
        strategy.min_bars = 0
        return backtest.run_python(db, cfg, robot, "X", "H1", strategy=strategy, **options)

    original = run()
    pinned = datasets.for_run(db, original["run_id"])
    trades = ledger.load(db, run_id=original["run_id"])
    old_features = cache.features(db, cfg, trades)
    old_signature = cache.signature(db, cfg, trades)
    table = db.find_table_for_symbol("X", "1 hora")
    with db.engine.begin() as conn:
        conn.execute(text(f'UPDATE "{table}" SET close=100.5'))
        conn.execute(text("UPDATE _symbol_specs SET tick_value=2 WHERE symbol='X'"))
        conn.execute(text("UPDATE _economic_events SET impact=3"))
    reproduced = run(data_manifest_id=pinned.manifest_id)
    assert reproduced["trades"] == original["trades"]
    assert reproduced["data_manifest_id"] == pinned.manifest_id
    assert cache.signature(db, cfg, trades) == old_signature
    # SQL normaliza largura de inteiros/bools no cache; valores e ausência precisam coincidir.
    pd.testing.assert_frame_equal(cache.features(db, cfg, trades), old_features, check_dtype=False)
    cache.invalidate(db, original["run_id"])
    pd.testing.assert_frame_equal(cache.features(db, cfg, trades), old_features, check_dtype=False)
    replay_data = runner.inputs(db, cfg, original["run_id"])
    assert replay_data["bars"].close.tolist() == [100] * 6
    assert replay_data["spec"].tick_value == 1
    runner.resimulate(db, cfg, original["run_id"], policy=ExitPolicy(), save=True)
    child = original["run_id"] + "#ponto_fixo"
    assert datasets.for_run(db, child).manifest_id == pinned.manifest_id
    changed = run()
    assert changed["data_manifest_id"] != pinned.manifest_id
    with db.engine.begin() as conn:
        conn.execute(text("DELETE FROM _dataset_references WHERE run_id=:run"), {"run": original["run_id"]})
    with pytest.raises(ValueError, match="indisponível"):
        runner.inputs(db, cfg, original["run_id"])
