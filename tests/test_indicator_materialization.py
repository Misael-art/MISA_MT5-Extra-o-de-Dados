"""T19.3: consumo real do registry/store, sem terminal, pesquisa ou ordens."""
import json

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import inspect, text

from mt5_extracao import cli
from mt5_extracao.analytics import (feature_store, indicator_context, indicator_materializer as materializer,
                                    indicator_registry, producer_jobs, resampling)
from mt5_extracao.data_exporter import DataExporter
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.trade_analytics import indicator_context as trade_context
from mt5_extracao.trade_analytics.trajectories.clocks import Clock
from tests.test_analytics_profiles import INFO


@pytest.fixture
def db(tmp_path):
    manager = DatabaseManager(db_path=str(tmp_path / "indicators.sqlite"))
    manager.broker_utc_offset = 0
    yield manager
    manager.close()


def source(count=45):
    count *= 5
    close = 100.0 + np.arange(count) // 5
    return pd.DataFrame(dict(time=pd.date_range("2026-09-01 09:00", periods=count, freq="min"),
                             open=close, high=close + 1, low=close - 1, close=close,
                             tick_volume=np.arange(count) + 1, real_volume=np.zeros(count), spread=0))


def seed(db, frame):
    assert db.save_ohlcv_data("TEST", "1 minuto", frame)
    resampling.resample_symbol(db, "TEST", ["M5"], symbol_info=INFO)


def facts(db):
    with db.engine.connect() as conn:
        return conn.execute(text("SELECT feature_id, algorithm_version, observation_time, available_at_utc, "
                                 "numeric_value, quality, source_revision, revision, value_hash, metadata_json "
                                 "FROM _feature_values ORDER BY feature_id, observation_time, revision")).all()


@pytest.mark.parametrize("prices,expected", [(np.arange(21), 1.0), (np.ones(21), 0.0),
                                            (np.array([0, 1] * 10 + [0]), 0.0)])
def test_er_uses_existing_formula_with_known_paths(prices, expected):
    frame = pd.DataFrame({"close": prices.astype(float) + 100})
    rows = indicator_registry.compute_core(frame, feature_ids=["trend.efficiency_ratio_20"])
    assert rows[19]["state"] == "WARMUP"
    assert rows[20]["value"] == expected and rows[20]["state"] == "AVAILABLE"
    assert rows == list(indicator_registry.iter_core(frame, feature_ids=["trend.efficiency_ratio_20"]))


def test_materialize_preserves_raw_bars_and_explicit_missing_states(db):
    seed(db, source())
    before = db.load_ohlcv("TEST", "1 minuto")
    result = materializer.materialize(db, "TEST", "M5", batch_size=100)
    assert result["inserted"] == 45 * len(indicator_registry.definitions())
    assert result["job"]["status"] == "COMPLETE" and result["evaluation"] == "exact_prefix"
    pd.testing.assert_frame_equal(before, db.load_ohlcv("TEST", "1 minuto"))
    version = materializer.algorithm_version("trend.efficiency_ratio_20")
    values = feature_store.query_as_of(db, "2026-10-19T00:00Z", symbol="TEST", timeframe="M5",
                                       feature_id="trend.efficiency_ratio_20", algorithm_version=version)
    assert values.iloc[0].quality == "UNAVAILABLE" and pd.isna(values.iloc[0].numeric_value)
    assert json.loads(values.iloc[0].metadata_json)["state"] == "WARMUP"
    assert values.iloc[-1].numeric_value == 1.0


def test_unchanged_source_skips_before_loading_bars(db, monkeypatch):
    seed(db, source())
    materializer.materialize(db, "TEST", "M5")
    before = facts(db)
    monkeypatch.setattr(resampling, "latest_bars", lambda *a, **k: pytest.fail("releu histórico inalterado"))
    result = materializer.materialize(db, "TEST", "M5")
    assert result["skipped"] and result["job"]["status"] == "SKIPPED"
    assert facts(db) == before


def test_batch_and_append_preserve_exact_values_hashes_and_prefix(db, tmp_path):
    frame = source(45)
    seed(db, frame.iloc[:125])
    materializer.materialize(db, "TEST", "M5")
    prefix = facts(db)
    seed(db, frame)
    result = materializer.materialize(db, "TEST", "M5")
    assert result["write_from"] == 25 and result["revised"] == 0
    assert result["inserted"] == 20 * len(indicator_registry.definitions())
    assert all(row in facts(db) for row in prefix)
    full = DatabaseManager(db_path=str(tmp_path / "full.sqlite"))
    full.broker_utc_offset = 0
    try:
        seed(full, frame)
        materializer.materialize(full, "TEST", "M5")
        assert facts(db) == facts(full)
    finally:
        full.close()


def test_failed_ack_resumes_only_confirmed_cursor(db, monkeypatch):
    seed(db, source(25))
    original = feature_store.save_values
    def fail_after_commit(db_, records, **kwargs):
        result = original(db_, records, **kwargs)
        if records:
            raise RuntimeError("falha após confirmação do lote")
        return result
    monkeypatch.setattr(feature_store, "save_values", fail_after_commit)
    with pytest.raises(RuntimeError, match="após confirmação"):
        materializer.materialize(db, "TEST", "M5", batch_size=7)
    prior = facts(db)
    checkpoint = producer_jobs.get_checkpoint(db, "TEST", "M5", materializer.FAMILY)
    assert len(prior) == checkpoint["state"]["next_record"] == 7
    monkeypatch.setattr(feature_store, "save_values", original)
    result = materializer.materialize(db, "TEST", "M5", batch_size=7)
    assert result["job"]["status"] == "COMPLETE"
    assert result["inserted"] == 25 * len(indicator_registry.definitions()) - 7
    assert all(row in facts(db) for row in prior)
    assert all(row[7] == 1 for row in facts(db))


def test_late_revision_never_changes_previous_asof(db):
    frame = source(30)
    seed(db, frame)
    materializer.materialize(db, "TEST", "M5")
    before = feature_store.query_as_of(db, "2026-09-02T00:00:00Z", symbol="TEST")
    changed = frame.iloc[[10]].copy()
    changed["close"] += 5
    changed["high"] = changed["close"] + 1
    seed(db, changed)
    revisions = resampling.latest_bars(db, "TEST", "M5")
    cutoff = revisions.loc[revisions.revision > 1, "computed_at_utc"].max()
    result = materializer.materialize(db, "TEST", "M5")
    assert result["write_from"] == 0 and result["revised"] > 0
    pd.testing.assert_frame_equal(before, feature_store.query_as_of(db, cutoff - pd.Timedelta(nanoseconds=1), symbol="TEST"))
    now = feature_store.query_as_of(db, cutoff, symbol="TEST")
    revised = now[now.revision > 1]
    assert revised.available_at_utc.ge(cutoff).all()


def test_implementation_change_creates_version_without_overwriting_legacy(db, monkeypatch):
    seed(db, source(25))
    monkeypatch.setattr(materializer, "implementation_signature", lambda: "1" * 64)
    materializer.materialize(db, "TEST", "M5", feature_ids=["price.return_1"])
    old = facts(db)
    monkeypatch.setattr(materializer, "implementation_signature", lambda: "2" * 64)
    result = materializer.materialize(db, "TEST", "M5", feature_ids=["price.return_1"])
    assert result["inserted"] == 25 and result["revised"] == 0
    assert all(row in facts(db) for row in old)
    assert len({row[1] for row in facts(db)}) == 2


def test_guard_and_cancel_preserve_confirmations(db):
    seed(db, source(25))
    with pytest.raises(RuntimeError, match="Libere recursos"):
        materializer.materialize(db, "TEST", "M5", write_guard=lambda: "disco cheio")
    assert not inspect(db.engine).has_table("_feature_values")
    calls = 0
    def cancel():
        nonlocal calls
        calls += 1
        return calls >= 4
    with pytest.raises(materializer.Cancelled):
        materializer.materialize(db, "TEST", "M5", batch_size=5, cancel_check=cancel)
    confirmed = facts(db)
    assert len(confirmed) == 5
    materializer.materialize(db, "TEST", "M5", batch_size=5)
    assert all(row in facts(db) for row in confirmed)


def test_context_requires_aware_decisions_preserves_zero_warmup_and_nanoseconds(db):
    seed(db, source(25))
    materializer.materialize(db, "TEST", "M5")
    times = ["2026-09-01T09:04:59.999999999Z", "2026-09-01T09:05:00Z", "2026-09-01T10:45:00.000000001Z"]
    rows = indicator_context.context(db, pd.DataFrame({"decision_time": times}), symbol="TEST", timeframe="M5",
                                     feature_ids=["volume.real", "trend.efficiency_ratio_20"])
    assert rows.iloc[0]["volume.real::state"] == "UNAVAILABLE"
    assert rows.iloc[1]["volume.real::value"] == 0 and rows.iloc[1]["volume.real::state"] == "AVAILABLE"
    assert rows.iloc[1]["trend.efficiency_ratio_20::state"] == "WARMUP"
    assert rows.iloc[2]["trend.efficiency_ratio_20::value"] == 1
    encoded = json.dumps(indicator_context.records(rows), allow_nan=False)
    assert "000000001+00:00" in encoded and "NaT" not in encoded
    with pytest.raises(ValueError, match="fuso"):
        indicator_context.context(db, pd.DataFrame({"decision_time": ["2026-09-01 10:45"]}), symbol="TEST", timeframe="M5")


def test_export_preserves_states_version_and_previous_destination(db, tmp_path):
    seed(db, source(25))
    materializer.materialize(db, "TEST", "M5")
    exporter = DataExporter(db)
    path = tmp_path / "com espaço" / "indicadores.csv"
    exporter.export_indicator_values(path, symbol="TEST", timeframe="M5", as_of="2026-10-19T00:00Z",
                                     feature_ids=["volume.real", "trend.efficiency_ratio_20"])
    rows = pd.read_csv(path, keep_default_na=False, float_precision="round_trip")
    assert len(rows) == 25
    assert rows.iloc[0]["trend.efficiency_ratio_20::state"] == "WARMUP"
    assert rows.iloc[0]["volume.real::value"] == 0
    assert "+" in rows.iloc[-1]["volume.real::algorithm_version"]
    before = path.read_bytes()
    with pytest.raises(Exception, match="cancelada"):
        exporter.export_indicator_frame(pd.DataFrame({"a": [1]}), path, cancel_check=lambda: True)
    assert path.read_bytes() == before and not list(path.parent.glob(".indicator-*"))


def test_cli_materialize_context_export_share_same_store(db, tmp_path, capsys):
    seed(db, source(25))
    config = tmp_path / "config.ini"
    config.write_text(f"[DATABASE]\ntype = sqlite\npath = {db.db_path}\n", encoding="utf-8")
    base = ["--config", str(config), "--json", "analytics", "features"]
    common = ["--symbol", "TEST", "--tf", "M5", "--features", "trend.efficiency_ratio_20"]
    assert cli.main(base + ["materialize"] + common) == 0
    assert json.loads(capsys.readouterr().out)["data"]["materialization"]["inserted"] == 25
    assert cli.main(base + ["context"] + common + ["--as-of", "2026-09-01T10:45:00Z"]) == 0
    assert json.loads(capsys.readouterr().out)["data"]["context"][0]["trend.efficiency_ratio_20::value"] == 1.0
    path = tmp_path / "CLI.csv"
    assert cli.main(base + ["export"] + common + ["--as-of", "2026-10-19T00:00Z", "--out", str(path)]) == 0
    assert json.loads(capsys.readouterr().out)["data"]["path"] == str(path)
    assert len(pd.read_csv(path)) == 25


def test_lab_adapter_requires_clock_and_preserves_input(db):
    seed(db, source(25))
    materializer.materialize(db, "TEST", "M5")
    trades = pd.DataFrame([dict(run_id="old", seq=1, symbol="TEST", timeframe="M5", entry_time="2026-09-01 10:45")])
    before = trades.copy(deep=True)
    missing = trade_context.for_trades(db, trades, feature_ids=["trend.efficiency_ratio_20"])
    assert missing.iloc[0]["trend.efficiency_ratio_20::reason"] == "CLOCK_REQUIRED"
    rows = trade_context.for_trades(db, trades, feature_ids=["trend.efficiency_ratio_20"], clocks={"old": Clock("utc")})
    assert rows.iloc[0]["trend.efficiency_ratio_20::value"] == 1
    pd.testing.assert_frame_equal(trades, before)


def test_open_upper_bar_is_absent_until_closed_without_new_source_sequence(db, monkeypatch):
    seed(db, source(25))
    monkeypatch.setattr(resampling, "_utc_now", lambda: pd.Timestamp("2026-09-01T09:12:00Z"))
    first = materializer.materialize(db, "TEST", "M5", feature_ids=["price.return_1"])
    assert first["bars"] == 2
    result = indicator_context.context(db, pd.DataFrame({"decision_time": ["2026-09-01T09:12:00Z"]}),
                                       symbol="TEST", timeframe="M5", feature_ids=["price.return_1"])
    assert result.iloc[0]["price.return_1::observation_time"] == pd.Timestamp("2026-09-01T09:05Z")
    monkeypatch.setattr(resampling, "_utc_now", lambda: pd.Timestamp("2026-09-01T09:17:00Z"))
    assert not resampling.has_closed_bars_after(db, "TEST", "M5", "2026-09-01T09:05Z")
    assert resampling.has_closed_bars_after(db, "TEST", "M5", "2026-09-01T09:05Z", require_complete_session=False)
    after = materializer.materialize(db, "TEST", "M5", feature_ids=["price.return_1"])
    assert not after["skipped"] and after["inserted"] == 1 and after["revised"] == 0


def test_resume_invalidation_keeps_stable_cursor_and_historical_values(db, monkeypatch):
    seed(db, source(5))
    materializer.materialize(db, "TEST", "M5", feature_ids=["volume.real"])
    original = feature_store.query_as_of(db, "2026-09-02T00:00Z", symbol="TEST")
    raw = resampling.latest_bars(db, "TEST", "M5", closed_only=False)
    cutoff = pd.Timestamp("2026-10-20T12:00Z")
    raw.loc[[2, 3], "revision"] = 2
    raw.loc[[2, 3], "computed_at_utc"] = cutoff
    raw.loc[[2, 3], "quality_status"] = "PARTIAL"
    closed = raw.drop(index=[2, 3]).reset_index(drop=True)
    monkeypatch.setattr(resampling, "latest_source_sequence", lambda *a, **k: 2)
    monkeypatch.setattr(resampling, "latest_bars", lambda *a, closed_only=True, **k: closed.copy() if closed_only else raw.copy())
    save = feature_store.save_values
    def fail_after_invalidation(db_, records, **kwargs):
        result = save(db_, records, **kwargs)
        if records and records[0]["quality"] == "INVALIDATED":
            raise RuntimeError("confirmação da invalidação interrompida")
        return result
    monkeypatch.setattr(feature_store, "save_values", fail_after_invalidation)
    with pytest.raises(RuntimeError, match="invalidação interrompida"):
        materializer.materialize(db, "TEST", "M5", feature_ids=["volume.real"], batch_size=1)
    monkeypatch.setattr(feature_store, "save_values", save)
    materializer.materialize(db, "TEST", "M5", feature_ids=["volume.real"], batch_size=1)
    before = feature_store.query_as_of(db, cutoff - pd.Timedelta(nanoseconds=1), symbol="TEST")
    pd.testing.assert_frame_equal(original, before)
    after = feature_store.query_as_of(db, cutoff, symbol="TEST")
    assert after.quality.eq("INVALIDATED").sum() == 2


def test_reopen_uses_persisted_checkpoint_and_legacy_database(db):
    from tests.test_analytics_relations_contract import test_feature_definition_migration_only_adds_typed_columns
    test_feature_definition_migration_only_adds_typed_columns(db)
    seed(db, source(25))
    materializer.materialize(db, "TEST", "M5", feature_ids=["trend.efficiency_ratio_20"])
    before = facts(db)
    db.close()
    reopened = DatabaseManager(db_path=db.db_path)
    try:
        assert materializer.materialize(reopened, "TEST", "M5", feature_ids=["trend.efficiency_ratio_20"])["skipped"]
        assert facts(reopened) == before
        with reopened.engine.connect() as conn:
            assert conn.execute(text("SELECT close FROM user_prices WHERE id=1")).scalar() == 1.25
    finally:
        reopened.close()


def test_frozen_run_is_not_enriched_from_unbound_live_store(db, monkeypatch):
    from types import SimpleNamespace
    monkeypatch.setattr(trade_context.datasets, "for_run", lambda *a: SimpleNamespace(manifest_id="frozen"))
    trades = pd.DataFrame([dict(run_id="fixed", seq=1, symbol="TEST", timeframe="M5", entry_time="2026-09-01T10:45Z")])
    result = trade_context.for_trades(db, trades, feature_ids=["trend.efficiency_ratio_20"])
    assert result.iloc[0]["trend.efficiency_ratio_20::reason"] == "MATERIALIZATION_DATASET_BINDING_REQUIRED"
    assert result.iloc[0]["run_manifest_id"] == "frozen"


def test_missing_source_does_not_publish_success_or_backdate_invalidations(db, monkeypatch):
    seed(db, source(5))
    materializer.materialize(db, "TEST", "M5", feature_ids=["volume.real"])
    before = facts(db)
    monkeypatch.setattr(resampling, "latest_source_sequence", lambda *a, **k: 2)
    monkeypatch.setattr(resampling, "latest_bars", lambda *a, **k: pd.DataFrame())
    with pytest.raises(ValueError, match="barra derivada"):
        materializer.materialize(db, "TEST", "M5", feature_ids=["volume.real"])
    assert facts(db) == before


def test_missing_source_observation_requires_reconciliation_without_rewriting_history(db, monkeypatch):
    seed(db, source(5))
    materializer.materialize(db, "TEST", "M5", feature_ids=["volume.real"])
    before = facts(db)
    remaining = resampling.latest_bars(db, "TEST", "M5", closed_only=False).drop(index=[2]).reset_index(drop=True)
    monkeypatch.setattr(resampling, "latest_source_sequence", lambda *a, **k: 2)
    monkeypatch.setattr(resampling, "latest_bars", lambda *a, **k: remaining.copy())
    with pytest.raises(ValueError, match="Restaure ou reconcilie"):
        materializer.materialize(db, "TEST", "M5", feature_ids=["volume.real"])
    assert facts(db) == before


def test_explicit_old_version_uses_its_persisted_contract(tmp_path):
    from mt5_extracao.analytics import features
    db = DatabaseManager(db_path=str(tmp_path / "old-contract.sqlite"))
    try:
        ident = "price.return_1"
        features.register(db, ident, "Legacy percent return", "legacy-percent", parameters={"period": 1, "scale": 100}, unit="percent", value_type="NUMBER")
        feature_store.save_values(db, [dict(symbol="TEST", timeframe="M5", feature_id=ident, algorithm_version="legacy-percent", observation_time="2026-09-01T09:00Z", available_at="2026-09-01T09:05Z", observed_at="2026-09-01T09:05Z", value=2.0)])
        frame = indicator_context.context(db, pd.DataFrame({"decision_time": ["2026-09-01T09:05Z"]}), symbol="TEST", timeframe="M5", feature_ids=[ident], algorithm_version="legacy-percent")
        assert frame.iloc[0][ident + "::unit"] == "percent"
        assert json.loads(frame.iloc[0][ident + "::parameters"]) == {"period": 1, "scale": 100}
        exported = indicator_context.projection(db, symbol="TEST", timeframe="M5", as_of="2026-09-01T09:05Z", feature_ids=[ident], algorithm_version="legacy-percent")
        assert exported.iloc[0][ident + "::unit"] == "percent"
        assert json.loads(exported.iloc[0][ident + "::parameters"]) == {"period": 1, "scale": 100}
    finally:
        db.close()


@pytest.mark.parametrize("operation", ["context", "projection"])
def test_explicit_unknown_version_is_rejected(db, operation):
    kwargs = dict(symbol="TEST", timeframe="M5", feature_ids=["price.return_1"], algorithm_version="unknown")
    with pytest.raises(ValueError, match="Confira a versão"):
        if operation == "context":
            indicator_context.context(db, pd.DataFrame({"decision_time": ["2026-09-01T09:05Z"]}), **kwargs)
        else:
            indicator_context.projection(db, as_of="2026-09-01T09:05Z", **kwargs)
