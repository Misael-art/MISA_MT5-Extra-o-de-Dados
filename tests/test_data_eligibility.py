"""T20.6: elegibilidade específica, origem consistente e causalidade sem MT5 real."""
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import inspect, text

from mt5_extracao.analytics import data_eligibility as q, datasets
from mt5_extracao.database_manager import DatabaseManager
from tests.test_backtester import bars, Manual


def prices():
    data = bars([[100, 101, 99, 100]] * 6, start="2025-01-06", freq="h")
    data["tick_volume"], data["real_volume"] = 50, 0
    return data


def ticks():
    epoch = int(pd.Timestamp("2025-01-06T00:00:00Z").value // 1_000_000)
    return pd.DataFrame({"time_msc": [epoch + n for n in (1000, 2000, 61000, 62000)],
                         "seq": [0, 0, 0, 0], "bid": 100., "ask": 101., "last": 100.5,
                         "volume": [1, 2, 2, 1], "volume_real": [1., 2., 2., 1.],
                         "flags": [56, 88, 2, 56]})


@pytest.fixture
def db(tmp_path):
    database = DatabaseManager(db_path=str(tmp_path / "quality.sqlite"), broker_utc_offset=0)
    database.save_ohlcv_data("X", "1 hora", prices())
    from mt5_extracao.strategies.specs import spec_from_symbol_info
    database.save_symbol_spec("X", spec_from_symbol_info(SimpleNamespace(point=1, trade_tick_size=1, trade_tick_value=1)))
    yield database
    database.close()


def save_ticks(db, origin="observed"):
    from datetime import datetime
    from mt5_extracao.tick_extractor import TickExtractor
    source = ticks().drop(columns="seq")
    connector = SimpleNamespace(get_ticks_range=lambda *args: source, data_origin=origin)
    result = TickExtractor(connector, db).extract("X", datetime(2025, 1, 6), datetime(2025, 1, 7))
    assert result["rows"] == 4


def test_missing_real_volume_blocks_only_real_volume_consumer():
    data = prices().drop(columns="real_volume")
    report = q.assess(data, bar_minutes=60)
    assert q.evaluate(report, ("ohlcv",))["eligible"]
    assert q.evaluate(report, ("tick_volume",))["eligible"]
    blocked = q.evaluate(report, ("real_volume",))
    assert not blocked["eligible"] and "real_volume" in blocked["reasons"][0]
    assert report["capabilities"]["real_volume"]["state"] == "missing"
    assert q.assess(prices())["capabilities"]["real_volume"]["state"] == "unknown"
    data["real_volume"] = [0, 1, 0, 2, 0, 3]
    actual = q.evaluate(q.assess(data), ("real_volume",))
    assert actual["eligible"] and actual["status"] == "limited"  # Valores não certificam a origem.
    data.loc[0, "high"] = 0  # Preço inválido não invalida um consumidor independente de quantidades.
    evidence = q.acquisition(data, "bars", origin="observed")
    independent = q.assess(data, evidence=[evidence])
    assert not q.evaluate(independent, ("ohlcv",))["eligible"]
    assert q.evaluate(independent, ("observed_real_volume",))["eligible"]


@pytest.mark.parametrize("problem", ["duplicate", "ohlc", "nonfinite", "timestamp"])
def test_invalid_bars_are_not_eligible_for_price_consumer(problem):
    frame = prices()
    if problem == "duplicate":
        frame = pd.concat([frame, frame.iloc[[0]]], ignore_index=True)
    elif problem == "ohlc":
        frame.loc[0, "close"] = 200
    elif problem == "nonfinite":
        frame.loc[0, "high"] = float("inf")
    else:
        frame.loc[0, "time"] = pd.NaT
    assert not q.evaluate(q.assess(frame), ("ohlcv",))["eligible"]


def test_gap_diagnostic_does_not_claim_full_coverage_or_block_unrelated_price_study():
    frame = prices()
    frame["time"] = pd.date_range("2025-01-06 09:00", periods=6, freq="h")
    frame = frame.drop(index=2)
    report = q.assess(frame, bar_minutes=60)
    assert report["ohlcv_diagnostic"]["gaps"]["count"] == 1
    assert q.evaluate(report, ("ohlcv",))["status"] == "limited"
    assert not q.evaluate(report, ("complete_bars",))["eligible"]


@pytest.mark.parametrize("origin", ["observed", "generated", "unknown"])
def test_origin_is_measured_without_turning_received_ticks_into_complete_history(origin):
    raw = ticks()
    evidence = q.acquisition(raw, "ticks", origin=origin)
    report = q.assess(prices(), ticks=raw, evidence=[evidence])
    measured = report["capabilities"]["ticks"]
    assert measured["origin"] == origin
    assert measured[origin + "_rows"] == 4
    assert q.evaluate(report, ("observed_ticks",))["eligible"] == (origin == "observed")
    assert report["capabilities"]["complete_real_ticks"]["coverage_fraction"] is None
    assert not q.evaluate(report, ("complete_real_ticks",))["eligible"]


def test_correction_or_conflicting_origin_invalidates_only_origin_proof():
    raw = ticks()
    evidence = q.acquisition(raw, "ticks", origin="observed")
    raw.loc[0, "last"] = 100.75
    report = q.assess(prices(), ticks=raw, evidence=[evidence])
    assert report["capabilities"]["ticks"]["unknown_rows"] == 2
    assert report["capabilities"]["ticks"]["observed_rows"] == 2
    assert q.evaluate(report, ("ticks",))["eligible"]
    assert not q.evaluate(report, ("observed_ticks",))["eligible"]
    original = ticks()
    opposite = q.acquisition(original, "ticks", origin="generated")
    assert q.verified_origin(original, "ticks", [evidence, opposite]) == "unknown"


def test_aggressor_uses_trade_flags_and_preserves_unknown_sides():
    raw = ticks()
    evidence = q.acquisition(raw, "ticks", origin="observed")
    report = q.assess(prices(), ticks=raw, evidence=[evidence])
    assert report["capabilities"]["aggressor"]["trades"] == 3  # Cotação carrega volume anterior, mas não é negócio.
    assert q.evaluate(report, ("aggressor",))["eligible"]
    raw.loc[0, "flags"] = 120  # Compra e venda simultaneamente: não atribuir lado arbitrário.
    evidence = q.acquisition(raw, "ticks", origin="observed")
    ambiguous = q.assess(prices(), ticks=raw, evidence=[evidence])
    assert ambiguous["capabilities"]["aggressor"]["classified_trades"] == 2
    assert not q.evaluate(ambiguous, ("aggressor",))["eligible"]


def test_l2_book_does_not_prove_queue_and_duplicate_tick_keys_are_invalid():
    raw = ticks()
    book = pd.DataFrame({"time_msc": [raw.time_msc.iloc[0]] * 2, "type": [1, 2], "price": [101., 100.], "volume": [2., 3.]})
    report = q.assess(prices(), book=book)
    assert q.evaluate(report, ("book_snapshots",))["eligible"]
    assert not q.evaluate(report, ("queue",))["eligible"]
    assert not q.evaluate(q.assess(prices(), ticks=pd.concat([raw, raw.iloc[[0]]])), ("ticks",))["eligible"]


@pytest.mark.parametrize("model", ["0", "1", "4"])
def test_tester_model_and_generic_history_quality_are_not_proof_of_real_ticks(model):
    report = q.tester_report("<table><tr><td>History Quality:</td><td>100%</td></tr></table>", model)
    assert report["history_quality_pct_reported"] == 100
    assert report["real_tick_fraction_reported"] is None
    assert report["origin"] == "unknown"
    assert not q.evaluate(report, ("complete_real_ticks",))["eligible"]


@pytest.mark.parametrize("label,value,expected", [("Real ticks:", "100%", 1), ("Ticks reais:", "75,5%", .755),
                                                 ("History Quality:", "0% real ticks", 0)])
def test_explicit_tester_coverage_is_reported_not_inferred(label, value, expected):
    report = q.tester_report(f"<table><tr><td>{label}</td><td><b>{value}</b></td></tr></table>", "4")
    assert report["real_tick_fraction_reported"] == expected
    assert q.evaluate(report, ("complete_real_ticks",))["eligible"] == (expected == 1)
    assert not q.evaluate(report, ("queue",))["eligible"]
    assert report["eligibility"]["status"] == "unqualified" and report["eligibility"]["eligible"] is None


def test_conflicting_report_or_unlabelled_trade_comment_is_not_coverage():
    html = "<table><tr><td>Comment</td><td>100% real ticks</td></tr></table>"
    assert q.tester_report(html, "4")["real_tick_fraction_reported"] is None
    html = "<tr><td>Real ticks</td><td>100%</td></tr><tr><td>Real ticks</td><td>50%</td></tr>"
    assert q.tester_report(html, "4")["real_tick_coverage_status"] == "conflicting"
    contradictory = q.tester_report("<tr><td>Real ticks</td><td>100%</td></tr>", "0")
    assert contradictory["real_tick_coverage_status"] == "model_report_conflict"
    assert not q.evaluate(contradictory, ("complete_real_ticks",))["eligible"]


def test_multi_timeframe_asset_join_uses_close_and_receipt_and_revision_cutoff():
    data = pd.DataFrame([
        dict(symbol="X", timeframe="H1", event_end="2025-01-06T11:00:00Z", available_at="2025-01-06T11:01:00Z", value=1),
        dict(symbol="X", timeframe="H1", event_end="2025-01-06T11:00:00Z", available_at="2025-01-06T12:00:00Z", value=2),
        dict(symbol="Y", timeframe="M5", event_end="2025-01-06T10:05:00Z", available_at="2025-01-06T10:06:00Z", value=3),
    ])
    early = q.as_of(data, "2025-01-06T10:30:00Z")
    assert early.symbol.tolist() == ["Y"]
    assert q.as_of(data, "2025-01-06T11:30:00Z").value.tolist() == [1, 3]
    assert q.as_of(data, "2025-01-06T12:00:00Z").value.tolist() == [2, 3]
    with pytest.raises(ValueError, match="fuso"):
        q.as_of(data, "2025-01-06 12:00")
    with pytest.raises(ValueError, match="explícitos"):
        q.as_of(data.drop(columns="available_at"), "2025-01-06T12:00:00Z")
    data.loc[0, "available_at"] = "2025-01-06T10:00:00Z"
    with pytest.raises(ValueError, match="anterior ao fechamento"):
        q.as_of(data, "2025-01-06T12:00:00Z")


def test_snapshot_raw_families_are_explicit_frozen_and_exportable(db, tmp_path):
    save_ticks(db)
    ordinary = datasets.capture(db, "X", "H1")
    assert ordinary.quality["capabilities"]["ticks"]["state"] == "not_captured"
    snapshot = datasets.capture(db, "X", "H1", families=["ticks"])
    assert snapshot.quality["capabilities"]["ticks"]["observed_rows"] == 4
    assert snapshot.quality["capabilities"]["complete_real_ticks"]["coverage_fraction"] is None
    table = db.find_table_for_symbol("X", "ticks")
    with db.engine.begin() as conn:
        conn.execute(text(f'UPDATE "{table}" SET last=200 WHERE time_msc=(SELECT MIN(time_msc) FROM "{table}")'))
    revised = datasets.capture(db, "X", "H1", families=["ticks"])
    assert revised.manifest_id != snapshot.manifest_id
    assert revised.quality["capabilities"]["ticks"]["unknown_rows"] == 2
    old = datasets.load(db, snapshot.manifest_id)
    assert old.market_frame("ticks")["last"].iloc[0] == 100.5
    assert old.quality["capabilities"]["ticks"]["observed_rows"] == 4
    with pytest.raises(ValueError, match="não capturada"):
        ordinary.market_frame("ticks")
    path = tmp_path / "quality.json"
    datasets.export_snapshot(db, snapshot.manifest_id, path)
    target = DatabaseManager(db_path=str(tmp_path / "target.sqlite"))
    try:
        copied = datasets.import_snapshot(target, path)
        assert copied.quality == snapshot.quality
        pd.testing.assert_frame_equal(copied.market_frame("ticks"), snapshot.market_frame("ticks"))
        assert not inspect(target.engine).has_table(table)
    finally:
        target.close()


def test_raw_period_uses_utc_and_excludes_end_boundary(db):
    raw = ticks()
    raw.loc[len(raw)] = [int(pd.Timestamp("2025-01-06T06:00:00Z").value // 1_000_000), 0, 100, 101, 100, 1, 1, 56]
    table = db.get_table_name_for_symbol("X", "ticks")
    db.save_ticks(table, raw)
    snapshot = datasets.capture(db, "X", "H1", families=["ticks"])
    assert len(snapshot.market_frame("ticks")) == 4
    db.broker_utc_offset = -3
    shifted = datasets.capture(db, "X", "H1", families=["ticks"])
    assert len(shifted.market_frame("ticks")) == 1  # 00:00–06:00 broker = 03:00–09:00 UTC.


def test_strategy_requirements_gate_and_resimulation_keep_same_quality(db):
    from mt5_extracao.robots import backtest, catalog, store
    from mt5_extracao.trade_analytics.execution import runner, engine
    robot = SimpleNamespace(kind=catalog.PYTHON, key="python:manual")
    strategy = Manual(long=[0], stop={0: 80})
    strategy.min_bars = 0
    first = backtest.run_python(db, None, robot, "X", "H1", strategy=strategy)
    assert first["metrics"]["data_quality"]["eligibility"]["eligible"]
    strategy.required_columns = (*strategy.required_columns, "real_volume")
    with pytest.raises(ValueError, match="Volume real"):
        backtest.run_python(db, None, robot, "X", "H1", strategy=strategy)
    assert len(store.load_runs(db)) == 1
    data = runner.inputs(db, None, first["run_id"])
    result = runner.resimulate(db, None, first["run_id"], engine.ExitPolicy(), data=data)
    assert result.valuation["data_quality"]["data_manifest_id"] == first["data_manifest_id"]


def test_registry_adapter_uses_existing_definitions_and_does_not_guess_new_sources():
    fixture = json.loads((Path(__file__).parent / "fixtures/data_quality/registry_definitions.json").read_text())
    actual = {item["feature_id"]: q.requirements(item) for item in fixture["definitions"]}
    assert actual["volume.real"] == ["real_volume"]
    assert actual["volume.ticks"] == ["tick_volume"]
    assert actual["momentum.rsi_14"] == ["price_close"]
    only_close = q.assess(prices().drop(columns=["open", "high", "low"]))
    assert q.evaluate(only_close, actual["momentum.rsi_14"])["eligible"]
    assert not q.evaluate(only_close, ("ohlcv",))["eligible"]
    atr = q.requirements(next(item for item in fixture["definitions"] if item["feature_id"] == "volatility.atr_14"))
    assert atr == ["hlc"]
    assert q.evaluate(q.assess(prices().drop(columns="open")), atr)["eligible"]
    bad = prices()
    bad.loc[0, "high"] = 98
    assert not q.evaluate(q.assess(bad), atr)["eligible"]
    with pytest.raises(ValueError, match="adaptador próprio"):
        q.requirements({"source_id": "book.queue", "required_columns": ["close"]})


def test_derived_coverage_requires_same_bar_values_and_scope_is_observed_sessions(db):
    from mt5_extracao.analytics.resampling import resample_symbol
    source = prices()
    source["time"] = pd.date_range("2025-01-06 09:00", periods=6, freq="min")
    db.save_ohlcv_data("X", "1 minuto", source)
    resample_symbol(db, "X", ["M5"])
    with db.engine.connect() as conn:
        frame = pd.read_sql(text("SELECT bar_start_broker AS time,open,high,low,close,tick_volume,real_volume,spread_min AS spread FROM _bars_derived"), conn)
    frame["time"] = pd.to_datetime(frame.time)
    db.save_ohlcv_data("X", "5 minutos", frame)
    full = datasets.capture(db, "X", "M5", end="2025-01-06 09:00:00")
    cap = full.quality["capabilities"]["observed_session_bars"]
    assert cap["state"] == "available" and cap["coverage_min"] == 1
    assert not q.evaluate(full.quality, ("complete_bars",))["eligible"]
    partial = datasets.capture(db, "X", "M5")
    assert partial.quality["capabilities"]["observed_session_bars"]["state"] == "missing"
    table = db.find_table_for_symbol("X", "5 minutos")
    with db.engine.begin() as conn:
        conn.execute(text(f'UPDATE "{table}" SET tick_volume=999'))
    changed = datasets.capture(db, "X", "M5")
    assert changed.quality["capabilities"]["observed_session_bars"]["state"] == "unknown"
    assert datasets.load(db, full.manifest_id).quality == full.quality


def test_existing_format_two_does_not_inherit_live_tick_evidence(db, tmp_path):
    save_ticks(db)
    fixture = Path(__file__).parent / "fixtures/data_quality/dataset_v2.json"
    legacy = datasets.import_snapshot(db, fixture)
    assert legacy.manifest["schema_version"] == 2
    assert legacy.quality["evidence_basis"] == "legacy_snapshot_no_acquisition_proof"
    assert legacy.quality["capabilities"]["ticks"]["state"] == "not_captured"
    output = tmp_path / "unchanged-v2.json"
    datasets.export_snapshot(db, legacy.manifest_id, output)
    assert output.read_bytes() == fixture.read_bytes()
    # Um campo livre no formato antigo não vira evidência validada pelo formato novo.
    import hashlib
    bundle = json.loads(fixture.read_text())
    bundle["manifest"]["derivation"]["data_quality"] = {"capabilities": {"complete_real_ticks": {"state": "available"}}}
    content = json.dumps(bundle["manifest"], ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    bundle["manifest_id"] = hashlib.sha256(content.encode()).hexdigest()
    unverified = tmp_path / "v2-unverified-quality.json"
    unverified.write_text(json.dumps(bundle))
    read = datasets.import_snapshot(db, unverified)
    assert not q.evaluate(read.quality, ("complete_real_ticks",))["eligible"]


def test_native_quality_excerpt_is_explicit_evidence_not_a_model_default():
    fixture = Path(__file__).parent / "fixtures/data_quality/native_quality_row.html"
    actual = q.tester_report(fixture.read_text(), "4")
    assert actual["real_tick_fraction_reported"] == 1
    assert actual["history_quality_pct_reported"] is None
    assert not q.evaluate(actual, ("queue",))["eligible"]
    assert "Requisitos específicos" in actual["eligibility"]["limitations"][0]


def test_new_book_collection_records_provenance_but_not_queue_or_venue_clock(db):
    from mt5_extracao.book_collector import BookCollector
    clock = int(pd.Timestamp("2025-01-06T00:00:00Z").timestamp())
    connector = SimpleNamespace(data_origin="observed", book_subscribe=lambda s: True,
                                book_release=lambda s: None,
                                book_snapshot=lambda s: [dict(type=1, price=101, volume=2), dict(type=2, price=100, volume=3)])
    summary = BookCollector(connector, db, clock=lambda: clock, sleep=lambda n: None).run(["X"], max_snapshots=1)
    assert summary["X"]["rows"] == 2
    snapshot = datasets.capture(db, "X", "H1", families=["book"])
    report = snapshot.quality
    assert report["capabilities"]["book_snapshots"]["origin"] == "observed"
    assert not q.evaluate(report, ("queue",))["eligible"]
    assert not q.evaluate(report, ("aggressor",))["eligible"]
    with pytest.raises(ValueError, match="explícitos"):
        q.as_of(snapshot.market_frame("book"), "2025-01-06T01:00:00Z")


def test_no_private_importer_metadata_is_automatically_exported(db, tmp_path):
    save_ticks(db)
    marker = "PRIVATE_IMPORTER_SENTINEL"
    with db.engine.begin() as conn:
        row = conn.execute(text("SELECT quality_json FROM _extraction_log WHERE source='mt5'")).scalar_one()
        metadata = json.loads(row)
        metadata["config"] = {"password": marker}
        metadata["origin_basis"] = "/private/" + marker
        metadata["minute_proofs"][0]["password"] = marker
        metadata["reported_fields"].append(marker)
        conn.execute(text("UPDATE _extraction_log SET quality_json=:q"), {"q": json.dumps(metadata)})
    snapshot = datasets.capture(db, "X", "H1", families=["ticks"])
    assert snapshot.quality["capabilities"]["ticks"]["origin"] == "observed"
    path = tmp_path / "safe.json"
    datasets.export_snapshot(db, snapshot.manifest_id, path)
    assert marker not in path.read_text()


def test_claimed_quality_cannot_override_actual_captured_measurements(db, tmp_path):
    import hashlib
    snapshot = datasets.capture(db, "X", "H1")
    path = tmp_path / "forged.json"
    datasets.export_snapshot(db, snapshot.manifest_id, path)
    bundle = json.loads(path.read_text())
    bundle["manifest"]["derivation"]["data_quality"]["capabilities"]["complete_real_ticks"]["state"] = "available"
    content = json.dumps(bundle["manifest"], ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    bundle["manifest_id"] = hashlib.sha256(content.encode()).hexdigest()
    path.write_text(json.dumps(bundle))
    with pytest.raises(ValueError, match="integridade"):
        datasets.import_snapshot(db, path)


def test_monthly_raw_window_uses_calendar_month_not_thirty_days(db):
    month = prices().iloc[[0]].copy()
    month["time"] = pd.to_datetime(["2025-01-01"])
    db.save_ohlcv_data("X", "1 mês", month)
    raw = ticks().iloc[:2].copy()
    raw["time_msc"] = [int(pd.Timestamp(x).value // 1_000_000) for x in ("2025-01-31T12:00:00Z", "2025-02-01T00:00:00Z")]
    db.save_ticks(db.get_table_name_for_symbol("X", "ticks"), raw)
    snapshot = datasets.capture(db, "X", "MN1", families=["ticks"])
    assert len(snapshot.market_frame("ticks")) == 1
    assert snapshot.market_frame("ticks").event_time_utc.iloc[0] == pd.Timestamp("2025-01-31T12:00:00Z")


def test_broker_origin_declaration_does_not_promote_custom_or_unknown_symbols(connector, monkeypatch):
    for custom, expected in ((False, "observed"), (True, "unknown"), (None, "unknown")):
        monkeypatch.setattr(connector, "get_symbol_info", lambda s, value=custom: SimpleNamespace(custom=value))
        assert connector.data_origin("X") == expected


def test_incomplete_book_schema_blocks_only_its_consumer(db):
    table = db.get_table_name_for_symbol("X", "book")
    epoch = int(pd.Timestamp("2025-01-06T00:00:00Z").value // 1_000_000)
    with db.engine.begin() as conn:
        conn.execute(text(f'CREATE TABLE "{table}" (time_msc BIGINT PRIMARY KEY)'))
        conn.execute(text(f'INSERT INTO "{table}" VALUES (:time)'), {"time": epoch})
    snapshot = datasets.capture(db, "X", "H1", families=["book"])
    assert q.evaluate(snapshot.quality, ("ohlcv",))["eligible"]
    assert not q.evaluate(snapshot.quality, ("book_snapshots",))["eligible"]


def test_bar_collector_preserves_source_proofs_and_reports_input_duplicates(db):
    from mt5_extracao.historical_extractor import HistoricalExtractor
    from mt5_extracao.indicator_calculator import IndicatorCalculator
    frame = prices()
    frame["real_volume"] = range(1, 7)
    frame = pd.concat([frame, frame.iloc[[0]]], ignore_index=True)
    extractor = HistoricalExtractor(SimpleNamespace(), db, IndicatorCalculator())
    table = db.find_table_for_symbol("X", "1 hora")
    saved, metadata = extractor._save_block("X", "1 hora", table, frame, frame.time.min(), False, origin="observed")
    assert saved == 6 and len(metadata["duplicate_bar_times"]) == 1
    db.record_block(table, frame.time.min().to_pydatetime(), frame.time.max().to_pydatetime(), saved, "ok", "fixture", quality=metadata)
    snapshot = datasets.capture(db, "X", "H1")
    assert snapshot.quality["capabilities"]["ohlcv"]["observed_rows"] == 6
    assert snapshot.quality["source_duplicate_bars_reported"] == 1
    assert q.evaluate(snapshot.quality, ("observed_real_volume",))["eligible"]
    assert q.evaluate(snapshot.quality, ("ohlcv",))["status"] == "limited"
    with db.engine.begin() as conn:
        conn.execute(text(f'UPDATE "{table}" SET close=100.5'))
    changed = datasets.capture(db, "X", "H1")
    assert changed.quality["capabilities"]["ohlcv"]["unknown_rows"] == 6
    assert datasets.load(db, snapshot.manifest_id).quality["capabilities"]["ohlcv"]["observed_rows"] == 6


def test_proof_from_another_timeframe_cannot_qualify_the_current_series():
    frame = prices()
    evidence = q.acquisition(frame, "bars", origin="observed", symbol="X", timeframe="M1")
    correct = q.assess(frame, evidence=[evidence], symbol="X", timeframe="M1")
    wrong = q.assess(frame, evidence=[evidence], symbol="X", timeframe="H1")
    assert correct["capabilities"]["ohlcv"]["origin"] == "observed"
    assert wrong["capabilities"]["ohlcv"]["origin"] == "unknown"


def test_new_quality_frames_share_same_transaction_with_raw_ticks(db, monkeypatch):
    import threading
    save_ticks(db)
    with db.engine.connect() as conn:
        conn.exec_driver_sql("PRAGMA journal_mode=WAL")
        conn.commit()
    table = db.find_table_for_symbol("X", "ticks")
    updated = ticks()
    updated["last"] = 100.75
    proof = q.acquisition(updated, "ticks", origin="observed", symbol="X", received_at="2025-01-07T00:00:00Z")
    errors = []
    original = db.load_market_frame

    def writer():
        try:
            with db.engine.begin() as conn:
                conn.execute(text(f'UPDATE "{table}" SET last=100.75'))
                conn.execute(text("UPDATE _extraction_log SET quality_json=:proof WHERE table_name=:table"),
                             {"proof": json.dumps(proof), "table": table})
        except Exception as error:
            errors.append(error)

    def reading(*args, **kwargs):
        data = original(*args, **kwargs)
        thread = threading.Thread(target=writer)
        thread.start()
        thread.join(timeout=5)
        assert not thread.is_alive() and not errors
        return data

    monkeypatch.setattr(db, "load_market_frame", reading)
    captured = datasets.capture(db, "X", "H1", families=["ticks"])
    assert captured.market_frame("ticks")["last"].eq(100.5).all()
    assert captured.quality["capabilities"]["ticks"]["origin"] == "observed"
    monkeypatch.setattr(db, "load_market_frame", original)
    revised = datasets.capture(db, "X", "H1", families=["ticks"])
    assert revised.market_frame("ticks")["last"].eq(100.75).all()
    assert revised.quality["capabilities"]["ticks"]["origin"] == "observed"


def test_missing_provenance_write_does_not_leak_book_subscription(db, monkeypatch):
    from mt5_extracao.book_collector import BookCollector
    released = []
    clock = int(pd.Timestamp("2025-01-06T00:00:00Z").timestamp())
    connector = SimpleNamespace(data_origin="observed", book_subscribe=lambda s: True,
                                book_release=released.append, book_snapshot=lambda s: [dict(type=1, price=101, volume=2)])
    def unavailable(*args, **kwargs):
        raise RuntimeError("fixture failure")
    monkeypatch.setattr(db, "record_block", unavailable)
    result = BookCollector(connector, db, clock=lambda: clock, sleep=lambda n: None).run(["X"], max_snapshots=1)
    assert result["X"]["rows"] == 1 and released == ["X"]
    snapshot = datasets.capture(db, "X", "H1", families=["book"])
    assert not q.evaluate(snapshot.quality, ("observed_book_snapshots",))["eligible"]
    assert q.evaluate(snapshot.quality, ("ohlcv",))["eligible"]
