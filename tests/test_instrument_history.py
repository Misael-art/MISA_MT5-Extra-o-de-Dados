"""T20.5: validade, conhecimento e revisões de instrumentos sem fabricar eventos."""
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import inspect, text

from mt5_extracao.analytics import datasets, instruments
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.strategies import specs
from tests.test_backtester import bars, Manual

FIRST = "2025-01-01T00:00:00Z"
OBSERVED = "2024-12-31T00:00:00Z"


def properties(step=1):
    return specs.spec_from_symbol_info(SimpleNamespace(point=1, trade_tick_size=1, trade_tick_value=1,
                                                       volume_min=step, volume_step=step, volume_max=100))


@pytest.fixture
def db(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "instruments.sqlite"), broker_utc_offset=0)
    db.save_symbol_spec("X", properties(5))
    frame = bars([[100, 101, 99, 100]] * 6, start="2025-01-06", freq="h")
    frame["tick_volume"], frame["real_volume"] = 1, 0
    db.save_ohlcv_data("X", "1 hora", frame)
    yield db
    db.close()


def record(db, step=1, **options):
    defaults = dict(source="fixture_contract_bulletin", known_at=OBSERVED, valid_from=FIRST)
    defaults.update(options)
    return instruments.record(db, "X", "specification", properties(step), **defaults)


def test_current_observation_has_source_but_does_not_prove_historical_validity(db):
    frame = instruments.history(db, "X")
    assert len(frame) == 1 and frame.source.tolist() == ["local_observation"]
    assert frame.valid_from.isna().all()
    assert instruments.specification(frame, "2025-01-06T00:00:00Z") is None
    assert specs.load_spec(db, "X").known  # Disponibilidade atual preservada.
    assert specs.load_spec(db, "X").provenance["specification_status"] == "unknown"
    # Observações atuais sem vigência não ganham validade por terem sido conhecidas antes.
    for step in (1, 2):
        instruments.record(db, "X", "specification", properties(step), source="observation_only",
                           known_at=OBSERVED)
    snapshot = datasets.capture(db, "X", "H1")
    assert snapshot.spec.volume_step == 5
    assert snapshot.spec.provenance["specification_status"] == "unknown"
    assert snapshot.spec.provenance["adjustments_status"] == "source_unavailable"


def test_late_knowledge_and_half_open_validity_do_not_look_ahead(db):
    first = record(db)
    second = record(db, 2, valid_from="2025-01-06T02:00:00Z", known_at="2025-01-07T00:00:00Z")
    history = instruments.history(db, "X")
    at = "2025-01-06T03:00:00Z"
    assert instruments.specification(history, at)["revision_id"] == first
    assert instruments.specification(history, at, known_at="2025-01-08T00:00:00Z")["revision_id"] == second
    # Uma correção que limita a vigência substitui o evento anterior antes do filtro temporal.
    record(db, valid_to="2025-01-06T02:00:00Z", known_at="2025-01-07T01:00:00Z")
    history = instruments.history(db, "X")
    assert instruments.specification(history, at, known_at="2025-01-08T00:00:00Z")["revision_id"] == second
    before = instruments.specification(history, "2025-01-06T01:59:59Z", known_at="2025-01-08T00:00:00Z")
    assert before["payload"]["volume_step"] == 1


def test_revision_is_idempotent_and_conflicting_same_time_is_not_arbitrarily_selected(db):
    revision = record(db)
    assert record(db) == revision
    assert len(instruments.history(db, "X")) == 2  # Atual + declaração histórica.
    record(db, 2)
    with pytest.raises(ValueError, match="conflitantes"):
        instruments.specification(instruments.history(db, "X"), "2025-01-06T00:00:00Z")


def test_historical_snapshot_resolves_declared_spec_not_current_cache(db):
    revision = record(db)
    snapshot = datasets.capture(db, "X", "H1")
    assert snapshot.spec.volume_step == 1
    assert snapshot.spec.provenance["specification_status"] == "known"
    assert snapshot.spec.provenance["specification_revisions"] == [revision]
    assert revision in snapshot.instrument_history.revision_id.tolist()
    assert snapshot.view().get_symbol_spec("X")["volume_step"] == 1
    assert specs.load_spec(snapshot.view(), "X").volume_step == 1
    assert db.get_symbol_spec("X")["volume_step"] == 5
    correction = record(db, 2, known_at="2025-01-05T00:00:00Z")
    revised = datasets.capture(db, "X", "H1")
    assert revised.manifest_id != snapshot.manifest_id and correction != revision
    assert revised.spec.volume_step == 2
    assert datasets.load(db, snapshot.manifest_id).spec.volume_step == 1


def test_step_change_in_period_is_visible_and_constant_spec_motors_refuse_it(db):
    from mt5_extracao.strategies.backtester import BacktestConfig, run_backtest
    from mt5_extracao.trade_analytics.execution import engine
    record(db)
    record(db, 2, valid_from="2025-01-06T03:00:00Z", known_at="2025-01-05T00:00:00Z")
    snapshot = datasets.capture(db, "X", "H1")
    context = snapshot.spec.provenance
    assert context["specification_status"] == "varying" and context["specification_value_sets"] == 2
    with pytest.raises(ValueError, match="variam no período"):
        run_backtest(snapshot.bars, Manual(), snapshot.spec)
    with pytest.raises(ValueError, match="variam no período"):
        engine.simulate(pd.DataFrame(), snapshot.bars, snapshot.spec, BacktestConfig(), engine.ExitPolicy())


def test_discontinued_symbol_belongs_to_historical_universe_without_current_listing(db):
    instruments.record(db, "OLD", "universe", {"universe": "reference", "member": True},
                       source="exchange", valid_from=FIRST, known_at=OBSERVED)
    instruments.record(db, "OLD", "universe", {"universe": "reference", "member": False},
                       source="exchange", valid_from="2025-02-01T00:00:00Z", known_at="2025-01-31T00:00:00Z")
    instruments.record(db, "OLD", "universe", {"universe": "other", "member": True},
                       source="exchange", valid_from=FIRST, known_at=OBSERVED)
    assert instruments.universe_at(db, "reference", "2025-01-10T00:00:00Z")["members"] == ["OLD"]
    retired = instruments.universe_at(db, "reference", "2025-02-01T00:00:00Z")
    assert retired["members"] == [] and retired["status"] == "declared"
    assert instruments.universe_at(db, "absent_feed", "2025-01-10T00:00:00Z")["status"] == "source_unavailable"
    assert instruments.universe_at(db, "other", "2025-02-01T00:00:00Z")["members"] == ["OLD"]


def test_rollover_adjustment_and_continuous_series_are_facts_not_implicit_orders(db):
    record(db)
    before = datasets.capture(db, "X", "H1")
    options = dict(source="reference_bulletin", valid_from=FIRST, known_at=OBSERVED)
    instruments.record(db, "X", "series", {"role": "continuous_study", "adjustment_basis": "ratio"}, **options)
    instruments.record(db, "X", "rollover", {"from_contract": "XF25", "to_contract": "XG25", "method": "none"}, **options)
    instruments.record(db, "X", "expiration", {"contract": "XF25", "settlement": "cash"}, **options)
    instruments.record(db, "X", "adjustment", {"method": "ratio", "factor": 0.5, "reason": "declared_split"}, **options)
    after = datasets.capture(db, "X", "H1")
    assert before.manifest_id != after.manifest_id
    context = after.spec.provenance
    assert context["series_role"] == "continuous_study"
    assert context["rollovers_status"] == "declared_not_executed"
    assert context["expirations_status"] == "declared_not_settled"
    assert context["expired_contracts"] == ["XF25"]
    assert context["adjustments_status"] == "declared_not_applied"
    pd.testing.assert_frame_equal(before.bars, after.bars)  # Nenhum ajuste inventado/aplicado duas vezes.
    instruments.record(db, "XF25", "series", {"role": "executable_contract", "execution_symbol": "XF25"}, **options)
    executable = instruments.select(instruments.history(db, "XF25"), "series", "2025-01-06T00:00:00Z")
    assert json.loads(executable.payload_json.iloc[0])["role"] == "executable_contract"
    assert "XF25" not in after.bars.columns


def test_version_two_export_keeps_facts_and_legacy_v1_still_imports(db, tmp_path):
    record(db)
    snapshot = datasets.capture(db, "X", "H1")
    path = tmp_path / "v2.json"
    datasets.export_snapshot(db, snapshot.manifest_id, path)
    target = DatabaseManager(db_path=str(tmp_path / "target.sqlite"))
    try:
        imported = datasets.import_snapshot(target, path)
        assert imported.spec.volume_step == 1 and imported.manifest_id == snapshot.manifest_id
        assert not inspect(target.engine).has_table(instruments.TABLE)
        legacy = datasets.import_snapshot(target, Path(__file__).parent / "fixtures/datasets/v1.json")
        assert legacy.manifest["schema_version"] == 1
        assert legacy.instrument_history is None
        assert legacy.spec.provenance["specification_status"] == "unknown"
        assert legacy.spec.known and legacy.spec.volume_step == 1
        output = tmp_path / "legacy-export.json"
        datasets.export_snapshot(target, legacy.manifest_id, output)
        assert output.read_bytes() == (Path(__file__).parent / "fixtures/datasets/v1.json").read_bytes()
    finally:
        target.close()


@pytest.mark.parametrize("options", [
    {"known_at": "2025-01-01"}, {"valid_from": "2025-01-01"},
    {"valid_from": FIRST, "valid_to": FIRST}, {"source": "file:///private/credentials"},
])
def test_bad_provenance_is_rejected_before_history_write(db, options):
    before = instruments.history(db, "X")
    with pytest.raises(ValueError):
        record(db, **options)
    pd.testing.assert_frame_equal(instruments.history(db, "X"), before)


def test_corrupted_instrument_revision_cannot_be_published_as_new_snapshot(db):
    record(db)
    with db.engine.begin() as conn:
        conn.execute(text("UPDATE _instrument_facts SET payload_json=:p WHERE valid_from IS NOT NULL"),
                     {"p": json.dumps(properties(2))})
    with pytest.raises(ValueError, match="integridade"):
        datasets.capture(db, "X", "H1")


def test_legacy_cache_is_read_without_inventing_history_or_rewriting_it(db):
    before = db.get_symbol_spec("X")
    with db.engine.begin() as conn:
        conn.execute(text("DROP TABLE _instrument_facts"))  # Fixture simula schema anterior à tarefa.
    assert specs.load_spec(db, "X").provenance["historical_validity"] == "not_established"
    snapshot = datasets.capture(db, "X", "H1")
    assert snapshot.spec.provenance["specification_status"] == "unknown"
    assert not inspect(db.engine).has_table(instruments.TABLE)
    assert db.get_symbol_spec("X") == before


def test_broker_time_is_converted_explicitly_when_resolving_validity(db):
    db.broker_utc_offset = -3
    revision = record(db, valid_from="2025-01-06T03:00:00Z")
    snapshot = datasets.capture(db, "X", "H1")
    assert snapshot.spec.provenance["specification_status"] == "known"
    assert snapshot.spec.provenance["specification_revisions"] == [revision]
    assert snapshot.spec.volume_step == 1


def test_f15_protects_instrument_history_and_backup_preserves_it(db):
    from mt5_extracao.resources import actions, files
    revision = record(db)
    result = actions.archive_tables(db, None, [instruments.TABLE])[0]
    assert not result["ok"] and "interna" in result["error"]
    copied = files.backup(db, None, prune=False)
    assert copied["ok"], copied["error"]
    restored = DatabaseManager(db_path=copied["path"])
    try:
        assert revision in instruments.history(restored, "X").revision_id.tolist()
    finally:
        restored.close()


def test_run_ledger_and_resimulation_reuse_frozen_historical_properties(db):
    from configparser import ConfigParser
    from mt5_extracao.robots import backtest, catalog, store
    from mt5_extracao.trade_analytics.execution import runner, engine
    cfg = ConfigParser()
    cfg.read_dict({"STRATEGY": {"initial_equity": "1000", "risk_per_trade": "0.03"}})
    robot = SimpleNamespace(kind=catalog.PYTHON, key="python:manual")

    def run(**kwargs):
        strategy = Manual(long=[0], stop={0: 80})
        strategy.min_bars = 0
        return backtest.run_python(db, cfg, robot, "X", "H1", strategy=strategy, **kwargs)

    unknown = run()
    assert not unknown["metrics"]["spec_historical_known"]
    assert unknown["valuation"]["status"] == "limited"
    revision = record(db)
    original = run()
    assert original["metrics"]["spec_historical_known"] and len(original["trades"]) == 1
    assert original["metrics"]["instrument_context"]["specification_revisions"] == [revision]
    record(db, 2, known_at="2025-01-05T00:00:00Z")
    replay = run(data_manifest_id=original["data_manifest_id"])
    assert replay["trades"] == original["trades"]
    data = runner.inputs(db, cfg, original["run_id"])
    assert data["spec"].volume_step == 1
    result = runner.resimulate(db, cfg, original["run_id"], engine.ExitPolicy(), data=data)
    assert result.valuation["instrument_context"]["specification_revisions"] == [revision]
    loaded = store.load_runs(db, run_id=original["run_id"])[0]
    assert loaded["metrics"]["instrument_context"] == original["metrics"]["instrument_context"]


def test_import_does_not_attach_valid_facts_to_the_wrong_instrument(db, tmp_path):
    import hashlib
    record(db)
    snapshot = datasets.capture(db, "X", "H1")
    path = tmp_path / "wrong-symbol.json"
    datasets.export_snapshot(db, snapshot.manifest_id, path)
    bundle = json.loads(path.read_text())
    bundle["manifest"]["symbol"] = "Y"
    bundle["manifest"]["specification"]["symbol"] = "Y"
    canonical = json.dumps(bundle["manifest"], ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    bundle["manifest_id"] = hashlib.sha256(canonical.encode()).hexdigest()
    path.write_text(json.dumps(bundle))
    with pytest.raises(ValueError, match="integridade"):
        datasets.import_snapshot(db, path)


def test_existing_symbol_names_with_separators_remain_valid(db):
    symbol = "BRK/B"
    db.save_symbol_spec(symbol, properties())
    db.save_ohlcv_data(symbol, "1 hora", db.load_ohlcv("X", "1 hora"))
    revision = instruments.record(db, symbol, "specification", properties(), source="fixture",
                                  valid_from=FIRST, known_at=OBSERVED)
    instruments.record(db, symbol, "rollover", {"from_contract": "OLD/A", "to_contract": symbol, "method": "none"},
                       source="fixture", valid_from=FIRST, known_at=OBSERVED)
    snapshot = datasets.capture(db, symbol, "H1")
    assert snapshot.spec.symbol == symbol and snapshot.spec.volume_step == 1
    assert snapshot.spec.provenance["specification_revisions"] == [revision]


@pytest.mark.parametrize("known_at", [OBSERVED, "2025-01-07T00:00:00Z"])
def test_expiration_is_explicit_and_period_after_maturity_is_not_simulated_as_executable(db, known_at):
    from mt5_extracao.strategies.backtester import run_backtest
    record(db)
    instruments.record(db, "X", "series", {"role": "executable_contract", "execution_symbol": "X"},
                       source="exchange", valid_from=FIRST, known_at=OBSERVED)
    instruments.record(db, "X", "expiration", {"contract": "X", "settlement": "physical"}, source="exchange",
                       valid_from="2025-01-06T03:00:00Z", known_at=known_at)
    snapshot = datasets.capture(db, "X", "H1")
    assert snapshot.spec.provenance["expired_contracts"] == ["X"]
    assert snapshot.spec.provenance["execution_symbols"] == ["X"]
    with pytest.raises(ValueError, match="vence no período"):
        run_backtest(snapshot.bars, Manual(), snapshot.spec)
    before = datasets.capture(db, "X", "H1", end="2025-01-06 02:00:00")
    assert before.spec.provenance["expired_contracts"] == []
    assert run_backtest(before.bars, Manual(), before.spec).trades == []


@pytest.mark.parametrize("correction", ["changed_value", "withdrawn_validity"])
def test_late_correction_is_not_reported_as_historically_known_execution(db, correction):
    from mt5_extracao.strategies.backtester import run_backtest
    record(db)
    if correction == "changed_value":
        revision = record(db, 2, valid_from="2025-01-06T03:00:00Z", known_at="2025-01-07T00:00:00Z")
    else:
        revision = record(db, valid_to="2025-01-06T03:00:00Z", known_at="2025-01-07T00:00:00Z")
    snapshot = datasets.capture(db, "X", "H1")
    assert snapshot.spec.provenance["specification_status"] == "late_revision"
    assert snapshot.spec.provenance["late_specification_revisions"] == [revision]
    with pytest.raises(ValueError, match="Revisão tardia"):
        run_backtest(snapshot.bars, Manual(), snapshot.spec)
