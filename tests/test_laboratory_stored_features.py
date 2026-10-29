"""Valores por versão no Laboratório: mesma junção causal, sem regra de direção inventada."""
import json

import pandas as pd
import pytest
from sqlalchemy import text

from mt5_extracao import cli, timeframes
from mt5_extracao.analytics import datasets, feature_store, indicator_registry
from mt5_extracao.laboratory import service
from tests.test_feature_snapshots import db, save

CUT = "2025-01-01T00:30Z"
PAIR = ("qa.metric", "v1")


def register(db, ident="qa.other", version="v1", kind="NUMBER", unit="ratio", family="qa"):
    feature_store.register_definition(db, ident, "QA " + ident, version, {"period":2}, [],
        value_type=kind, unit=unit, family=family)


def fill(db, values, ident="qa.metric", version="v1", tf="M5"):
    minutes = timeframes.parse(tf).minutes
    for i, value in enumerate(values):
        at = pd.Timestamp("2025-01-01T00:00Z") + pd.Timedelta(minutes=minutes * i)
        available = at + pd.Timedelta(minutes=minutes)
        feature_store.save_value(db, "QA", tf, ident, version, at, value,
                                 available_at=available, observed_at=available)


def report(db, pairs=(PAIR,), **changes):
    snaps = service.capture_selection(db, symbol="QA", timeframes_selected=["M5"], stored_features=pairs)
    return service.analyze(snaps, feature_ids=[], stored_features=pairs, as_of=CUT, **changes)


def test_raw_spearman_matches_independent_rank_oracle_and_never_creates_intentions(db):
    fill(db, [1., 2., 3., 4.])
    register(db)
    fill(db, [4., 1., 3., 2.], "qa.other")
    result = report(db, (PAIR, ("qa.other", "v1")))
    channels = {c["feature"]:c for c in result["channels"]}
    assert channels["qa.metric"]["values"] == [1., 2., 3., 4.]
    assert channels["qa.other"]["values"] == [4., 1., 3., 2.]
    matrix = service.relation_matrix(result)
    i, j = [next(k for k, c in enumerate(result["channels"]) if c["feature"] == ident)
            for ident in ("qa.metric", "qa.other")]
    # Sem empates: r_s = 1 - 6*sum(d^2)/(n*(n^2-1)); d=(-3,1,0,2).
    assert matrix["cells"][i][j]["effect"] == pytest.approx(1 - 6 * 14 / (4 * 15))
    assert matrix["cells"][i][j]["unique_source_pairs"] == 4
    assert all(c["directions"] == [None] * 4 for c in result["channels"])
    assert all(p["reason"] == "NO_DIRECTION_RULE" for p in result["pairs"])
    assert service.research_candidates(result) == []
    assert service.direction_outcomes(result, channels["qa.metric"]["id"])["direction_diagnosis"]
    json.dumps(result, allow_nan=False)


def test_late_revision_is_not_leaked_and_frozen_report_never_reads_current_db(db, monkeypatch):
    fill(db, [1., 2., 3., 4.])
    save(db, 999., available="2025-01-01T00:06Z")
    snaps = service.capture_selection(db, symbol="QA", timeframes_selected=["M5"], stored_features=[PAIR])
    save(db, 777., observation="2025-01-01T00:05Z", available="2025-01-01T00:11Z")
    monkeypatch.setattr(indicator_registry, "iter_core", lambda *a, **k: pytest.fail("não calcular fatos"))
    monkeypatch.setattr(feature_store, "query_history", lambda *a, **k: pytest.fail("não ler base atual"))
    result = service.analyze(snaps, feature_ids=[], stored_features=[PAIR], as_of=CUT)
    assert result["channels"][0]["values"] == [1., 2., 3., 4.]
    restored = service.load_report(json.dumps(result))
    assert restored == result
    assert restored["channels"][0]["frozen_revision_count"] == 5
    assert restored["channels"][0]["source_available"] == 4


def test_versions_units_parameters_and_missing_metadata_remain_distinct(db):
    register(db, ident="qa.metric", version="v2", unit="fraction")
    fill(db, [12., 13., 14., 15.])
    fill(db, [.12, .13, .14, .15], version="v2")
    result = report(db, (PAIR, ("qa.metric", "v2")))
    a, b = result["channels"]
    assert a["id"] != b["id"]
    assert [(c["algorithm_version"], c["unit"], c["parameters"]) for c in (a, b)] == [
        ("v1", "percent", {"period":5}), ("v2", "fraction", {"period":2})]
    assert a["values"] == [12., 13., 14., 15.] and b["values"] == [.12, .13, .14, .15]
    register(db, ident="qa.unknown", unit=None)
    fill(db, [0.], ident="qa.unknown")
    unknown = report(db, (("qa.unknown", "v1"),))["channels"][0]
    assert unknown["unit"] is None and unknown["values"] == [0.] * 4
    assert "Unidade não declarada" in " ".join(unknown["eligibility"]["limitations"])


def test_same_id_can_be_selected_as_explicit_local_formula_and_explicit_stored_version(db):
    ident = "price.return_1"
    version = indicator_registry.get_definition(ident)["algorithm_version"]
    register(db, ident=ident, version=version, unit="percent")
    fill(db, [11., 22., 33., 44.], ident, version)
    pairs = [(ident, version)]
    snapshots = service.capture_selection(db, symbol="QA", timeframes_selected=["M5"], stored_features=pairs)
    result = service.analyze(snapshots, feature_ids=[ident], stored_features=pairs, as_of=CUT)
    local, stored = result["channels"]
    assert local["id"] != stored["id"] and local["feature"] == stored["feature"]
    assert stored["values"] == [11., 22., 33., 44.]
    assert local["values"] == [None, 0., 0., 0.] and stored["source_kind"] == "stored"


@pytest.mark.parametrize("kind,value,raw", [("CATEGORY", "compra", "compra"), ("BOOLEAN", False, "false"),
                                          (None, 7., 7.)])
def test_typed_and_undeclared_outputs_are_preserved_but_not_encoded_as_numeric(db, kind, value, raw):
    register(db, ident="qa.typed", kind=kind)
    fill(db, [value], "qa.typed")
    result = report(db, (("qa.typed", "v1"),))
    c = result["channels"][0]
    assert c["raw_values"] == [raw] * 4
    assert c["values"] == c["directions"] == [None] * 4
    assert c["available"] == 0 and c["stored_available"] == 4
    assert c["diagnosis"]["code"] == "NON_NUMERIC_STORED_FEATURE"
    assert service.relation_matrix(result)["cells"][0][0]["reason"] == "NON_NUMERIC_STORED_FEATURE"
    assert service.target_matrix(result)["cells"][0][0]["reason"] == "NON_NUMERIC_STORED_FEATURE"


def test_stored_pivot_is_drawable_but_not_promoted_to_verified_events(db):
    register(db, ident="structure.qa_pivot", family="structure")
    fill(db, [1., 2., 3., 4.], "structure.qa_pivot")
    result = report(db, (("structure.qa_pivot", "v1"),))
    c = result["channels"][0]
    assert c["values"] == [1., 2., 3., 4.]
    assert c["events"] is None and c["diagnosis"]["code"] == "UNVERIFIED_STORED_EVENT"
    assert service.relation_matrix(result)["cells"][0][0]["effect"] is None
    assert service.target_matrix(result)["cells"][0][0]["reason"] == "UNVERIFIED_STORED_EVENT"


def test_global_grid_sees_between_closes_availability_expiry_and_window_limits(db):
    db.save_ohlcv_data("QA", timeframes.parse("M1").label, pd.DataFrame(dict(
        time=pd.date_range("2025-01-01", periods=20, freq="min"),
        open=100., high=101., low=99., close=100., tick_volume=10., real_volume=0., spread=1.)))
    feature_store.save_value(db, "QA", "M5", *PAIR, "2025-01-01T00:00Z", 0.,
        available_at="2025-01-01T00:06Z", observed_at="2025-01-01T00:06Z", valid_until="2025-01-01T00:08Z")
    snaps = service.capture_selection(db, symbol="QA", timeframes_selected=["M1", "M5"], stored_features=[PAIR])
    result = service.analyze(snaps, feature_ids=[], stored_features=[PAIR], as_of=CUT)
    c = next(c for c in result["channels"] if c["timeframe"] == "M5")
    assert c["values"][:9] == [None, None, None, None, 1., 0., 0., None, None]
    assert c["source_ends"][5] == "2025-01-01T00:06:00+00:00"
    assert c["source_available"] == 1 and c["stored_available"] == 3
    m1 = next(c for c in result["channels"] if c["timeframe"] == "M1")
    assert m1["diagnosis"]["code"] == "NO_FROZEN_VALUES"
    # M5 restrito a duas barras: não prolongar o último fato fora do snapshot selecionado.
    short = datasets.capture(db, "QA", "M5", start="2025-01-01T00:05", end="2025-01-01T00:10", stored_features=[PAIR])
    result = service.analyze([snaps[0], short], feature_ids=[], stored_features=[PAIR], as_of=CUT)
    c = next(c for c in result["channels"] if c["timeframe"] == "M5")
    assert c["values"] == [None] * 20
    assert c["states"]["OUTSIDE_SNAPSHOT_WINDOW"] == 9


def test_disjoint_snapshot_windows_are_explicitly_empty(db):
    db.save_ohlcv_data("QA", timeframes.parse("M1").label, pd.DataFrame(dict(
        time=pd.date_range("2025-01-02", periods=4, freq="min"),
        open=100., high=101., low=99., close=100., tick_volume=10., real_volume=0., spread=1.)))
    snaps = service.capture_selection(db, symbol="QA", timeframes_selected=["M1", "M5"], stored_features=[PAIR])
    result = service.analyze(snaps, feature_ids=[], stored_features=[PAIR], as_of="2025-01-03T00:00Z")
    c = next(c for c in result["channels"] if c["timeframe"] == "M5")
    assert c["values"] == [None] * 4 and c["states"] == {"OUTSIDE_SNAPSHOT_WINDOW":4}


def test_missing_pair_quality_and_channel_budget_fail_with_action(db, monkeypatch):
    old = datasets.capture(db, "QA", "M5")
    with pytest.raises(ValueError, match="sem contratos"):
        service.analyze([old], feature_ids=[], stored_features=[PAIR], as_of=CUT)
    snap = datasets.capture(db, "QA", "M5", stored_features=[PAIR])
    with pytest.raises(ValueError, match="não congeladas.*Capture"):
        service.analyze([snap], feature_ids=[], stored_features=[("qa.metric", "absent")], as_of=CUT)
    monkeypatch.setattr(service, "MAX_CHANNELS", 1)
    with pytest.raises(ValueError, match="excede 1"):
        service.analyze([snap], feature_ids=["price.return_1"], stored_features=[PAIR], as_of=CUT)
    with db.engine.begin() as conn:
        table = db.find_table_for_symbol("QA", timeframes.parse("M5").label)
        conn.execute(text(f'UPDATE "{table}" SET high=0'))
    c = report(db)["channels"][0]
    assert c["values"] == [None] * 4 and c["diagnosis"]["code"] == "INELIGIBLE_DATA"


def test_cli_freeze_analyze_stored_only_and_strategy_conflict(db, monkeypatch, capsys):
    fill(db, [1., 2., 3., 4.])
    monkeypatch.setattr(cli.Context, "db", property(lambda _:db))
    assert cli.main(["--json", "laboratory", "freeze", "--symbol", "QA", "--tf", "M5",
                     "--stored-feature", *PAIR]) == 0
    payload = json.loads(capsys.readouterr().out)["data"]
    manifest = payload["manifests"][0]["id"]
    argv = ["--json", "laboratory", "analyze", "--manifest", manifest, "--as-of", CUT,
            "--stored-feature", *PAIR]
    assert cli.main(argv) == 0
    result = json.loads(capsys.readouterr().out)["data"]
    assert result["selection"]["feature_ids"] == [] and len(result["channels"]) == 1
    assert result == report(db)
    assert cli.main(argv + ["--strategy-only"]) == 2
    assert not json.loads(capsys.readouterr().out)["ok"]
