import copy

import pandas as pd
import pytest

from mt5_extracao.analytics import indicator_registry
from mt5_extracao.chart_study import catalog, series, snapshot
from mt5_extracao.chart_study.service import StudyService, atomic
from mt5_extracao.strategies import REGISTRY, get_strategy
from tests.test_chart_study import capture, db


def test_catalog_uses_all_existing_strategies_and_features_with_explicit_rules():
    entries = catalog.entries()
    strategies = [v for v in entries if v["origin"] == "extractor_strategy"]
    features = [v for v in entries if v["origin"] == "extractor_indicator"]
    assert {s["id"] for s in strategies} == {"strategy:"+key for key in REGISTRY}
    assert {s["id"] for s in features} == {"feature:"+spec["feature_id"] for spec in indicator_registry.definitions()}
    assert all(s["legend"] and s["parameters"] is not None for s in entries)
    assert catalog.get("feature:momentum.rsi_14")["rule"] == "midpoint_50"
    assert catalog.get("feature:volatility.atr_14")["rule"] == "rising"


def test_indicator_projection_matches_registry_and_does_not_fabricate_warmup(capture):
    projection = series.build(capture, "feature:momentum.rsi_14")
    original = list(indicator_registry.iter_core(pd.DataFrame(capture["rates"]), feature_ids=["momentum.rsi_14"]))
    assert [r["value"] for r in projection["rows"]] == [r["value"] for r in original]
    assert all(r["direction"] is None for r in projection["rows"] if r["state"] != "AVAILABLE")
    for row in projection["rows"]:
        if row["value"] is not None:
            assert row["direction"] == (1 if row["value"] > 50 else -1 if row["value"] < 50 else 0)
    prefix = copy.deepcopy(capture)
    prefix["rates"] = prefix["rates"][:100]
    prefix["bar_epoch"] = prefix["rates"][-1]["time"]
    assert series.build(prefix, "feature:momentum.rsi_14")["rows"] == projection["rows"][:100]


@pytest.mark.parametrize("key", list(REGISTRY))
def test_strategy_projection_uses_existing_signal_generator_without_position_simulation(capture, key):
    last = capture["rates"][-1]
    for i in range(1,181):
        capture["rates"].append({**last, "time": last["time"]+i*60, "close": last["close"]})
    capture["bar_epoch"] = capture["rates"][-1]["time"]
    projection = series.build(capture, "strategy:"+key)
    frame = pd.DataFrame(capture["rates"])
    frame["time"] = pd.to_datetime(frame["time"], unit="s")
    strategy = get_strategy(key)
    expected = strategy.gerar_sinais(frame)
    for position, row in enumerate(projection["rows"]):
        if position+1 < strategy.min_bars:
            assert row["direction"] is None and row["state"] == "WARMUP"
        else:
            signal = expected.iloc[position]
            assert row["direction"] == (1 if signal["entry_long"] else -1 if signal["entry_short"] else 0)


def test_stable_series_mailbox_replaces_projection_and_preserves_notes(db, tmp_path, capture):
    root = tmp_path / "HDK_Study"
    service = StudyService(db, None, common_dir=tmp_path)
    layer = "s_layer_1"
    request = dict(contract=1, kind="study_series", request_id=layer, note_id=layer,
                   feature_id="feature:momentum.rsi_14", revision=1)
    capture["revision"] = 1
    atomic(root / "series_sources" / (layer+".json"), snapshot.json_text(capture))
    atomic(root / "requests" / (layer+".request.json"), snapshot.json_text(request))
    assert service.poll()[0]["state"] == "SERIES"
    first = (root / "series" / (layer+".tsv")).read_text()
    assert first.startswith("META\t1\tfeature:momentum.rsi_14")
    request["revision"] = capture["revision"] = 2
    request["feature_id"] = "feature:volatility.atr_14"
    atomic(root / "series_sources" / (layer+".json"), snapshot.json_text(capture))
    atomic(root / "requests" / (layer+".request.json"), snapshot.json_text(request))
    assert service.poll()[0]["revision"] == 2
    assert (root / "series" / (layer+".tsv")).read_text().startswith("META\t2\tfeature:volatility.atr_14")
    assert len(list((root / "processed").iterdir())) == 1
    assert not (root / "notes").exists()
    request["revision"] = 3
    atomic(root / "requests" / (layer+".request.json"), snapshot.json_text(request))
    assert service.poll()[0]["state"] == "ERROR"


def test_catalog_request_is_available_without_mt5_or_database_write(db, tmp_path):
    root = tmp_path / "HDK_Study"
    request = dict(contract=1, kind="study_catalog", request_id="catalog_1", note_id="catalog_1")
    atomic(root / "requests/catalog_1.request.json", snapshot.json_text(request))
    assert StudyService(db, None, common_dir=tmp_path).poll()[0]["state"] == "CATALOG"
    rows = (root / "catalog.tsv").read_text().splitlines()
    assert len(rows) == len(catalog.entries())
    assert rows[0].startswith("strategy:")


def test_changed_series_slot_is_not_archived_with_previous_request(db, tmp_path, capture, monkeypatch):
    root = tmp_path / "HDK_Study"
    service = StudyService(db, None, common_dir=tmp_path)
    layer = "s_changed"
    path = root / "requests" / (layer+".request.json")
    source = root / "series_sources" / (layer+".json")
    request = dict(contract=1, kind="study_series", request_id=layer, note_id=layer,
                   feature_id="feature:momentum.rsi_14", revision=1)
    capture["revision"] = 1
    atomic(source, snapshot.json_text(capture));atomic(path, snapshot.json_text(request))
    build = series.build
    def changed(captured, ident):
        # O usuário escolheu outra fonte enquanto a versão anterior era calculada.
        capture["revision"] = 2
        atomic(source, snapshot.json_text(capture))
        atomic(path, snapshot.json_text({**request, "revision": 2, "feature_id": "feature:volatility.atr_14"}))
        return build(captured, ident)
    monkeypatch.setattr(series, "build", changed)
    assert service.poll()[0]["revision"] == 1
    assert path.exists()
    monkeypatch.setattr(series, "build", build)
    assert service.poll()[0]["revision"] == 2
    assert not path.exists()
    assert (root / "series" / (layer+".tsv")).read_text().startswith("META\t2\tfeature:volatility.atr_14")


def test_malformed_request_is_rejected_and_does_not_block_next_note(db, tmp_path):
    root = tmp_path / "HDK_Study"
    service = StudyService(db, None, common_dir=tmp_path)
    atomic(root / "requests/a.request.json", "[")
    atomic(root / "requests/b.request.json", "[]")
    assert [row["state"] for row in service.poll()] == ["ERROR", "ERROR"]
    assert service.poll() == []
    assert len(list((root / "rejected").iterdir())) == 2
