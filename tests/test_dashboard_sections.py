"""Faixa de regime (DASH-04): adapta fatos persistidos, sem calcular nem preencher ausências."""
import pandas as pd
import pytest

from mt5_extracao.analytics import datasets, feature_store
from mt5_extracao.dashboard import document, generator

METHOD = "regime.confidence = fração (1/3, 2/3, 1) de ADX, eficiência e R² que concordam; não é probabilidade"


@pytest.fixture
def db(tmp_path):
    from mt5_extracao.database_manager import DatabaseManager
    manager = DatabaseManager(db_path=str(tmp_path / "qa.sqlite"))
    datasets.identity(manager)
    yield manager
    manager.close()


def register(db, version="v1", method=METHOD):
    params = {"confidence": method} if method else {}
    feature_store.register_definition(db, "regime.state", "regime state", version, params, [],
        family="regime", timeframe="M5", value_type="CATEGORY")
    feature_store.register_definition(db, "regime.confidence", "regime confidence", version, params, [],
        family="regime", unit="fração", timeframe="M5", value_type="NUMBER")


def save(db, feature, value, observation, *, version="v1", until=None):
    feature_store.save_value(db, "QA", "M5", feature, version, observation, value,
        available_at=observation + pd.Timedelta(minutes=5), observed_at=observation + pd.Timedelta(minutes=5),
        valid_until=until, source_revision="qa-source", metadata={"reconstructed": True})


def cut_and_hours():
    cut = pd.Timestamp.now(tz="UTC").floor("h") + pd.Timedelta(minutes=30)
    return cut, cut.floor("h"), cut.floor("h") - pd.Timedelta(hours=1)


def regime_section(result):
    return next(s for s in result["sections"] if s["kind"] == "regime")


def test_regime_heatmap_marks_cut_cell_and_keeps_confidence_semantics(db):
    register(db)
    cut, now, before = cut_and_hours()
    save(db, "regime.state", "RANGE", before)
    save(db, "regime.confidence", 1 / 3, before)
    save(db, "regime.state", "TREND", now)
    save(db, "regime.confidence", 2 / 3, now)
    result = generator.generate(db, symbol="QA", timeframe="M5", as_of=cut)
    section = regime_section(result)
    assert section["default_visible"] and section["display_state"] == "AVAILABLE"
    assert section["visual"]["value"] == "Tendência" and section["n_observations"] == 2
    current = section["details"]["current_cell"]
    assert (current["day"], current["hour"]) == (cut.weekday(), cut.hour)
    assert current["intensity"] == pytest.approx(2 / 3)
    assert section["evidence"]["confidence_is_probability"] is False
    assert section["evidence"]["confidence_method"] == METHOD
    assert result["capabilities"]["regime"]["state"] == "AVAILABLE"
    generator.validate(result)


def test_undeclared_confidence_method_leaves_intensity_empty(db):
    register(db, method=None)
    cut, now, _ = cut_and_hours()
    save(db, "regime.state", "RANGE", now)
    save(db, "regime.confidence", 1.0, now)
    section = regime_section(generator.generate(db, symbol="QA", timeframe="M5", as_of=cut))
    assert section["visual"]["cells"][0]["intensity"] is None
    assert section["visual"]["intensity_label"] == "Concordância não declarada"


def test_confidence_from_other_version_is_not_joined(db):
    register(db, "v1")
    register(db, "v2")
    cut, now, _ = cut_and_hours()
    save(db, "regime.state", "TREND", now, version="v1")
    save(db, "regime.confidence", 1.0, now, version="v2")
    result = generator.generate(db, symbol="QA", timeframe="M5", as_of=cut,
                                versions={"regime.state": "v1", "regime.confidence": "v2"})
    assert regime_section(result)["visual"]["cells"][0]["intensity"] is None


def test_stale_latest_state_hides_section_without_falling_back(db):
    register(db)
    cut, now, before = cut_and_hours()
    save(db, "regime.state", "RANGE", before)
    save(db, "regime.state", "TREND", now, until=cut - pd.Timedelta(minutes=1))
    result = generator.generate(db, symbol="QA", timeframe="M5", as_of=cut)
    section = regime_section(result)
    assert not section["default_visible"] and section["visual"]["value"] is None
    assert section["hidden_reason_code"] == "NO_DATA"
    assert result["capabilities"]["regime"]["state"] == "NO_DATA"


def test_no_regime_is_unavailable_not_zero(db):
    result = generator.generate(db, symbol="QA", timeframe="M5", as_of=cut_and_hours()[0])
    section = regime_section(result)
    assert section["display_state"] == "UNAVAILABLE" and section["visual"]["value"] is None
    assert "cells" not in section["visual"]


def test_document_embeds_regime_renderer_and_partial():
    assert "js/charts/regime.js" in document.RESOURCES
    assert "sections/regime.html" in document.TEMPLATES
