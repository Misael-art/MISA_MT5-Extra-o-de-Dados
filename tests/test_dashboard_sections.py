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
    # O contrato é registrado agora. O corte nunca pode cair antes do registro
    # quando a suíte roda na segunda metade da hora.
    cut = pd.Timestamp.now(tz="UTC").ceil("h") + pd.Timedelta(minutes=30)
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


def register_volume(db, source="tick", weekday=False, version="v1"):
    prefix = "rvol.same_weekday" if weekday else "rvol"
    params = {"volume_source": source, "same_weekday": weekday, "lookback": 20, "min_sessions": 5}
    for ident in (prefix + "." + source, prefix + ".reference_sessions"):
        feature_store.register_definition(db, ident, ident, version, params, [],
            family="rvol", unit="razão" if ident.endswith(source) else "sessões", timeframe="M5", value_type="NUMBER")
    return prefix + "." + source


def volume_sections(result):
    return [s for s in result["sections"] if s["kind"] == "volume"]


def test_volume_uses_persisted_ratio_and_exact_sample_clocks(db):
    ident = register_volume(db)
    cut, now, before = cut_and_hours()
    save(db, ident, 0.5, before)
    save(db, ident, 2.4, now)
    save(db, "rvol.reference_sessions", 7, now)
    result = generator.generate(db, symbol="QA", timeframe="M5", as_of=cut)
    section = volume_sections(result)[0]
    assert section["default_visible"] and section["visual"]["value"] == 2.4
    assert section["visual"]["baseline"] == section["visual"]["threshold"] == 1
    assert section["n_observations"] == 7
    assert [p["value"] for p in section["visual"]["series"]] == [0.5, 2.4]
    assert section["visual"]["series"][0]["reference_sessions"] is None
    assert section["visual"]["series"][-1]["reference_temporal"]["source_revision"] == "qa-source"
    generator.validate(result)


@pytest.mark.parametrize("quality", ["STALE", "INVALIDATED"])
def test_latest_ineligible_volume_never_uses_previous_value(db, quality):
    ident = register_volume(db)
    cut, now, before = cut_and_hours()
    save(db, ident, 1.8, before)
    if quality == "STALE":
        save(db, ident, 2.0, now, until=cut - pd.Timedelta(minutes=1))
    else:
        feature_store.save_value(db, "QA", "M5", ident, "v1", now, None,
            available_at=now + pd.Timedelta(minutes=5), observed_at=now + pd.Timedelta(minutes=5),
            quality="INVALIDATED", source_revision="qa-source")
    section = volume_sections(generator.generate(db, symbol="QA", timeframe="M5", as_of=cut))[0]
    assert section["visual"]["value"] is None and not section["default_visible"]


def test_volume_keeps_real_ticks_and_weekday_separate(db):
    cut, now, _ = cut_and_hours()
    for source, weekday, value in [("tick", False, 0), ("real", False, 3), ("tick", True, 1.2)]:
        ident = register_volume(db, source, weekday, version=source)
        save(db, ident, value, now, version=source)
    result = generator.generate(db, symbol="QA", timeframe="M5", as_of=cut,
                                versions={"rvol.reference_sessions": "tick"})
    sections = volume_sections(result)
    assert len(sections) == 3
    assert [s["visual"]["value"] for s in sections] == [0, 3, 1.2]
    assert all(s["n_observations"] is None for s in sections)


def test_volume_does_not_borrow_reference_from_other_version(db):
    ident = register_volume(db)
    register_volume(db, version="v2")
    cut, now, _ = cut_and_hours()
    save(db, ident, 1.5, now)
    save(db, "rvol.reference_sessions", 20, now, version="v2")
    section = volume_sections(generator.generate(db, symbol="QA", timeframe="M5", as_of=cut,
        versions={ident: "v1", "rvol.reference_sessions": "v2"}))[0]
    assert section["n_observations"] is None


def test_volume_late_revision_is_excluded_and_invalid_points_break_series(db):
    ident = register_volume(db)
    cut, now, before = cut_and_hours()
    save(db, ident, 1, before, until=cut - pd.Timedelta(minutes=1))
    save(db, ident, 2, now)
    feature_store.save_value(db, "QA", "M5", ident, "v1", now, 99,
        available_at=cut + pd.Timedelta(minutes=1), observed_at=cut + pd.Timedelta(minutes=1),
        source_revision="future")
    section = volume_sections(generator.generate(db, symbol="QA", timeframe="M5", as_of=cut))[0]
    assert [p["value"] for p in section["visual"]["series"]] == [None, 2]


def test_volume_requires_declared_method_and_source(db):
    feature_store.register_definition(db, "rvol.tick", "RVOL", "v1", {}, [], family="rvol", timeframe="M5")
    cut, now, _ = cut_and_hours()
    save(db, "rvol.tick", 2, now)
    section = volume_sections(generator.generate(db, symbol="QA", timeframe="M5", as_of=cut))[0]
    assert not section["default_visible"]
    assert "compatível" in section["details"]["reason"]


def test_document_embeds_volume_and_absence_is_not_zero(db):
    assert "js/charts/volume.js" in document.RESOURCES
    assert "sections/volume.html" in document.TEMPLATES
    section = volume_sections(generator.generate(db, symbol="QA", timeframe="M5", as_of=cut_and_hours()[0]))[0]
    assert section["visual"]["value"] is None


def test_volume_canonical_producer_roundtrip(db):
    from test_analytics_profiles import bars_frame
    from mt5_extracao.analytics import producers
    cut = pd.Timestamp.now(tz="UTC") + pd.Timedelta(seconds=2)
    start = (cut - pd.Timedelta(days=8)).strftime("%Y-%m-%d 12:00")
    bars = bars_frame(sessions=8, start=start, volume=lambda session, bar: 25 if session == 7 else 10)
    params = {"lookback": 20, "min_sessions": 5, "same_weekday": False}
    rows, source = producers._family_rows("rvol", bars, params, "tick")
    producers.register_definitions(db, "rvol", {r["feature_id"] for r in rows}, params, "M5", source, "qa-canonical")
    feature_store.save_values(db, [dict(symbol="QA", timeframe="M5", feature_id=r["feature_id"],
        algorithm_version="qa-canonical", observation_time=r["observation_time"], value=r["value"],
        available_at=r["available_at"], observed_at=r["available_at"], source_revision=r["source_revision"],
        metadata=r["metadata"]) for r in rows])
    result = generator.generate(db, symbol="QA", timeframe="M5", as_of=pd.Timestamp.now(tz="UTC"))
    section = volume_sections(result)[0]
    assert section["visual"]["value"] == 2.5 and section["n_observations"] == 7
    assert section["visual"]["series"][6]["break_before"]
    assert not section["visual"]["series"][7]["break_before"]
    assert section["visual"]["series"][-1]["minute_of_session"] == 30
    other_tf = generator.generate(db, symbol="QA", timeframe="M15", as_of=pd.Timestamp.now(tz="UTC"))
    assert volume_sections(other_tf)[0]["visual"]["value"] is None


@pytest.mark.parametrize("difference", ["minute", "revision"])
def test_volume_sample_requires_matching_minute_and_lineage(db, difference):
    ident = register_volume(db)
    cut, now, _ = cut_and_hours()
    for feature in (ident, "rvol.reference_sessions"):
        is_count = feature.endswith("sessions")
        feature_store.save_value(db, "QA", "M5", feature, "v1", now, 7 if is_count else 2,
            available_at=now + pd.Timedelta(minutes=5), observed_at=now + pd.Timedelta(minutes=5),
            source_revision="other" if is_count and difference == "revision" else "same",
            metadata={"minute_of_session": 10 if is_count and difference == "minute" else 5})
    section = volume_sections(generator.generate(db, symbol="QA", timeframe="M5", as_of=cut))[0]
    assert section["visual"]["value"] == 2 and section["n_observations"] is None


def test_volume_chart_click_uses_timestamp_not_shifted_render_index():
    """ECharts pode normalizar a data; os separadores nulos deslocam dataIndex."""
    import json
    import shutil
    import subprocess

    node = shutil.which("node")
    if node is None:
        pytest.skip("Node indisponível; interação do gráfico exige validação de Browser")
    source = document.ROOT / "static/js/charts/volume.js"
    code = r"""
const assert = require('node:assert/strict');
global.window = {};
const readout = {}, handlers = {}, element = {before(){},addEventListener(){}};
const context = {querySelector(){return readout;},remove(){}};
global.document = {querySelector(){return {content:{cloneNode(){return {querySelector(){return context;}};}}};}};
global.echarts = {init(){return {setOption(){},on(name,fn){handlers[name]=fn;},resize(){},dispose(){}};}};
require(process.argv[1]);
const times = ['2026-10-29T12:00:00+00:00','2026-10-30T12:00:00+00:00'];
const series = times.map((time,i)=>({time,value:i+1,reference_sessions:5+i,minute_of_session:5,break_before:i>0}));
window.MT5XCharts.volume(element,{title:'Volume',visual:{series,baseline:1}},{});
assert.match(readout.textContent,/2×/);
handlers.click({value:[Date.parse(times[0]),1],dataIndex:99});
assert.match(readout.textContent,/1×/);
assert.ok(readout.textContent.includes(times[0]));
handlers.click({value:[times[1],2],dataIndex:0});
assert.match(readout.textContent,/2×/);
handlers.click({value:['invalid',null]});
assert.match(readout.textContent,/2×/);
"""
    result = subprocess.run([node, "-e", code, str(source)], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
